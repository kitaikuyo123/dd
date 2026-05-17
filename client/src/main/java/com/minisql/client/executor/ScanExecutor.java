package com.minisql.client.executor;

import com.google.protobuf.ByteString;
import com.minisql.client.Router;
import com.minisql.common.model.Column;
import com.minisql.common.model.KeyValue;
import com.minisql.common.model.RowAssembler;
import com.minisql.common.model.Table;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.MasterProto;
import com.minisql.common.proto.MasterServiceGrpc;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.common.proto.RegionServerServiceGrpc;
import com.minisql.common.rpc.GrpcChannelFactory;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.execution.Row;
import com.minisql.sql.execution.operators.FilterOperator;
import com.minisql.sql.execution.operators.ListSourceOperator;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 分布式扫描执行器。
 *
 * <p>负责：并行 Region 扫描调度、gRPC ScanRequest 构建、
 * KeyValue → Row 组装、客户端过滤兜底、Region 路由。
 */
public class ScanExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ScanExecutor.class);

    private final ExecutorService executor;
    private final MasterServiceGrpc.MasterServiceBlockingStub masterStub;
    private final Router router;
    private final long queryTimeoutSeconds;
    private final List<String> replicaReadWarnings = java.util.Collections.synchronizedList(new ArrayList<>());

    public ScanExecutor(ExecutorService executor,
                        MasterServiceGrpc.MasterServiceBlockingStub masterStub,
                        Router router,
                        long queryTimeoutSeconds) {
        this.executor = executor;
        this.masterStub = masterStub;
        this.router = router;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    // ── Region 路由结果 ──

    public static class RegionLocation {
        private String regionId;
        private String tableName;
        private String serverHost;
        private int serverPort;
        private List<Router.ServerAddress> replicaServers;

        public String getRegionId() { return regionId; }
        public String getTableName() { return tableName; }
        public String getServerHost() { return serverHost; }
        public int getServerPort() { return serverPort; }
        public List<Router.ServerAddress> getReplicaServers() { return replicaServers; }
    }

    // ── 扫描入口（无排序分页） ──

    public List<com.minisql.common.model.Row> fetchSourceRows(String tableName,
                                                                byte[] rowKey,
                                                                Condition whereCondition,
                                                                String whereClause,
                                                                List<String> projectedQualifiers) throws SQLException {
        return fetchSourceRows(tableName, rowKey, whereCondition, whereClause,
            projectedQualifiers, null, 0, 0);
    }

    // ── 扫描入口（含排序分页） ──

    public List<com.minisql.common.model.Row> fetchSourceRows(String tableName,
                                                                byte[] rowKey,
                                                                Condition whereCondition,
                                                                String whereClause,
                                                                List<String> projectedQualifiers,
                                                                List<SelectStatement.OrderByElement> orderBy,
                                                                int limit,
                                                                int offset) throws SQLException {
        List<RegionLocation> targets = rowKey != null
            ? getTargetRegion(tableName, rowKey)
            : getAllRegionsForTable(tableName);

        if (targets.isEmpty()) {
            throw new SQLException("No regions found for table: " + tableName);
        }

        List<Future<List<com.minisql.common.model.Row>>> futures = new ArrayList<>();
        for (RegionLocation location : targets) {
            futures.add(executor.submit(() -> fetchRowsFromRegion(
                location, whereCondition, whereClause, projectedQualifiers, orderBy, limit, offset)));
        }

        List<com.minisql.common.model.Row> rows = new ArrayList<>();
        try {
            for (Future<List<com.minisql.common.model.Row>> future : futures) {
                rows.addAll(future.get(queryTimeoutSeconds, TimeUnit.SECONDS));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Query execution interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new SQLException("Query execution failed: " + e.getMessage(), e);
        }
        return rows;
    }

    // ── 单 Region 数据拉取（含 replica 回退） ──

    private List<com.minisql.common.model.Row> fetchRowsFromRegion(RegionLocation location,
                                                                     Condition whereCondition,
                                                                     String whereClause,
                                                                     List<String> projectedQualifiers,
                                                                     List<SelectStatement.OrderByElement> orderBy,
                                                                     int limit,
                                                                     int offset) throws SQLException {
        Table schema = getTableSchema(location.tableName);

        Router.ServerAddress primaryAddr = new Router.ServerAddress(location.serverHost, location.serverPort);
        List<Router.ServerAddress> servers = new ArrayList<>();
        servers.add(primaryAddr);
        if (location.replicaServers != null) {
            for (Router.ServerAddress replica : location.replicaServers) {
                if (!replica.equals(primaryAddr)) {
                    servers.add(replica);
                }
            }
        }

        SQLException lastException = null;
        for (int i = 0; i < servers.size(); i++) {
            Router.ServerAddress server = servers.get(i);
            try {
                ManagedChannel channel = GrpcChannelFactory.forAddress(server.getHost(), server.getPort());
                RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                    RegionServerServiceGrpc.newBlockingStub(channel);
                List<KeyValue> keyValues = scanKeyValues(stub, location.regionId, location.tableName,
                    whereClause, projectedQualifiers, orderBy, limit, offset);
                List<com.minisql.common.model.Row> rows = RowAssembler.assemble(keyValues, schema);

                if (i > 0) {
                    String msg = String.format(
                        "Stale read: region %s read from replica %s:%d (primary %s:%d unavailable)",
                        location.regionId, server.getHost(), server.getPort(),
                        location.serverHost, location.serverPort);
                    replicaReadWarnings.add(msg);
                    logger.warn(msg);
                }

                return (whereCondition != null && whereClause == null) ? filterRows(rows, whereCondition) : rows;
            } catch (SQLException e) {
                lastException = e;
                if (isUnavailableError(e) && i < servers.size() - 1) {
                    logger.info("Server {}:{} unavailable for region {}, trying next server",
                        server.getHost(), server.getPort(), location.regionId);
                    continue;
                }
                throw e;
            }
        }
        throw lastException != null ? lastException
            : new SQLException("All servers failed for region: " + location.regionId);
    }

    public List<String> drainReplicaReadWarnings() {
        synchronized (replicaReadWarnings) {
            List<String> warnings = new ArrayList<>(replicaReadWarnings);
            replicaReadWarnings.clear();
            return warnings;
        }
    }

    private boolean isUnavailableError(SQLException e) {
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof io.grpc.StatusRuntimeException) {
                Status.Code code = ((io.grpc.StatusRuntimeException) cause).getStatus().getCode();
                return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED;
            }
            cause = cause.getCause();
        }
        return false;
    }

    // ── gRPC ScanRequest 构建与执行 ──

    public List<KeyValue> scanKeyValues(RegionServerServiceGrpc.RegionServerServiceBlockingStub stub,
                                         String regionId, String tableName,
                                         String whereClause, List<String> projectedQualifiers) throws SQLException {
        return scanKeyValues(stub, regionId, tableName, whereClause, projectedQualifiers, null, 0, 0);
    }

    public List<KeyValue> scanKeyValues(RegionServerServiceGrpc.RegionServerServiceBlockingStub stub,
                                         String regionId, String tableName,
                                         String whereClause, List<String> projectedQualifiers,
                                         List<SelectStatement.OrderByElement> orderBy,
                                         int limit, int offset) throws SQLException {
        RegionServerProto.ScanRequest.Builder requestBuilder = RegionServerProto.ScanRequest.newBuilder()
            .setRegionId(regionId)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.copyFrom(new byte[]{(byte) 0xFF}))
            .setTableName(tableName == null ? "" : tableName);
        if (projectedQualifiers != null && !projectedQualifiers.isEmpty()) {
            requestBuilder.addAllQualifiers(projectedQualifiers);
        }
        if (whereClause != null && !whereClause.isBlank()) {
            requestBuilder.setWhereClause(whereClause);
        }
        if (orderBy != null && !orderBy.isEmpty()) {
            for (SelectStatement.OrderByElement elem : orderBy) {
                requestBuilder.addOrderBy(RegionServerProto.OrderByElement.newBuilder()
                    .setColumn(elem.getColumn())
                    .setAscending(elem.isAscending())
                    .build());
            }
        }
        if (limit > 0) requestBuilder.setLimit(limit);
        if (offset > 0) requestBuilder.setOffset(offset);
        RegionServerProto.ScanRequest request = requestBuilder.build();

        try {
            List<KeyValue> keyValues = new ArrayList<>();
            java.util.Iterator<RegionServerProto.ScanResponse> responses = stub.scan(request);
            while (responses.hasNext()) {
                RegionServerProto.ScanResponse response = responses.next();
                if (!response.getStatus().getSuccess()) {
                    throw new SQLException("Scan failed: " + response.getStatus().getMessage());
                }
                for (CommonProto.KeyValue kvProto : response.getKeyValuesList()) {
                    KeyValue kv = new KeyValue();
                    kv.setRowKey(kvProto.getRowKey().toByteArray());
                    kv.setFamily(kvProto.getColumnFamily());
                    kv.setQualifier(kvProto.getQualifier());
                    kv.setTimestamp(kvProto.getTimestamp());
                    kv.setValue(kvProto.getValue().toByteArray());
                    kv.setType(kvProto.getType() == CommonProto.KeyValueType.PUT
                        ? KeyValue.Type.PUT : KeyValue.Type.DELETE);
                    keyValues.add(kv);
                }
            }
            return keyValues;
        } catch (RuntimeException e) {
            throw new SQLException("Failed to scan region " + regionId, e);
        }
    }

    // ── Schema 与路由 ──

    public Table getTableSchema(String tableName) throws SQLException {
        MasterProto.GetTableSchemaResponse response = masterStub.getTableSchema(
            MasterProto.GetTableSchemaRequest.newBuilder().setTableName(tableName).build()
        );
        if (!response.getStatus().getSuccess()) {
            throw new SQLException("Failed to get table schema: " + response.getStatus().getMessage());
        }
        Table table = new Table();
        table.setTableName(response.getSchema().getTableName());
        table.setPrimaryKey(response.getSchema().getPrimaryKey());
        table.setPartitionKeys(new ArrayList<>(response.getSchema().getPartitionKeysList()));
        table.setClusteringKeys(new ArrayList<>(response.getSchema().getClusteringKeysList()));
        List<Column> columns = new ArrayList<>();
        for (CommonProto.ColumnSchema proto : response.getSchema().getColumnsList()) {
            Column column = new Column();
            column.setName(proto.getName());
            column.setType(Column.ColumnType.valueOf(proto.getType()));
            column.setNullable(proto.getNullable());
            column.setLength(proto.getMaxLength());
            columns.add(column);
        }
        table.setColumns(columns);
        return table;
    }

    private List<RegionLocation> getTargetRegion(String tableName, byte[] rowKey) {
        if (router == null) return new ArrayList<>();
        try {
            router.refreshRouteCache(tableName);
            Router.RegionRouteInfo info = router.getTargetRegionLocation(tableName, rowKey);
            if (info == null) return new ArrayList<>();
            List<RegionLocation> locations = new ArrayList<>();
            RegionLocation loc = new RegionLocation();
            loc.regionId = info.getRegionId();
            loc.tableName = tableName;
            loc.serverHost = info.getPrimaryServer().getHost();
            loc.serverPort = info.getPrimaryServer().getPort();
            loc.replicaServers = info.getReplicaServers();
            locations.add(loc);
            return locations;
        } catch (Exception e) {
            logger.warn("Exception while getting region location for table: {}", tableName, e);
            return new ArrayList<>();
        }
    }

    public List<RegionLocation> getAllRegionsForTable(String tableName) {
        if (router == null) return new ArrayList<>();
        try {
            router.refreshRouteCache(tableName);
            List<Router.RegionRouteInfo> routeInfos = router.getAllRegionLocations(tableName);
            if (routeInfos == null || routeInfos.isEmpty()) return new ArrayList<>();
            List<RegionLocation> locations = new ArrayList<>();
            for (Router.RegionRouteInfo info : routeInfos) {
                RegionLocation loc = new RegionLocation();
                loc.regionId = info.getRegionId();
                loc.tableName = tableName;
                loc.serverHost = info.getPrimaryServer().getHost();
                loc.serverPort = info.getPrimaryServer().getPort();
                loc.replicaServers = info.getReplicaServers();
                locations.add(loc);
            }
            return locations;
        } catch (Exception e) {
            logger.warn("Exception while getting regions for table: {}", tableName, e);
            return new ArrayList<>();
        }
    }

    // ── 客户端行过滤（WHERE 未能下推时的兜底） ──

    private List<com.minisql.common.model.Row> filterRows(List<com.minisql.common.model.Row> rows,
                                                            Condition condition) {
        if (rows.isEmpty()) return rows;
        List<com.minisql.common.model.Row> filtered = new ArrayList<>();
        for (com.minisql.common.model.Row row : rows) {
            Row evalRow = new Row();
            for (String columnName : row.getColumnNames()) {
                evalRow.addColumn(columnName, row.getColumn(columnName));
            }
            if (condition.evaluate(evalRow)) filtered.add(row);
        }
        return filtered;
    }
}
