package com.minisql.client.executor;

import com.google.protobuf.ByteString;
import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Table;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.common.proto.RegionServerServiceGrpc;
import com.minisql.common.proto.RegionServerServiceGrpc.RegionServerServiceBlockingStub;
import com.minisql.common.rpc.GrpcChannelFactory;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SelectStatement;
import io.grpc.ManagedChannel;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 两阶段聚合执行器。
 *
 * <p>Phase 1：将聚合请求发送到每个 Region，各 Region 本地计算局部聚合结果。
 * Phase 2：客户端合并所有 Region 的局部结果（SUM 累加、COUNT 累加、
 * MIN/MAX 取极值、AVG = SUM/COUNT）。
 */
public class AggregationExecutor {

    private final ScanExecutor scanExecutor;
    private final ExecutorService executor;
    private final long queryTimeoutSeconds;

    public AggregationExecutor(ScanExecutor scanExecutor, ExecutorService executor, long queryTimeoutSeconds) {
        this.scanExecutor = scanExecutor;
        this.executor = executor;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    /**
     * 执行两阶段分布式聚合，返回合并后的行。
     */
    public List<com.minisql.common.model.Row> fetchAggregatedRows(
            String tableName,
            Condition whereCondition,
            String whereClause,
            List<String> projectedQualifiers,
            List<QueryPlanner.AggregateExpression> aggregateExpressions,
            List<String> groupByColumns) throws SQLException {

        List<ScanExecutor.RegionLocation> targets = scanExecutor.getAllRegionsForTable(tableName);
        if (targets.isEmpty()) {
            throw new SQLException("No regions found for table: " + tableName);
        }

        List<Future<List<RegionServerProto.AggregateGroup>>> futures = new ArrayList<>();
        for (ScanExecutor.RegionLocation location : targets) {
            futures.add(executor.submit(() -> fetchAggregateGroupsFromRegion(
                location, whereClause, projectedQualifiers, aggregateExpressions, groupByColumns)));
        }

        // 合并结果
        Map<List<String>, double[]> merged = new LinkedHashMap<>();
        Map<List<String>, String[]> minMax = new LinkedHashMap<>();

        try {
            for (Future<List<RegionServerProto.AggregateGroup>> future : futures) {
                for (RegionServerProto.AggregateGroup group : future.get(queryTimeoutSeconds, TimeUnit.SECONDS)) {
                    List<String> key = new ArrayList<>();
                    for (ByteString bs : group.getGroupByKeyList()) {
                        key.add(bs.isEmpty() ? null : bs.toStringUtf8());
                    }
                    for (RegionServerProto.AggregateResult res : group.getResultsList()) {
                        List<String> compositeKey = new ArrayList<>(key);
                        compositeKey.add(res.getOutputName());

                        double[] acc = merged.computeIfAbsent(compositeKey, k -> new double[2]);
                        acc[0] += res.getSumValue();
                        acc[1] += res.getCountValue();

                        String[] mm = minMax.computeIfAbsent(compositeKey, k -> new String[]{null, null});
                        if (!res.getMaxValue().isEmpty()) {
                            String maxStr = res.getMaxValue().toStringUtf8();
                            if (mm[1] == null || maxStr.compareTo(mm[1]) > 0) mm[1] = maxStr;
                        }
                        if (!res.getMinValue().isEmpty()) {
                            String minStr = res.getMinValue().toStringUtf8();
                            if (mm[0] == null || minStr.compareTo(mm[0]) < 0) mm[0] = minStr;
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Query execution interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new SQLException("Query execution failed: " + e.getMessage(), e);
        }

        // 构建合并后的行
        Map<List<String>, com.minisql.common.model.Row> outputRows = new LinkedHashMap<>();
        for (Map.Entry<List<String>, double[]> entry : merged.entrySet()) {
            List<String> compositeKey = entry.getKey();
            String outputName = compositeKey.get(compositeKey.size() - 1);
            List<String> groupKey = compositeKey.subList(0, compositeKey.size() - 1);

            com.minisql.common.model.Row row = outputRows.computeIfAbsent(groupKey, k -> {
                com.minisql.common.model.Row r = new com.minisql.common.model.Row();
                for (int i = 0; i < groupByColumns.size() && i < k.size(); i++) {
                    String val = k.get(i);
                    r.setColumn(groupByColumns.get(i), val != null ? parseValue(val) : null);
                }
                return r;
            });

            String func = null;
            for (QueryPlanner.AggregateExpression expr : aggregateExpressions) {
                if (expr.outputName.equals(outputName)) {
                    func = expr.function;
                    break;
                }
            }

            double[] acc = entry.getValue();
            String[] mm = minMax.get(compositeKey);
            Object result;
            if ("COUNT".equalsIgnoreCase(func)) {
                result = (long) acc[1];
            } else if ("SUM".equalsIgnoreCase(func)) {
                result = acc[0];
            } else if ("AVG".equalsIgnoreCase(func)) {
                result = acc[1] == 0 ? null : acc[0] / acc[1];
            } else if ("MAX".equalsIgnoreCase(func)) {
                result = mm != null && mm[1] != null ? parseValue(mm[1]) : null;
            } else if ("MIN".equalsIgnoreCase(func)) {
                result = mm != null && mm[0] != null ? parseValue(mm[0]) : null;
            } else {
                result = acc[0];
            }
            row.setColumn(outputName, result);
        }

        return new ArrayList<>(outputRows.values());
    }

    // ── 单 Region 聚合请求 ──

    private List<RegionServerProto.AggregateGroup> fetchAggregateGroupsFromRegion(
            ScanExecutor.RegionLocation location,
            String whereClause,
            List<String> projectedQualifiers,
            List<QueryPlanner.AggregateExpression> aggregateExpressions,
            List<String> groupByColumns) throws SQLException {

        ManagedChannel channel = GrpcChannelFactory.forAddress(location.getServerHost(), location.getServerPort());
        try {
            RegionServerServiceBlockingStub stub = RegionServerServiceGrpc.newBlockingStub(channel);

            RegionServerProto.ScanRequest.Builder reqBuilder = RegionServerProto.ScanRequest.newBuilder()
                .setRegionId(location.getRegionId())
                .setStartKey(ByteString.EMPTY)
                .setEndKey(ByteString.copyFrom(new byte[]{(byte) 0xFF}))
                .setTableName(location.getTableName() == null ? "" : location.getTableName());

            if (projectedQualifiers != null && !projectedQualifiers.isEmpty()) {
                reqBuilder.addAllQualifiers(projectedQualifiers);
            }
            if (whereClause != null && !whereClause.isBlank()) {
                reqBuilder.setWhereClause(whereClause);
            }
            for (QueryPlanner.AggregateExpression expr : aggregateExpressions) {
                reqBuilder.addAggregates(RegionServerProto.AggregateSpec.newBuilder()
                    .setFunction(expr.function)
                    .setColumn(expr.column == null ? "" : expr.column)
                    .setOutputName(expr.outputName)
                    .build());
            }
            if (groupByColumns != null) {
                reqBuilder.addAllGroupByColumns(groupByColumns);
            }

            RegionServerProto.ScanRequest request = reqBuilder.build();
            List<RegionServerProto.AggregateGroup> allGroups = new ArrayList<>();

            java.util.Iterator<RegionServerProto.ScanResponse> responses = stub.scan(request);
            while (responses.hasNext()) {
                RegionServerProto.ScanResponse response = responses.next();
                if (!response.getStatus().getSuccess()) {
                    throw new SQLException("Scan failed: " + response.getStatus().getMessage());
                }
                allGroups.addAll(response.getAggregateGroupsList());
            }
            return allGroups;
        } catch (RuntimeException e) {
            throw new SQLException("Failed to scan region " + location.getRegionId(), e);
        }
    }

    private static Object parseValue(String val) {
        if (val == null) return null;
        try { return Long.parseLong(val); } catch (NumberFormatException ignored) {}
        try { return Double.parseDouble(val); } catch (NumberFormatException ignored) {}
        return val;
    }
}
