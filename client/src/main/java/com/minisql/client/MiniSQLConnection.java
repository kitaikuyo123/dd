package com.minisql.client;

import com.minisql.common.Constants;
import com.minisql.common.model.*;
import com.minisql.common.proto.*;
import com.minisql.common.utils.BytesUtil;
import com.minisql.sql.SQLParser;
import com.minisql.zookeeper.ZkClient;
import com.minisql.zookeeper.ZkPayloads;
import com.minisql.common.rpc.GrpcChannelFactory;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * MiniSQL 连接实现
 * 负责模块：开发者 C
 */
public class MiniSQLConnection implements Connection {

    private static final Logger logger = LoggerFactory.getLogger(MiniSQLConnection.class);

    private boolean closed = false;
    private boolean autoCommit = true;

    // ZooKeeper 连接信息
    private String zkHost;
    private int zkPort;

    // ZooKeeper 客户端
    private ZkClient zkClient;

    // Master 连接（直接使用）
    private ManagedChannel masterChannel;
    private MasterServiceGrpc.MasterServiceBlockingStub masterStub;

    // 连接的路由信息
    private final Router router;

    // 事务状态
    private java.util.List<String> transactionOperations = new java.util.ArrayList<>();

    // RegionServer 连接缓存
    private final Map<String, RegionServerServiceGrpc.RegionServerServiceBlockingStub> regionServerStubs = new HashMap<>();

    // 并行查询执行器
    private final ParallelQueryExecutor parallelQueryExecutor;

    // 表结构缓存
    private final Map<String, com.minisql.common.model.Table> tableSchemaCache = new ConcurrentHashMap<>();

    private final ExecutorService sqlMetricsExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "MiniSQL-SqlMetrics");
        t.setDaemon(true);
        return t;
    });

    public MiniSQLConnection(String url, Properties info) throws SQLException {
        this.router = new Router();

        // 解析 URL 获取 ZooKeeper 地址
        // jdbc:minisql://zkhost:2181
        parseUrl(url);

        // 初始化连接
        initialize();

        // 初始化并行查询执行器
        this.parallelQueryExecutor = new ParallelQueryExecutor(masterStub, 30, router);
    }

    private void parseUrl(String url) throws SQLException {
        if (!url.startsWith("jdbc:minisql://")) {
            throw new SQLException("Invalid URL format. Expected: jdbc:minisql://zkhost:port");
        }

        // 解析 ZooKeeper 地址
        // 格式：jdbc:minisql://zkhost:2181[/database][?param1=value1&param2=value2]
        String addressPart = url.substring("jdbc:minisql://".length());

        // 去掉后面的路径和参数
        int slashIndex = addressPart.indexOf('/');
        if (slashIndex > 0) {
            addressPart = addressPart.substring(0, slashIndex);
        }
        int questionIndex = addressPart.indexOf('?');
        if (questionIndex > 0) {
            addressPart = addressPart.substring(0, questionIndex);
        }

        // 解析主机和端口
        int colonIndex = addressPart.lastIndexOf(':');
        if (colonIndex > 0) {
            zkHost = addressPart.substring(0, colonIndex);
            try {
                zkPort = Integer.parseInt(addressPart.substring(colonIndex + 1));
            } catch (NumberFormatException e) {
                throw new SQLException("Invalid port number in URL: " + url);
            }
        } else {
            zkHost = addressPart;
            zkPort = 2181; // 默认 ZooKeeper 端口
        }
    }

    private void initialize() throws SQLException {
        try {
            // 连接 ZooKeeper
            String zkConnectString = zkHost + ":" + zkPort;
            zkClient = new ZkClient(zkConnectString);
            zkClient.start();

            // 等待连接建立
            int retry = 0;
            while (!zkClient.isConnected() && retry < 10) {
                Thread.sleep(100);
                retry++;
            }

            if (!zkClient.isConnected()) {
                throw new SQLException("Failed to connect to ZooKeeper: " + zkConnectString);
            }

            // 初始化 Router 的 ZooKeeper 客户端
            router.setZkClient(zkClient);

            // 从 ZooKeeper 获取 Master 地址
            refreshMasterAddress();

        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to initialize connection", e);
        }
    }

    /**
     * 从 ZooKeeper 刷新 Master 地址
     */
    private void refreshMasterAddress() throws SQLException {
        try {
            String masterPath = Constants.ZK_MASTER_LEADER_PATH;
            if (zkClient.exists(masterPath)) {
                byte[] data = zkClient.getData(masterPath);
                String masterAddress = ZkPayloads.decodeLeaderAddress(data);
                connectToMaster(masterAddress);
            } else {
                throw new SQLException("Master not found in ZooKeeper");
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to get master from ZooKeeper", e);
        }
    }

    /**
     * 连接到 Master
     */
    private void connectToMaster(String address) {
        // 解析地址
        String[] parts = address.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : Constants.DEFAULT_MASTER_PORT;

        // 使用 GrpcChannelFactory 获取复用的 channel
        masterChannel = GrpcChannelFactory.forAddress(host, port);
        masterStub = MasterServiceGrpc.newBlockingStub(masterChannel);

        logger.info("Connected to Master: {}", address);
    }

    /**
     * 获取或创建 RegionServer 连接
     */
    private RegionServerServiceGrpc.RegionServerServiceBlockingStub getRegionServerStub(String address) {
        return regionServerStubs.computeIfAbsent(address, addr -> {
            String[] parts = addr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : Constants.DEFAULT_REGIONSERVER_PORT;

            logger.debug("Creating RegionServer stub for: {} (host={}, port={})", addr, host, port);
            ManagedChannel channel = GrpcChannelFactory.forAddress(host, port);
            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub = RegionServerServiceGrpc.newBlockingStub(channel);
            logger.debug("Stub created successfully, channel state: {}", channel.getState(true));
            return stub;
        });
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException {
        checkClosed();
        return new MiniSQLStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new MiniSQLPreparedStatement(this, sql);
    }

    /**
     * 执行 SQL 并返回结果
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();

        try {
            SQLParser parser = new SQLParser(sql);
            com.minisql.sql.ast.Statement stmt = parser.parse();

            if (stmt instanceof com.minisql.sql.ast.SelectStatement) {
                com.minisql.sql.ast.SelectStatement select = (com.minisql.sql.ast.SelectStatement) stmt;
                String tableName = select.getTable();

                List<com.minisql.sql.execution.Row> rows =
                    parallelQueryExecutor.executeQuery(select, sql);

                // 转换为 ResultSet
                List<String> projectedColumns = null;
                if (select.getColumns() != null && !select.getColumns().isEmpty()) {
                    projectedColumns = new ArrayList<>();
                    for (String col : select.getColumns()) {
                        if (!"*".equals(col)) {
                            projectedColumns.add(col);
                        }
                    }
                }
                return convertToResultSet(rows, tableName, projectedColumns);
            } else if (stmt instanceof com.minisql.sql.ast.ShowTablesStatement) {
                List<String> tableNames = listTables();
                return convertToTableListResultSet(tableNames);
            } else {
                throw new SQLException("Expected SELECT or SHOW TABLES statement");
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to execute query", e);
        }
    }

    /**
     * 将 Row 列表转换为 ResultSet（带表名和列名信息）
     */
    private ResultSet convertToResultSet(List<com.minisql.sql.execution.Row> rows, String tableName, List<String> projectedColumns) {
        MiniSQLResultSet resultSet = new MiniSQLResultSet();
        resultSet.setTableName(tableName);

        if (rows.isEmpty()) {
            return resultSet;
        }

        // 从第一行提取列名
        com.minisql.sql.execution.Row firstRow = rows.get(0);
        List<String> columnNames = firstRow.getColumnNames();
        resultSet.setColumnNames(columnNames);

        // 获取表结构以获取列类型
        List<String> columnTypes = new ArrayList<>();
        if (tableName != null) {
            try {
                Table schema = getTableSchema(tableName);
                if (schema != null) {
                    // 根据列名顺序获取类型
                    for (String colName : columnNames) {
                        String type = "VARCHAR"; // 默认类型
                        for (Column col : schema.getColumns()) {
                            if (col.getName().equalsIgnoreCase(colName)) {
                                type = col.getType().name();
                                break;
                            }
                        }
                        columnTypes.add(type);
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to get table schema for metadata: {}", e.getMessage());
            }
        }

        // 如果没有从表结构获取类型，使用默认值
        if (columnTypes.isEmpty()) {
            for (int i = 0; i < columnNames.size(); i++) {
                columnTypes.add("VARCHAR");
            }
        }

        resultSet.setColumnTypes(columnTypes);

        // 将 Row 转换为 Result
        for (com.minisql.sql.execution.Row row : rows) {
            com.minisql.sql.execution.Row resultRow = new com.minisql.sql.execution.Row();
            for (String columnName : columnNames) {
                Object value = row.getColumnValue(columnName);
                resultRow.addColumn(columnName, value);
            }
            resultSet.addRow(resultRow);
        }

        return resultSet;
    }

    /**
     * 将表名列表转换为 ResultSet
     */
    private ResultSet convertToTableListResultSet(List<String> tableNames) {
        MiniSQLResultSet resultSet = new MiniSQLResultSet();
        resultSet.setTableName(null);

        // 设置列名
        List<String> columnNames = new ArrayList<>();
        columnNames.add("table_name");
        resultSet.setColumnNames(columnNames);

        // 设置列类型
        List<String> columnTypes = new ArrayList<>();
        columnTypes.add("VARCHAR");
        resultSet.setColumnTypes(columnTypes);

        // 添加数据行
        for (String tableName : tableNames) {
            com.minisql.sql.execution.Row row = new com.minisql.sql.execution.Row();
            row.addColumn("table_name", tableName);
            resultSet.addRow(row);
        }

        return resultSet;
    }

    /**
     * 将 Row 列表转换为 ResultSet（带表名和列名信息）
     */
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();

        logger.debug("Executing SQL: {}", sql);

        try {
            SQLParser parser = new SQLParser(sql);
            com.minisql.sql.ast.Statement stmt = parser.parse();

            logger.debug("Parsed statement type: {}", stmt.getClass().getSimpleName());

            if (stmt instanceof com.minisql.sql.ast.InsertStatement) {
                return executeInsert((com.minisql.sql.ast.InsertStatement) stmt);
            } else if (stmt instanceof com.minisql.sql.ast.DeleteStatement) {
                return executeDelete((com.minisql.sql.ast.DeleteStatement) stmt);
            } else if (stmt instanceof com.minisql.sql.ast.UpdateStatement) {
                return executeUpdate((com.minisql.sql.ast.UpdateStatement) stmt);
            } else if (stmt instanceof com.minisql.sql.ast.CreateTableStatement) {
                return executeCreateTable((com.minisql.sql.ast.CreateTableStatement) stmt);
            } else if (stmt instanceof com.minisql.sql.ast.DropTableStatement) {
                return executeDropTable((com.minisql.sql.ast.DropTableStatement) stmt);
            } else {
                throw new SQLException("Unsupported statement type: " + stmt.getClass().getName());
            }
        } catch (SQLException e) {
            logger.error("SQL Exception: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Exception: {}", e.getMessage());
            throw new SQLException("Failed to execute update", e);
        }
    }

    private int executeInsert(com.minisql.sql.ast.InsertStatement stmt) throws SQLException {
        String tableName = stmt.getTable();
        com.minisql.common.model.Table schema = getTableSchema(tableName);
        Row row = buildRowFromInsert(stmt, schema);
        byte[] rowKey = KeyValueConverter.createRowKeyFromRow(row, schema);
        row.setRowKey(rowKey);
        row.setTimestamp(System.currentTimeMillis());

        executeTypedPutMutation(tableName, schema, rowKey, KeyValueConverter.rowToKeyValues(row, schema), "Insert");
        clearTableSchemaCache(tableName);
        return 1;
    }

    private int executeDelete(com.minisql.sql.ast.DeleteStatement stmt) throws SQLException {
        String tableName = stmt.getTable();
        com.minisql.common.model.Table schema = getTableSchema(tableName);
        byte[] rowKey = resolveRowKeyForMutation(stmt.getWhere(), schema, "DELETE");

        if (rowKey != null) {
            return deleteSingleRow(tableName, schema, rowKey);
        }

        // Scan-based: group matching rows by target RegionServer and batch-delete per region
        List<Row> matchingRows = scanRowsForMutation(tableName, schema, stmt.getWhere(), "DELETE");
        if (matchingRows.isEmpty()) {
            return 0;
        }

        long timestamp = System.currentTimeMillis();
        List<String> qualifiers = schema.getColumns().stream()
                .map(com.minisql.common.model.Column::getName)
                .collect(Collectors.toList());
        Map<String, List<byte[]>> regionRows = new LinkedHashMap<>();

        for (Row row : matchingRows) {
            MutationTarget target = resolveMutationTarget(tableName, row.getRowKey());
            regionRows.computeIfAbsent(target.regionId, k -> new ArrayList<>()).add(row.getRowKey());
        }

        for (Map.Entry<String, List<byte[]>> entry : regionRows.entrySet()) {
            String regionId = entry.getKey();
            List<byte[]> rowKeys = entry.getValue();
            // Batch all tombstones for this region into a single PutRequest
            RegionServerProto.PutRequest.Builder batch = RegionServerProto.PutRequest.newBuilder()
                .setRegionId(regionId)
                .setDurable(true);
            for (byte[] rk : rowKeys) {
                for (String q : qualifiers) {
                    batch.addKeyValues(CommonProto.KeyValue.newBuilder()
                        .setRowKey(com.google.protobuf.ByteString.copyFrom(rk))
                        .setColumnFamily("")
                        .setQualifier(q)
                        .setTimestamp(timestamp)
                        .setValue(com.google.protobuf.ByteString.EMPTY)
                        .setType(CommonProto.KeyValueType.DELETE)
                        .build());
                }
            }
            MutationTarget target = resolveMutationTarget(tableName, rowKeys.get(0));
            RegionServerProto.PutResponse response = target.stub.put(batch.build());
            if (!response.getStatus().getSuccess()) {
                throw new SQLException("Batch delete failed on " + regionId + ": " + response.getStatus().getMessage());
            }
        }

        clearTableSchemaCache(tableName);
        return matchingRows.size();
    }

    private int deleteSingleRow(String tableName, com.minisql.common.model.Table schema, byte[] rowKey) throws SQLException {
        MutationTarget target = resolveMutationTarget(tableName, rowKey);
        List<String> qualifiers = schema.getColumns().stream()
                .map(com.minisql.common.model.Column::getName)
                .collect(Collectors.toList());
        long timestamp = System.currentTimeMillis();
        RegionServerProto.PutRequest.Builder deleteRequest = RegionServerProto.PutRequest.newBuilder()
                .setRegionId(target.regionId)
                .setDurable(true);
        for (String qualifier : qualifiers) {
            deleteRequest.addKeyValues(CommonProto.KeyValue.newBuilder()
                    .setRowKey(com.google.protobuf.ByteString.copyFrom(rowKey))
                    .setColumnFamily("")
                    .setQualifier(qualifier)
                    .setTimestamp(timestamp)
                    .setValue(com.google.protobuf.ByteString.EMPTY)
                    .setType(CommonProto.KeyValueType.DELETE)
                    .build());
        }
        RegionServerProto.PutResponse response = target.stub.put(deleteRequest.build());
        if (!response.getStatus().getSuccess()) {
            throw new SQLException("Delete failed: " + response.getStatus().getMessage());
        }
        return 1;
    }

    private int executeUpdate(com.minisql.sql.ast.UpdateStatement stmt) throws SQLException {
        String tableName = stmt.getTable();
        com.minisql.common.model.Table schema = getTableSchema(tableName);

        for (com.minisql.sql.ast.Assignment assignment : stmt.getAssignments()) {
            if (schema.getAllPrimaryKeys().contains(assignment.getColumn())) {
                throw new SQLException("UPDATE of primary key columns is not supported: " + assignment.getColumn());
            }
        }

        byte[] rowKey = resolveRowKeyForMutation(stmt.getWhere(), schema, "UPDATE");
        if (rowKey != null) {
            return updateSingleRow(tableName, schema, rowKey, stmt.getAssignments());
        }

        // Scan-based: group matching rows by RegionServer and batch-update per region
        List<Row> matchingRows = scanRowsForMutation(tableName, schema, stmt.getWhere(), "UPDATE");
        if (matchingRows.isEmpty()) {
            return 0;
        }

        // Group KeyValues by region for batch put
        Map<String, List<KeyValue>> regionKvs = new LinkedHashMap<>();
        long timestamp = System.currentTimeMillis();
        for (Row row : matchingRows) {
            Row existing = fetchExistingRowWithRetry(tableName, row.getRowKey(), schema, "Update");
            for (com.minisql.sql.ast.Assignment assignment : stmt.getAssignments()) {
                Column.ColumnType type = findColumnType(schema, assignment.getColumn());
                existing.setColumn(assignment.getColumn(), convertValue(assignment.getValue(), type));
            }
            existing.setRowKey(row.getRowKey());
            existing.setTimestamp(timestamp);

            MutationTarget target = resolveMutationTarget(tableName, row.getRowKey());
            KeyValue[] kvs = KeyValueConverter.rowToKeyValues(existing, schema);
            regionKvs.computeIfAbsent(target.regionId, k -> new ArrayList<>())
                .addAll(Arrays.asList(kvs));
        }

        // Send one batch PutRequest per region
        for (Map.Entry<String, List<KeyValue>> entry : regionKvs.entrySet()) {
            RegionServerProto.PutRequest.Builder batch = RegionServerProto.PutRequest.newBuilder()
                .setRegionId(entry.getKey())
                .setDurable(true);
            for (KeyValue kv : entry.getValue()) {
                batch.addKeyValues(convertToProto(kv));
            }
            byte[] firstRk = matchingRows.get(0).getRowKey();
            MutationTarget target = resolveMutationTarget(tableName, firstRk);
            RegionServerProto.PutResponse response = target.stub.put(batch.build());
            if (!response.getStatus().getSuccess()) {
                throw new SQLException("Batch update failed on " + entry.getKey() + ": " + response.getStatus().getMessage());
            }
        }

        clearTableSchemaCache(tableName);
        return matchingRows.size();
    }

    private int updateSingleRow(String tableName, com.minisql.common.model.Table schema,
                                 byte[] rowKey, List<com.minisql.sql.ast.Assignment> assignments) throws SQLException {
        // 1. fetch
        Row existingRow = fetchExistingRowWithRetry(tableName, rowKey, schema, "Update");
        // 2. modify
        for (com.minisql.sql.ast.Assignment assignment : assignments) {
            Column.ColumnType type = findColumnType(schema, assignment.getColumn());
            Object converted = convertValue(assignment.getValue(), type);
            existingRow.setColumn(assignment.getColumn(), converted);
        }
        existingRow.setRowKey(rowKey);
        existingRow.setTimestamp(System.currentTimeMillis());
        // 3. serialize + write
        executeTypedPutMutation(tableName, schema, rowKey,
            KeyValueConverter.rowToKeyValues(existingRow, schema), "Update");
        return 1;
    }

    /** Mutable region + stub pair for building batch requests. */
    private static class MutationTarget {
        final String regionId;
        final RegionServerServiceGrpc.RegionServerServiceBlockingStub stub;
        MutationTarget(String regionId, RegionServerServiceGrpc.RegionServerServiceBlockingStub stub) {
            this.regionId = regionId; this.stub = stub;
        }
    }

    private Row buildRowFromInsert(com.minisql.sql.ast.InsertStatement stmt,
                                   com.minisql.common.model.Table schema) throws SQLException {
        logger.debug("Table schema: tableName={}, primaryKey={}", schema.getTableName(), schema.getPrimaryKey());
        logger.debug("Insert columns: {}", stmt.getColumns());
        logger.debug("Insert values: {}", stmt.getValues());

        List<String> columns = stmt.getColumns();
        List<String> values = stmt.getValues();
        if (columns.size() != values.size()) {
            throw new SQLException("Column count does not match value count");
        }

        Row row = new Row();
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            Column.ColumnType type = findColumnType(schema, columnName);
            Object convertedValue = convertValue(values.get(i), type);
            row.setColumn(columnName, convertedValue);
        }
        return row;
    }

    private MutationTarget resolveMutationTarget(String tableName, byte[] rowKey) throws SQLException {
        router.refreshRouteCache(tableName);
        Router.ServerAddress serverAddress = router.route(tableName, rowKey);
        if (serverAddress == null) {
            throw new SQLException("No available RegionServer for table: " + tableName);
        }

        String regionId = getRegionId(tableName, rowKey);
        RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
            getRegionServerStub(serverAddress.toString());
        return new MutationTarget(regionId, stub);
    }

    /**
     * Resolve a single row key from WHERE condition.
     * Returns null when not all primary key columns are specified — caller should
     * fall back to scan-based mutation.
     */
    private byte[] resolveRowKeyForMutation(com.minisql.sql.ast.Condition where,
                                            com.minisql.common.model.Table schema,
                                            String operation) throws SQLException {
        if (where == null) {
            return null;
        }

        Map<String, String> equalityConditions = new HashMap<>();
        if (!collectEqualityConditions(where, equalityConditions)) {
            return null; // Complex condition — needs scan
        }

        List<String> primaryKeys = schema.getAllPrimaryKeys();
        if (primaryKeys.isEmpty()) {
            throw new SQLException("Table does not define a primary key: " + schema.getTableName());
        }

        Row keyRow = new Row();
        for (String primaryKey : primaryKeys) {
            String rawValue = equalityConditions.get(primaryKey);
            if (rawValue == null) {
                return null; // Partial key — fall back to scan
            }
            keyRow.setColumn(primaryKey, convertValue(rawValue, findColumnType(schema, primaryKey)));
        }

        return KeyValueConverter.createRowKeyFromRow(keyRow, schema);
    }

    /**
     * Scan matching rows for UPDATE/DELETE by reusing the SELECT path.
     * No new Router APIs needed — just queries the table and filters client-side.
     */
    private List<Row> scanRowsForMutation(String tableName, com.minisql.common.model.Table schema,
                                          com.minisql.sql.ast.Condition where,
                                          String operation) throws SQLException {
        try {
            com.minisql.sql.ast.SelectStatement selectStmt = new com.minisql.sql.ast.SelectStatement();
            selectStmt.setTable(tableName);
            selectStmt.setSelectAll(true);
            selectStmt.setWhere(where);

            List<com.minisql.sql.execution.Row> rows = parallelQueryExecutor.executeQuery(selectStmt,
                "SELECT * FROM " + tableName);

            List<Row> result = new ArrayList<>();
            for (com.minisql.sql.execution.Row row : rows) {
                Row r = new Row();
                for (String col : row.getColumns()) {
                    r.setColumn(col, row.getValue(col));
                }
                byte[] reconstructedKey = KeyValueConverter.createRowKeyFromRow(r, schema);
                r.setRowKey(reconstructedKey);
                result.add(r);
            }
            return result;
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Scan-based " + operation + " failed: " + e.getMessage(), e);
        }
    }

    private boolean collectEqualityConditions(com.minisql.sql.ast.Condition condition, Map<String, String> values) {
        if (condition == null) {
            return false;
        }
        if (condition instanceof com.minisql.sql.ast.SimpleCondition) {
            com.minisql.sql.ast.SimpleCondition simple = (com.minisql.sql.ast.SimpleCondition) condition;
            if (!"=".equals(simple.getOperator())) {
                return false;
            }
            values.put(simple.getColumn(), simple.getValue());
            return true;
        }
        if (condition instanceof com.minisql.sql.ast.CompoundCondition) {
            com.minisql.sql.ast.CompoundCondition compound = (com.minisql.sql.ast.CompoundCondition) condition;
            if (!"AND".equalsIgnoreCase(compound.getOperator())) {
                return false;
            }
            return collectEqualityConditions(compound.getLeft(), values)
                && collectEqualityConditions(compound.getRight(), values);
        }
        return false;
    }

    private Row fetchExistingRow(MutationTarget target, byte[] rowKey,
                                 com.minisql.common.model.Table schema,
                                 String operation) throws SQLException {
        RegionServerProto.GetResponse response = target.stub.get(
            RegionServerProto.GetRequest.newBuilder()
                .setRegionId(target.regionId)
                .setRowKey(com.google.protobuf.ByteString.copyFrom(rowKey))
                .build());

        if (!response.getStatus().getSuccess()) {
            throw new SQLException(operation + " failed: cannot read existing row - " + response.getStatus().getMessage());
        }

        List<KeyValue> existingKVs = new ArrayList<>();
        for (CommonProto.KeyValue kvProto : response.getKeyValuesList()) {
            KeyValue kv = new KeyValue();
            kv.setRowKey(kvProto.getRowKey().toByteArray());
            kv.setFamily(kvProto.getColumnFamily());
            kv.setQualifier(kvProto.getQualifier());
            kv.setTimestamp(kvProto.getTimestamp());
            kv.setValue(kvProto.getValue().toByteArray());
            kv.setType(kvProto.getType() == CommonProto.KeyValueType.PUT ? KeyValue.Type.PUT : KeyValue.Type.DELETE);
            existingKVs.add(kv);
        }

        if (existingKVs.isEmpty()) {
            throw new SQLException(operation + " failed: row not found");
        }
        return RowAssembler.mergeToRow(existingKVs, schema);
    }

    private Row fetchExistingRowWithRetry(String tableName, byte[] rowKey,
                                          com.minisql.common.model.Table schema,
                                          String operation) throws SQLException {
        SQLException lastError = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            MutationTarget target = resolveMutationTarget(tableName, rowKey);
            try {
                return fetchExistingRow(target, rowKey, schema, operation);
            } catch (SQLException e) {
                lastError = e;
                if (attempt == 0 && shouldRetryFetchExistingRow(e.getMessage())) {
                    router.refreshRouteCache(tableName);
                    continue;
                }
                throw e;
            }
        }
        throw lastError != null
            ? lastError
            : new SQLException(operation + " failed: unable to read existing row");
    }

    private boolean shouldRetryFetchExistingRow(String message) {
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("row not found")
            || normalized.contains("cannot read existing row")
            || isRetryableWriteRouteError(message);
    }

    private void executeTypedPutMutation(String tableName, com.minisql.common.model.Table schema,
                                         byte[] rowKey, KeyValue[] keyValues, String operation) throws SQLException {
        RegionServerProto.PutRequest request = RegionServerProto.PutRequest.newBuilder()
            .setRegionId("")
            .setRowKey(com.google.protobuf.ByteString.copyFrom(rowKey))
            .addAllKeyValues(Arrays.stream(keyValues).map(this::convertToProto).collect(Collectors.toList()))
            .setDurable(true)
            .build();

        SQLException lastError = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            MutationTarget target = resolveMutationTarget(tableName, rowKey);
            RegionServerProto.PutResponse response = target.stub.put(
                request.toBuilder()
                    .setRegionId(target.regionId)
                    .build());

            if (response.getStatus().getSuccess()) {
                return;
            }

            String message = response.getStatus().getMessage();
            lastError = new SQLException(operation + " failed: " + message);
            if (attempt == 0 && isRetryableWriteRouteError(message)) {
                router.refreshRouteCache(tableName);
                continue;
            }
            throw lastError;
        }

        throw lastError != null ? lastError : new SQLException(operation + " failed");
    }

    private boolean isRetryableWriteRouteError(String message) {
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("not primary")
            || normalized.contains("primary changed")
            || normalized.contains("stale route")
            || normalized.contains("read-only during migration")
            || normalized.contains("region is not open")
            || normalized.contains("region storage not found")
            || normalized.contains("region not found");
    }

    private Column.ColumnType findColumnType(com.minisql.common.model.Table schema, String columnName) throws SQLException {
        for (Column column : schema.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column.getType();
            }
        }
        throw new SQLException("Column not found in schema: " + columnName);
    }

    private int executeCreateTable(com.minisql.sql.ast.CreateTableStatement stmt) throws SQLException {
        if (masterStub == null) {
            throw new SQLException("Master not available");
        }

        logger.info("Creating table: {}", stmt.getTable());
        logger.info("Columns count: {}", stmt.getColumns() != null ? stmt.getColumns().size() : 0);

        // 构建表模式
        CommonProto.TableSchema.Builder schemaBuilder = CommonProto.TableSchema.newBuilder()
                .setTableName(stmt.getTable());

        // 添加列定义
        if (stmt.getColumns() != null) {
            for (com.minisql.sql.ast.ColumnDef column : stmt.getColumns()) {
                String typeName = column.getType().name();
                logger.info("Column: {}, Type: {}, Nullable: {}, Length: {}",
                    column.getName(), typeName, column.isNullable(), column.getLength());
                CommonProto.ColumnSchema.Builder columnBuilder = CommonProto.ColumnSchema.newBuilder()
                        .setName(column.getName())
                        .setType(typeName);

                // 设置是否可为空
                columnBuilder.setNullable(column.isNullable());

                // 设置长度 (对于 VARCHAR 等)
                if (column.getLength() > 0) {
                    columnBuilder.setMaxLength(column.getLength());
                }

                schemaBuilder.addColumns(columnBuilder.build());
            }
        }

        // 设置主键 - 支持复合主键
        // 优先使用分区键 + 聚类键
        if (stmt.getPartitionKeys() != null && !stmt.getPartitionKeys().isEmpty()) {
            schemaBuilder.addAllPartitionKeys(stmt.getPartitionKeys());
            logger.info("Partition keys: {}", stmt.getPartitionKeys());
        }
        if (stmt.getClusteringKeys() != null && !stmt.getClusteringKeys().isEmpty()) {
            schemaBuilder.addAllClusteringKeys(stmt.getClusteringKeys());
            logger.info("Clustering keys: {}", stmt.getClusteringKeys());
        }
        // 向后兼容：如果只有 primaryKey，使用它
        if (stmt.getPrimaryKey() != null && stmt.getPartitionKeys() == null) {
            schemaBuilder.setPrimaryKey(stmt.getPrimaryKey());
            logger.info("Primary key (legacy): {}", stmt.getPrimaryKey());
        }

        MasterProto.CreateTableRequest request = MasterProto.CreateTableRequest.newBuilder()
                .setSchema(schemaBuilder.build())
                .setNumRegions(1) // 默认单个 Region
                .build();

        logger.info("Sending create table request to Master...");

        MasterProto.CreateTableResponse response = masterStub.createTable(request);

        logger.info("Response received: success={}, message={}",
            response.getStatus().getSuccess(), response.getStatus().getMessage());

        if (!response.getStatus().getSuccess()) {
            throw new SQLException("Create table failed: " + response.getStatus().getMessage());
        }

        // 清除表结构缓存，确保下次获取时使用新结构
        clearTableSchemaCache(stmt.getTable());

        return 0;
    }

    private int executeDropTable(com.minisql.sql.ast.DropTableStatement stmt) throws SQLException {
        if (masterStub == null) {
            throw new SQLException("Master not available");
        }

        MasterProto.DeleteTableRequest request = MasterProto.DeleteTableRequest.newBuilder()
                .setTableName(stmt.getTable())
                .build();

        MasterProto.DeleteTableResponse response = masterStub.deleteTable(request);

        if (!response.getStatus().getSuccess()) {
            throw new SQLException("Drop table failed: " + response.getStatus().getMessage());
        }

        // 清除表结构缓存
        clearTableSchemaCache(stmt.getTable());

        return 0;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;

        // 清理资源
        try {
            // 清理 RegionServer 连接引用（channel 由 GrpcChannelFactory 管理）
            regionServerStubs.clear();

            // 清理 Master 连接引用（channel 由 GrpcChannelFactory 管理）
            masterChannel = null;
            masterStub = null;

            // 关闭 ZooKeeper 连接
            if (zkClient != null) {
                zkClient.close();
                zkClient = null;
            }

            // 清理事务状态
            transactionOperations.clear();

            // 关闭并行查询执行器
            if (parallelQueryExecutor != null) {
                parallelQueryExecutor.shutdown();
            }

            sqlMetricsExecutor.shutdownNow();

        } catch (Exception e) {
            throw new SQLException("Error closing connection", e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    void reportSqlExecution(String sql, boolean success, long latencyMs, String errorMessage) {
        if (masterStub == null || sql == null) {
            return;
        }
        String sqlType = detectSqlType(sql);
        String tableName = extractTableNameForMetrics(sql);
        List<String> regionIds = collectRegionIds(tableName);
        sqlMetricsExecutor.submit(() -> {
            try {
                MasterProto.ReportSqlMetricsRequest request = MasterProto.ReportSqlMetricsRequest.newBuilder()
                    .setSqlType(sqlType)
                    .setTableName(tableName == null ? "" : tableName)
                    .addAllRegionIds(regionIds)
                    .setSuccess(success)
                    .setLatencyMs(latencyMs)
                    .setTimestamp(System.currentTimeMillis())
                    .setErrorMessage(errorMessage == null ? "" : errorMessage)
                    .setSource("client")
                    .build();
                masterStub.reportSqlMetrics(request);
            } catch (Exception e) {
                logger.debug("Failed to report SQL metrics: {}", e.getMessage());
            }
        });
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (!autoCommit) {
            throw new SQLFeatureNotSupportedException("Manual transactions are not implemented");
        }
        this.autoCommit = true;
        transactionOperations.clear();
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Manual transactions are not implemented");
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Manual transactions are not implemented");
    }

    // ... 其他 Connection 接口方法的空实现

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Database metadata is not implemented");
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly) {
            throw new SQLFeatureNotSupportedException("Read-only mode is not implemented");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {}

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException("Transaction isolation is not implemented");
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {}

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {}

    @Override
    public void setHoldability(int holdability) throws SQLException {}

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not implemented");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not implemented");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not implemented");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not implemented");
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !closed && zkClient != null && zkClient.isConnected();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {}

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {}

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {}

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {}

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {}

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /**
     * 获取所有表列表
     * @return 表名列表
     * @throws SQLException 当获取失败时抛出
     */
    public List<String> listTables() throws SQLException {
        try {
            MasterProto.ListTablesRequest request = MasterProto.ListTablesRequest.newBuilder().build();
            MasterProto.ListTablesResponse response = masterStub.listTables(request);

            if (!response.getStatus().getSuccess()) {
                throw new SQLException("Failed to list tables: " + response.getStatus().getMessage());
            }

            // 提取表名列表
            List<String> tableNames = new ArrayList<>();
            for (CommonProto.TableSchema schema : response.getTablesList()) {
                tableNames.add(schema.getTableName());
            }
            return tableNames;
        } catch (Exception e) {
            throw new SQLException("Failed to list tables: " + e.getMessage(), e);
        }
    }

    /**
     * 获取表结构（带缓存）
     */
    private com.minisql.common.model.Table getTableSchema(String tableName) throws SQLException {
        // 先从缓存获取
        com.minisql.common.model.Table cached = tableSchemaCache.get(tableName);
        if (cached != null) {
            return cached;
        }

        // 通过 RPC 获取表结构
        try {
            MasterProto.GetTableSchemaRequest request = MasterProto.GetTableSchemaRequest.newBuilder()
                    .setTableName(tableName)
                    .build();

            MasterProto.GetTableSchemaResponse response = masterStub.getTableSchema(request);

            if (!response.getStatus().getSuccess()) {
                throw new SQLException("Failed to get table schema: " + response.getStatus().getMessage());
            }

            // 转换 protobuf TableSchema 到模型 Table
            com.minisql.common.model.Table table = convertProtoToTable(response.getSchema());
            tableSchemaCache.put(tableName, table);
            return table;
        } catch (Exception e) {
            throw new SQLException("Failed to get table schema: " + e.getMessage(), e);
        }
    }

    /**
     * 将 protobuf TableSchema 转换为模型 Table
     */
    private com.minisql.common.model.Table convertProtoToTable(CommonProto.TableSchema proto) {
        com.minisql.common.model.Table table = new com.minisql.common.model.Table();
        table.setTableName(proto.getTableName());

        if (proto.getPrimaryKey() != null && !proto.getPrimaryKey().isEmpty()) {
            table.setPrimaryKey(proto.getPrimaryKey());
        }

        List<com.minisql.common.model.Column> columns = new ArrayList<>();
        for (CommonProto.ColumnSchema colProto : proto.getColumnsList()) {
            com.minisql.common.model.Column column = new com.minisql.common.model.Column();
            column.setName(colProto.getName());
            column.setType(com.minisql.common.model.Column.ColumnType.valueOf(colProto.getType()));
            column.setNullable(colProto.getNullable());
            columns.add(column);
        }
        table.setColumns(columns);

        return table;
    }

    /**
     * 清除表结构缓存
     */
    private void clearTableSchemaCache(String tableName) {
        if (tableName != null) {
            tableSchemaCache.remove(tableName);
        } else {
            tableSchemaCache.clear();
        }
    }

    /**
     * 根据列类型转换字符串值
     */
    private Object convertValue(String rawValue, Column.ColumnType type) {
        if (rawValue == null) {
            return null;
        }

        // 类型为空时返回原始值（不应该发生，通常是表结构缓存不一致导致）
        if (type == null) {
            logger.warn("Column type is null, returning raw value: " + rawValue);
            return rawValue;
        }

        try {
            switch (type) {
                case INT:
                    return Integer.parseInt(rawValue);
                case BIGINT:
                    return Long.parseLong(rawValue);
                case FLOAT:
                    return Float.parseFloat(rawValue);
                case DOUBLE:
                    return Double.parseDouble(rawValue);
                case BOOLEAN:
                    return Boolean.parseBoolean(rawValue);
                case VARCHAR:
                case CHAR:
                case STRING:
                    return rawValue;
                case TIMESTAMP:
                    // 假设时间戳是毫秒数
                    return Long.parseLong(rawValue);
                case BLOB:
                    // BLOB 作为字符串处理
                    return rawValue.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                default:
                    return rawValue;
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException("Cannot convert '" + rawValue + "' to " + type, e);
        }
    }

    /**
     * 将模型 KeyValue 转换为 protobuf KeyValue
     */
    private CommonProto.KeyValue convertToProto(com.minisql.common.model.KeyValue kv) {
        CommonProto.KeyValue.Builder builder = CommonProto.KeyValue.newBuilder()
                .setRowKey(com.google.protobuf.ByteString.copyFrom(kv.getRowKey()))
                .setColumnFamily(kv.getFamily() != null ? kv.getFamily() : "")
                .setQualifier(kv.getQualifier())
                .setTimestamp(kv.getTimestamp())
                .setValue(com.google.protobuf.ByteString.copyFrom(kv.getValue()))
                .setType(kv.getType() == com.minisql.common.model.KeyValue.Type.PUT ?
                        CommonProto.KeyValueType.PUT : CommonProto.KeyValueType.DELETE);
        return builder.build();
    }

    /**
     * 获取真实的 RegionId
     */
    private String getRegionId(String tableName, byte[] rowKey) throws SQLException {
        // 从 Router 的路由缓存中获取真实的 RegionId
        List<Router.RegionRouteInfo> regions = router.getRouteCache(tableName);
        if (regions == null || regions.isEmpty()) {
            // 如果缓存为空，刷新路由
            router.refreshRouteCache(tableName);
            regions = router.getRouteCache(tableName);
        }

        if (regions != null && !regions.isEmpty()) {
            // 根据 rowKey 找到对应的 Region
            for (Router.RegionRouteInfo region : regions) {
                if (BytesUtil.isKeyInRange(rowKey, region.getStartKey(), region.getEndKey())) {
                    return region.getRegionId();
                }
            }
            // 如果没有匹配的 Region，返回第一个
            // no fallback-to-first-region; retry with fresh cache below
        }

        // 兜底：返回硬编码的 RegionId（可能会导致错误，但保持向后兼容）
        // Retry once with a fresh route cache to absorb split/migration propagation delay.
        router.refreshRouteCache(tableName);
        regions = router.getRouteCache(tableName);
        if (regions != null && !regions.isEmpty()) {
            for (Router.RegionRouteInfo region : regions) {
                if (BytesUtil.isKeyInRange(rowKey, region.getStartKey(), region.getEndKey())) {
                    return region.getRegionId();
                }
            }
        }
        throw new SQLException("No matching region found for row key in table: " + tableName);
    }

    private String detectSqlType(String sql) {
        String normalized = sql == null ? "" : sql.trim().toUpperCase(Locale.ROOT);
        if (normalized.startsWith("SELECT")) return "SELECT";
        if (normalized.startsWith("INSERT")) return "INSERT";
        if (normalized.startsWith("UPDATE")) return "UPDATE";
        if (normalized.startsWith("DELETE")) return "DELETE";
        if (normalized.startsWith("CREATE") || normalized.startsWith("DROP")) return "DDL";
        if (normalized.startsWith("SHOW")) return "SHOW";
        if (normalized.startsWith("DESCRIBE")) return "DESCRIBE";
        return "OTHER";
    }

    private String extractTableNameForMetrics(String sql) {
        try {
            SQLParser parser = new SQLParser(sql);
            com.minisql.sql.ast.Statement statement = parser.parse();
            if (statement instanceof com.minisql.sql.ast.SelectStatement) {
                return ((com.minisql.sql.ast.SelectStatement) statement).getTable();
            }
            if (statement instanceof com.minisql.sql.ast.InsertStatement) {
                return ((com.minisql.sql.ast.InsertStatement) statement).getTable();
            }
            if (statement instanceof com.minisql.sql.ast.UpdateStatement) {
                return ((com.minisql.sql.ast.UpdateStatement) statement).getTable();
            }
            if (statement instanceof com.minisql.sql.ast.DeleteStatement) {
                return ((com.minisql.sql.ast.DeleteStatement) statement).getTable();
            }
            if (statement instanceof com.minisql.sql.ast.CreateTableStatement) {
                return ((com.minisql.sql.ast.CreateTableStatement) statement).getTable();
            }
            if (statement instanceof com.minisql.sql.ast.DropTableStatement) {
                return ((com.minisql.sql.ast.DropTableStatement) statement).getTable();
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private List<String> collectRegionIds(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return Collections.emptyList();
        }
        List<Router.RegionRouteInfo> routeInfos = router.getRouteCache(tableName);
        if (routeInfos == null) {
            return Collections.emptyList();
        }
        return routeInfos.stream()
            .map(Router.RegionRouteInfo::getRegionId)
            .distinct()
            .collect(Collectors.toList());
    }
}
