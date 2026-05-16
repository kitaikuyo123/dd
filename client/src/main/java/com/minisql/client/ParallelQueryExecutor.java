package com.minisql.client;

import com.google.protobuf.ByteString;
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
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.ConditionSplitter;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.ast.SimpleCondition;
import com.minisql.sql.AggregateExpr;
import com.minisql.sql.AggregateType;
import com.minisql.sql.JoinType;
import com.minisql.sql.execution.Row;
import com.minisql.sql.execution.operators.AggregateOperator;
import com.minisql.sql.execution.operators.FilterOperator;
import com.minisql.sql.execution.operators.JoinOperator;
import com.minisql.sql.execution.operators.LimitOperator;
import com.minisql.sql.execution.operators.ListSourceOperator;
import com.minisql.sql.execution.operators.ProjectOperator;
import com.minisql.sql.execution.operators.SortOperator;
import io.grpc.ManagedChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes distributed SELECT queries: parallel region scanning + SQL semantics
 * via the Operator pipeline. Distributed logic (routing, gRPC, two-phase
 * aggregation merge) lives here; SQL processing (filter, join, aggregate,
 * sort, limit, project) is delegated to the Volcano-style Operator tree.
 */
public class ParallelQueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ParallelQueryExecutor.class);

    private final ExecutorService executor;
    private final MasterServiceGrpc.MasterServiceBlockingStub masterStub;
    private final Router router;
    private final long queryTimeoutSeconds;

    public ParallelQueryExecutor(MasterServiceGrpc.MasterServiceBlockingStub masterStub,
                                 long queryTimeoutSeconds,
                                 Router router) {
        this.masterStub = masterStub;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.router = router;
        this.executor = Executors.newFixedThreadPool(10);
    }

    ParallelQueryExecutor(MasterServiceGrpc.MasterServiceBlockingStub masterStub,
                          long queryTimeoutSeconds) {
        this(masterStub, queryTimeoutSeconds, null);
    }

    // ── Query entry point ──────────────────────────────────────────────

    public List<Row> executeQuery(SelectStatement ast, String rawSql) throws SQLException {
        String tableName = ast.getTable();
        String tableAlias = ast.getTableAlias();
        boolean hasJoin = ast.getJoinTable() != null;
        boolean hasAggregation = ast.hasAggregation();

        // Build aggregate expressions from AST
        List<AggregateExpression> aggregateExpressions = buildAggregateExpressions(ast);

        // Determine pushdown WHERE clause
        String whereClause = (ast.getWhere() != null && canPushDownCondition(ast.getWhere()) && !hasJoin)
            ? conditionToSql(ast.getWhere()) : null;

        // Determine projected qualifiers for column pushdown
        List<String> projectedQualifiers;
        if (hasJoin) {
            List<String> joinLeftCols = new ArrayList<>();
            List<String> joinRightCols = new ArrayList<>();
            extractJoinConditionColumns(ast.getJoinCondition(), joinLeftCols, joinRightCols,
                tableAlias != null ? tableAlias : tableName,
                ast.getJoinTableAlias() != null ? ast.getJoinTableAlias() : ast.getJoinTable());
            projectedQualifiers = determineJoinProjectedQualifiersFromAst(
                ast, tableName, tableAlias, joinLeftCols, aggregateExpressions);
        } else {
            projectedQualifiers = determineProjectedQualifiersFromAst(ast, hasAggregation);
        }

        // ORDER BY + LIMIT pushdown
        boolean canPushDownOrderBy = !hasJoin && !hasAggregation
            && ast.getOrderBy() != null && !ast.getOrderBy().isEmpty();
        int userLimit = ast.getLimit() != null ? ast.getLimit() : -1;
        int userOffset = ast.getOffset() != null ? ast.getOffset() : 0;
        List<SelectStatement.OrderByElement> pushDownOrderBy = null;
        int pushDownLimit = 0;
        if (canPushDownOrderBy) {
            pushDownOrderBy = ast.getOrderBy();
            if (userLimit >= 0) {
                pushDownLimit = userLimit + userOffset;
            }
        }

        // Aggregate pushdown: only when no JOIN and no HAVING
        boolean canPushDownAggregation = hasAggregation && !hasJoin && ast.getHaving() == null;

        // ── Data fetching phase ──

        if (canPushDownAggregation) {
            List<String> groupByColumns = ast.getGroupByColumns() != null
                ? ast.getGroupByColumns() : Collections.emptyList();
            List<com.minisql.common.model.Row> rows = fetchAggregatedRows(
                tableName, ast.getWhere(), whereClause,
                projectedQualifiers, aggregateExpressions, groupByColumns);
            String[] columns = determineOutputColumnsFromRows(rows);
            com.minisql.sql.execution.Operator pipeline = new ListSourceOperator(rows, columns);
            pipeline = appendOrderByLimitProject(pipeline, ast, userLimit, userOffset);
            return drainPipeline(pipeline);
        }

        if (hasJoin) {
            return executeJoinQuery(ast, aggregateExpressions, projectedQualifiers,
                whereClause, userLimit, userOffset);
        }

        // Simple (non-JOIN, non-pushdown-agg) query
        List<com.minisql.common.model.Row> rows = fetchSourceRows(
            tableName, null, ast.getWhere(), whereClause, projectedQualifiers,
            pushDownOrderBy, pushDownLimit, 0);

        Table schema = getTableSchema(tableName);
        String[] columns = schema.getColumns().stream()
            .map(Column::getName).toArray(String[]::new);

        com.minisql.sql.execution.Operator pipeline = new ListSourceOperator(rows, columns);

        // Client-side filter when WHERE wasn't pushed down
        if (ast.getWhere() != null && whereClause == null) {
            Condition cond = ast.getWhere();
            pipeline = new FilterOperator(pipeline, row -> cond.evaluate(row));
        }

        // Aggregation
        if (hasAggregation) {
            pipeline = appendAggregate(pipeline, ast);
        }

        pipeline = appendOrderByLimitProject(pipeline, ast, userLimit, userOffset);
        return drainPipeline(pipeline);
    }

    // ── JOIN query pipeline ────────────────────────────────────────────

    private List<Row> executeJoinQuery(SelectStatement ast,
                                       List<AggregateExpression> aggregateExpressions,
                                       List<String> leftProjectedQualifiers,
                                       String leftWhereClause,
                                       int userLimit, int userOffset) throws SQLException {
        String leftTable = ast.getTable();
        String leftAlias = ast.getTableAlias();
        String rightTable = ast.getJoinTable();
        String rightAlias = ast.getJoinTableAlias();
        String leftQualifier = leftAlias != null ? leftAlias : leftTable;
        String rightQualifier = rightAlias != null ? rightAlias : rightTable;

        // Split WHERE for JOIN
        Condition leftWhereCondition = ast.getWhere();
        Condition rightWhereCondition = null;
        Condition crossTableCondition = null;

        if (ast.getWhere() != null) {
            ConditionSplitter splitter = new ConditionSplitter(
                leftTable, leftAlias, rightTable, rightAlias);
            ConditionSplitter.SplitResult split = splitter.split(ast.getWhere());
            leftWhereCondition = split.leftOnly;
            leftWhereClause = (split.leftOnly != null && canPushDownCondition(split.leftOnly))
                ? conditionToSql(split.leftOnly) : null;
            rightWhereCondition = split.rightOnly;
            crossTableCondition = split.crossTable;
        }

        // Fetch left rows
        List<com.minisql.common.model.Row> leftRows = fetchSourceRows(
            leftTable, null, leftWhereCondition, leftWhereClause, leftProjectedQualifiers);

        // Determine right-side projected qualifiers
        List<String> leftJoinCols = new ArrayList<>();
        List<String> rightJoinCols = new ArrayList<>();
        extractJoinConditionColumns(ast.getJoinCondition(), leftJoinCols, rightJoinCols,
            leftQualifier, rightQualifier);

        List<String> rightProjectedQualifiers = determineJoinProjectedQualifiersFromAst(
            ast, rightTable, rightAlias, rightJoinCols, aggregateExpressions);

        String rightWhereClause = (rightWhereCondition != null && canPushDownCondition(rightWhereCondition))
            ? conditionToSql(rightWhereCondition) : null;

        // Fetch right rows
        List<com.minisql.common.model.Row> rightRows = fetchSourceRows(
            rightTable, null, rightWhereCondition, rightWhereClause, rightProjectedQualifiers);

        // Build ListSourceOperators with qualified column names for JOIN
        Table leftSchema = getTableSchema(leftTable);
        Table rightSchema = getTableSchema(rightTable);

        String[] leftColumns = buildQualifiedColumns(leftSchema, leftQualifier);
        String[] rightColumns = buildQualifiedColumns(rightSchema, rightQualifier);

        com.minisql.sql.execution.Operator leftSource = new ListSourceOperator(leftRows, leftColumns);
        com.minisql.sql.execution.Operator rightSource = new ListSourceOperator(rightRows, rightColumns);

        // Build JoinOperator
        JoinOperator.JoinCondition joinCond = buildJoinCondition(
            ast.getJoinCondition(), leftQualifier, rightQualifier, leftColumns, rightColumns);
        JoinType joinType = ast.getJoinType() != null ? ast.getJoinType() : JoinType.INNER;

        com.minisql.sql.execution.Operator pipeline = new JoinOperator(
            leftSource, rightSource, joinType, joinCond);

        // Cross-table WHERE
        final Condition finalCrossCond = crossTableCondition;
        if (finalCrossCond != null) {
            pipeline = new FilterOperator(pipeline, row -> finalCrossCond.evaluate(row));
        }

        // Aggregation
        if (ast.hasAggregation()) {
            pipeline = appendAggregate(pipeline, ast);
        }

        pipeline = appendOrderByLimitProject(pipeline, ast, userLimit, userOffset);
        return drainPipeline(pipeline);
    }

    // ── Operator pipeline helpers ──────────────────────────────────────

    private com.minisql.sql.execution.Operator appendAggregate(
            com.minisql.sql.execution.Operator input, SelectStatement ast) {
        List<AggregateExpr> aggregates = new ArrayList<>();
        if (ast.getAggregates() != null) {
            for (SelectStatement.AggregateExpr agg : ast.getAggregates()) {
                AggregateType type = AggregateType.valueOf(agg.getFunction().toUpperCase());
                AggregateExpr expr = new AggregateExpr(type, agg.getColumn());
                expr.setAlias(agg.getOutputName());
                aggregates.add(expr);
            }
        }
        List<String> groupByColumns = ast.getGroupByColumns();
        com.minisql.sql.execution.Operator pipeline = new AggregateOperator(input, aggregates, groupByColumns);

        if (ast.getHaving() != null) {
            Condition having = ast.getHaving();
            pipeline = new FilterOperator(pipeline, row -> having.evaluate(row));
        }
        return pipeline;
    }

    private com.minisql.sql.execution.Operator appendOrderByLimitProject(
            com.minisql.sql.execution.Operator pipeline, SelectStatement ast,
            int userLimit, int userOffset) {

        // ORDER BY
        if (ast.getOrderBy() != null && !ast.getOrderBy().isEmpty()) {
            List<SortOperator.SortKey> sortKeys = new ArrayList<>();
            for (SelectStatement.OrderByElement elem : ast.getOrderBy()) {
                sortKeys.add(new SortOperator.SortKey(elem.getColumn(), elem.isAscending()));
            }
            pipeline = new SortOperator(pipeline, sortKeys);
        }

        // LIMIT / OFFSET
        if (userLimit >= 0) {
            pipeline = new LimitOperator(pipeline, userLimit, Math.max(0, userOffset));
        }

        // Projection
        if (ast.isSelectAll()) {
            pipeline = new ProjectOperator(pipeline, Collections.emptyList(), true);
        } else if (ast.getColumns() != null && !ast.getColumns().isEmpty()) {
            List<String> outputNames = new ArrayList<>();
            List<String> sourceNames = new ArrayList<>();
            List<String> aliases = ast.getColumnAliases();
            for (int i = 0; i < ast.getColumns().size(); i++) {
                String col = ast.getColumns().get(i);
                sourceNames.add(col);
                outputNames.add((aliases != null && i < aliases.size() && aliases.get(i) != null)
                    ? aliases.get(i) : col);
            }
            pipeline = new ProjectOperator(pipeline, sourceNames, outputNames);
        }

        return pipeline;
    }

    private static List<Row> drainPipeline(com.minisql.sql.execution.Operator root) throws SQLException {
        try {
            root.open();
            List<Row> results = new ArrayList<>();
            while (root.hasMore()) {
                Row row = root.nextRow();
                if (row != null) {
                    results.add(row);
                }
            }
            root.close();
            return results;
        } catch (IOException e) {
            throw new SQLException("Operator pipeline failed", e);
        }
    }

    private String[] buildQualifiedColumns(Table schema, String qualifier) {
        List<String> cols = new ArrayList<>();
        for (Column col : schema.getColumns()) {
            cols.add(col.getName());
            cols.add(qualifier + "." + col.getName());
        }
        return cols.toArray(new String[0]);
    }

    private JoinOperator.JoinCondition buildJoinCondition(Condition condition,
                                                          String leftQualifier,
                                                          String rightQualifier,
                                                          String[] leftColumns,
                                                          String[] rightColumns) {
        // Find the equality condition in the ON clause
        if (condition instanceof SimpleCondition) {
            SimpleCondition simple = (SimpleCondition) condition;
            if ("=".equals(simple.getOperator()) && simple.isValueColumnReference()) {
                String col1 = simple.getColumn();
                String col2 = simple.getValue();
                String leftCol = qualifyColumn(col1, leftQualifier);
                String rightCol = qualifyColumn(col2, rightQualifier);
                if (belongsToTable(col1, leftQualifier)) {
                    return new JoinOperator.JoinCondition(leftCol, rightCol,
                        JoinOperator.JoinOperatorType.EQUALS);
                } else {
                    return new JoinOperator.JoinCondition(rightCol, leftCol,
                        JoinOperator.JoinOperatorType.EQUALS);
                }
            }
        }
        // For compound conditions, take the first equality (simplified)
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            if (buildJoinCondition(compound.getLeft(), leftQualifier, rightQualifier, leftColumns, rightColumns) != null) {
                return buildJoinCondition(compound.getLeft(), leftQualifier, rightQualifier, leftColumns, rightColumns);
            }
            return buildJoinCondition(compound.getRight(), leftQualifier, rightQualifier, leftColumns, rightColumns);
        }
        throw new IllegalArgumentException("Unsupported JOIN condition: " + condition);
    }

    private String qualifyColumn(String col, String qualifier) {
        if (col.contains(".")) return col;
        return qualifier + "." + col;
    }

    private String[] determineOutputColumnsFromRows(List<com.minisql.common.model.Row> rows) {
        if (rows.isEmpty()) return new String[0];
        return rows.get(0).getColumnNames().toArray(new String[0]);
    }

    // ── Distributed scan ───────────────────────────────────────────────

    private List<com.minisql.common.model.Row> fetchSourceRows(String tableName,
                                                               byte[] rowKey,
                                                               Condition whereCondition,
                                                               String whereClause,
                                                               List<String> projectedQualifiers) throws SQLException {
        return fetchSourceRows(tableName, rowKey, whereCondition, whereClause, projectedQualifiers, null, 0, 0);
    }

    private List<com.minisql.common.model.Row> fetchSourceRows(String tableName,
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

    private List<com.minisql.common.model.Row> fetchRowsFromRegion(RegionLocation location,
                                                                   Condition whereCondition,
                                                                   String whereClause,
                                                                   List<String> projectedQualifiers,
                                                                   List<SelectStatement.OrderByElement> orderBy,
                                                                   int limit,
                                                                   int offset) throws SQLException {
        ManagedChannel channel = GrpcChannelFactory.forAddress(location.serverHost, location.serverPort);

        RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
            RegionServerServiceGrpc.newBlockingStub(channel);
        Table schema = getTableSchema(location.tableName);
        List<KeyValue> keyValues = scanKeyValues(stub, location.regionId, location.tableName,
            whereClause, projectedQualifiers, orderBy, limit, offset);
        List<com.minisql.common.model.Row> rows = RowAssembler.assemble(keyValues, schema);
        return (whereCondition != null && whereClause == null) ? filterRows(rows, whereCondition) : rows;
    }

    private List<KeyValue> scanKeyValues(RegionServerServiceGrpc.RegionServerServiceBlockingStub stub,
                                         String regionId,
                                         String tableName,
                                         String whereClause,
                                         List<String> projectedQualifiers) throws SQLException {
        return scanKeyValues(stub, regionId, tableName, whereClause, projectedQualifiers, null, 0, 0);
    }

    private List<KeyValue> scanKeyValues(RegionServerServiceGrpc.RegionServerServiceBlockingStub stub,
                                         String regionId,
                                         String tableName,
                                         String whereClause,
                                         List<String> projectedQualifiers,
                                         List<SelectStatement.OrderByElement> orderBy,
                                         int limit,
                                         int offset) throws SQLException {
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
        if (limit > 0) {
            requestBuilder.setLimit(limit);
        }
        if (offset > 0) {
            requestBuilder.setOffset(offset);
        }
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
                        ? KeyValue.Type.PUT
                        : KeyValue.Type.DELETE);
                    keyValues.add(kv);
                }
            }
            return keyValues;
        } catch (RuntimeException e) {
            throw new SQLException("Failed to scan region " + regionId, e);
        }
    }

    // ── Two-phase aggregation ──────────────────────────────────────────

    private List<com.minisql.common.model.Row> fetchAggregatedRows(
            String tableName,
            Condition whereCondition,
            String whereClause,
            List<String> projectedQualifiers,
            List<AggregateExpression> aggregateExpressions,
            List<String> groupByColumns) throws SQLException {

        List<RegionLocation> targets = getAllRegionsForTable(tableName);
        if (targets.isEmpty()) {
            throw new SQLException("No regions found for table: " + tableName);
        }

        List<Future<List<RegionServerProto.AggregateGroup>>> futures = new ArrayList<>();
        for (RegionLocation location : targets) {
            futures.add(executor.submit(() -> fetchAggregateGroupsFromRegion(
                location, whereCondition, whereClause, projectedQualifiers,
                aggregateExpressions, groupByColumns)));
        }

        Map<List<String>, double[]> merged = new LinkedHashMap<>();
        Map<List<String>, String[]> minMax = new LinkedHashMap<>();

        try {
            for (int fi = 0; fi < futures.size(); fi++) {
                List<RegionServerProto.AggregateGroup> groups = futures.get(fi)
                    .get(queryTimeoutSeconds, TimeUnit.SECONDS);
                for (RegionServerProto.AggregateGroup group : groups) {
                    List<String> key = new ArrayList<>();
                    for (com.google.protobuf.ByteString bs : group.getGroupByKeyList()) {
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
            for (AggregateExpression expr : aggregateExpressions) {
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

    private Object parseValue(String val) {
        if (val == null) return null;
        try { return Long.parseLong(val); } catch (NumberFormatException ignored) {}
        try { return Double.parseDouble(val); } catch (NumberFormatException ignored) {}
        return val;
    }

    private List<RegionServerProto.AggregateGroup> fetchAggregateGroupsFromRegion(
            RegionLocation location,
            Condition whereCondition,
            String whereClause,
            List<String> projectedQualifiers,
            List<AggregateExpression> aggregateExpressions,
            List<String> groupByColumns) throws SQLException {

        ManagedChannel channel = GrpcChannelFactory.forAddress(location.serverHost, location.serverPort);

        try {
            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                RegionServerServiceGrpc.newBlockingStub(channel);

            RegionServerProto.ScanRequest.Builder reqBuilder = RegionServerProto.ScanRequest.newBuilder()
                .setRegionId(location.regionId)
                .setStartKey(ByteString.EMPTY)
                .setEndKey(ByteString.copyFrom(new byte[]{(byte) 0xFF}))
                .setTableName(location.tableName == null ? "" : location.tableName);

            if (projectedQualifiers != null && !projectedQualifiers.isEmpty()) {
                reqBuilder.addAllQualifiers(projectedQualifiers);
            }
            if (whereClause != null && !whereClause.isBlank()) {
                reqBuilder.setWhereClause(whereClause);
            }
            for (AggregateExpression expr : aggregateExpressions) {
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
            throw new SQLException("Failed to scan region " + location.regionId, e);
        }
    }

    // ── AST helpers ────────────────────────────────────────────────────

    private boolean canPushDownCondition(Condition condition) {
        if (condition == null) return false;
        if (condition instanceof SimpleCondition) {
            String column = ((SimpleCondition) condition).getColumn();
            return column != null && !column.isBlank() && !column.contains(".");
        }
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            return canPushDownCondition(compound.getLeft()) && canPushDownCondition(compound.getRight());
        }
        return false;
    }

    private String conditionToSql(Condition condition) {
        if (condition == null) return null;
        if (condition instanceof SimpleCondition) {
            SimpleCondition simple = (SimpleCondition) condition;
            if (simple.isValueColumnReference()) {
                return simple.getColumn() + " " + simple.getOperator() + " " + simple.getValue();
            }
            return simple.getColumn() + " " + simple.getOperator() + " '" + simple.getValue() + "'";
        }
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            return "(" + conditionToSql(compound.getLeft()) + " " + compound.getOperator()
                + " " + conditionToSql(compound.getRight()) + ")";
        }
        return null;
    }

    private void extractJoinConditionColumns(Condition condition,
                                             List<String> leftCols,
                                             List<String> rightCols,
                                             String leftTable,
                                             String rightTable) {
        if (condition instanceof SimpleCondition) {
            SimpleCondition simple = (SimpleCondition) condition;
            if ("=".equals(simple.getOperator()) && simple.isValueColumnReference()) {
                String col1 = simple.getColumn();
                String col2 = simple.getValue();
                if (belongsToTable(col1, leftTable)) {
                    leftCols.add(col1);
                    rightCols.add(col2);
                } else {
                    leftCols.add(col2);
                    rightCols.add(col1);
                }
            }
        } else if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            if ("AND".equalsIgnoreCase(compound.getOperator())) {
                extractJoinConditionColumns(compound.getLeft(), leftCols, rightCols, leftTable, rightTable);
                extractJoinConditionColumns(compound.getRight(), leftCols, rightCols, leftTable, rightTable);
            }
        }
    }

    private boolean belongsToTable(String column, String table) {
        if (column == null || table == null) return false;
        int dot = column.indexOf('.');
        if (dot < 0) return true;
        return column.substring(0, dot).equalsIgnoreCase(table);
    }

    private List<String> determineProjectedQualifiersFromAst(SelectStatement ast, boolean hasAggregation) {
        if (ast.isSelectAll() || hasAggregation) return null;
        List<String> columns = new ArrayList<>();
        if (ast.getColumns() != null) {
            for (String col : ast.getColumns()) {
                String qualifier = toSimpleQualifier(col);
                if (qualifier != null && !columns.contains(qualifier)) columns.add(qualifier);
            }
        }
        if (ast.getOrderBy() != null) {
            for (SelectStatement.OrderByElement elem : ast.getOrderBy()) {
                String qualifier = toSimpleQualifier(elem.getColumn());
                if (qualifier != null && !columns.contains(qualifier)) columns.add(qualifier);
            }
        }
        collectConditionColumns(ast.getWhere(), columns);
        return columns.isEmpty() ? null : columns;
    }

    private List<String> determineJoinProjectedQualifiersFromAst(SelectStatement ast,
                                                                  String tableName,
                                                                  String tableAlias,
                                                                  List<String> joinColumns,
                                                                  List<AggregateExpression> aggregates) {
        List<String> columns = new ArrayList<>();
        addJoinColumns(columns, joinColumns);
        if (!ast.isSelectAll() && ast.getColumns() != null) {
            for (String col : ast.getColumns()) {
                String qualifier = qualifyForSource(col, tableName, tableAlias);
                if (qualifier != null && !columns.contains(qualifier)) columns.add(qualifier);
            }
        }
        if (ast.getOrderBy() != null) {
            for (SelectStatement.OrderByElement elem : ast.getOrderBy()) {
                String qualifier = qualifyForSource(elem.getColumn(), tableName, tableAlias);
                if (qualifier != null && !columns.contains(qualifier)) columns.add(qualifier);
            }
        }
        if (ast.getGroupByColumns() != null) {
            for (String col : ast.getGroupByColumns()) {
                String qualifier = qualifyForSource(col, tableName, tableAlias);
                if (qualifier != null && !columns.contains(qualifier)) columns.add(qualifier);
            }
        }
        collectConditionColumnsForSource(ast.getWhere(), tableName, tableAlias, columns);
        collectConditionColumnsForSource(ast.getHaving(), tableName, tableAlias, columns);
        for (AggregateExpression expr : aggregates) {
            String qualifier = qualifyForSource(expr.column, tableName, tableAlias);
            if (qualifier != null && !columns.contains(qualifier)) columns.add(qualifier);
        }
        return columns.isEmpty() ? null : columns;
    }

    private List<AggregateExpression> buildAggregateExpressions(SelectStatement ast) {
        List<AggregateExpression> list = new ArrayList<>();
        if (ast.getAggregates() != null) {
            for (SelectStatement.AggregateExpr agg : ast.getAggregates()) {
                AggregateExpression expr = new AggregateExpression();
                expr.function = agg.getFunction();
                expr.column = agg.getColumn();
                expr.outputName = agg.getOutputName();
                list.add(expr);
            }
        }
        return list;
    }

    private void collectConditionColumnsForSource(Condition condition,
                                                  String tableName, String tableAlias,
                                                  List<String> target) {
        if (condition == null) return;
        if (condition instanceof SimpleCondition) {
            String qualifier = qualifyForSource(((SimpleCondition) condition).getColumn(), tableName, tableAlias);
            if (qualifier != null && !target.contains(qualifier)) target.add(qualifier);
            return;
        }
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            collectConditionColumnsForSource(compound.getLeft(), tableName, tableAlias, target);
            collectConditionColumnsForSource(compound.getRight(), tableName, tableAlias, target);
        }
    }

    private void addJoinColumns(List<String> target, List<String> joinColumns) {
        if (joinColumns == null) return;
        for (String column : joinColumns) {
            String qualifier = toSimpleQualifier(column);
            if (qualifier != null && !target.contains(qualifier)) target.add(qualifier);
        }
    }

    private String qualifyForSource(String column, String tableName, String tableAlias) {
        if (column == null || column.isBlank()) return null;
        String trimmed = column.trim();
        if ("*".equals(trimmed) || trimmed.contains("(")) return null;
        int qi = trimmed.indexOf('.');
        if (qi < 0) return toSimpleQualifier(trimmed);
        String qualifier = trimmed.substring(0, qi);
        String unqualified = trimmed.substring(qi + 1);
        if (qualifier.equalsIgnoreCase(tableName) || (tableAlias != null && qualifier.equalsIgnoreCase(tableAlias))) {
            return toSimpleQualifier(unqualified);
        }
        return null;
    }

    private void collectConditionColumns(Condition condition, List<String> target) {
        if (condition == null) return;
        if (condition instanceof SimpleCondition) {
            String qualifier = toSimpleQualifier(((SimpleCondition) condition).getColumn());
            if (qualifier != null && !target.contains(qualifier)) target.add(qualifier);
            return;
        }
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            collectConditionColumns(compound.getLeft(), target);
            collectConditionColumns(compound.getRight(), target);
        }
    }

    private String toSimpleQualifier(String column) {
        if (column == null || column.isBlank()) return null;
        String trimmed = column.trim();
        if ("*".equals(trimmed) || trimmed.contains("(")) return null;
        int qi = trimmed.indexOf('.');
        return qi >= 0 ? trimmed.substring(qi + 1) : trimmed;
    }

    private List<com.minisql.common.model.Row> filterRows(List<com.minisql.common.model.Row> rows, Condition condition) {
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

    // ── Schema / routing ───────────────────────────────────────────────

    private Table getTableSchema(String tableName) throws SQLException {
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
            locations.add(loc);
            return locations;
        } catch (Exception e) {
            logger.warn("Exception while getting region location for table: {}", tableName, e);
            return new ArrayList<>();
        }
    }

    private List<RegionLocation> getAllRegionsForTable(String tableName) {
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
                locations.add(loc);
            }
            return locations;
        } catch (Exception e) {
            logger.warn("Exception while getting regions for table: {}", tableName, e);
            return new ArrayList<>();
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

    // ── Inner classes ──────────────────────────────────────────────────

    private static class RegionLocation {
        private String regionId;
        private String tableName;
        private String serverHost;
        private int serverPort;

        public String getRegionId() { return regionId; }
        public String getTableName() { return tableName; }
        public String getServerHost() { return serverHost; }
        public int getServerPort() { return serverPort; }
    }

    private static class AggregateExpression {
        private String function;
        private String column;
        private String outputName;
    }
}
