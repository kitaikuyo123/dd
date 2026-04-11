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
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.ast.SimpleCondition;
import com.minisql.sql.execution.QueryPlan.JoinType;
import com.minisql.sql.execution.Row;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
 * Executes distributed SELECT queries by scanning regions and applying SQL
 * semantics client-side. This avoids relying on RegionServer-side SQL rewrite
 * paths that do not match the KV storage model.
 */
public class ParallelQueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ParallelQueryExecutor.class);

    private final ExecutorService executor;
    private final MasterServiceGrpc.MasterServiceBlockingStub masterStub;
    private final Map<String, HikariDataSource> connectionPools;
    private final long queryTimeoutSeconds;

    public ParallelQueryExecutor(MasterServiceGrpc.MasterServiceBlockingStub masterStub,
                                 Map<String, HikariDataSource> connectionPools,
                                 long queryTimeoutSeconds) {
        this.masterStub = masterStub;
        this.connectionPools = connectionPools;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.executor = Executors.newFixedThreadPool(10);
    }

    /**
     * AST-driven query execution entry point.
     * Replaces the old string-parsing executeQuery methods.
     */
    public List<Row> executeQuery(SelectStatement ast, String rawSql) throws SQLException {
        String tableName = ast.getTable();
        String tableAlias = ast.getTableAlias();
        boolean selectAll = ast.isSelectAll();
        boolean hasJoin = ast.getJoinTable() != null;
        boolean hasAggregation = ast.hasAggregation();

        // Build aggregate expressions from AST
        List<AggregateExpression> aggregateExpressions = new ArrayList<>();
        if (ast.getAggregates() != null) {
            for (SelectStatement.AggregateExpr agg : ast.getAggregates()) {
                AggregateExpression expr = new AggregateExpression();
                expr.function = agg.getFunction();
                expr.column = agg.getColumn();
                expr.outputName = agg.getOutputName();
                aggregateExpressions.add(expr);
            }
        }

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

        // Fetch base rows
        List<com.minisql.common.model.Row> rows = fetchSourceRows(
            tableName, null, ast.getWhere(), whereClause, projectedQualifiers);

        // JOIN
        if (hasJoin) {
            rows = executeJoinFromAst(ast, rows);
        } else if (ast.getWhere() != null && whereClause == null) {
            // Client-side filter for conditions that couldn't be pushed down
            rows = filterRows(rows, ast.getWhere());
        }

        // Aggregation
        if (hasAggregation) {
            List<String> groupByColumns = ast.getGroupByColumns() != null ? ast.getGroupByColumns() : Collections.emptyList();
            rows = aggregateRowsFromAst(groupByColumns, aggregateExpressions, rows);
            if (ast.getHaving() != null) {
                rows = filterRows(rows, ast.getHaving());
            }
        }

        // ORDER BY
        if (ast.getOrderBy() != null && !ast.getOrderBy().isEmpty()) {
            List<String> orderByColumns = new ArrayList<>();
            List<Boolean> orderAscending = new ArrayList<>();
            for (SelectStatement.OrderByElement elem : ast.getOrderBy()) {
                orderByColumns.add(elem.getColumn());
                orderAscending.add(elem.isAscending());
            }
            rows.sort(createSortComparator(orderByColumns, orderAscending));
        }

        // LIMIT / OFFSET
        int limit = ast.getLimit() != null ? ast.getLimit() : -1;
        int offset = ast.getOffset() != null ? ast.getOffset() : 0;
        rows = applyLimitOffset(rows, limit, offset);

        // Project
        return projectRowsFromAst(ast, rows, hasAggregation);
    }


    private String buildJoinKey(com.minisql.common.model.Row row, List<String> columns) {
        List<String> values = new ArrayList<>();
        for (String column : columns) {
            Object value = resolveColumnValue(row, column);
            if (value == null) {
                return null;
            }
            values.add(String.valueOf(value));
        }
        return String.join("\u0001", values);
    }

    private Comparator<com.minisql.common.model.Row> createSortComparator(List<String> columns,
                                                                          List<Boolean> ascending) {
        return (left, right) -> {
            for (int i = 0; i < columns.size(); i++) {
                Object leftValue = resolveColumnValue(left, columns.get(i));
                Object rightValue = resolveColumnValue(right, columns.get(i));
                int cmp = compareValues(leftValue, rightValue);
                if (cmp != 0) {
                    boolean asc = ascending.size() > i ? ascending.get(i) : true;
                    return asc ? cmp : -cmp;
                }
            }
            return 0;
        };
    }

    private List<com.minisql.common.model.Row> applyLimitOffset(List<com.minisql.common.model.Row> rows,
                                                                int limit,
                                                                int offset) {
        int fromIndex = Math.max(0, offset);
        if (fromIndex >= rows.size()) {
            return new ArrayList<>();
        }
        int toIndex = limit < 0 ? rows.size() : Math.min(rows.size(), fromIndex + limit);
        return new ArrayList<>(rows.subList(fromIndex, toIndex));
    }

    private List<com.minisql.common.model.Row> fetchSourceRows(String tableName,
                                                               byte[] rowKey,
                                                               Condition whereCondition,
                                                               String whereClause,
                                                               List<String> projectedQualifiers) throws SQLException {
        List<RegionLocation> targets = rowKey != null
            ? getTargetRegion(tableName, rowKey)
            : getAllRegionsForTable(tableName);

        if (targets.isEmpty()) {
            throw new SQLException("No regions found for table: " + tableName);
        }

        List<Future<List<com.minisql.common.model.Row>>> futures = new ArrayList<>();
        for (RegionLocation location : targets) {
            futures.add(executor.submit(() -> fetchRowsFromRegion(location, whereCondition, whereClause, projectedQualifiers)));
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
                                                                   List<String> projectedQualifiers) throws SQLException {
        ManagedChannel channel = ManagedChannelBuilder
            .forAddress(location.serverHost, location.serverPort)
            .usePlaintext()
            .build();

        try {
            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                RegionServerServiceGrpc.newBlockingStub(channel);
            Table schema = getTableSchema(location.tableName);
            List<KeyValue> keyValues = scanKeyValues(stub, location.regionId, location.tableName, whereClause, projectedQualifiers);
            List<com.minisql.common.model.Row> rows = RowAssembler.assemble(keyValues, schema);
            return whereCondition != null ? filterRows(rows, whereCondition) : rows;
        } finally {
            channel.shutdown();
        }
    }

    private List<KeyValue> scanKeyValues(RegionServerServiceGrpc.RegionServerServiceBlockingStub stub,
                                         String regionId,
                                         String tableName,
                                         String whereClause,
                                         List<String> projectedQualifiers) throws SQLException {
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

    private boolean canPushDownCondition(Condition condition) {
        if (condition == null) {
            return false;
        }
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

    // ── AST helper methods ──────────────────────────────────────────────

    /**
     * Serialize a Condition AST back to a SQL WHERE clause for pushdown.
     */
    private String conditionToSql(Condition condition) {
        if (condition == null) {
            return null;
        }
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

    /**
     * Extract left/right column pairs from a JOIN ON condition tree.
     */
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
        if (dot < 0) return true; // unqualified → belongs to any table
        return column.substring(0, dot).equalsIgnoreCase(table);
    }

    /**
     * Determine column qualifiers to project when there is no JOIN.
     */
    private List<String> determineProjectedQualifiersFromAst(SelectStatement ast, boolean hasAggregation) {
        if (ast.isSelectAll() || hasAggregation) {
            return null;
        }
        List<String> columns = new ArrayList<>();
        if (ast.getColumns() != null) {
            for (String col : ast.getColumns()) {
                String qualifier = toSimpleQualifier(col);
                if (qualifier != null && !columns.contains(qualifier)) {
                    columns.add(qualifier);
                }
            }
        }
        if (ast.getOrderBy() != null) {
            for (SelectStatement.OrderByElement elem : ast.getOrderBy()) {
                String qualifier = toSimpleQualifier(elem.getColumn());
                if (qualifier != null && !columns.contains(qualifier)) {
                    columns.add(qualifier);
                }
            }
        }
        collectConditionColumns(ast.getWhere(), columns);
        return columns.isEmpty() ? null : columns;
    }

    /**
     * Determine column qualifiers to project for one side of a JOIN.
     */
    private List<String> determineJoinProjectedQualifiersFromAst(SelectStatement ast,
                                                                  String tableName,
                                                                  String tableAlias,
                                                                  List<String> joinColumns,
                                                                  List<AggregateExpression> aggregates) {
        List<String> columns = new ArrayList<>();
        addJoinColumns(columns, joinColumns);
        // SELECT columns belonging to this table
        if (!ast.isSelectAll() && ast.getColumns() != null) {
            for (String col : ast.getColumns()) {
                String qualifier = qualifyForSource(col, tableName, tableAlias);
                if (qualifier != null && !columns.contains(qualifier)) {
                    columns.add(qualifier);
                }
            }
        }
        // ORDER BY
        if (ast.getOrderBy() != null) {
            for (SelectStatement.OrderByElement elem : ast.getOrderBy()) {
                String qualifier = qualifyForSource(elem.getColumn(), tableName, tableAlias);
                if (qualifier != null && !columns.contains(qualifier)) {
                    columns.add(qualifier);
                }
            }
        }
        // GROUP BY
        if (ast.getGroupByColumns() != null) {
            for (String col : ast.getGroupByColumns()) {
                String qualifier = qualifyForSource(col, tableName, tableAlias);
                if (qualifier != null && !columns.contains(qualifier)) {
                    columns.add(qualifier);
                }
            }
        }
        // WHERE
        collectConditionColumnsForSource(ast.getWhere(), tableName, tableAlias, columns);
        // HAVING
        collectConditionColumnsForSource(ast.getHaving(), tableName, tableAlias, columns);
        // Aggregate columns
        for (AggregateExpression expr : aggregates) {
            String qualifier = qualifyForSource(expr.column, tableName, tableAlias);
            if (qualifier != null && !columns.contains(qualifier)) {
                columns.add(qualifier);
            }
        }
        return columns.isEmpty() ? null : columns;
    }

    /**
     * Execute JOIN using AST fields.
     */
    private List<com.minisql.common.model.Row> executeJoinFromAst(
            SelectStatement ast, List<com.minisql.common.model.Row> leftRows) throws SQLException {

        String leftTable = ast.getTable();
        String leftAlias = ast.getTableAlias();
        String rightTable = ast.getJoinTable();
        String rightAlias = ast.getJoinTableAlias();
        JoinType joinType = ast.getJoinType() != null ? ast.getJoinType() : JoinType.INNER;

        // Extract join columns
        List<String> leftJoinCols = new ArrayList<>();
        List<String> rightJoinCols = new ArrayList<>();
        String leftQualifier = leftAlias != null ? leftAlias : leftTable;
        String rightQualifier = rightAlias != null ? rightAlias : rightTable;
        extractJoinConditionColumns(ast.getJoinCondition(), leftJoinCols, rightJoinCols, leftQualifier, rightQualifier);

        // Determine right-side projected qualifiers
        List<String> rightProjectedQualifiers = determineJoinProjectedQualifiersFromAst(
            ast, rightTable, rightAlias, rightJoinCols,
            ast.getAggregates() != null ? buildAggregateExpressions(ast) : Collections.emptyList());

        List<com.minisql.common.model.Row> rightRows = fetchSourceRows(
            rightTable, null, null, null, rightProjectedQualifiers);
        Table rightSchema = getTableSchema(rightTable);

        // Hash join
        Map<String, List<com.minisql.common.model.Row>> rightBuckets = new LinkedHashMap<>();
        for (com.minisql.common.model.Row rightRow : rightRows) {
            String bucketKey = buildJoinKey(rightRow, rightJoinCols);
            if (bucketKey != null) {
                rightBuckets.computeIfAbsent(bucketKey, ignored -> new ArrayList<>()).add(rightRow);
            }
        }

        boolean isLeftJoin = joinType == JoinType.LEFT;
        List<com.minisql.common.model.Row> joinedRows = new ArrayList<>();
        for (com.minisql.common.model.Row leftRow : leftRows) {
            String bucketKey = buildJoinKey(leftRow, leftJoinCols);
            List<com.minisql.common.model.Row> matches = bucketKey != null ? rightBuckets.get(bucketKey) : null;
            if (matches == null) {
                if (isLeftJoin) {
                    joinedRows.add(mergeLogicalRows(leftRow, leftQualifier, null, rightQualifier, rightSchema));
                }
                continue;
            }
            for (com.minisql.common.model.Row rightRow : matches) {
                joinedRows.add(mergeLogicalRows(leftRow, leftQualifier, rightRow, rightQualifier, rightSchema));
            }
        }
        return joinedRows;
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

    /**
     * Aggregation using explicit parameters from AST.
     */
    private List<com.minisql.common.model.Row> aggregateRowsFromAst(
            List<String> groupByColumns,
            List<AggregateExpression> aggregateExpressions,
            List<com.minisql.common.model.Row> sourceRows) {
        Map<List<Object>, AggregateAccumulator> groups = new LinkedHashMap<>();
        for (com.minisql.common.model.Row sourceRow : sourceRows) {
            List<Object> groupKey = new ArrayList<>();
            if (groupByColumns != null) {
                for (String column : groupByColumns) {
                    groupKey.add(resolveColumnValue(sourceRow, column));
                }
            }
            AggregateAccumulator accumulator = groups.computeIfAbsent(
                groupKey, key -> new AggregateAccumulator(aggregateExpressions));
            accumulator.addRow(sourceRow);
        }

        if (groups.isEmpty() && (groupByColumns == null || groupByColumns.isEmpty())) {
            groups.put(Collections.emptyList(), new AggregateAccumulator(aggregateExpressions));
        }

        List<com.minisql.common.model.Row> aggregatedRows = new ArrayList<>();
        for (Map.Entry<List<Object>, AggregateAccumulator> entry : groups.entrySet()) {
            com.minisql.common.model.Row row = new com.minisql.common.model.Row();
            if (groupByColumns != null) {
                for (int i = 0; i < groupByColumns.size(); i++) {
                    row.setColumn(groupByColumns.get(i), entry.getKey().get(i));
                }
            }
            entry.getValue().appendResults(row);
            aggregatedRows.add(row);
        }
        return aggregatedRows;
    }

    /**
     * Project rows using AST fields instead of QuerySpec.
     */
    private List<Row> projectRowsFromAst(SelectStatement ast,
                                         List<com.minisql.common.model.Row> rows,
                                         boolean hasAggregation) {
        List<Row> projected = new ArrayList<>();
        for (com.minisql.common.model.Row source : rows) {
            Row row = new Row();
            if (ast.isSelectAll() && !hasAggregation) {
                for (String columnName : source.getColumnNames()) {
                    row.addColumn(columnName, source.getColumn(columnName));
                }
            } else if (hasAggregation) {
                // For aggregation, output columns = groupBy columns + aggregate output names
                List<String> groupByColumns = ast.getGroupByColumns();
                if (groupByColumns != null) {
                    for (String col : groupByColumns) {
                        row.addColumn(col, resolveColumnValue(source, col));
                    }
                }
                if (ast.getAggregates() != null) {
                    for (SelectStatement.AggregateExpr agg : ast.getAggregates()) {
                        row.addColumn(agg.getOutputName(), resolveColumnValue(source, agg.getOutputName()));
                    }
                }
            } else {
                // Explicit column list
                List<String> columns = ast.getColumns();
                List<String> aliases = ast.getColumnAliases();
                if (columns != null) {
                    for (int i = 0; i < columns.size(); i++) {
                        String col = columns.get(i);
                        String outputName = (aliases != null && i < aliases.size() && aliases.get(i) != null)
                            ? aliases.get(i) : col;
                        row.addColumn(outputName, resolveColumnValue(source, col));
                    }
                }
            }
            projected.add(row);
        }
        return projected;
    }

    private void collectConditionColumnsForSource(Condition condition,
                                                  String tableName,
                                                  String tableAlias,
                                                  List<String> target) {
        if (condition == null) {
            return;
        }
        if (condition instanceof SimpleCondition) {
            String qualifier = qualifyForSource(((SimpleCondition) condition).getColumn(), tableName, tableAlias);
            if (qualifier != null && !target.contains(qualifier)) {
                target.add(qualifier);
            }
            return;
        }
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            collectConditionColumnsForSource(compound.getLeft(), tableName, tableAlias, target);
            collectConditionColumnsForSource(compound.getRight(), tableName, tableAlias, target);
        }
    }

    private void addJoinColumns(List<String> target, List<String> joinColumns) {
        if (joinColumns == null) {
            return;
        }
        for (String column : joinColumns) {
            String qualifier = toSimpleQualifier(column);
            if (qualifier != null && !target.contains(qualifier)) {
                target.add(qualifier);
            }
        }
    }

    private String qualifyForSource(String column, String tableName, String tableAlias) {
        if (column == null || column.isBlank()) {
            return null;
        }
        String trimmed = column.trim();
        if ("*".equals(trimmed) || trimmed.contains("(")) {
            return null;
        }
        int qualifierIndex = trimmed.indexOf('.');
        if (qualifierIndex < 0) {
            return toSimpleQualifier(trimmed);
        }

        String qualifier = trimmed.substring(0, qualifierIndex);
        String unqualified = trimmed.substring(qualifierIndex + 1);
        if (qualifier.equalsIgnoreCase(tableName) || (tableAlias != null && qualifier.equalsIgnoreCase(tableAlias))) {
            return toSimpleQualifier(unqualified);
        }
        return null;
    }

    private void collectConditionColumns(Condition condition, List<String> target) {
        if (condition == null) {
            return;
        }
        if (condition instanceof SimpleCondition) {
            String qualifier = toSimpleQualifier(((SimpleCondition) condition).getColumn());
            if (qualifier != null && !target.contains(qualifier)) {
                target.add(qualifier);
            }
            return;
        }
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            collectConditionColumns(compound.getLeft(), target);
            collectConditionColumns(compound.getRight(), target);
        }
    }

    private String toSimpleQualifier(String column) {
        if (column == null || column.isBlank()) {
            return null;
        }
        String trimmed = column.trim();
        if ("*".equals(trimmed) || trimmed.contains("(")) {
            return null;
        }
        int qualifierIndex = trimmed.indexOf('.');
        return qualifierIndex >= 0 ? trimmed.substring(qualifierIndex + 1) : trimmed;
    }

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

    private List<com.minisql.common.model.Row> filterRows(List<com.minisql.common.model.Row> rows, Condition condition) {
        if (rows.isEmpty()) {
            return rows;
        }

        List<com.minisql.common.model.Row> filtered = new ArrayList<>();
        for (com.minisql.common.model.Row row : rows) {
            Row evalRow = new Row();
            for (String columnName : row.getColumnNames()) {
                evalRow.addColumn(columnName, row.getColumn(columnName));
            }
            if (condition.evaluate(evalRow)) {
                filtered.add(row);
            }
        }
        return filtered;
    }

    private Object resolveColumnValue(com.minisql.common.model.Row row, String columnName) {
        if (row == null || columnName == null) {
            return null;
        }

        if (row.hasColumn(columnName)) {
            return row.getColumn(columnName);
        }

        int qualifierIndex = columnName.indexOf('.');
        if (qualifierIndex >= 0) {
            String unqualified = columnName.substring(qualifierIndex + 1);
            if (row.hasColumn(unqualified)) {
                return row.getColumn(unqualified);
            }
        }

        for (String existing : row.getColumnNames()) {
            if (existing.equalsIgnoreCase(columnName)) {
                return row.getColumn(existing);
            }
            int existingQualifier = existing.indexOf('.');
            if (existingQualifier >= 0 && existing.substring(existingQualifier + 1).equalsIgnoreCase(columnName)) {
                return row.getColumn(existing);
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private int compareValues(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left instanceof Comparable && left.getClass().isInstance(right)) {
            return ((Comparable<Object>) left).compareTo(right);
        }
        return left.toString().compareTo(right.toString());
    }

    private com.minisql.common.model.Row mergeLogicalRows(com.minisql.common.model.Row leftRow,
                                                          String leftTable,
                                                          com.minisql.common.model.Row rightRow,
                                                          String rightTable,
                                                          Table rightSchema) {
        com.minisql.common.model.Row merged = new com.minisql.common.model.Row();
        for (String columnName : leftRow.getColumnNames()) {
            Object value = leftRow.getColumn(columnName);
            merged.setColumn(columnName, value);
            merged.setColumn(leftTable + "." + columnName, value);
        }
        if (rightRow != null) {
            for (String columnName : rightRow.getColumnNames()) {
                Object value = rightRow.getColumn(columnName);
                if (merged.hasColumn(columnName)) {
                    merged.setColumn(rightTable + "." + columnName, value);
                } else {
                    merged.setColumn(columnName, value);
                }
                merged.setColumn(rightTable + "." + columnName, value);
            }
        } else if (rightSchema != null && rightSchema.getColumns() != null) {
            for (Column column : rightSchema.getColumns()) {
                String columnName = column.getName();
                if (!merged.hasColumn(columnName)) {
                    merged.setColumn(columnName, null);
                }
                merged.setColumn(rightTable + "." + columnName, null);
            }
        }
        return merged;
    }

    private List<Row> mergeAndSort(List<List<Row>> allResults,
                                   List<String> orderByColumns,
                                   List<Boolean> ascending,
                                   int limit,
                                   int offset) {
        List<Row> allRows = new ArrayList<>();
        for (List<Row> rows : allResults) {
            allRows.addAll(rows);
        }

        if (orderByColumns != null && !orderByColumns.isEmpty()) {
            allRows.sort(ResultSetMerger.createComparator(orderByColumns, ascending));
        }

        int fromIndex = Math.max(0, offset);
        int toIndex = limit < 0 ? allRows.size() : Math.min(allRows.size(), fromIndex + limit);
        if (fromIndex >= allRows.size()) {
            return new ArrayList<>();
        }
        return new ArrayList<>(allRows.subList(fromIndex, toIndex));
    }

    private List<RegionLocation> getTargetRegion(String tableName, byte[] rowKey) {
        try {
            MasterProto.GetLocationRequest request = MasterProto.GetLocationRequest.newBuilder()
                .setTableName(tableName)
                .setRowKey(ByteString.copyFrom(rowKey))
                .build();

            MasterProto.GetLocationResponse response = masterStub.getRegionLocation(request);
            if (response.getStatus().getSuccess()) {
                List<RegionLocation> locations = new ArrayList<>();
                locations.add(RegionLocation.fromProto(response));
                return locations;
            }
            logger.warn("Failed to get region location for table " + tableName +
                ": " + response.getStatus().getMessage());
        } catch (Exception e) {
            logger.warn("Exception while getting region location for table: {}", tableName, e);
        }
        return new ArrayList<>();
    }

    private List<RegionLocation> getAllRegionsForTable(String tableName) {
        try {
            MasterProto.GetTableRegionsResponse response = masterStub.getTableRegions(
                MasterProto.GetTableRegionsRequest.newBuilder().setTableName(tableName).build()
            );

            if (!response.getStatus().getSuccess()) {
                logger.warn("Failed to get regions for table {}: {}",
                    tableName, response.getStatus().getMessage());
                return new ArrayList<>();
            }

            List<RegionLocation> locations = new ArrayList<>();
            for (CommonProto.RegionInfo regionInfo : response.getRegionsList()) {
                RegionLocation location = new RegionLocation();
                location.regionId = regionInfo.getRegionId();
                location.tableName = regionInfo.getTableName();


                if (regionInfo.hasPrimary()) {
                    location.serverHost = regionInfo.getPrimary().getHost();
                    location.serverPort = regionInfo.getPrimary().getPort();
                } else if (regionInfo.getReplicasCount() > 0) {
                    CommonProto.ServerId replica = regionInfo.getReplicas(0);
                    location.serverHost = replica.getHost();
                    location.serverPort = replica.getPort();
                } else {
                    logger.warn("Skipping region {}: no server address available", location.regionId);
                    continue;
                }

                locations.add(location);
            }
            return locations;
        } catch (Exception e) {
            logger.warn("Exception while getting regions for table: {}", tableName, e);
            return new ArrayList<>();
        }
    }

    public void shutdown() {
        executor.shutdown();
        for (HikariDataSource ds : connectionPools.values()) {
            ds.close();
        }
    }

    public static class RegionLocation {
        private String regionId;
        private String tableName;
        private String serverHost;
        private int serverPort;

        public static RegionLocation fromProto(MasterProto.GetLocationResponse response) {
            RegionLocation location = new RegionLocation();
            location.regionId = response.getRegion().getRegionId();
            location.tableName = response.getRegion().getTableName();
            location.serverHost = response.getServerId().getHost();
            location.serverPort = response.getServerId().getPort();

            return location;
        }

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

    private static class AggregateAccumulator {
        private final List<AggregateValue> values;

        private AggregateAccumulator(List<AggregateExpression> expressions) {
            this.values = new ArrayList<>();
            for (AggregateExpression expression : expressions) {
                values.add(new AggregateValue(expression));
            }
        }

        private void addRow(com.minisql.common.model.Row row) {
            for (AggregateValue value : values) {
                value.addRow(row);
            }
        }

        private void appendResults(com.minisql.common.model.Row row) {
            for (AggregateValue value : values) {
                row.setColumn(value.expression.outputName, value.getResult());
            }
        }
    }

    private static class AggregateValue {
        private final AggregateExpression expression;
        private long count;
        private double sum;
        private Object max;
        private Object min;

        private AggregateValue(AggregateExpression expression) {
            this.expression = expression;
        }

        private void addRow(com.minisql.common.model.Row row) {
            Object value = "*".equals(expression.column) ? null : resolveStaticColumnValue(row, expression.column);
            switch (expression.function) {
                case "COUNT":
                    count++;
                    break;
                case "SUM":
                    if (value instanceof Number) {
                        sum += ((Number) value).doubleValue();
                    }
                    break;
                case "AVG":
                    if (value instanceof Number) {
                        sum += ((Number) value).doubleValue();
                        count++;
                    }
                    break;
                case "MAX":
                    if (value != null && (max == null || compareStatic(value, max) > 0)) {
                        max = value;
                    }
                    break;
                case "MIN":
                    if (value != null && (min == null || compareStatic(value, min) < 0)) {
                        min = value;
                    }
                    break;
                default:
                    break;
            }
        }

        private static Object resolveStaticColumnValue(com.minisql.common.model.Row row, String columnName) {
            if (row.hasColumn(columnName)) {
                return row.getColumn(columnName);
            }
            int qualifierIndex = columnName.indexOf('.');
            if (qualifierIndex >= 0) {
                String unqualified = columnName.substring(qualifierIndex + 1);
                if (row.hasColumn(unqualified)) {
                    return row.getColumn(unqualified);
                }
            }
            for (String existing : row.getColumnNames()) {
                if (existing.equalsIgnoreCase(columnName)) {
                    return row.getColumn(existing);
                }
            }
            return null;
        }

        private Object getResult() {
            switch (expression.function) {
                case "COUNT":
                    return count;
                case "SUM":
                    return sum;
                case "AVG":
                    return count == 0 ? null : sum / count;
                case "MAX":
                    return max;
                case "MIN":
                    return min;
                default:
                    return null;
            }
        }

        @SuppressWarnings("unchecked")
        private static int compareStatic(Object left, Object right) {
            if (left == null && right == null) {
                return 0;
            }
            if (left == null) {
                return -1;
            }
            if (right == null) {
                return 1;
            }
            if (left instanceof Comparable && left.getClass().isInstance(right)) {
                return ((Comparable<Object>) left).compareTo(right);
            }
            return left.toString().compareTo(right.toString());
        }
    }
}
