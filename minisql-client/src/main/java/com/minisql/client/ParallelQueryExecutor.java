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
import com.minisql.sql.SQLParser;
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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public List<Row> executeQuery(String sql, String tableName, byte[] rowKey) throws SQLException {
        QuerySpec spec = parseQuerySpec(sql);
        return executeQuerySpec(spec, rowKey);
    }

    public List<Row> executeQueryWithOrder(String sql, String tableName,
                                           List<String> orderByColumns,
                                           List<Boolean> ascending,
                                           int limit, int offset) throws SQLException {
        List<Row> rows = executeQuery(sql, tableName, null);
        List<List<Row>> wrapped = new ArrayList<>();
        wrapped.add(rows);
        return mergeAndSort(wrapped, orderByColumns, ascending, limit, offset);
    }

    public List<Row> executeAggregationQuery(String sql, String tableName,
                                             List<String> groupByColumns,
                                             List<ResultSetMerger.AggregateFunction> aggregateFunctions)
        throws SQLException {
        QuerySpec spec = parseQuerySpec(sql);
        return executeQuerySpec(spec, null);
    }

    private List<Row> executeQuerySpec(QuerySpec spec, byte[] rowKey) throws SQLException {
        List<com.minisql.common.model.Row> rows = fetchBaseRows(spec, rowKey);

        if (spec.joinSpec != null) {
            rows = executeJoin(spec, rows);
        }

        if (spec.whereCondition != null) {
            rows = filterRows(rows, spec.whereCondition);
        }

        if (spec.hasAggregation()) {
            rows = aggregateRows(spec, rows);
            if (spec.havingCondition != null) {
                rows = filterRows(rows, spec.havingCondition);
            }
        }

        if (!spec.orderByColumns.isEmpty()) {
            rows.sort(createSortComparator(spec.orderByColumns, spec.orderAscending));
        }

        rows = applyLimitOffset(rows, spec.limit, spec.offset);
        return projectRows(spec, rows);
    }

    private List<com.minisql.common.model.Row> fetchBaseRows(QuerySpec spec, byte[] rowKey) throws SQLException {
        if (spec.joinSpec != null) {
            List<String> leftProjectedQualifiers = determineJoinProjectedQualifiers(
                spec,
                spec.tableName,
                spec.tableAlias,
                spec.joinSpec.leftConditions
            );
            return fetchSourceRows(spec.tableName, null, null, null, leftProjectedQualifiers);
        }
        String pushdownWhereClause = canPushDownCondition(spec.whereCondition) ? spec.whereClause : null;
        List<String> projectedQualifiers = determineProjectedQualifiers(spec);
        return fetchSourceRows(spec.tableName, rowKey, spec.whereCondition, pushdownWhereClause, projectedQualifiers);
    }

    private List<com.minisql.common.model.Row> executeJoin(QuerySpec spec,
                                                           List<com.minisql.common.model.Row> leftRows) throws SQLException {
        List<String> rightProjectedQualifiers = determineJoinProjectedQualifiers(
            spec,
            spec.joinSpec.rightTable,
            spec.joinSpec.rightAlias,
            spec.joinSpec.rightConditions
        );
        List<com.minisql.common.model.Row> rightRows = fetchSourceRows(
            spec.joinSpec.rightTable,
            null,
            null,
            null,
            rightProjectedQualifiers
        );
        Table rightSchema = getTableSchema(spec.joinSpec.rightTable);
        String leftQualifier = spec.tableAlias != null ? spec.tableAlias : spec.tableName;
        String rightQualifier = spec.joinSpec.rightAlias != null ? spec.joinSpec.rightAlias : spec.joinSpec.rightTable;
        Map<String, List<com.minisql.common.model.Row>> rightBuckets = new LinkedHashMap<>();

        for (com.minisql.common.model.Row rightRow : rightRows) {
            String bucketKey = buildJoinKey(rightRow, spec.joinSpec.rightConditions);
            if (bucketKey == null) {
                continue;
            }
            rightBuckets.computeIfAbsent(bucketKey, ignored -> new ArrayList<>()).add(rightRow);
        }

        List<com.minisql.common.model.Row> joinedRows = new ArrayList<>();
        for (com.minisql.common.model.Row leftRow : leftRows) {
            String bucketKey = buildJoinKey(leftRow, spec.joinSpec.leftConditions);
            if (bucketKey == null) {
                continue;
            }

            List<com.minisql.common.model.Row> matches = rightBuckets.get(bucketKey);
            if (matches == null) {
                if (spec.joinSpec.isLeftJoin()) {
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

    private List<com.minisql.common.model.Row> aggregateRows(QuerySpec spec,
                                                             List<com.minisql.common.model.Row> sourceRows) {
        Map<List<Object>, AggregateAccumulator> groups = new LinkedHashMap<>();
        for (com.minisql.common.model.Row sourceRow : sourceRows) {
            List<Object> groupKey = new ArrayList<>();
            for (String column : spec.groupByColumns) {
                groupKey.add(resolveColumnValue(sourceRow, column));
            }

            AggregateAccumulator accumulator = groups.computeIfAbsent(
                groupKey,
                key -> new AggregateAccumulator(spec.aggregateExpressions)
            );
            accumulator.addRow(sourceRow);
        }

        if (groups.isEmpty() && spec.groupByColumns.isEmpty()) {
            groups.put(Collections.emptyList(), new AggregateAccumulator(spec.aggregateExpressions));
        }

        List<com.minisql.common.model.Row> aggregatedRows = new ArrayList<>();
        for (Map.Entry<List<Object>, AggregateAccumulator> entry : groups.entrySet()) {
            com.minisql.common.model.Row row = new com.minisql.common.model.Row();
            for (int i = 0; i < spec.groupByColumns.size(); i++) {
                row.setColumn(spec.groupByColumns.get(i), entry.getKey().get(i));
            }
            entry.getValue().appendResults(row);
            aggregatedRows.add(row);
        }
        return aggregatedRows;
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

    private List<Row> projectRows(QuerySpec spec, List<com.minisql.common.model.Row> rows) {
        List<Row> projected = new ArrayList<>();
        for (com.minisql.common.model.Row source : rows) {
            Row row = new Row();
            if (spec.selectAll && !spec.hasAggregation()) {
                for (String columnName : source.getColumnNames()) {
                    row.addColumn(columnName, source.getColumn(columnName));
                }
            } else {
                for (SelectItem item : spec.selectItems) {
                    String outputName = item.outputName();
                    Object value = item.aggregate
                        ? resolveColumnValue(source, outputName)
                        : resolveColumnValue(source, item.expression);
                    row.addColumn(outputName, value);
                }
            }
            projected.add(row);
        }
        return projected;
    }

    private List<Row> executeSingle(RegionLocation location, String sql) throws SQLException {
        ManagedChannel channel = ManagedChannelBuilder
            .forAddress(location.serverHost, location.serverPort)
            .usePlaintext()
            .build();

        try {
            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                RegionServerServiceGrpc.newBlockingStub(channel);
            return scanAndEvaluate(location, stub, sql);
        } finally {
            channel.shutdown();
        }
    }

    private List<Row> executeParallel(List<RegionLocation> locations, String sql) throws SQLException {
        List<Future<List<Row>>> futures = new ArrayList<>();
        for (RegionLocation location : locations) {
            futures.add(executor.submit(() -> executeSingle(location, sql)));
        }

        List<Row> allRows = new ArrayList<>();
        try {
            for (Future<List<Row>> future : futures) {
                allRows.addAll(future.get(queryTimeoutSeconds, TimeUnit.SECONDS));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Query execution interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new SQLException("Query execution failed: " + e.getMessage(), e);
        }
        return allRows;
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

    private List<Row> scanAndEvaluate(RegionLocation location,
                                      RegionServerServiceGrpc.RegionServerServiceBlockingStub stub,
                                      String sql) throws SQLException {
        SelectStatement select = parseSelect(sql);

        Table schema = getTableSchema(location.tableName);
        List<KeyValue> keyValues = scanKeyValues(stub, location.regionId, location.tableName, null, null);
        List<com.minisql.common.model.Row> sourceRows = RowAssembler.assemble(keyValues, schema);

        if (select.getWhere() != null) {
            sourceRows = filterRows(sourceRows, select.getWhere());
        }
        if (select.getOrderBy() != null && !select.getOrderBy().isEmpty()) {
            sourceRows.sort(createComparator(select.getOrderBy()));
        }

        int offset = select.getOffset() != null ? Math.max(0, select.getOffset()) : 0;
        int limit = select.getLimit() != null ? select.getLimit() : -1;
        List<String> resultColumns = determineResultColumns(select, schema);

        List<Row> rows = new ArrayList<>();
        for (int i = offset; i < sourceRows.size(); i++) {
            if (limit >= 0 && rows.size() >= limit) {
                break;
            }

            com.minisql.common.model.Row sourceRow = sourceRows.get(i);
            Row row = new Row();
            for (String columnName : resultColumns) {
                row.addColumn(columnName, sourceRow.getColumn(columnName));
            }
            rows.add(row);
        }
        return rows;
    }

    private List<Row> executeJoinQuery(SelectStatement select) throws SQLException {
        JoinSpec joinSpec = parseJoinSpec(select.getJoinCondition());

        List<com.minisql.common.model.Row> leftRows = fetchSourceRows(select.getTable(), null, null, null, null);
        List<com.minisql.common.model.Row> rightRows = fetchSourceRows(select.getJoinTable(), null, null, null, null);

        Map<String, List<com.minisql.common.model.Row>> rightBuckets = new LinkedHashMap<>();
        for (com.minisql.common.model.Row rightRow : rightRows) {
            Object joinValue = rightRow.getColumn(joinSpec.rightColumn);
            if (joinValue == null) {
                continue;
            }
            rightBuckets.computeIfAbsent(String.valueOf(joinValue), ignored -> new ArrayList<>()).add(rightRow);
        }

        List<Row> joinedRows = new ArrayList<>();
        for (com.minisql.common.model.Row leftRow : leftRows) {
            Object joinValue = leftRow.getColumn(joinSpec.leftColumn);
            if (joinValue == null) {
                continue;
            }

            List<com.minisql.common.model.Row> candidates = rightBuckets.get(String.valueOf(joinValue));
            if (candidates == null) {
                continue;
            }

            for (com.minisql.common.model.Row rightRow : candidates) {
                Row joined = mergeRows(leftRow, select.getTable(), rightRow, select.getJoinTable());
                if (select.getWhere() == null || evaluateRow(joined, select.getWhere())) {
                    joinedRows.add(joined);
                }
            }
        }

        if (select.getOrderBy() != null && !select.getOrderBy().isEmpty()) {
            joinedRows.sort(ResultSetMerger.createComparator(
                extractOrderByColumns(select.getOrderBy()),
                extractOrderDirections(select.getOrderBy())
            ));
        }

        int offset = select.getOffset() != null ? Math.max(0, select.getOffset()) : 0;
        int limit = select.getLimit() != null ? select.getLimit() : -1;
        List<String> resultColumns = determineJoinResultColumns(select, joinedRows);

        List<Row> results = new ArrayList<>();
        for (int i = offset; i < joinedRows.size(); i++) {
            if (limit >= 0 && results.size() >= limit) {
                break;
            }

            Row source = joinedRows.get(i);
            if (select.isSelectAll()) {
                results.add(source);
                continue;
            }

            Row projected = new Row();
            for (String column : resultColumns) {
                projected.addColumn(column, source.getColumnValue(column));
            }
            results.add(projected);
        }
        return results;
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

    private List<String> determineProjectedQualifiers(QuerySpec spec) {
        if (spec == null || spec.selectAll || spec.joinSpec != null || spec.hasAggregation()) {
            return null;
        }

        List<String> columns = new ArrayList<>();
        addSimpleColumns(columns, spec.selectItems);
        addSimpleColumns(columns, spec.orderByColumns);
        collectConditionColumns(spec.whereCondition, columns);
        return columns.isEmpty() ? null : columns;
    }

    private List<String> determineJoinProjectedQualifiers(QuerySpec spec,
                                                          String tableName,
                                                          String tableAlias,
                                                          List<String> joinColumns) {
        if (spec == null || tableName == null) {
            return null;
        }

        List<String> columns = new ArrayList<>();
        addJoinColumns(columns, joinColumns);
        collectSelectColumnsForSource(spec.selectItems, tableName, tableAlias, columns);
        collectStringColumnsForSource(spec.orderByColumns, tableName, tableAlias, columns);
        collectStringColumnsForSource(spec.groupByColumns, tableName, tableAlias, columns);
        collectConditionColumnsForSource(spec.whereCondition, tableName, tableAlias, columns);
        collectConditionColumnsForSource(spec.havingCondition, tableName, tableAlias, columns);
        for (AggregateExpression expression : spec.aggregateExpressions) {
            String qualifier = qualifyForSource(expression.column, tableName, tableAlias);
            if (qualifier != null && !columns.contains(qualifier)) {
                columns.add(qualifier);
            }
        }
        return columns.isEmpty() ? null : columns;
    }

    private void addSimpleColumns(List<String> target, List<?> items) {
        if (items == null) {
            return;
        }
        for (Object item : items) {
            String column = null;
            if (item instanceof SelectItem) {
                SelectItem selectItem = (SelectItem) item;
                if (!selectItem.aggregate) {
                    column = selectItem.expression;
                }
            } else if (item instanceof String) {
                column = (String) item;
            }
            String qualifier = toSimpleQualifier(column);
            if (qualifier != null && !target.contains(qualifier)) {
                target.add(qualifier);
            }
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

    private void collectSelectColumnsForSource(List<SelectItem> items,
                                               String tableName,
                                               String tableAlias,
                                               List<String> target) {
        if (items == null) {
            return;
        }
        for (SelectItem item : items) {
            if (item.aggregate) {
                continue;
            }
            String qualifier = qualifyForSource(item.expression, tableName, tableAlias);
            if (qualifier != null && !target.contains(qualifier)) {
                target.add(qualifier);
            }
        }
    }

    private void collectStringColumnsForSource(List<String> items,
                                               String tableName,
                                               String tableAlias,
                                               List<String> target) {
        if (items == null) {
            return;
        }
        for (String item : items) {
            String qualifier = qualifyForSource(item, tableName, tableAlias);
            if (qualifier != null && !target.contains(qualifier)) {
                target.add(qualifier);
            }
        }
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

    private SelectStatement parseSelect(String sql) throws SQLException {
        try {
            return (SelectStatement) new SQLParser(sql).parse();
        } catch (Exception e) {
            throw new SQLException("Failed to parse SELECT statement", e);
        }
    }

    private QuerySpec parseQuerySpec(String sql) throws SQLException {
        String normalized = normalizeSql(sql);
        String upper = normalized.toUpperCase();
        if (!upper.startsWith("SELECT ")) {
            throw new SQLException("Only SELECT queries are supported");
        }

        int fromIndex = upper.indexOf(" FROM ");
        if (fromIndex < 0) {
            throw new SQLException("SELECT query is missing FROM");
        }

        int whereIndex = upper.indexOf(" WHERE ", fromIndex);
        int groupByIndex = upper.indexOf(" GROUP BY ", fromIndex);
        int havingIndex = upper.indexOf(" HAVING ", fromIndex);
        int orderByIndex = upper.indexOf(" ORDER BY ", fromIndex);
        int limitIndex = upper.indexOf(" LIMIT ", fromIndex);
        int offsetIndex = upper.indexOf(" OFFSET ", fromIndex);

        QuerySpec spec = new QuerySpec();
        String selectPart = normalized.substring("SELECT ".length(), fromIndex).trim();
        int fromEnd = firstPositive(whereIndex, groupByIndex, havingIndex, orderByIndex, limitIndex, offsetIndex, normalized.length());
        String fromPart = normalized.substring(fromIndex + " FROM ".length(), fromEnd).trim();

        spec.selectItems = parseSelectItems(selectPart);
        spec.selectAll = spec.selectItems.size() == 1 && "*".equals(spec.selectItems.get(0).expression);

        parseFromPart(spec, fromPart);

        String whereClause = extractClause(normalized, whereIndex, " WHERE ", groupByIndex, havingIndex, orderByIndex, limitIndex, offsetIndex);
        String groupByClause = extractClause(normalized, groupByIndex, " GROUP BY ", havingIndex, orderByIndex, limitIndex, offsetIndex);
        String havingClause = extractClause(normalized, havingIndex, " HAVING ", orderByIndex, limitIndex, offsetIndex);
        String orderByClause = extractClause(normalized, orderByIndex, " ORDER BY ", limitIndex, offsetIndex);
        String limitClause = extractClause(normalized, limitIndex, " LIMIT ", offsetIndex);
        String offsetClause = extractClause(normalized, offsetIndex, " OFFSET ");

        spec.whereClause = whereClause;
        spec.whereCondition = parseClauseCondition(spec.tableName, whereClause);
        spec.groupByColumns = splitCommaSeparated(groupByClause);
        spec.orderByColumns = new ArrayList<>();
        spec.orderAscending = new ArrayList<>();
        spec.limit = limitClause == null || limitClause.isEmpty() ? -1 : Integer.parseInt(limitClause.trim());
        spec.offset = offsetClause == null || offsetClause.isEmpty() ? 0 : Integer.parseInt(offsetClause.trim());

        for (SelectItem item : spec.selectItems) {
            if (item.aggregate) {
                spec.aggregateExpressions.add(item.toAggregateExpression());
            }
        }

        spec.havingCondition = parseHavingCondition(spec, havingClause);

        if (orderByClause != null && !orderByClause.isEmpty()) {
            for (String item : splitCommaSeparated(orderByClause)) {
                String trimmed = item.trim();
                boolean ascending = true;
                String upperItem = trimmed.toUpperCase();
                if (upperItem.endsWith(" DESC")) {
                    ascending = false;
                    trimmed = trimmed.substring(0, trimmed.length() - 5).trim();
                } else if (upperItem.endsWith(" ASC")) {
                    trimmed = trimmed.substring(0, trimmed.length() - 4).trim();
                }
                spec.orderByColumns.add(trimmed);
                spec.orderAscending.add(ascending);
            }
        }

        return spec;
    }

    private String normalizeSql(String sql) {
        String normalized = sql.trim();
        if (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }
        return normalized;
    }

    private void parseFromPart(QuerySpec spec, String fromPart) throws SQLException {
        String upper = fromPart.toUpperCase();
        int leftJoinIndex = upper.indexOf(" LEFT JOIN ");
        int innerJoinIndex = upper.indexOf(" INNER JOIN ");
        int plainJoinIndex = upper.indexOf(" JOIN ");
        int joinIndex = -1;
        JoinType joinType = JoinType.INNER;

        if (leftJoinIndex >= 0) {
            joinIndex = leftJoinIndex;
            joinType = JoinType.LEFT;
        } else if (innerJoinIndex >= 0) {
            joinIndex = innerJoinIndex;
            joinType = JoinType.INNER;
        } else if (plainJoinIndex >= 0) {
            joinIndex = plainJoinIndex;
            joinType = JoinType.INNER;
        }

        if (joinIndex < 0) {
            TableSource source = parseTableSource(fromPart.trim());
            spec.tableName = source.tableName;
            spec.tableAlias = source.alias;
            return;
        }

        TableSource leftSource = parseTableSource(fromPart.substring(0, joinIndex).trim());
        spec.tableName = leftSource.tableName;
        spec.tableAlias = leftSource.alias;
        String joinKeyword = joinType == com.minisql.sql.execution.QueryPlan.JoinType.LEFT ? " LEFT JOIN "
            : (innerJoinIndex >= 0 ? " INNER JOIN " : " JOIN ");
        String joinPart = fromPart.substring(joinIndex + joinKeyword.length()).trim();
        int onIndex = joinPart.toUpperCase().indexOf(" ON ");
        if (onIndex < 0) {
            throw new SQLException("JOIN query is missing ON clause");
        }

        JoinQuerySpec joinSpec = new JoinQuerySpec();
        joinSpec.joinType = joinType.name();
        TableSource rightSource = parseTableSource(joinPart.substring(0, onIndex).trim());
        joinSpec.rightTable = rightSource.tableName;
        joinSpec.rightAlias = rightSource.alias;
        String conditionPart = joinPart.substring(onIndex + " ON ".length()).trim();
        parseJoinConditions(joinSpec, conditionPart);
        spec.joinSpec = joinSpec;
    }

    private TableSource parseTableSource(String sourcePart) throws SQLException {
        String trimmed = sourcePart == null ? "" : sourcePart.trim();
        if (trimmed.isEmpty()) {
            throw new SQLException("Table source cannot be empty");
        }

        Matcher asMatcher = Pattern.compile("(?i)^([A-Za-z_][A-Za-z0-9_]*)\\s+AS\\s+([A-Za-z_][A-Za-z0-9_]*)$").matcher(trimmed);
        if (asMatcher.matches()) {
            return new TableSource(asMatcher.group(1).trim(), asMatcher.group(2).trim());
        }

        String[] parts = trimmed.split("\\s+");
        if (parts.length == 1) {
            return new TableSource(parts[0].trim(), null);
        }
        if (parts.length == 2) {
            return new TableSource(parts[0].trim(), parts[1].trim());
        }

        throw new SQLException("Unsupported table source: " + trimmed);
    }

    private void parseJoinConditions(JoinQuerySpec joinSpec, String conditionPart) throws SQLException {
        List<String> parts = splitByLogicalAnd(conditionPart);
        joinSpec.leftConditions = new ArrayList<>();
        joinSpec.rightConditions = new ArrayList<>();
        for (String part : parts) {
            Matcher matcher = Pattern.compile("(?i)^([A-Za-z_][A-Za-z0-9_\\.]*?)\\s*=\\s*([A-Za-z_][A-Za-z0-9_\\.]*)$").matcher(part.trim());
            if (!matcher.matches()) {
                throw new SQLException("Only equality JOIN conditions are supported: " + part.trim());
            }
            joinSpec.leftConditions.add(matcher.group(1).trim());
            joinSpec.rightConditions.add(matcher.group(2).trim());
        }
    }

    private List<String> splitByLogicalAnd(String input) {
        List<String> parts = new ArrayList<>();
        for (String part : input.split("(?i)\\s+AND\\s+")) {
            if (!part.trim().isEmpty()) {
                parts.add(part.trim());
            }
        }
        return parts;
    }

    private List<SelectItem> parseSelectItems(String selectPart) throws SQLException {
        List<SelectItem> items = new ArrayList<>();
        for (String rawItem : splitCommaSeparated(selectPart)) {
            String item = rawItem.trim();
            if ("*".equals(item)) {
                items.add(new SelectItem("*", null, false));
                continue;
            }

            Matcher aliasMatcher = Pattern.compile("(?i)^(.*?)(?:\\s+AS\\s+)([A-Za-z_][A-Za-z0-9_\\.]*)$").matcher(item);
            String expression = item;
            String alias = null;
            if (aliasMatcher.matches()) {
                expression = aliasMatcher.group(1).trim();
                alias = aliasMatcher.group(2).trim();
            }

            Matcher aggregateMatcher = Pattern.compile("(?i)^(COUNT|SUM|AVG|MAX|MIN)\\s*\\((\\*|[A-Za-z_][A-Za-z0-9_\\.]*)\\)$")
                .matcher(expression);
            if (aggregateMatcher.matches()) {
                items.add(new SelectItem(expression, alias, true));
            } else {
                items.add(new SelectItem(expression, alias, false));
            }
        }

        if (items.isEmpty()) {
            throw new SQLException("SELECT list cannot be empty");
        }
        return items;
    }

    private Condition parseClauseCondition(String tableName, String clause) throws SQLException {
        if (clause == null || clause.isEmpty()) {
            return null;
        }

        String syntheticSql = "SELECT * FROM " + tableName + " WHERE " + clause;
        SelectStatement parsed = parseSelect(syntheticSql);
        return parsed.getWhere();
    }

    private Condition parseHavingCondition(QuerySpec spec, String clause) throws SQLException {
        if (clause == null || clause.isEmpty()) {
            return null;
        }

        try {
            return parseClauseCondition(spec.tableName, clause);
        } catch (SQLException ignored) {
            return parseManualCondition(spec, clause);
        }
    }

    private Condition parseManualCondition(QuerySpec spec, String clause) throws SQLException {
        List<String> andParts = splitByLogicalAnd(clause);
        if (andParts.isEmpty()) {
            throw new SQLException("Condition clause cannot be empty");
        }

        Condition condition = null;
        for (String part : andParts) {
            Condition simple = parseManualSimpleCondition(spec, part);
            condition = condition == null ? simple : new com.minisql.sql.ast.CompoundCondition(condition, simple, "AND");
        }
        return condition;
    }

    private Condition parseManualSimpleCondition(QuerySpec spec, String expression) throws SQLException {
        Matcher matcher = Pattern.compile("^(.+?)\\s*(>=|<=|<>|!=|=|==|>|<|LIKE)\\s*(.+)$", Pattern.CASE_INSENSITIVE)
            .matcher(expression.trim());
        if (!matcher.matches()) {
            throw new SQLException("Unsupported condition expression: " + expression);
        }

        String left = canonicalizeConditionOperand(spec, matcher.group(1).trim());
        String operator = matcher.group(2).trim();
        String right = stripConditionLiteral(matcher.group(3).trim());
        return new com.minisql.sql.ast.SimpleCondition(left, operator, right);
    }

    private String canonicalizeConditionOperand(QuerySpec spec, String operand) {
        if (operand == null) {
            return null;
        }

        String trimmed = operand.trim();
        for (SelectItem item : spec.selectItems) {
            if (item.aggregate && item.expression.equalsIgnoreCase(trimmed)) {
                return item.outputName();
            }
            if (item.alias != null && item.alias.equalsIgnoreCase(trimmed)) {
                return item.outputName();
            }
        }

        for (AggregateExpression expression : spec.aggregateExpressions) {
            if (expression.outputName != null && expression.outputName.equalsIgnoreCase(trimmed)) {
                return expression.outputName;
            }
            String aggregateSignature = expression.function + "(" + expression.column + ")";
            if (aggregateSignature.equalsIgnoreCase(trimmed)) {
                return expression.outputName;
            }
        }

        return trimmed;
    }

    private String stripConditionLiteral(String value) {
        if (value.length() >= 2) {
            if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith("\"") && value.endsWith("\""))) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    private AggregationQuerySpec parseAggregationQuery(String sql) throws SQLException {
        String normalized = sql.trim();
        if (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }

        String upper = normalized.toUpperCase();
        if (!upper.startsWith("SELECT ")) {
            throw new SQLException("Aggregation query must be a SELECT statement");
        }

        int fromIndex = upper.indexOf(" FROM ");
        if (fromIndex < 0) {
            throw new SQLException("Aggregation query is missing FROM");
        }

        String selectPart = normalized.substring("SELECT ".length(), fromIndex).trim();
        int whereIndex = upper.indexOf(" WHERE ", fromIndex);
        int groupByIndex = upper.indexOf(" GROUP BY ", fromIndex);
        int orderByIndex = upper.indexOf(" ORDER BY ", fromIndex);
        int limitIndex = upper.indexOf(" LIMIT ", fromIndex);
        int offsetIndex = upper.indexOf(" OFFSET ", fromIndex);

        int fromEnd = firstPositive(whereIndex, groupByIndex, orderByIndex, limitIndex, offsetIndex, normalized.length());
        String tableName = normalized.substring(fromIndex + " FROM ".length(), fromEnd).trim();
        if (tableName.isEmpty()) {
            throw new SQLException("Aggregation query table name is empty");
        }

        String whereClause = extractClause(normalized, whereIndex, " WHERE ", groupByIndex, orderByIndex, limitIndex, offsetIndex);
        String groupByClause = extractClause(normalized, groupByIndex, " GROUP BY ", orderByIndex, limitIndex, offsetIndex);
        String orderByClause = extractClause(normalized, orderByIndex, " ORDER BY ", limitIndex, offsetIndex);
        String limitClause = extractClause(normalized, limitIndex, " LIMIT ", offsetIndex);
        String offsetClause = extractClause(normalized, offsetIndex, " OFFSET ");

        AggregationQuerySpec spec = new AggregationQuerySpec();
        spec.tableName = tableName;
        spec.aggregateExpressions = new ArrayList<>();
        spec.groupByColumns = splitCommaSeparated(groupByClause);
        spec.orderByColumns = new ArrayList<>();
        spec.orderAscending = new ArrayList<>();
        spec.limit = limitClause == null || limitClause.isEmpty() ? -1 : Integer.parseInt(limitClause.trim());
        spec.offset = offsetClause == null || offsetClause.isEmpty() ? 0 : Integer.parseInt(offsetClause.trim());
        spec.whereCondition = parseWhereCondition(tableName, whereClause);

        List<String> selectItems = splitCommaSeparated(selectPart);
        for (String item : selectItems) {
            AggregateExpression aggregate = parseAggregateExpression(item);
            if (aggregate != null) {
                spec.aggregateExpressions.add(aggregate);
            } else if (!spec.groupByColumns.contains(item.trim())) {
                throw new SQLException("Non-aggregate column must appear in GROUP BY: " + item.trim());
            }
        }

        if (spec.aggregateExpressions.isEmpty()) {
            throw new SQLException("No aggregate functions found in aggregation query");
        }

        if (orderByClause != null && !orderByClause.isEmpty()) {
            for (String item : splitCommaSeparated(orderByClause)) {
                String trimmed = item.trim();
                boolean ascending = true;
                String upperItem = trimmed.toUpperCase();
                if (upperItem.endsWith(" DESC")) {
                    ascending = false;
                    trimmed = trimmed.substring(0, trimmed.length() - 5).trim();
                } else if (upperItem.endsWith(" ASC")) {
                    trimmed = trimmed.substring(0, trimmed.length() - 4).trim();
                }
                spec.orderByColumns.add(trimmed);
                spec.orderAscending.add(ascending);
            }
        }

        return spec;
    }

    private Condition parseWhereCondition(String tableName, String whereClause) throws SQLException {
        if (whereClause == null || whereClause.isEmpty()) {
            return null;
        }

        String syntheticSql = "SELECT * FROM " + tableName + " WHERE " + whereClause;
        SelectStatement parsed = parseSelect(syntheticSql);
        return parsed.getWhere();
    }

    private AggregateExpression parseAggregateExpression(String selectItem) {
        String trimmed = selectItem.trim();
        String alias = null;

        Matcher aliasMatcher = Pattern.compile("(?i)^(.*?)(?:\\s+AS\\s+)([A-Za-z_][A-Za-z0-9_]*)$").matcher(trimmed);
        if (aliasMatcher.matches()) {
            trimmed = aliasMatcher.group(1).trim();
            alias = aliasMatcher.group(2).trim();
        }

        Matcher aggregateMatcher = Pattern.compile("(?i)^(COUNT|SUM|AVG|MAX|MIN)\\s*\\((\\*|[A-Za-z_][A-Za-z0-9_]*)\\)$")
            .matcher(trimmed);
        if (!aggregateMatcher.matches()) {
            return null;
        }

        AggregateExpression expression = new AggregateExpression();
        expression.function = aggregateMatcher.group(1).toUpperCase();
        expression.column = aggregateMatcher.group(2);
        expression.outputName = alias != null ? alias : trimmed;
        return expression;
    }

    private String extractClause(String sql, int clauseIndex, String clauseKeyword, int... followingClauseIndexes) {
        if (clauseIndex < 0) {
            return null;
        }
        int start = clauseIndex + clauseKeyword.length();
        int end = firstPositive(followingClauseIndexes.length == 0 ? new int[]{sql.length()} : append(sql.length(), followingClauseIndexes));
        return sql.substring(start, end).trim();
    }

    private int[] append(int last, int... values) {
        int[] result = new int[values.length + 1];
        System.arraycopy(values, 0, result, 0, values.length);
        result[values.length] = last;
        return result;
    }

    private int firstPositive(int... indexes) {
        int candidate = Integer.MAX_VALUE;
        for (int index : indexes) {
            if (index >= 0 && index < candidate) {
                candidate = index;
            }
        }
        return candidate == Integer.MAX_VALUE ? -1 : candidate;
    }

    private List<String> splitCommaSeparated(String input) {
        if (input == null || input.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> parts = new ArrayList<>();
        int depth = 0;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth = Math.max(0, depth - 1);
            }

            if (ch == ',' && depth == 0) {
                parts.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }

        if (current.length() > 0) {
            parts.add(current.toString().trim());
        }
        return parts;
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

    private boolean evaluateRow(Row row, Condition condition) {
        return condition.evaluate(row);
    }

    private Comparator<com.minisql.common.model.Row> createComparator(List<SelectStatement.OrderByElement> orderBy) {
        return (left, right) -> {
            for (SelectStatement.OrderByElement element : orderBy) {
                Object leftValue = left.getColumn(element.getColumn());
                Object rightValue = right.getColumn(element.getColumn());
                int cmp = compareValues(leftValue, rightValue);
                if (cmp != 0) {
                    return element.isAscending() ? cmp : -cmp;
                }
            }
            return 0;
        };
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

    private List<String> determineResultColumns(SelectStatement select, Table schema) {
        if (select.isSelectAll() || select.getColumns() == null || select.getColumns().isEmpty()) {
            if (schema == null || schema.getColumns() == null) {
                return Collections.emptyList();
            }
            List<String> columns = new ArrayList<>();
            for (Column column : schema.getColumns()) {
                columns.add(column.getName());
            }
            return columns;
        }

        List<String> columns = new ArrayList<>();
        for (String column : select.getColumns()) {
            if (!"*".equals(column)) {
                columns.add(column);
            }
        }
        return columns;
    }

    private List<String> determineJoinResultColumns(SelectStatement select, List<Row> rows) {
        if (select.isSelectAll() || select.getColumns() == null || select.getColumns().isEmpty()) {
            if (rows.isEmpty()) {
                return Collections.emptyList();
            }
            return rows.get(0).getColumnNames();
        }

        List<String> columns = new ArrayList<>();
        for (String column : select.getColumns()) {
            if (!"*".equals(column)) {
                columns.add(column);
            }
        }
        return columns;
    }

    private List<String> extractOrderByColumns(List<SelectStatement.OrderByElement> orderBy) {
        List<String> columns = new ArrayList<>();
        for (SelectStatement.OrderByElement element : orderBy) {
            columns.add(element.getColumn());
        }
        return columns;
    }

    private List<Boolean> extractOrderDirections(List<SelectStatement.OrderByElement> orderBy) {
        List<Boolean> directions = new ArrayList<>();
        for (SelectStatement.OrderByElement element : orderBy) {
            directions.add(element.isAscending());
        }
        return directions;
    }

    private JoinSpec parseJoinSpec(Condition condition) throws SQLException {
        if (condition instanceof com.minisql.sql.ast.SimpleCondition) {
            com.minisql.sql.ast.SimpleCondition simple = (com.minisql.sql.ast.SimpleCondition) condition;
            if ("=".equals(simple.getOperator()) || "==".equals(simple.getOperator())) {
                return new JoinSpec(simple.getColumn(), simple.getValue());
            }
        }
        throw new SQLException("Only single-column equality JOIN is supported");
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

    private Row mergeRows(com.minisql.common.model.Row leftRow,
                          String leftTable,
                          com.minisql.common.model.Row rightRow,
                          String rightTable) {
        com.minisql.common.model.Row merged = mergeLogicalRows(leftRow, leftTable, rightRow, rightTable, null);
        Row row = new Row();
        for (String columnName : merged.getColumnNames()) {
            row.addColumn(columnName, merged.getColumn(columnName));
        }
        return row;
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

    private Row readRow(ResultSet rs) throws SQLException {
        Row row = new Row();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            row.addColumn(metaData.getColumnLabel(i), rs.getObject(i));
        }
        return row;
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

    private static class JoinSpec {
        private final String leftColumn;
        private final String rightColumn;

        private JoinSpec(String leftColumn, String rightColumn) {
            this.leftColumn = leftColumn;
            this.rightColumn = rightColumn;
        }
    }

    private static class QuerySpec {
        private String tableName;
        private String tableAlias;
        private boolean selectAll;
        private List<SelectItem> selectItems = new ArrayList<>();
        private JoinQuerySpec joinSpec;
        private Condition whereCondition;
        private String whereClause;
        private List<String> groupByColumns = new ArrayList<>();
        private Condition havingCondition;
        private List<AggregateExpression> aggregateExpressions = new ArrayList<>();
        private List<String> orderByColumns = new ArrayList<>();
        private List<Boolean> orderAscending = new ArrayList<>();
        private int limit;
        private int offset;

        private boolean hasAggregation() {
            return !aggregateExpressions.isEmpty() || !groupByColumns.isEmpty();
        }
    }

    private static class JoinQuerySpec {
        private String joinType = JoinType.INNER.name();
        private String rightTable;
        private String rightAlias;
        private List<String> leftConditions;
        private List<String> rightConditions;

        private boolean isLeftJoin() {
            return JoinType.LEFT.name().equalsIgnoreCase(joinType);
        }
    }

    private static class TableSource {
        private final String tableName;
        private final String alias;

        private TableSource(String tableName, String alias) {
            this.tableName = tableName;
            this.alias = alias;
        }
    }

    private static class SelectItem {
        private final String expression;
        private final String alias;
        private final boolean aggregate;

        private SelectItem(String expression, String alias, boolean aggregate) {
            this.expression = expression;
            this.alias = alias;
            this.aggregate = aggregate;
        }

        private String outputName() {
            return alias != null ? alias : expression;
        }

        private AggregateExpression toAggregateExpression() {
            AggregateExpression expression = new AggregateExpression();
            Matcher aggregateMatcher = Pattern.compile("(?i)^(COUNT|SUM|AVG|MAX|MIN)\\s*\\((\\*|[A-Za-z_][A-Za-z0-9_\\.]*)\\)$")
                .matcher(this.expression);
            if (!aggregateMatcher.matches()) {
                return expression;
            }
            expression.function = aggregateMatcher.group(1).toUpperCase();
            expression.column = aggregateMatcher.group(2);
            expression.outputName = outputName();
            return expression;
        }
    }

    private static class AggregationQuerySpec {
        private String tableName;
        private Condition whereCondition;
        private List<String> groupByColumns;
        private List<AggregateExpression> aggregateExpressions;
        private List<String> orderByColumns;
        private List<Boolean> orderAscending;
        private int limit;
        private int offset;
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
