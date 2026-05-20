package com.minisql.client;

import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.ExistsCondition;
import com.minisql.sql.ast.InSubqueryCondition;
import com.minisql.sql.ast.NotCondition;

import java.util.HashSet;
import java.util.Set;

import com.minisql.client.executor.AggregationExecutor;
import com.minisql.client.executor.ConditionSerializer;
import com.minisql.client.executor.JoinExecutor;
import com.minisql.client.executor.PipelineAssembler;
import com.minisql.client.executor.QueryPlanner;
import com.minisql.client.executor.ScanExecutor;
import com.minisql.common.model.Column;
import com.minisql.common.model.Table;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.execution.Row;
import com.minisql.sql.execution.operators.ListSourceOperator;
import com.minisql.common.proto.MasterServiceGrpc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 分布式查询执行门面。
 *
 * <p>分析 AST 并委托给专职执行器：
 * <ul>
 *   <li>{@link ScanExecutor} — 并行 Region 扫描 + Region 裁剪</li>
 *   <li>{@link JoinExecutor} — JOIN 管道构建</li>
 *   <li>{@link AggregationExecutor} — 两阶段聚合</li>
 *   <li>{@link PipelineAssembler} — 算子管道组装</li>
 *   <li>{@link QueryPlanner} — AST 分析与列投影计算</li>
 *   <li>{@link ConditionSerializer} — 条件 → SQL 序列化（全部下推，由 RS 决定执行方式）</li>
 * </ul>
 */
public class ParallelQueryExecutor {

    private final ExecutorService executor;
    private final ScanExecutor scanExecutor;
    private final JoinExecutor joinExecutor;
    private final AggregationExecutor aggregationExecutor;

    public ParallelQueryExecutor(MasterServiceGrpc.MasterServiceBlockingStub masterStub,
                                 long queryTimeoutSeconds,
                                 Router router) {
        this.executor = Executors.newFixedThreadPool(10);
        this.scanExecutor = new ScanExecutor(executor, masterStub, router, queryTimeoutSeconds);
        this.joinExecutor = new JoinExecutor(scanExecutor);
        this.aggregationExecutor = new AggregationExecutor(scanExecutor, executor, queryTimeoutSeconds);
    }

    ParallelQueryExecutor(MasterServiceGrpc.MasterServiceBlockingStub masterStub,
                          long queryTimeoutSeconds) {
        this(masterStub, queryTimeoutSeconds, null);
    }

    public List<String> drainReplicaReadWarnings() {
        return scanExecutor.drainReplicaReadWarnings();
    }

    // ── 查询入口 ──

    public List<Row> executeQuery(SelectStatement ast, String rawSql) throws SQLException {
        // Resolve subqueries in WHERE before executing the main query
        resolveSubqueries(ast.getWhere());

        String tableName = ast.getTable();
        String tableAlias = ast.getTableAlias();
        boolean hasJoin = ast.getJoinTable() != null;
        boolean hasAggregation = ast.hasAggregation();

        List<QueryPlanner.AggregateExpression> aggregateExpressions =
            QueryPlanner.buildAggregateExpressions(ast);

        // WHERE 下推：所有条件都发给 RS，由 RS 决定哪些可以在存储层执行
        String whereClause = (ast.getWhere() != null && !hasJoin)
            ? ConditionSerializer.toSql(ast.getWhere()) : null;

        // 列投影
        List<String> projectedQualifiers;
        if (hasJoin) {
            List<String> joinLeftCols = new ArrayList<>();
            List<String> joinRightCols = new ArrayList<>();
            QueryPlanner.extractJoinConditionColumns(ast.getJoinCondition(),
                joinLeftCols, joinRightCols,
                tableAlias != null ? tableAlias : tableName,
                ast.getJoinTableAlias() != null ? ast.getJoinTableAlias() : ast.getJoinTable());
            projectedQualifiers = QueryPlanner.determineJoinProjectedQualifiers(
                ast, tableName, tableAlias, joinLeftCols, aggregateExpressions);
        } else {
            projectedQualifiers = QueryPlanner.determineProjectedQualifiers(ast, hasAggregation);
        }

        // ORDER BY + LIMIT 下推
        boolean canPushDownOrderBy = !hasJoin && !hasAggregation
            && ast.getOrderBy() != null && !ast.getOrderBy().isEmpty();
        int userLimit = ast.getLimit() != null ? ast.getLimit() : -1;
        int userOffset = ast.getOffset() != null ? ast.getOffset() : 0;
        List<SelectStatement.OrderByElement> pushDownOrderBy = null;
        int pushDownLimit = 0;
        if (canPushDownOrderBy) {
            pushDownOrderBy = ast.getOrderBy();
            if (userLimit >= 0) pushDownLimit = userLimit + userOffset;
        }

        boolean canPushDownAggregation = hasAggregation && !hasJoin && ast.getHaving() == null;

        // ── 两阶段聚合路径 ──
        if (canPushDownAggregation) {
            List<String> groupByColumns = ast.getGroupByColumns() != null
                ? ast.getGroupByColumns() : Collections.emptyList();
            List<com.minisql.common.model.Row> rows = aggregationExecutor.fetchAggregatedRows(
                tableName, ast.getWhere(), whereClause,
                projectedQualifiers, aggregateExpressions, groupByColumns);
            String[] columns = rows.isEmpty() ? new String[0]
                : rows.get(0).getColumnNames().toArray(new String[0]);
            com.minisql.sql.execution.Operator pipeline = new ListSourceOperator(rows, columns);
            pipeline = PipelineAssembler.appendOrderByLimitProject(pipeline, ast, userLimit, userOffset);
            return PipelineAssembler.drain(pipeline);
        }

        // ── JOIN 路径 ──
        if (hasJoin) {
            return joinExecutor.execute(ast, aggregateExpressions, projectedQualifiers,
                whereClause, userLimit, userOffset);
        }

        // ── 简单扫描路径 ──
        List<com.minisql.common.model.Row> rows = scanExecutor.fetchSourceRows(
            tableName, null, ast.getWhere(), whereClause, projectedQualifiers,
            pushDownOrderBy, pushDownLimit, 0);

        Table schema = scanExecutor.getTableSchema(tableName);
        String[] columns = schema.getColumns().stream()
            .map(Column::getName).toArray(String[]::new);

        com.minisql.sql.execution.Operator pipeline = new ListSourceOperator(rows, columns);

        if (hasAggregation) {
            pipeline = PipelineAssembler.appendAggregate(pipeline, ast);
        }

        pipeline = PipelineAssembler.appendOrderByLimitProject(pipeline, ast, userLimit, userOffset);
        return PipelineAssembler.drain(pipeline);
    }

    /**
     * Walk the condition tree and resolve any subquery nodes by executing them.
     * Injects results into InSubqueryCondition.resolvedValues and ExistsCondition.hasResults.
     */
    private void resolveSubqueries(Condition condition) throws SQLException {
        if (condition == null) return;
        if (condition instanceof InSubqueryCondition) {
            InSubqueryCondition isc = (InSubqueryCondition) condition;
            SelectStatement subAst = isc.getSubquery().getSelectStatement();
            List<Row> subResults = executeQuery(subAst, "");
            Set<Object> values = new HashSet<>();
            for (Row row : subResults) {
                if (row.getColumnCount() > 0) {
                    values.add(row.getValue(0));
                }
            }
            isc.setResolvedValues(values);
        } else if (condition instanceof ExistsCondition) {
            ExistsCondition ec = (ExistsCondition) condition;
            SelectStatement subAst = ec.getSubquery().getSelectStatement();
            List<Row> subResults = executeQuery(subAst, "");
            ec.setHasResults(!subResults.isEmpty());
        } else if (condition instanceof CompoundCondition) {
            CompoundCondition cc = (CompoundCondition) condition;
            resolveSubqueries(cc.getLeft());
            resolveSubqueries(cc.getRight());
        } else if (condition instanceof NotCondition) {
            resolveSubqueries(((NotCondition) condition).getInner());
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}
