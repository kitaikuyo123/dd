package com.minisql.client.executor;

import com.minisql.common.model.Column;
import com.minisql.common.model.Table;
import com.minisql.sql.JoinType;
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.ConditionSplitter;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.ast.SimpleCondition;
import com.minisql.sql.execution.Row;
import com.minisql.sql.execution.operators.FilterOperator;
import com.minisql.sql.execution.operators.JoinOperator;
import com.minisql.sql.execution.operators.ListSourceOperator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * JOIN 查询执行器。
 *
 * <p>负责：WHERE 条件拆分（单表 / 跨表）、左右表数据拉取、
 * 限定列名构建、JoinOperator 管道组装。
 */
public class JoinExecutor {

    private final ScanExecutor scanExecutor;

    public JoinExecutor(ScanExecutor scanExecutor) {
        this.scanExecutor = scanExecutor;
    }

    /**
     * 执行 JOIN 查询管道。
     *
     * <p>步骤：
     * <ol>
     *   <li>用 {@link ConditionSplitter} 将 WHERE 拆分为左表/右表/跨表条件</li>
     *   <li>并行拉取左右表数据</li>
     *   <li>构建带表限定列名的 ListSourceOperator</li>
     *   <li>构建 JoinOperator</li>
     *   <li>叠加跨表过滤 → 聚合 → 排序 → 分页</li>
     * </ol>
     */
    public List<Row> execute(SelectStatement ast,
                              List<QueryPlanner.AggregateExpression> aggregateExpressions,
                              List<String> leftProjectedQualifiers,
                              String leftWhereClause,
                              int userLimit, int userOffset) throws SQLException {
        String leftTable = ast.getTable();
        String leftAlias = ast.getTableAlias();
        String rightTable = ast.getJoinTable();
        String rightAlias = ast.getJoinTableAlias();
        String leftQualifier = leftAlias != null ? leftAlias : leftTable;
        String rightQualifier = rightAlias != null ? rightAlias : rightTable;

        // WHERE 条件拆分
        Condition leftWhereCondition = ast.getWhere();
        Condition rightWhereCondition = null;
        Condition crossTableCondition = null;

        if (ast.getWhere() != null) {
            ConditionSplitter splitter = new ConditionSplitter(
                leftTable, leftAlias, rightTable, rightAlias);
            ConditionSplitter.SplitResult split = splitter.split(ast.getWhere());
            leftWhereCondition = split.leftOnly;
            leftWhereClause = split.leftOnly != null
                ? ConditionSerializer.toSql(split.leftOnly) : null;
            rightWhereCondition = split.rightOnly;
            crossTableCondition = split.crossTable;
        }

        // 拉取左表数据
        List<com.minisql.common.model.Row> leftRows = scanExecutor.fetchSourceRows(
            leftTable, null, leftWhereCondition, leftWhereClause, leftProjectedQualifiers);

        // 计算右表列投影
        List<String> leftJoinCols = new ArrayList<>();
        List<String> rightJoinCols = new ArrayList<>();
        QueryPlanner.extractJoinConditionColumns(ast.getJoinCondition(),
            leftJoinCols, rightJoinCols, leftQualifier, rightQualifier);

        List<String> rightProjectedQualifiers = QueryPlanner.determineJoinProjectedQualifiers(
            ast, rightTable, rightAlias, rightJoinCols, aggregateExpressions);

        String rightWhereClause = rightWhereCondition != null
            ? ConditionSerializer.toSql(rightWhereCondition) : null;

        // 拉取右表数据
        List<com.minisql.common.model.Row> rightRows = scanExecutor.fetchSourceRows(
            rightTable, null, rightWhereCondition, rightWhereClause, rightProjectedQualifiers);

        // 构建带限定列名的数据源算子
        Table leftSchema = scanExecutor.getTableSchema(leftTable);
        Table rightSchema = scanExecutor.getTableSchema(rightTable);
        String[] leftColumns = buildQualifiedColumns(leftSchema, leftQualifier);
        String[] rightColumns = buildQualifiedColumns(rightSchema, rightQualifier);

        com.minisql.sql.execution.Operator leftSource = new ListSourceOperator(leftRows, leftColumns);
        com.minisql.sql.execution.Operator rightSource = new ListSourceOperator(rightRows, rightColumns);

        // 构建 JoinOperator（RIGHT JOIN 转为交换左右 + LEFT JOIN）
        JoinOperator.JoinCondition joinCond = buildJoinCondition(
            ast.getJoinCondition(), leftQualifier, rightQualifier, leftColumns, rightColumns);
        JoinType joinType = ast.getJoinType() != null ? ast.getJoinType() : JoinType.INNER;

        com.minisql.sql.execution.Operator pipeline;
        if (joinType == JoinType.RIGHT) {
            // RIGHT JOIN = swap sources + swap join condition + LEFT JOIN
            JoinOperator.JoinCondition swappedCond = new JoinOperator.JoinCondition(
                joinCond.getRightColumn(), joinCond.getLeftColumn(), joinCond.getOperator());
            pipeline = new JoinOperator(rightSource, leftSource, JoinType.LEFT, swappedCond);
        } else {
            pipeline = new JoinOperator(leftSource, rightSource, joinType, joinCond);
        }

        // 跨表 WHERE 过滤
        final Condition finalCrossCond = crossTableCondition;
        if (finalCrossCond != null) {
            pipeline = new FilterOperator(pipeline, row -> finalCrossCond.evaluate(row));
        }

        // 聚合
        if (ast.hasAggregation()) {
            pipeline = PipelineAssembler.appendAggregate(pipeline, ast);
        }

        pipeline = PipelineAssembler.appendOrderByLimitProject(pipeline, ast, userLimit, userOffset);
        return PipelineAssembler.drain(pipeline);
    }

    // ── JOIN 条件构建 ──

    private JoinOperator.JoinCondition buildJoinCondition(Condition condition,
                                                            String leftQualifier,
                                                            String rightQualifier,
                                                            String[] leftColumns,
                                                            String[] rightColumns) {
        if (condition instanceof SimpleCondition) {
            SimpleCondition simple = (SimpleCondition) condition;
            if ("=".equals(simple.getOperator()) && simple.isValueColumnReference()) {
                String col1 = simple.getColumn();
                String col2 = simple.getValue();
                String leftCol = qualifyColumn(col1, leftQualifier);
                String rightCol = qualifyColumn(col2, rightQualifier);
                if (belongsToTable(col1, leftQualifier)) {
                    return new JoinOperator.JoinCondition(leftCol, rightCol, JoinOperator.JoinOperatorType.EQUALS);
                } else {
                    return new JoinOperator.JoinCondition(rightCol, leftCol, JoinOperator.JoinOperatorType.EQUALS);
                }
            }
        }
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            JoinOperator.JoinCondition left = buildJoinCondition(compound.getLeft(), leftQualifier, rightQualifier, leftColumns, rightColumns);
            if (left != null) return left;
            return buildJoinCondition(compound.getRight(), leftQualifier, rightQualifier, leftColumns, rightColumns);
        }
        throw new IllegalArgumentException("Unsupported JOIN condition: " + condition);
    }

    // ── 工具方法 ──

    private String[] buildQualifiedColumns(Table schema, String qualifier) {
        List<String> cols = new ArrayList<>();
        for (Column col : schema.getColumns()) {
            cols.add(col.getName());
            cols.add(qualifier + "." + col.getName());
        }
        return cols.toArray(new String[0]);
    }

    private String qualifyColumn(String col, String qualifier) {
        if (col.contains(".")) return col;
        return qualifier + "." + col;
    }

    private boolean belongsToTable(String column, String table) {
        if (column == null || table == null) return false;
        int dot = column.indexOf('.');
        if (dot < 0) return true;
        return column.substring(0, dot).equalsIgnoreCase(table);
    }
}
