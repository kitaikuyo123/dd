package com.minisql.client.executor;

import com.minisql.sql.AggregateExpr;
import com.minisql.sql.AggregateType;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.execution.Row;
import com.minisql.sql.execution.operators.AggregateOperator;
import com.minisql.sql.execution.operators.DistinctOperator;
import com.minisql.sql.execution.operators.FilterOperator;
import com.minisql.sql.execution.operators.LimitOperator;
import com.minisql.sql.execution.operators.ProjectOperator;
import com.minisql.sql.execution.operators.SortOperator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Volcano 算子管道组装器。
 *
 * <p>负责在已有数据源算子之上依次叠加聚合、HAVING 过滤、排序、
 * 分页和列投影算子，形成完整的查询管道。
 */
public final class PipelineAssembler {

    private PipelineAssembler() {}

    /**
     * 叠加聚合算子 + 可选 HAVING 过滤。
     */
    public static com.minisql.sql.execution.Operator appendAggregate(
            com.minisql.sql.execution.Operator input, SelectStatement ast) {
        List<AggregateExpr> aggregates = new ArrayList<>();
        if (ast.getAggregates() != null) {
            for (SelectStatement.AggregateExpr agg : ast.getAggregates()) {
                AggregateType type = AggregateType.valueOf(agg.getFunction().toUpperCase());
                AggregateExpr expr = new AggregateExpr(type, agg.getColumn());
                // Use raw name (e.g. "SUM(amount)") so HAVING can reference it
                expr.setAlias(null);
                aggregates.add(expr);
            }
        }
        List<String> groupByColumns = ast.getGroupByColumns();
        com.minisql.sql.execution.Operator pipeline =
            new AggregateOperator(input, aggregates, groupByColumns);

        if (ast.getHaving() != null) {
            Condition having = ast.getHaving();
            pipeline = new FilterOperator(pipeline, row -> having.evaluate(row));
        }
        return pipeline;
    }

    /**
     * 叠加 ORDER BY → LIMIT/OFFSET → 列投影（→ DISTINCT → LIMIT 顺序）。
     *
     * <p>管道数据流：Sort → Project → [Distinct] → Limit
     */
    public static com.minisql.sql.execution.Operator appendOrderByLimitProject(
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

        // Projection
        if (ast.isSelectAll()) {
            pipeline = new ProjectOperator(pipeline, Collections.emptyList(), true);
        } else if (ast.getColumns() != null && !ast.getColumns().isEmpty()) {
            // Build mapping from alias to raw aggregate name for source column lookup
            Map<String, String> aliasToRaw = new HashMap<>();
            if (ast.getAggregates() != null) {
                for (SelectStatement.AggregateExpr agg : ast.getAggregates()) {
                    if (agg.getAlias() != null) {
                        String rawName = agg.getFunction().toUpperCase() + "(" + agg.getColumn() + ")";
                        aliasToRaw.put(agg.getAlias(), rawName);
                    }
                }
            }
            List<String> outputNames = new ArrayList<>();
            List<String> sourceNames = new ArrayList<>();
            List<String> aliases = ast.getColumnAliases();
            for (int i = 0; i < ast.getColumns().size(); i++) {
                String col = ast.getColumns().get(i);
                sourceNames.add(aliasToRaw.getOrDefault(col, col));
                outputNames.add((aliases != null && i < aliases.size() && aliases.get(i) != null)
                    ? aliases.get(i) : col);
            }
            pipeline = new ProjectOperator(pipeline, sourceNames, outputNames);
        }

        // DISTINCT
        if (ast.isDistinct()) pipeline = new DistinctOperator(pipeline);

        // LIMIT / OFFSET
        if (userLimit >= 0) {
            pipeline = new LimitOperator(pipeline, userLimit, Math.max(0, userOffset));
        }

        return pipeline;
    }

    /**
     * 拉取管道中所有行：open → nextRow → close。
     */
    public static List<Row> drain(com.minisql.sql.execution.Operator root) throws SQLException {
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
}
