package com.minisql.client.executor;

import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.ast.SimpleCondition;

import java.util.ArrayList;
import java.util.List;

/**
 * 查询计划分析器（纯 AST 分析，无 IO 操作）。
 *
 * <p>负责从 AST 中提取列投影、聚合表达式、JOIN 条件列等信息，
 * 为 {@link ScanExecutor} 和 {@link JoinExecutor} 提供执行参数。
 */
public final class QueryPlanner {

    private QueryPlanner() {}

    // ── 聚合表达式构建 ──

    /** 聚合表达式内部表示 */
    public static class AggregateExpression {
        public final String function;
        public final String column;
        public final String outputName;

        public AggregateExpression(String function, String column, String outputName) {
            this.function = function;
            this.column = column;
            this.outputName = outputName;
        }
    }

    /** 从 AST 的 aggregates 列表构建内部表示 */
    public static List<AggregateExpression> buildAggregateExpressions(SelectStatement ast) {
        List<AggregateExpression> list = new ArrayList<>();
        if (ast.getAggregates() != null) {
            for (SelectStatement.AggregateExpr agg : ast.getAggregates()) {
                list.add(new AggregateExpression(agg.getFunction(), agg.getColumn(), agg.getOutputName()));
            }
        }
        return list;
    }

    // ── 列投影计算 ──

    /** 单表查询的列投影 */
    public static List<String> determineProjectedQualifiers(SelectStatement ast, boolean hasAggregation) {
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

    /** JOIN 查询中单侧表的列投影 */
    public static List<String> determineJoinProjectedQualifiers(SelectStatement ast,
                                                                  String tableName, String tableAlias,
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

    // ── JOIN 条件列提取 ──

    /** 从 ON 条件中提取等值连接涉及的左右表列名 */
    public static void extractJoinConditionColumns(Condition condition,
                                                    List<String> leftCols, List<String> rightCols,
                                                    String leftTable, String rightTable) {
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

    // ── 内部工具方法 ──

    private static boolean belongsToTable(String column, String table) {
        if (column == null || table == null) return false;
        int dot = column.indexOf('.');
        if (dot < 0) return true;
        return column.substring(0, dot).equalsIgnoreCase(table);
    }

    private static void collectConditionColumnsForSource(Condition condition,
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

    private static void addJoinColumns(List<String> target, List<String> joinColumns) {
        if (joinColumns == null) return;
        for (String column : joinColumns) {
            String qualifier = toSimpleQualifier(column);
            if (qualifier != null && !target.contains(qualifier)) target.add(qualifier);
        }
    }

    private static String qualifyForSource(String column, String tableName, String tableAlias) {
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

    private static void collectConditionColumns(Condition condition, List<String> target) {
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

    private static String toSimpleQualifier(String column) {
        if (column == null || column.isBlank()) return null;
        String trimmed = column.trim();
        if ("*".equals(trimmed) || trimmed.contains("(")) return null;
        int qi = trimmed.indexOf('.');
        return qi >= 0 ? trimmed.substring(qi + 1) : trimmed;
    }
}
