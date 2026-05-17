package com.minisql.sql.ast;

import com.minisql.sql.JoinType;

import java.util.ArrayList;
import java.util.List;

/**
 * SELECT 语句
 */
public class SelectStatement extends Statement {
    private List<String> columns;
    private boolean selectAll;
    private String table;

    // 表别名
    private String tableAlias;

    // JOIN
    private String joinTable;
    private String joinTableAlias;
    private JoinType joinType;
    private Condition joinCondition;

    private Condition where;

    // 聚合 + GROUP BY
    private List<AggregateExpr> aggregates;
    private List<String> groupByColumns;

    // HAVING
    private Condition having;

    // SELECT 列别名（有序，与 columns 平行）
    private List<String> columnAliases;

    // ORDER BY 子句
    private List<OrderByElement> orderBy;

    // LIMIT 子句
    private Integer limit;
    private Integer offset;

    // DISTINCT
    private boolean distinct;

    @Override
    public StatementType getType() {
        return StatementType.SELECT;
    }

    public boolean hasAggregation() {
        return (aggregates != null && !aggregates.isEmpty())
            || (groupByColumns != null && !groupByColumns.isEmpty());
    }

    // Getters and Setters
    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }
    public boolean isSelectAll() { return selectAll; }
    public void setSelectAll(boolean selectAll) { this.selectAll = selectAll; }
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public String getTableAlias() { return tableAlias; }
    public void setTableAlias(String tableAlias) { this.tableAlias = tableAlias; }
    public String getJoinTable() { return joinTable; }
    public void setJoinTable(String joinTable) { this.joinTable = joinTable; }
    public String getJoinTableAlias() { return joinTableAlias; }
    public void setJoinTableAlias(String joinTableAlias) { this.joinTableAlias = joinTableAlias; }
    public JoinType getJoinType() { return joinType; }
    public void setJoinType(JoinType joinType) { this.joinType = joinType; }
    public Condition getJoinCondition() { return joinCondition; }
    public void setJoinCondition(Condition joinCondition) { this.joinCondition = joinCondition; }
    public Condition getWhere() { return where; }
    public void setWhere(Condition where) { this.where = where; }
    public List<AggregateExpr> getAggregates() { return aggregates; }
    public void setAggregates(List<AggregateExpr> aggregates) { this.aggregates = aggregates; }
    public List<String> getGroupByColumns() { return groupByColumns; }
    public void setGroupByColumns(List<String> groupByColumns) { this.groupByColumns = groupByColumns; }
    public Condition getHaving() { return having; }
    public void setHaving(Condition having) { this.having = having; }
    public List<String> getColumnAliases() { return columnAliases; }
    public void setColumnAliases(List<String> columnAliases) { this.columnAliases = columnAliases; }
    public List<OrderByElement> getOrderBy() { return orderBy; }
    public void setOrderBy(List<OrderByElement> orderBy) { this.orderBy = orderBy; }
    public Integer getLimit() { return limit; }
    public void setLimit(Integer limit) { this.limit = limit; }
    public Integer getOffset() { return offset; }
    public void setOffset(Integer offset) { this.offset = offset; }
    public boolean isDistinct() { return distinct; }
    public void setDistinct(boolean distinct) { this.distinct = distinct; }

    // Convenience methods for adding aggregates/aliases during parsing
    public void addAggregate(AggregateExpr expr) {
        if (aggregates == null) {
            aggregates = new ArrayList<>();
        }
        aggregates.add(expr);
    }

    public void addColumnAlias(String alias) {
        if (columnAliases == null) {
            columnAliases = new ArrayList<>();
        }
        columnAliases.add(alias);
    }

    /**
     * 聚合表达式
     */
    public static class AggregateExpr {
        private final String function;   // "COUNT", "SUM", "AVG", "MAX", "MIN"
        private final String column;     // 列名或 "*"
        private String alias;            // AS 后的别名

        public AggregateExpr(String function, String column) {
            this.function = function;
            this.column = column;
        }

        public String getFunction() { return function; }
        public String getColumn() { return column; }
        public String getAlias() { return alias; }
        public void setAlias(String alias) { this.alias = alias; }

        /**
         * 输出名称：有别名用别名，否则用 function(column) 格式
         */
        public String getOutputName() {
            if (alias != null && !alias.isEmpty()) {
                return alias;
            }
            return function + "(" + column + ")";
        }
    }

    /**
     * ORDER BY 元素
     */
    public static class OrderByElement {
        private String column;
        private boolean ascending;  // true = ASC, false = DESC

        public OrderByElement(String column, boolean ascending) {
            this.column = column;
            this.ascending = ascending;
        }

        public String getColumn() { return column; }
        public void setColumn(String column) { this.column = column; }

        public boolean isAscending() { return ascending; }
        public void setAscending(boolean ascending) { this.ascending = ascending; }
    }
}
