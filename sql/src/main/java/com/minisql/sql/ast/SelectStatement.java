package com.minisql.sql.ast;

import java.util.List;

/**
 * SELECT 语句
 */
public class SelectStatement extends Statement {
    private List<String> columns;
    private boolean selectAll;
    private String table;
    private String joinTable;
    private Condition joinCondition;
    private Condition where;

    // ORDER BY 子句
    private List<OrderByElement> orderBy;

    // LIMIT 子句
    private Integer limit;
    private Integer offset;

    @Override
    public StatementType getType() {
        return StatementType.SELECT;
    }

    // Getters and Setters
    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }
    public boolean isSelectAll() { return selectAll; }
    public void setSelectAll(boolean selectAll) { this.selectAll = selectAll; }
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public String getJoinTable() { return joinTable; }
    public void setJoinTable(String joinTable) { this.joinTable = joinTable; }
    public Condition getJoinCondition() { return joinCondition; }
    public void setJoinCondition(Condition joinCondition) { this.joinCondition = joinCondition; }
    public Condition getWhere() { return where; }
    public void setWhere(Condition where) { this.where = where; }

    public List<OrderByElement> getOrderBy() { return orderBy; }
    public void setOrderBy(List<OrderByElement> orderBy) { this.orderBy = orderBy; }

    public Integer getLimit() { return limit; }
    public void setLimit(Integer limit) { this.limit = limit; }

    public Integer getOffset() { return offset; }
    public void setOffset(Integer offset) { this.offset = offset; }

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
