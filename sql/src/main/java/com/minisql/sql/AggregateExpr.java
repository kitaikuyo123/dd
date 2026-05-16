package com.minisql.sql;

/**
 * 聚合表达式（执行层表示）
 */
public class AggregateExpr {
    private AggregateType type;
    private String column;
    private String alias;

    public AggregateExpr(AggregateType type, String column) {
        this.type = type;
        this.column = column;
    }

    public AggregateType getType() { return type; }
    public void setType(AggregateType type) { this.type = type; }
    public String getColumn() { return column; }
    public void setColumn(String column) { this.column = column; }
    public String getAlias() { return alias; }
    public void setAlias(String alias) { this.alias = alias; }
}
