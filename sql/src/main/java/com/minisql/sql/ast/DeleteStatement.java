package com.minisql.sql.ast;

/**
 * DELETE 语句
 */
public class DeleteStatement extends Statement {
    private String table;
    private Condition where;

    @Override
    public StatementType getType() {
        return StatementType.DELETE;
    }

    // Getters and Setters
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public Condition getWhere() { return where; }
    public void setWhere(Condition where) { this.where = where; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        sb.append(table);
        if (where != null) {
            sb.append(" WHERE ");
            sb.append(whereToString(where));
        }
        return sb.toString();
    }

}
