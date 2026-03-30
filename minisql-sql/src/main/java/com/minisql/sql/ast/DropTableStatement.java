package com.minisql.sql.ast;

/**
 * DROP TABLE 语句
 */
public class DropTableStatement extends Statement {
    private String table;

    @Override
    public StatementType getType() {
        return StatementType.DROP_TABLE;
    }

    // Getters and Setters
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
}
