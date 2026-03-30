package com.minisql.sql.ast;

/**
 * 赋值操作
 */
public class Assignment {
    private String column;
    private String value;

    public Assignment(String column, String value) {
        this.column = column;
        this.value = value;
    }

    // Getters
    public String getColumn() { return column; }
    public String getValue() { return value; }
}
