package com.minisql.sql.ast;

/**
 * 列定义
 */
public class ColumnDef {
    private String name;
    private ColumnType type;
    private int length;
    private boolean nullable;

    public ColumnDef(String name, ColumnType type, int length, boolean nullable) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.nullable = nullable;
    }

    // Getters
    public String getName() { return name; }
    public ColumnType getType() { return type; }
    public int getLength() { return length; }
    public boolean isNullable() { return nullable; }
}
