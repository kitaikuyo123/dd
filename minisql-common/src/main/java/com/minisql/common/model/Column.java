package com.minisql.common.model;

import java.io.Serializable;

/**
 * 列定义
 */
public class Column implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private ColumnType type;
    private int length;
    private boolean nullable;
    private Object defaultValue;

    public Column() {
        this.nullable = true;
    }

    public Column(String name, ColumnType type) {
        this();
        this.name = name;
        this.type = type;
    }

    public Column(String name, ColumnType type, int length) {
        this(name, type);
        this.length = length;
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String toString() {
        return name + " " + type + (length > 0 ? "(" + length + ")" : "");
    }

    /**
     * 列类型枚举
     */
    public enum ColumnType {
        INT,        // 整数
        BIGINT,     // 长整数
        FLOAT,      // 浮点数
        DOUBLE,     // 双精度浮点数
        VARCHAR,    // 可变长度字符串
        CHAR,       // 固定长度字符串
        BOOLEAN,    // 布尔值
        TIMESTAMP,  // 时间戳
        BLOB,       // 二进制数据
        STRING,     // 字符串
        TEXT        // 长文本
    }
}
