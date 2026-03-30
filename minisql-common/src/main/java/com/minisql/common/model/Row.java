package com.minisql.common.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据行
 */
public class Row {
    private byte[] rowKey;
    private Map<String, Object> values;
    private long timestamp;

    public Row() {
        this.values = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
    }

    public Row(byte[] rowKey) {
        this();
        this.rowKey = rowKey;
    }

    public void setValue(String columnName, Object value) {
        this.values.put(columnName, value);
    }

    public Object getValue(String columnName) {
        return this.values.get(columnName);
    }

    /**
     * 获取列值（别名方法）
     */
    public Object getColumn(String columnName) {
        return this.values.get(columnName);
    }

    /**
     * 设置列值
     */
    public void setColumn(String columnName, Object value) {
        this.values.put(columnName, value);
    }

    /**
     * 是否包含列
     */
    public boolean hasColumn(String columnName) {
        return this.values.containsKey(columnName);
    }

    /**
     * 获取所有列名
     */
    public java.util.Set<String> getColumnNames() {
        return this.values.keySet();
    }

    // Getters and Setters
    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
