package com.minisql.sql.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 查询结果行
 */
public class Row {
    private String[] columns;
    private Object[] values;
    private byte[] rowKey;  // 底层 row key

    public Row() {
        this.columns = new String[0];
        this.values = new Object[0];
    }

    public Row(Object[] values) {
        this.columns = new String[0];
        this.values = values;
    }

    public Row(String[] columns, Object[] values) {
        this.columns = columns;
        this.values = values;
    }

    public Row(String[] columns, Object[] values, byte[] rowKey) {
        this.columns = columns;
        this.values = values;
        this.rowKey = rowKey;
    }

    /**
     * 获取指定列的值
     */
    public Object getValue(int index) {
        if (index < 0 || index >= values.length) {
            return null;
        }
        return values[index];
    }

    /**
     * 根据列名获取值
     */
    public Object getValue(String columnName) {
        if (columns == null) return null;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equalsIgnoreCase(columnName)) {
                return values[i];
            }
        }
        return null;
    }

    public Object[] getValues() {
        return values;
    }

    public int getColumnCount() {
        return values.length;
    }

    public String[] getColumns() {
        return columns;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public void addColumn(String name, Object value) {
        int currentSize = columns == null ? 0 : columns.length;
        String[] nextColumns = Arrays.copyOf(columns == null ? new String[0] : columns, currentSize + 1);
        Object[] nextValues = Arrays.copyOf(values == null ? new Object[0] : values, currentSize + 1);
        nextColumns[currentSize] = name;
        nextValues[currentSize] = value;
        this.columns = nextColumns;
        this.values = nextValues;
    }

    public Object getColumnValue(String name) {
        return getValue(name);
    }

    public Object getColumnValue(int index) {
        return getValue(index);
    }

    public List<String> getColumnNames() {
        if (columns == null || columns.length == 0) {
            return new ArrayList<>();
        }
        return new ArrayList<>(Arrays.asList(columns));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Row{");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(", ");
            if (columns != null && i < columns.length) {
                sb.append(columns[i]).append("=");
            }
            sb.append(values[i]);
        }
        sb.append("}");
        return sb.toString();
    }
}
