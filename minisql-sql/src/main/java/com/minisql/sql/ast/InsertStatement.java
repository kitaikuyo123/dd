package com.minisql.sql.ast;

import java.util.List;
import java.util.stream.Collectors;

/**
 * INSERT 语句
 */
public class InsertStatement extends Statement {
    private String table;
    private List<String> columns;
    private List<String> values;

    @Override
    public StatementType getType() {
        return StatementType.INSERT;
    }

    // Getters and Setters
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }
    public List<String> getValues() { return values; }
    public void setValues(List<String> values) { this.values = values; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(table);
        if (columns != null && !columns.isEmpty()) {
            sb.append(" (");
            sb.append(columns.stream().collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(" VALUES (");
        if (values != null) {
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) sb.append(", ");
                String value = values.get(i);
                // 判断是否是数字，不是数字则加引号
                if (value == null || value.isEmpty()) {
                    sb.append("NULL");
                } else if (isNumeric(value)) {
                    sb.append(value);
                } else {
                    sb.append("'").append(value.replace("'", "''")).append("'");
                }
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) return false;
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
