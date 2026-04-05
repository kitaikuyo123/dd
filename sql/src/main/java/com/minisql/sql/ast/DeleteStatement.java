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

    private String whereToString(Condition condition) {
        if (condition instanceof SimpleCondition) {
            SimpleCondition sc = (SimpleCondition) condition;
            return sc.getColumn() + " " + sc.getOperator() + " " + formatValue(sc.getValue());
        }
        return "1=1"; // 默认条件
    }

    private String formatValue(String value) {
        if (value == null || value.isEmpty()) {
            return "NULL";
        } else if (isNumeric(value)) {
            return value;
        } else {
            return "'" + value.replace("'", "''") + "'";
        }
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
