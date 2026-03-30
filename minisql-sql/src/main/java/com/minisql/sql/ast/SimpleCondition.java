package com.minisql.sql.ast;

import com.minisql.sql.execution.Row;

/**
 * Simple comparison condition.
 */
public class SimpleCondition extends Condition {
    private final String column;
    private final String operator;
    private final String value;
    private final boolean valueColumnReference;

    public SimpleCondition(String column, String operator, String value) {
        this(column, operator, value, false);
    }

    public SimpleCondition(String column, String operator, String value, boolean valueColumnReference) {
        this.column = column;
        this.operator = operator;
        this.value = value;
        this.valueColumnReference = valueColumnReference;
    }

    @Override
    public boolean evaluate(Row row) {
        Object columnValue = resolveValue(row, column);
        if (columnValue == null) {
            return false;
        }

        Object rightValue = valueColumnReference ? resolveValue(row, value) : value;
        if (rightValue == null) {
            return false;
        }

        String columnText = String.valueOf(columnValue);
        String valueText = String.valueOf(rightValue);
        switch (operator.toUpperCase()) {
            case "=":
            case "==":
                return columnText.equals(valueText);
            case "!=":
            case "<>":
                return !columnText.equals(valueText);
            case ">":
                return compareValues(columnText, valueText) > 0;
            case ">=":
                return compareValues(columnText, valueText) >= 0;
            case "<":
                return compareValues(columnText, valueText) < 0;
            case "<=":
                return compareValues(columnText, valueText) <= 0;
            case "LIKE":
                return matchLike(columnText, valueText);
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + operator);
        }
    }

    private Object resolveValue(Row row, String reference) {
        Object direct = row.getValue(reference);
        if (direct != null || reference == null || !reference.contains(".")) {
            return direct;
        }
        return row.getValue(reference.substring(reference.lastIndexOf('.') + 1));
    }

    private int compareValues(String a, String b) {
        try {
            return Double.compare(Double.parseDouble(a), Double.parseDouble(b));
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }

    private boolean matchLike(String str, String pattern) {
        return str.matches(pattern.replace("%", ".*").replace("_", "."));
    }

    public String getColumn() {
        return column;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }

    public boolean isValueColumnReference() {
        return valueColumnReference;
    }
}
