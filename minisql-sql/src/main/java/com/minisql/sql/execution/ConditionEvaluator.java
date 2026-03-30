package com.minisql.sql.execution;

import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SimpleCondition;

/**
 * Evaluates condition trees against execution rows.
 */
public class ConditionEvaluator {

    public boolean evaluate(Condition condition, Row row) {
        if (condition == null) {
            return true;
        }
        if (condition instanceof CompoundCondition) {
            return evaluateCompound((CompoundCondition) condition, row);
        }
        if (condition instanceof SimpleCondition) {
            return evaluateSimple((SimpleCondition) condition, row);
        }
        throw new IllegalArgumentException("Unsupported condition type: " + condition.getClass().getName());
    }

    private boolean evaluateCompound(CompoundCondition condition, Row row) {
        if ("AND".equalsIgnoreCase(condition.getOperator())) {
            return evaluate(condition.getLeft(), row) && evaluate(condition.getRight(), row);
        }
        if ("OR".equalsIgnoreCase(condition.getOperator())) {
            return evaluate(condition.getLeft(), row) || evaluate(condition.getRight(), row);
        }
        throw new IllegalArgumentException("Unsupported compound operator: " + condition.getOperator());
    }

    private boolean evaluateSimple(SimpleCondition condition, Row row) {
        Object leftValue = resolveRowValue(row, condition.getColumn());
        if (leftValue == null) {
            return false;
        }

        Object rightValue = condition.isValueColumnReference()
            ? resolveRowValue(row, condition.getValue())
            : condition.getValue();
        if (rightValue == null) {
            return false;
        }

        String leftText = String.valueOf(leftValue);
        String rightText = String.valueOf(rightValue);
        switch (condition.getOperator().toUpperCase()) {
            case "=":
            case "==":
                return leftText.equals(rightText);
            case "!=":
            case "<>":
                return !leftText.equals(rightText);
            case ">":
                return compare(leftText, rightText) > 0;
            case ">=":
                return compare(leftText, rightText) >= 0;
            case "<":
                return compare(leftText, rightText) < 0;
            case "<=":
                return compare(leftText, rightText) <= 0;
            case "LIKE":
                return leftText.matches(rightText.replace("%", ".*").replace("_", "."));
            default:
                throw new IllegalArgumentException("Unsupported operator: " + condition.getOperator());
        }
    }

    private Object resolveRowValue(Row row, String columnName) {
        Object direct = row.getValue(columnName);
        if (direct != null || columnName == null || !columnName.contains(".")) {
            return direct;
        }
        return row.getValue(columnName.substring(columnName.lastIndexOf('.') + 1));
    }

    private int compare(String left, String right) {
        try {
            return Double.compare(Double.parseDouble(left), Double.parseDouble(right));
        } catch (NumberFormatException ignored) {
            return left.compareTo(right);
        }
    }
}
