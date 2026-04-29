package com.minisql.sql.execution;

import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SimpleCondition;

/**
 * 条件表达式求值器
 *
 * 对 WHERE 条件树进行递归求值，支持:
 *   - 复合条件（AND / OR）的短路求值
 *   - 简单比较条件（=, !=, >, >=, <, <=, LIKE）
 *   - 列间比较（用于 JOIN ON 条件）
 */
public class ConditionEvaluator {

    /**
     * 对条件树求值
     *
     * @param condition 条件树根节点，null 时返回 true
     * @param row       当前数据行
     * @return 条件是否成立
     */
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

    /** 递归求值复合条件，AND 短路返回 false，OR 短路返回 true */
    private boolean evaluateCompound(CompoundCondition condition, Row row) {
        if ("AND".equalsIgnoreCase(condition.getOperator())) {
            return evaluate(condition.getLeft(), row) && evaluate(condition.getRight(), row);
        }
        if ("OR".equalsIgnoreCase(condition.getOperator())) {
            return evaluate(condition.getLeft(), row) || evaluate(condition.getRight(), row);
        }
        throw new IllegalArgumentException("Unsupported compound operator: " + condition.getOperator());
    }

    /**
     * 求值简单比较条件
     *
     * 右侧值可以是字面量或列引用（通过 isValueColumnReference 判断）
     * 比较时统一转为字符串，数值类型优先按数值比较
     */
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

    /** 解析行中的列值，支持 table.column 格式的列名回退查找 */
    private Object resolveRowValue(Row row, String columnName) {
        Object direct = row.getValue(columnName);
        if (direct != null || columnName == null || !columnName.contains(".")) {
            return direct;
        }
        return row.getValue(columnName.substring(columnName.lastIndexOf('.') + 1));
    }

    /** 比较两个字符串，优先尝试数值比较，失败则回退到字典序比较 */
    private int compare(String left, String right) {
        try {
            return Double.compare(Double.parseDouble(left), Double.parseDouble(right));
        } catch (NumberFormatException ignored) {
            return left.compareTo(right);
        }
    }
}
