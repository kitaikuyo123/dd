package com.minisql.sql.ast;

import com.minisql.sql.execution.Row;

/**
 * 复合条件
 */
public class CompoundCondition extends Condition {
    private Condition left;
    private Condition right;
    private String operator;  // AND or OR

    public CompoundCondition(Condition left, Condition right, String operator) {
        this.left = left;
        this.right = right;
        this.operator = operator;
    }

    @Override
    public boolean evaluate(Row row) {
        boolean leftResult = left.evaluate(row);
        boolean rightResult = right.evaluate(row);

        if ("AND".equals(operator)) {
            return leftResult && rightResult;
        } else {
            return leftResult || rightResult;
        }
    }

    // Getters
    public Condition getLeft() { return left; }
    public Condition getRight() { return right; }
    public String getOperator() { return operator; }
}
