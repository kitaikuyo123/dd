package com.minisql.sql.ast;

/**
 * 复合条件（纯数据 AST 节点）。
 *
 * <p>用 AND / OR 连接两个子条件。
 * 求值逻辑已迁移到
 * {@link com.minisql.sql.execution.ConditionEvaluatorFactory#createCompound}。
 */
public class CompoundCondition extends Condition {
    private final Condition left;
    private final Condition right;
    private final String operator;  // "AND" 或 "OR"

    public CompoundCondition(Condition left, Condition right, String operator) {
        this.left = left;
        this.right = right;
        this.operator = operator;
    }

    public Condition getLeft() {
        return left;
    }

    public Condition getRight() {
        return right;
    }

    public String getOperator() {
        return operator;
    }
}
