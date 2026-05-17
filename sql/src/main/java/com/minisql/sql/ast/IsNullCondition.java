package com.minisql.sql.ast;

/**
 * IS NULL / IS NOT NULL 条件（纯数据 AST 节点）。
 *
 * <p>表示 {@code column IS [NOT] NULL}。
 * 求值逻辑在 {@link com.minisql.sql.execution.ConditionEvaluatorFactory} 中。
 */
public class IsNullCondition extends Condition {
    private final String column;
    private final boolean negated;

    public IsNullCondition(String column, boolean negated) {
        this.column = column;
        this.negated = negated;
    }

    public String getColumn() { return column; }
    public boolean isNegated() { return negated; }
}
