package com.minisql.sql.ast;

/**
 * BETWEEN 条件（纯数据 AST 节点）。
 *
 * <p>表示 {@code column [NOT] BETWEEN low AND high}。
 * 求值逻辑在 {@link com.minisql.sql.execution.ConditionEvaluatorFactory} 中。
 */
public class BetweenCondition extends Condition {
    private final String column;
    private final String low;
    private final String high;
    private final boolean negated;

    public BetweenCondition(String column, String low, String high, boolean negated) {
        this.column = column;
        this.low = low;
        this.high = high;
        this.negated = negated;
    }

    public String getColumn() { return column; }
    public String getLow() { return low; }
    public String getHigh() { return high; }
    public boolean isNegated() { return negated; }
}
