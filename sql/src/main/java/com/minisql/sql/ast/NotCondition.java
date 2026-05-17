package com.minisql.sql.ast;

/**
 * NOT 条件（纯数据 AST 节点）。
 *
 * <p>表示 {@code NOT innerCondition}。
 * 求值逻辑在 {@link com.minisql.sql.execution.ConditionEvaluatorFactory} 中。
 */
public class NotCondition extends Condition {
    private final Condition inner;

    public NotCondition(Condition inner) {
        this.inner = inner;
    }

    public Condition getInner() { return inner; }
}
