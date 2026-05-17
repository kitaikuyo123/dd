package com.minisql.sql.ast;

import java.util.List;

/**
 * IN 条件（纯数据 AST 节点）。
 *
 * <p>表示 {@code column [NOT] IN (v1, v2, ...)}。
 * 求值逻辑在 {@link com.minisql.sql.execution.ConditionEvaluatorFactory} 中。
 */
public class InCondition extends Condition {
    private final String column;
    private final List<String> values;
    private final boolean negated;

    public InCondition(String column, List<String> values, boolean negated) {
        this.column = column;
        this.values = values;
        this.negated = negated;
    }

    public String getColumn() { return column; }
    public List<String> getValues() { return values; }
    public boolean isNegated() { return negated; }
}
