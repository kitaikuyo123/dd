package com.minisql.sql.ast;

import java.util.Set;

/**
 * IN 子查询条件（纯数据 AST 节点）。
 *
 * <p>表示 {@code column [NOT] IN (SELECT ...)}。
 * 子查询在执行前预求值，结果缓存在 resolvedValues 中。
 */
public class InSubqueryCondition extends Condition {
    private final String column;
    private final SubqueryExpression subquery;
    private final boolean negated;

    /** 子查询预求值结果，由执行层注入 */
    private Set<Object> resolvedValues;

    public InSubqueryCondition(String column, SubqueryExpression subquery, boolean negated) {
        this.column = column;
        this.subquery = subquery;
        this.negated = negated;
    }

    public String getColumn() { return column; }
    public SubqueryExpression getSubquery() { return subquery; }
    public boolean isNegated() { return negated; }
    public Set<Object> getResolvedValues() { return resolvedValues; }
    public void setResolvedValues(Set<Object> resolvedValues) { this.resolvedValues = resolvedValues; }
}
