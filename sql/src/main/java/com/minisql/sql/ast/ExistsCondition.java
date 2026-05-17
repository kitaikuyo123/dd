package com.minisql.sql.ast;

/**
 * EXISTS 子查询条件（纯数据 AST 节点）。
 *
 * <p>表示 {@code [NOT] EXISTS (SELECT ...)}。
 * 子查询在执行前预求值，结果缓存在 hasResults 中。
 */
public class ExistsCondition extends Condition {
    private final SubqueryExpression subquery;
    private final boolean negated;

    /** 子查询预求值结果，由执行层注入 */
    private Boolean hasResults;

    public ExistsCondition(SubqueryExpression subquery, boolean negated) {
        this.subquery = subquery;
        this.negated = negated;
    }

    public SubqueryExpression getSubquery() { return subquery; }
    public boolean isNegated() { return negated; }
    public Boolean getHasResults() { return hasResults; }
    public void setHasResults(Boolean hasResults) { this.hasResults = hasResults; }
}
