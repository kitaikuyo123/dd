package com.minisql.sql.ast;

/**
 * 子查询表达式（纯数据 AST 节点）。
 *
 * <p>包装一个嵌套的 {@link SelectStatement}，
 * 用于 {@code IN (SELECT ...)} 和 {@code EXISTS (SELECT ...)}。
 */
public class SubqueryExpression {
    private final SelectStatement selectStatement;

    public SubqueryExpression(SelectStatement selectStatement) {
        this.selectStatement = selectStatement;
    }

    public SelectStatement getSelectStatement() { return selectStatement; }
}
