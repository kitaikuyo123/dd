package com.minisql.sql.ast;

/**
 * 简单比较条件（纯数据 AST 节点）。
 *
 * <p>表示 {@code 列名 运算符 值} 的二元比较。
 * 当 {@code valueColumnReference} 为 true 时，右侧值是列引用而非字面量。
 *
 * <p>求值逻辑已迁移到
 * {@link com.minisql.sql.execution.ConditionEvaluatorFactory#createSimple}。
 */
public class SimpleCondition extends Condition {
    private final String column;
    private final String operator;
    private final String value;
    private final boolean valueColumnReference;

    public SimpleCondition(String column, String operator, String value) {
        this(column, operator, value, false);
    }

    public SimpleCondition(String column, String operator, String value, boolean valueColumnReference) {
        this.column = column;
        this.operator = operator;
        this.value = value;
        this.valueColumnReference = valueColumnReference;
    }

    public String getColumn() {
        return column;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }

    public boolean isValueColumnReference() {
        return valueColumnReference;
    }
}
