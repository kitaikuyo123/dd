package com.minisql.sql.execution;

/**
 * 条件求值器。
 *
 * <p>从 AST 条件节点中分离出的运行时求值接口，
 * 保持 AST 节点作为纯数据结构，求值逻辑集中在实现类中。
 *
 * @see com.minisql.sql.execution.ConditionEvaluatorFactory
 */
@FunctionalInterface
public interface ConditionEvaluator {
    boolean evaluate(Row row);
}
