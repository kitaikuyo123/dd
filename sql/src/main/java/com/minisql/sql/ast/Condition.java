package com.minisql.sql.ast;

import com.minisql.sql.execution.ConditionEvaluator;
import com.minisql.sql.execution.ConditionEvaluatorFactory;
import com.minisql.sql.execution.Row;

/**
 * 条件表达式基类（纯数据）。
 *
 * <p>子类只承载 AST 结构信息（列名、运算符、值等），不包含求值逻辑。
 * 求值通过 {@link ConditionEvaluatorFactory#create} 生成的
 * {@link ConditionEvaluator} 完成。
 *
 * <p>本类保留 {@link #evaluate} 作为过渡桥接方法，委托给工厂。
 * 新代码应直接使用 {@code ConditionEvaluatorFactory.create(condition)} 。
 */
public abstract class Condition {

    /**
     * 过渡桥接：委托给 {@link ConditionEvaluatorFactory}。
     *
     * @deprecated 请使用 {@code ConditionEvaluatorFactory.create(condition).evaluate(row)}
     */
    @Deprecated
    public boolean evaluate(Row row) {
        return ConditionEvaluatorFactory.create(this).evaluate(row);
    }
}
