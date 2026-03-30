package com.minisql.sql.ast;

import com.minisql.sql.execution.Row;

/**
 * 条件表达式
 */
public abstract class Condition {
    public abstract boolean evaluate(Row row);
}
