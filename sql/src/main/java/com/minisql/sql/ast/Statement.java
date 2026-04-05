package com.minisql.sql.ast;

/**
 * SQL AST 节点定义
 * 负责模块: 开发者C
 */

// 语句基类
public abstract class Statement {
    public abstract StatementType getType();

    public enum StatementType {
        SELECT, INSERT, UPDATE, DELETE, CREATE_TABLE, DROP_TABLE, SHOW_TABLES
    }
}
