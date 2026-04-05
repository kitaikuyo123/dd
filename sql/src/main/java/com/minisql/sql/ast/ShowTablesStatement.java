package com.minisql.sql.ast;

/**
 * SHOW TABLES 语句
 */
public class ShowTablesStatement extends Statement {

    @Override
    public StatementType getType() {
        return StatementType.SHOW_TABLES;
    }
}
