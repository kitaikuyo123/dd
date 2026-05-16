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

    /**
     * 判断字符串是否为数字
     */
    protected static boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) return false;
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 格式化值：NULL / 数字原样 / 字符串加引号并转义
     */
    protected static String formatValue(String value) {
        if (value == null || value.isEmpty()) {
            return "NULL";
        } else if (isNumeric(value)) {
            return value;
        } else {
            return "'" + value.replace("'", "''") + "'";
        }
    }

    /**
     * 将 WHERE 条件转换为 SQL 字符串
     */
    protected static String whereToString(Condition condition) {
        if (condition instanceof SimpleCondition) {
            SimpleCondition sc = (SimpleCondition) condition;
            return sc.getColumn() + " " + sc.getOperator() + " " + formatValue(sc.getValue());
        }
        return "1=1"; // 默认条件
    }
}
