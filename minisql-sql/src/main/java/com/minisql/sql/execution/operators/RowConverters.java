package com.minisql.sql.execution.operators;

import com.minisql.common.model.Row;
import com.minisql.sql.execution.Operator;

/**
 * Row 转换工具类
 * 提供不同 Row 类型之间的转换方法
 */
public class RowConverters {

    private RowConverters() {
        // 工具类，禁止实例化
    }

    /**
     * 将 common.model.Row 转换为 sql.execution.Row
     *
     * @param commonRow 源 Row 对象
     * @param outputColumns 输出列名数组
     * @return 转换后的 sql.execution.Row
     */
    public static com.minisql.sql.execution.Row toSqlRow(
            com.minisql.common.model.Row commonRow,
            String[] outputColumns) {
        if (commonRow == null) {
            return null;
        }

        Object[] values = new Object[outputColumns.length];
        for (int i = 0; i < outputColumns.length; i++) {
            values[i] = commonRow.getColumn(outputColumns[i]);
        }

        return new com.minisql.sql.execution.Row(outputColumns, values, commonRow.getRowKey());
    }

    /**
     * 将 common.model.Row 转换为 sql.execution.Row（从算子获取列名）
     *
     * @param commonRow 源 Row 对象
     * @param operator 提供输出列名的算子
     * @return 转换后的 sql.execution.Row
     */
    public static com.minisql.sql.execution.Row toSqlRow(
            com.minisql.common.model.Row commonRow,
            Operator operator) {
        if (commonRow == null) {
            return null;
        }
        return toSqlRow(commonRow, operator.getOutputColumns());
    }
}
