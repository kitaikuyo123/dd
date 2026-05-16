package com.minisql.sql.execution;

import java.io.IOException;
import java.util.Iterator;

/**
 * 查询执行算子基类
 * 实现 Volcano 迭代器模型
 */
public abstract class Operator implements Iterator<Row>, AutoCloseable {

    /**
     * 打开算子，准备执行
     */
    public abstract void open() throws IOException;

    /**
     * 关闭算子，释放资源
     */
    public abstract void close() throws IOException;

    /**
     * 检查是否还有下一条记录（Iterator 兼容）
     */
    @Override
    public boolean hasNext() {
        try {
            return hasMore();
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 获取下一条记录（Iterator 兼容）
     */
    @Override
    public Row next() {
        try {
            return nextRow();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取下一条记录
     * @return 下一行数据，如果没有更多数据返回 null
     */
    public abstract Row nextRow() throws IOException;

    /**
     * 检查是否还有更多数据
     */
    public abstract boolean hasMore() throws IOException;

    /**
     * 重置算子状态
     */
    public abstract void reset() throws IOException;

    /**
     * 获取算子返回的列名
     */
    public abstract String[] getOutputColumns();

    /**
     * 在列名数组中查找指定列的位置
     *
     * @param columns         列名数组
     * @param columnName      目标列名，支持 table.column 格式
     * @param throwIfNotFound true 时找不到抛异常，false 时返回 -1
     */
    protected static int findColumnIndex(String[] columns, String columnName, boolean throwIfNotFound) {
        String fallback = columnName != null && columnName.contains(".")
            ? columnName.substring(columnName.lastIndexOf('.') + 1)
            : columnName;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equalsIgnoreCase(columnName) || columns[i].equalsIgnoreCase(fallback)) {
                return i;
            }
        }
        if (throwIfNotFound) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        return -1;
    }
}
