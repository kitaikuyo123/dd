package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;

import java.io.IOException;

/**
 * 限制算子
 * 实现 LIMIT 和 OFFSET
 */
public class LimitOperator extends Operator {

    private final Operator child;
    private final int limit;
    private final int offset;

    private int count;
    private boolean opened;

    public LimitOperator(Operator child, int limit) {
        this(child, limit, 0);
    }

    public LimitOperator(Operator child, int limit, int offset) {
        this.child = child;
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public void open() throws IOException {
        child.open();
        opened = true;
        count = 0;

        // 跳过 offset 行
        for (int i = 0; i < offset && child.hasMore(); i++) {
            child.nextRow();
        }
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            open();
        }

        if (count >= limit) {
            return null;
        }

        Row row = child.nextRow();
        if (row != null) {
            count++;
        }
        return row;
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) {
            open();
        }
        return count < limit && child.hasMore();
    }

    @Override
    public void close() throws IOException {
        opened = false;
        child.close();
    }

    @Override
    public void reset() throws IOException {
        close();
        open();
    }

    @Override
    public String[] getOutputColumns() {
        return child.getOutputColumns();
    }
}
