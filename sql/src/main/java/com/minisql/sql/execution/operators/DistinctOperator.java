package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;

/**
 * DISTINCT 去重算子。
 *
 * <p>在 {@code open()} 阶段物化子算子的所有行，
 * 基于 {@link RowKey}（行值数组的 equals/hashCode）去重，
 * 然后迭代输出去重后的行。
 *
 * <p>管道位置：Sort → Project → <b>Distinct</b> → Limit
 */
public class DistinctOperator extends Operator {

    private final Operator child;
    private Iterator<Row> distinctIterator;
    private Row nextRow;
    private boolean opened;

    public DistinctOperator(Operator child) {
        this.child = child;
    }

    @Override
    public void open() throws IOException {
        if (opened) return;
        child.open();

        // 物化 + 去重
        LinkedHashSet<RowKey> seen = new LinkedHashSet<>();
        List<Row> rows = new ArrayList<>();
        while (child.hasMore()) {
            Row row = child.nextRow();
            if (row != null) {
                RowKey key = new RowKey(row.getValues());
                if (seen.add(key)) {
                    rows.add(row);
                }
            }
        }
        child.close();
        distinctIterator = rows.iterator();
        opened = true;
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) open();
        return nextRow;
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) open();
        if (distinctIterator != null && distinctIterator.hasNext()) {
            nextRow = distinctIterator.next();
            return true;
        }
        nextRow = null;
        return false;
    }

    @Override
    public void close() throws IOException {
        distinctIterator = null;
        opened = false;
    }

    @Override
    public void reset() throws IOException {
        close();
    }

    @Override
    public String[] getOutputColumns() {
        return child.getOutputColumns();
    }

    /** 基于 Object[] 的去重键 */
    private static class RowKey {
        final Object[] values;

        RowKey(Object[] values) {
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RowKey)) return false;
            return Arrays.equals(values, ((RowKey) o).values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
