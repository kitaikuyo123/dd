package com.minisql.sql.execution.operators;

import com.minisql.common.utils.ValueComparator;
import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;

import java.io.IOException;
import java.util.*;

/**
 * 排序算子
 * 实现 ORDER BY
 */
public class SortOperator extends Operator {

    private final Operator child;
    private final List<SortKey> sortKeys;

    private List<Row> sortedRows;
    private Iterator<Row> iterator;
    private boolean opened;

    public SortOperator(Operator child, List<SortKey> sortKeys) {
        this.child = child;
        this.sortKeys = sortKeys;
    }

    @Override
    public void open() throws IOException {
        child.open();
        opened = true;

        // 加载所有数据到内存
        sortedRows = new ArrayList<>();
        while (child.hasMore()) {
            sortedRows.add(child.nextRow());
        }

        // 排序
        String[] columns = child.getOutputColumns();
        int[] keyIndices = new int[sortKeys.size()];
        boolean[] ascending = new boolean[sortKeys.size()];

        for (int i = 0; i < sortKeys.size(); i++) {
            keyIndices[i] = findColumnIndex(columns, sortKeys.get(i).getColumn());
            ascending[i] = sortKeys.get(i).isAscending();
        }

        sortedRows.sort((r1, r2) -> {
            for (int i = 0; i < keyIndices.length; i++) {
                int idx = keyIndices[i];
                Object v1 = r1.getValue(idx);
                Object v2 = r2.getValue(idx);

                int cmp = ValueComparator.compare(v1, v2);

                if (!ascending[i]) {
                    cmp = -cmp;
                }

                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });

        iterator = sortedRows.iterator();
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            open();
        }
        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) {
            open();
        }
        return iterator.hasNext();
    }

    @Override
    public void close() throws IOException {
        opened = false;
        sortedRows = null;
        iterator = null;
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

    private int findColumnIndex(String[] columns, String columnName) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    /**
     * 排序键
     */
    public static class SortKey {
        private final String column;
        private final boolean ascending;

        public SortKey(String column, boolean ascending) {
            this.column = column;
            this.ascending = ascending;
        }

        public String getColumn() {
            return column;
        }

        public boolean isAscending() {
            return ascending;
        }
    }
}
