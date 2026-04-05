package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;

import java.io.IOException;
import java.util.*;

/**
 * 投影算子
 * 选择特定的列
 */
public class ProjectOperator extends Operator {

    private final Operator child;
    private final List<String> columns;
    private final boolean selectAll;
    private int[] columnIndices;
    private boolean opened;

    public ProjectOperator(Operator child, List<String> columns) {
        this(child, columns, false);
    }

    public ProjectOperator(Operator child, List<String> columns, boolean selectAll) {
        this.child = child;
        this.columns = columns != null ? columns : Collections.emptyList();
        this.selectAll = selectAll;
    }

    @Override
    public void open() throws IOException {
        child.open();
        opened = true;

        if (!selectAll && !columns.isEmpty()) {
            // 计算列索引映射
            String[] inputColumns = child.getOutputColumns();
            columnIndices = new int[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                columnIndices[i] = findColumnIndex(inputColumns, columns.get(i));
            }
        }
    }

    private int findColumnIndex(String[] columns, String target) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equalsIgnoreCase(target)) {
                return i;
            }
        }
        return -1;  // 未找到
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            open();
        }

        Row inputRow = child.nextRow();
        if (inputRow == null) {
            return null;
        }

        if (selectAll) {
            return inputRow;
        }

        // 投影选定的列
        Object[] projectedValues = new Object[columnIndices.length];
        String[] projectedColumns = new String[columnIndices.length];

        for (int i = 0; i < columnIndices.length; i++) {
            int idx = columnIndices[i];
            if (idx >= 0 && idx < inputRow.getColumnCount()) {
                projectedValues[i] = inputRow.getValue(idx);
                projectedColumns[i] = columns.get(i);
            } else {
                projectedValues[i] = null;
                projectedColumns[i] = columns.get(i);
            }
        }

        return new Row(projectedColumns, projectedValues, inputRow.getRowKey());
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) {
            open();
        }
        return child.hasMore();
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
        if (selectAll) {
            return child.getOutputColumns();
        }
        return columns.toArray(new String[0]);
    }
}
