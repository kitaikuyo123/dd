package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Operator that wraps a pre-fetched list of common.model.Row objects.
 * Used as the entry point into the Operator pipeline after distributed scan.
 */
public class ListSourceOperator extends Operator {

    private final List<com.minisql.common.model.Row> sourceRows;
    private final String[] outputColumns;
    private Iterator<com.minisql.common.model.Row> iterator;
    private boolean opened;

    public ListSourceOperator(List<com.minisql.common.model.Row> sourceRows,
                              String[] outputColumns) {
        this.sourceRows = sourceRows;
        this.outputColumns = outputColumns;
    }

    @Override
    public void open() throws IOException {
        iterator = sourceRows.iterator();
        opened = true;
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            open();
        }
        if (!iterator.hasNext()) {
            return null;
        }
        com.minisql.common.model.Row src = iterator.next();
        Object[] values = new Object[outputColumns.length];
        for (int i = 0; i < outputColumns.length; i++) {
            values[i] = resolveValue(src, outputColumns[i]);
        }
        return new Row(outputColumns, values, src.getRowKey());
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
        iterator = null;
    }

    @Override
    public void reset() throws IOException {
        close();
        open();
    }

    @Override
    public String[] getOutputColumns() {
        return outputColumns;
    }

    private static Object resolveValue(com.minisql.common.model.Row src, String columnName) {
        if (src.hasColumn(columnName)) {
            return src.getColumn(columnName);
        }
        int dot = columnName.indexOf('.');
        if (dot >= 0) {
            String unqualified = columnName.substring(dot + 1);
            if (src.hasColumn(unqualified)) {
                return src.getColumn(unqualified);
            }
        }
        for (String existing : src.getColumnNames()) {
            if (existing.equalsIgnoreCase(columnName)) {
                return src.getColumn(existing);
            }
        }
        return null;
    }
}
