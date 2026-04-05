package com.minisql.sql.execution.operators;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Table;
import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;
import com.minisql.storage.StorageEngine;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Local scan operator.
 */
public class LocalScanOperator extends Operator {

    private final StorageEngine storage;
    private final Table tableSchema;
    private final byte[] startKey;
    private final byte[] endKey;
    private final String[] outputColumns;

    private Iterator<KeyValue> kvIterator;
    private Iterator<Row> rowIterator;
    private boolean opened;

    public LocalScanOperator(StorageEngine storage, Table tableSchema,
                             byte[] startKey, byte[] endKey, String[] outputColumns) {
        this.storage = storage;
        this.tableSchema = tableSchema;
        this.startKey = startKey;
        this.endKey = endKey;
        this.outputColumns = outputColumns;
    }

    public LocalScanOperator(StorageEngine storage, Table tableSchema,
                             byte[] startKey, byte[] endKey) {
        this(storage, tableSchema, startKey, endKey, null);
    }

    @Override
    public void open() throws IOException {
        if (opened) {
            return;
        }
        if (storage == null) {
            throw new IOException("StorageEngine is null");
        }
        kvIterator = storage.scan(startKey, endKey);
        rowIterator = new KeyValueRowIterator(kvIterator, tableSchema, getOutputColumns());
        opened = true;
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            open();
        }
        return rowIterator != null && rowIterator.hasNext() ? rowIterator.next() : null;
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) {
            open();
        }
        return rowIterator != null && rowIterator.hasNext();
    }

    @Override
    public void close() {
        opened = false;
        kvIterator = null;
        rowIterator = null;
    }

    @Override
    public void reset() throws IOException {
        close();
        open();
    }

    @Override
    public String[] getOutputColumns() {
        if (outputColumns != null && outputColumns.length > 0) {
            return outputColumns;
        }
        if (tableSchema != null && tableSchema.getColumns() != null) {
            List<com.minisql.common.model.Column> columns = tableSchema.getColumns();
            String[] names = new String[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                names[i] = columns.get(i).getName();
            }
            return names;
        }
        return new String[0];
    }

    public StorageEngine getStorage() {
        return storage;
    }

    public Table getTableSchema() {
        return tableSchema;
    }
}
