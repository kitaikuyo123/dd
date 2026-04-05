package com.minisql.sql.execution.operators;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.RowAssembler;
import com.minisql.common.model.Table;
import com.minisql.sql.execution.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Groups ordered key-values by row key and materializes SQL rows.
 */
public class KeyValueRowIterator implements Iterator<Row> {

    private final Iterator<KeyValue> source;
    private final Table schema;
    private final String[] outputColumns;

    private KeyValue buffered;
    private Row nextRow;

    public KeyValueRowIterator(Iterator<KeyValue> source, Table schema, String[] outputColumns) {
        this.source = source;
        this.schema = schema;
        this.outputColumns = outputColumns;
    }

    @Override
    public boolean hasNext() {
        if (nextRow != null) {
            return true;
        }
        nextRow = fetchNextRow();
        return nextRow != null;
    }

    @Override
    public Row next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Row result = nextRow;
        nextRow = null;
        return result;
    }

    private Row fetchNextRow() {
        List<KeyValue> rowValues = new ArrayList<>();
        KeyValue first = nextKeyValue();
        while (first != null && first.isDelete()) {
            first = nextKeyValue();
        }
        if (first == null) {
            return null;
        }

        byte[] currentRowKey = first.getRowKey();
        rowValues.add(first);
        while (true) {
            KeyValue next = nextKeyValue();
            if (next == null) {
                break;
            }
            if (!java.util.Arrays.equals(currentRowKey, next.getRowKey())) {
                buffered = next;
                break;
            }
            if (!next.isDelete()) {
                rowValues.add(next);
            }
        }

        com.minisql.common.model.Row commonRow = RowAssembler.mergeToRow(rowValues, schema);
        return commonRow == null ? null : RowConverters.toSqlRow(commonRow, outputColumns);
    }

    private KeyValue nextKeyValue() {
        if (buffered != null) {
            KeyValue result = buffered;
            buffered = null;
            return result;
        }
        return source.hasNext() ? source.next() : null;
    }
}
