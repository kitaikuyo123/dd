package com.minisql.sql.execution.operators;

import com.minisql.common.model.Column;
import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Table;
import com.minisql.sql.execution.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueRowIteratorTest {

    @Test
    void keepsFirstCellOfNextRowWhenGrouping() {
        Table schema = new Table("users");
        schema.setPrimaryKey("id");
        schema.addColumn(new Column("id", Column.ColumnType.STRING));
        schema.addColumn(new Column("name", Column.ColumnType.STRING));

        List<KeyValue> values = List.of(
            new KeyValue.Builder("row1".getBytes()).family("").qualifier("name").value("alice".getBytes()).build(),
            new KeyValue.Builder("row2".getBytes()).family("").qualifier("name").value("bob".getBytes()).build()
        );

        KeyValueRowIterator iterator = new KeyValueRowIterator(values.iterator(), schema, new String[] {"id", "name"});
        Row first = iterator.next();
        Row second = iterator.next();

        assertEquals("alice", first.getValue("name"));
        assertEquals("bob", second.getValue("name"));
        assertTrue(Arrays.equals("row1".getBytes(), first.getRowKey()));
        assertTrue(Arrays.equals("row2".getBytes(), second.getRowKey()));
    }
}
