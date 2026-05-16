package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SortOperator 单元测试")
class SortOperatorTest {

    @Test
    @DisplayName("升序排序")
    void sortAscending() throws IOException {
        SortOperator op = new SortOperator(
            mockRows(
                new Row(new String[]{"name"}, new Object[]{"charlie"}),
                new Row(new String[]{"name"}, new Object[]{"alice"}),
                new Row(new String[]{"name"}, new Object[]{"bob"})
            ),
            List.of(new SortOperator.SortKey("name", true))
        );

        op.open();
        assertEquals("alice", op.nextRow().getValue("name"));
        assertEquals("bob", op.nextRow().getValue("name"));
        assertEquals("charlie", op.nextRow().getValue("name"));
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("降序排序")
    void sortDescending() throws IOException {
        SortOperator op = new SortOperator(
            mockRows(
                new Row(new String[]{"age"}, new Object[]{10}),
                new Row(new String[]{"age"}, new Object[]{30}),
                new Row(new String[]{"age"}, new Object[]{20})
            ),
            List.of(new SortOperator.SortKey("age", false))
        );

        op.open();
        assertEquals(30, op.nextRow().getValue("age"));
        assertEquals(20, op.nextRow().getValue("age"));
        assertEquals(10, op.nextRow().getValue("age"));
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("空输入排序")
    void sortEmptyInput() throws IOException {
        SortOperator op = new SortOperator(
            new MockOperator(Collections.emptyList(), new String[]{"name"}),
            List.of(new SortOperator.SortKey("name", true))
        );

        op.open();
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("单行排序")
    void sortSingleRow() throws IOException {
        SortOperator op = new SortOperator(
            mockRows(new Row(new String[]{"name"}, new Object[]{"alice"})),
            List.of(new SortOperator.SortKey("name", true))
        );

        op.open();
        assertTrue(op.hasMore());
        assertEquals("alice", op.nextRow().getValue("name"));
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("null 值排在最后")
    void sortWithNulls() throws IOException {
        SortOperator op = new SortOperator(
            mockRows(
                new Row(new String[]{"val"}, new Object[]{3}),
                new Row(new String[]{"val"}, new Object[]{null}),
                new Row(new String[]{"val"}, new Object[]{1})
            ),
            List.of(new SortOperator.SortKey("val", true))
        );

        op.open();
        assertEquals(1, op.nextRow().getValue("val"));
        assertEquals(3, op.nextRow().getValue("val"));
        assertNull(op.nextRow().getValue("val"));
        op.close();
    }

    @Test
    @DisplayName("getOutputColumns 透传子算子列")
    void getOutputColumns() {
        SortOperator op = new SortOperator(
            mockRows(new Row(new String[]{"a", "b"}, new Object[]{1, 2})),
            List.of(new SortOperator.SortKey("a", true))
        );
        String[] cols = op.getOutputColumns();
        assertArrayEquals(new String[]{"a", "b"}, cols);
    }

    @Test
    @DisplayName("reset 重新排序")
    void reset() throws IOException {
        SortOperator op = new SortOperator(
            mockRows(
                new Row(new String[]{"v"}, new Object[]{2}),
                new Row(new String[]{"v"}, new Object[]{1})
            ),
            List.of(new SortOperator.SortKey("v", true))
        );

        op.open();
        assertEquals(1, op.nextRow().getValue("v"));
        op.reset();
        assertEquals(1, op.nextRow().getValue("v"));
        op.close();
    }

    // ---- helper ----

    private static MockOperator mockRows(Row... rows) {
        return new MockOperator(Arrays.asList(rows), rows.length > 0 ? rows[0].getColumns() : new String[0]);
    }

    private static class MockOperator extends Operator {
        private final List<Row> rows;
        private final String[] columns;
        private int index;

        MockOperator(List<Row> rows, String[] columns) {
            this.rows = rows;
            this.columns = columns;
        }

        @Override public void open() { index = 0; }
        @Override public void close() { index = rows.size(); }
        @Override public Row nextRow() { return hasMore() ? rows.get(index++) : null; }
        @Override public boolean hasMore() { return index < rows.size(); }
        @Override public void reset() { index = 0; }
        @Override public String[] getOutputColumns() { return columns; }
    }
}
