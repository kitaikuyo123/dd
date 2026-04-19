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

@DisplayName("ProjectOperator 单元测试")
class ProjectOperatorTest {

    private static final String[] COLS = new String[]{"id", "name", "age"};

    private MockOperator source(Row... rows) {
        return new MockOperator(Arrays.asList(rows), COLS);
    }

    @Test
    @DisplayName("投影指定列")
    void projectColumns() throws IOException {
        ProjectOperator op = new ProjectOperator(
            source(new Row(COLS, new Object[]{1, "alice", 20})),
            List.of("name", "age")
        );

        op.open();
        assertTrue(op.hasMore());
        Row row = op.nextRow();
        assertEquals("alice", row.getValue("name"));
        assertEquals(20, row.getValue("age"));
        assertNull(row.getValue("id")); // id 被排除
        op.close();
    }

    @Test
    @DisplayName("投影列顺序可重排")
    void projectReorder() throws IOException {
        ProjectOperator op = new ProjectOperator(
            source(new Row(COLS, new Object[]{1, "alice", 20})),
            List.of("age", "name")
        );

        op.open();
        Row row = op.nextRow();
        String[] cols = row.getColumns();
        assertEquals("age", cols[0]);
        assertEquals("name", cols[1]);
        op.close();
    }

    @Test
    @DisplayName("投影空输入")
    void projectEmptyInput() throws IOException {
        ProjectOperator op = new ProjectOperator(
            new MockOperator(Collections.emptyList(), COLS),
            List.of("name")
        );

        op.open();
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("getOutputColumns 返回投影列")
    void getOutputColumns() {
        ProjectOperator op = new ProjectOperator(
            source(new Row(COLS, new Object[]{1, "a", 1})),
            List.of("name", "age")
        );
        String[] cols = op.getOutputColumns();
        assertArrayEquals(new String[]{"name", "age"}, cols);
    }

    @Test
    @DisplayName("selectAll 透传所有列")
    void selectAll() throws IOException {
        ProjectOperator op = new ProjectOperator(
            source(new Row(COLS, new Object[]{1, "alice", 20})),
            null, true
        );

        op.open();
        Row row = op.nextRow();
        assertEquals(1, row.getValue("id"));
        assertEquals("alice", row.getValue("name"));
        assertEquals(20, row.getValue("age"));
        op.close();
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
