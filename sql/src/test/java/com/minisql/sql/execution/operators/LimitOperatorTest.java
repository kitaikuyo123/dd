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

@DisplayName("LimitOperator 单元测试")
class LimitOperatorTest {

    private static MockOperator rows(Object... vals) {
        Row[] arr = new Row[vals.length];
        for (int i = 0; i < vals.length; i++) {
            arr[i] = new Row(new String[]{"v"}, new Object[]{vals[i]});
        }
        return new MockOperator(Arrays.asList(arr), new String[]{"v"});
    }

    @Test
    @DisplayName("LIMIT 只取前 N 行")
    void limitOnly() throws IOException {
        LimitOperator op = new LimitOperator(rows(1, 2, 3, 4, 5), 3);
        op.open();

        assertEquals(1, op.nextRow().getValue("v"));
        assertEquals(2, op.nextRow().getValue("v"));
        assertEquals(3, op.nextRow().getValue("v"));
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("LIMIT + OFFSET 跳过前 M 行再取 N 行")
    void limitWithOffset() throws IOException {
        LimitOperator op = new LimitOperator(rows(1, 2, 3, 4, 5), 2, 2);
        op.open();

        assertEquals(3, op.nextRow().getValue("v"));
        assertEquals(4, op.nextRow().getValue("v"));
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("LIMIT 超过总行数返回所有行")
    void limitExceedsRowCount() throws IOException {
        LimitOperator op = new LimitOperator(rows(1, 2), 100);
        op.open();

        assertEquals(1, op.nextRow().getValue("v"));
        assertEquals(2, op.nextRow().getValue("v"));
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("OFFSET 超过总行数返回空")
    void offsetExceedsRowCount() throws IOException {
        LimitOperator op = new LimitOperator(rows(1, 2), 10, 100);
        op.open();
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("LIMIT 0 返回空")
    void zeroLimit() throws IOException {
        LimitOperator op = new LimitOperator(rows(1, 2, 3), 0);
        op.open();
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("getOutputColumns 透传")
    void getOutputColumns() {
        LimitOperator op = new LimitOperator(rows(1), 1);
        assertArrayEquals(new String[]{"v"}, op.getOutputColumns());
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
