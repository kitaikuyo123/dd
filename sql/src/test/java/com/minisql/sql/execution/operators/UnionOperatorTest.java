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

@DisplayName("UnionOperator 单元测试")
class UnionOperatorTest {

    @Test
    @DisplayName("合并两个数据源")
    void unionTwoSources() throws IOException {
        MockOperator src1 = new MockOperator(Arrays.asList(
            new Row(new String[]{"v"}, new Object[]{1}),
            new Row(new String[]{"v"}, new Object[]{2})
        ), new String[]{"v"});
        MockOperator src2 = new MockOperator(Arrays.asList(
            new Row(new String[]{"v"}, new Object[]{3}),
            new Row(new String[]{"v"}, new Object[]{4})
        ), new String[]{"v"});

        UnionOperator op = new UnionOperator(List.of(src1, src2));
        op.open();

        assertEquals(1, op.nextRow().getValue("v"));
        assertEquals(2, op.nextRow().getValue("v"));
        assertEquals(3, op.nextRow().getValue("v"));
        assertEquals(4, op.nextRow().getValue("v"));
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("合并空数据源")
    void unionEmptySources() throws IOException {
        MockOperator empty = new MockOperator(Collections.emptyList(), new String[]{"v"});
        MockOperator hasData = new MockOperator(List.of(
            new Row(new String[]{"v"}, new Object[]{1})
        ), new String[]{"v"});

        // 第一个空，第二个有数据
        UnionOperator op = new UnionOperator(List.of(empty, hasData));
        op.open();

        assertTrue(op.hasMore());
        assertEquals(1, op.nextRow().getValue("v"));
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("全部空数据源")
    void unionAllEmpty() throws IOException {
        MockOperator empty = new MockOperator(Collections.emptyList(), new String[]{"v"});
        UnionOperator op = new UnionOperator(List.of(empty, empty));
        op.open();
        assertFalse(op.hasMore());
        op.close();
    }

    @Test
    @DisplayName("getOutputColumns 返回第一个子算子的列")
    void getOutputColumns() {
        MockOperator src = new MockOperator(Collections.emptyList(), new String[]{"a", "b"});
        UnionOperator op = new UnionOperator(List.of(src));
        assertArrayEquals(new String[]{"a", "b"}, op.getOutputColumns());
    }

    @Test
    @DisplayName("空子列表 getOutputColumns 返回空数组")
    void getOutputColumnsEmpty() {
        UnionOperator op = new UnionOperator(Collections.emptyList());
        assertArrayEquals(new String[0], op.getOutputColumns());
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
