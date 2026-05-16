package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.JoinType;
import com.minisql.sql.execution.Row;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("JoinOperator 单元测试")
class JoinOperatorTest {

    // ---- 原有测试 ----

    @Test
    @DisplayName("INNER JOIN EQUALS: hasMore 不消耗首行，连接结果正确")
    void hasMoreDoesNotConsumeFirstRow() throws IOException {
        JoinOperator operator = new JoinOperator(
            new MockOperator(List.of(
                new Row(new String[] {"id", "name"}, new Object[] {1, "alice"}),
                new Row(new String[] {"id", "name"}, new Object[] {2, "bob"})
            )),
            new MockOperator(List.of(
                new Row(new String[] {"user_id", "city"}, new Object[] {1, "shanghai"}),
                new Row(new String[] {"user_id", "city"}, new Object[] {2, "beijing"})
            )),
            JoinType.INNER,
            new JoinOperator.JoinCondition("id", "user_id", JoinOperator.JoinOperatorType.EQUALS)
        );

        operator.open();
        assertTrue(operator.hasMore());
        Row first = operator.nextRow();
        assertEquals(1, first.getValue("id"));
        assertEquals("shanghai", first.getValue("city"));

        assertTrue(operator.hasMore());
        Row second = operator.nextRow();
        assertEquals(2, second.getValue("id"));
        assertEquals("beijing", second.getValue("city"));
        assertFalse(operator.hasMore());
    }

    // ---- 补充测试 ----

    @Test
    @DisplayName("INNER JOIN 无匹配行返回空")
    void innerJoinNoMatch() throws IOException {
        JoinOperator operator = new JoinOperator(
            new MockOperator(List.of(
                new Row(new String[] {"id"}, new Object[] {1})
            )),
            new MockOperator(List.of(
                new Row(new String[] {"user_id"}, new Object[] {99})
            )),
            JoinType.INNER,
            new JoinOperator.JoinCondition("id", "user_id", JoinOperator.JoinOperatorType.EQUALS)
        );

        operator.open();
        assertFalse(operator.hasMore());
    }

    @Test
    @DisplayName("INNER JOIN 左表为空返回空")
    void innerJoinEmptyLeft() throws IOException {
        JoinOperator operator = new JoinOperator(
            new MockOperator(Collections.emptyList(), new String[] {"id"}),
            new MockOperator(List.of(
                new Row(new String[] {"user_id"}, new Object[] {1})
            ), new String[] {"user_id"}),
            JoinType.INNER,
            new JoinOperator.JoinCondition("id", "user_id", JoinOperator.JoinOperatorType.EQUALS)
        );

        operator.open();
        assertFalse(operator.hasMore());
    }

    @Test
    @DisplayName("INNER JOIN 右表为空返回空")
    void innerJoinEmptyRight() throws IOException {
        JoinOperator operator = new JoinOperator(
            new MockOperator(List.of(
                new Row(new String[] {"id"}, new Object[] {1})
            ), new String[] {"id"}),
            new MockOperator(Collections.emptyList(), new String[] {"user_id"}),
            JoinType.INNER,
            new JoinOperator.JoinCondition("id", "user_id", JoinOperator.JoinOperatorType.EQUALS)
        );

        operator.open();
        assertFalse(operator.hasMore());
    }

    @Test
    @DisplayName("INNER JOIN 一对多连接")
    void innerJoinOneToMany() throws IOException {
        JoinOperator operator = new JoinOperator(
            new MockOperator(List.of(
                new Row(new String[] {"id", "name"}, new Object[] {1, "alice"})
            )),
            new MockOperator(List.of(
                new Row(new String[] {"user_id", "city"}, new Object[] {1, "shanghai"}),
                new Row(new String[] {"user_id", "city"}, new Object[] {1, "beijing"})
            )),
            JoinType.INNER,
            new JoinOperator.JoinCondition("id", "user_id", JoinOperator.JoinOperatorType.EQUALS)
        );

        operator.open();
        assertTrue(operator.hasMore());
        operator.nextRow();
        assertTrue(operator.hasMore());
        operator.nextRow();
        assertFalse(operator.hasMore());
    }

    @Test
    @DisplayName("getOutputColumns 包含左右表所有列")
    void getOutputColumns() {
        JoinOperator operator = new JoinOperator(
            new MockOperator(List.of(new Row(new String[] {"id", "name"}, new Object[] {1, "a"}))),
            new MockOperator(List.of(new Row(new String[] {"user_id", "city"}, new Object[] {1, "b"}))),
            JoinType.INNER,
            new JoinOperator.JoinCondition("id", "user_id", JoinOperator.JoinOperatorType.EQUALS)
        );

        String[] cols = operator.getOutputColumns();
        assertEquals(4, cols.length);
        assertEquals("id", cols[0]);
        assertEquals("name", cols[1]);
        assertEquals("user_id", cols[2]);
        assertEquals("city", cols[3]);
    }

    @Test
    @DisplayName("close 和 reset 生命周期")
    void closeAndReset() throws IOException {
        JoinOperator operator = new JoinOperator(
            new MockOperator(List.of(
                new Row(new String[] {"id"}, new Object[] {1})
            ), new String[] {"id"}),
            new MockOperator(List.of(
                new Row(new String[] {"user_id"}, new Object[] {1})
            ), new String[] {"user_id"}),
            JoinType.INNER,
            new JoinOperator.JoinCondition("id", "user_id", JoinOperator.JoinOperatorType.EQUALS)
        );

        operator.open();
        assertTrue(operator.hasMore());
        operator.close();

        // reset 重新打开
        operator.reset();
        assertTrue(operator.hasMore());
    }

    private static class MockOperator extends Operator {
        private final List<Row> rows;
        private final String[] columns;
        private int index;

        private MockOperator(List<Row> rows) {
            this.rows = rows;
            this.columns = rows.isEmpty() ? new String[0] : rows.get(0).getColumns();
        }

        private MockOperator(List<Row> rows, String[] columns) {
            this.rows = rows;
            this.columns = columns;
        }

        @Override
        public void open() {
            index = 0;
        }

        @Override
        public void close() {
            index = rows.size();
        }

        @Override
        public Row nextRow() {
            return hasNext() ? rows.get(index++) : null;
        }

        @Override
        public boolean hasMore() {
            return index < rows.size();
        }

        @Override
        public void reset() {
            index = 0;
        }

        @Override
        public String[] getOutputColumns() {
            return columns;
        }
    }
}
