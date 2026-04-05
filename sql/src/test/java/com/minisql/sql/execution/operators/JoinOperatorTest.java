package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.QueryPlan;
import com.minisql.sql.execution.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinOperatorTest {

    @Test
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
            QueryPlan.JoinType.INNER,
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

    private static class MockOperator extends Operator {
        private final List<Row> rows;
        private int index;

        private MockOperator(List<Row> rows) {
            this.rows = rows;
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
            return rows.isEmpty() ? new String[0] : Arrays.copyOf(rows.get(0).getColumns(), rows.get(0).getColumns().length);
        }
    }
}
