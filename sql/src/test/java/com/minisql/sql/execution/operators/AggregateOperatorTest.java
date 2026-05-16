package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.AggregateExpr;
import com.minisql.sql.AggregateType;
import com.minisql.sql.execution.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AggregateOperator 单元测试
 */
@DisplayName("AggregateOperator 单元测试")
class AggregateOperatorTest {

    @Test
    @DisplayName("测试 COUNT 聚合")
    void testCountAggregate() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));
        testData.add(createRow(3, "charlie", 35));

        MockOperator mockOperator = new MockOperator(testData);

        List<AggregateExpr> aggregates = new ArrayList<>();
        aggregates.add(new AggregateExpr(AggregateType.COUNT, "*"));

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);
        aggregate.open();

        assertTrue(aggregate.hasMore());
        Row result = aggregate.nextRow();

        // COUNT(*) 结果应该在最后一列
        Object countValue = result.getValue(0);
        assertEquals(3, ((Number) countValue).longValue());

        assertFalse(aggregate.hasMore());
    }

    @Test
    @DisplayName("测试 SUM 聚合")
    void testSumAggregate() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));
        testData.add(createRow(3, "charlie", 35));

        MockOperator mockOperator = new MockOperator(testData);

        List<AggregateExpr> aggregates = new ArrayList<>();
        aggregates.add(new AggregateExpr(AggregateType.SUM, "age"));

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);
        aggregate.open();

        assertTrue(aggregate.hasMore());
        Row result = aggregate.nextRow();

        // SUM(age) = 25 + 30 + 35 = 90
        Object sumValue = result.getValue(0);
        assertEquals(90.0, ((Number) sumValue).doubleValue(), 0.001);

        assertFalse(aggregate.hasMore());
    }

    @Test
    @DisplayName("测试 AVG 聚合")
    void testAvgAggregate() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));
        testData.add(createRow(3, "charlie", 35));

        MockOperator mockOperator = new MockOperator(testData);

        List<AggregateExpr> aggregates = new ArrayList<>();
        aggregates.add(new AggregateExpr(AggregateType.AVG, "age"));

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);
        aggregate.open();

        assertTrue(aggregate.hasMore());
        Row result = aggregate.nextRow();

        // AVG(age) = (25 + 30 + 35) / 3 = 30
        Object avgValue = result.getValue(0);
        assertEquals(30.0, ((Number) avgValue).doubleValue(), 0.001);

        assertFalse(aggregate.hasMore());
    }

    @Test
    @DisplayName("测试 MAX 聚合")
    void testMaxAggregate() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));
        testData.add(createRow(3, "charlie", 35));

        MockOperator mockOperator = new MockOperator(testData);

        List<AggregateExpr> aggregates = new ArrayList<>();
        aggregates.add(new AggregateExpr(AggregateType.MAX, "age"));

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);
        aggregate.open();

        assertTrue(aggregate.hasMore());
        Row result = aggregate.nextRow();

        Object maxValue = result.getValue(0);
        assertEquals(35, ((Number) maxValue).intValue());

        assertFalse(aggregate.hasMore());
    }

    @Test
    @DisplayName("测试 MIN 聚合")
    void testMinAggregate() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));
        testData.add(createRow(3, "charlie", 35));

        MockOperator mockOperator = new MockOperator(testData);

        List<AggregateExpr> aggregates = new ArrayList<>();
        aggregates.add(new AggregateExpr(AggregateType.MIN, "age"));

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);
        aggregate.open();

        assertTrue(aggregate.hasMore());
        Row result = aggregate.nextRow();

        Object minValue = result.getValue(0);
        assertEquals(25, ((Number) minValue).intValue());

        assertFalse(aggregate.hasMore());
    }

    @Test
    @DisplayName("测试多个聚合函数")
    void testMultipleAggregates() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));
        testData.add(createRow(3, "charlie", 35));

        MockOperator mockOperator = new MockOperator(testData);

        List<AggregateExpr> aggregates = new ArrayList<>();
        aggregates.add(new AggregateExpr(AggregateType.COUNT, "*"));
        aggregates.add(new AggregateExpr(AggregateType.SUM, "age"));
        aggregates.add(new AggregateExpr(AggregateType.AVG, "age"));
        aggregates.add(new AggregateExpr(AggregateType.MAX, "age"));
        aggregates.add(new AggregateExpr(AggregateType.MIN, "age"));

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);
        aggregate.open();

        assertTrue(aggregate.hasMore());
        Row result = aggregate.nextRow();

        assertEquals(5, result.getColumnCount());
        assertEquals(3, ((Number) result.getValue(0)).longValue()); // COUNT
        assertEquals(90.0, ((Number) result.getValue(1)).doubleValue(), 0.001); // SUM
        assertEquals(30.0, ((Number) result.getValue(2)).doubleValue(), 0.001); // AVG
        assertEquals(35, ((Number) result.getValue(3)).intValue()); // MAX
        assertEquals(25, ((Number) result.getValue(4)).intValue()); // MIN

        assertFalse(aggregate.hasMore());
    }

    @Test
    @DisplayName("测试 GROUP BY 单列分组")
    void testGroupBySingleColumn() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "A", 25));
        testData.add(createRow(2, "A", 30));
        testData.add(createRow(3, "B", 35));
        testData.add(createRow(4, "B", 40));
        testData.add(createRow(5, "C", 20));

        MockOperator mockOperator = new MockOperator(testData);

        List<AggregateExpr> aggregates = new ArrayList<>();
        aggregates.add(new AggregateExpr(AggregateType.COUNT, "*"));
        aggregates.add(new AggregateExpr(AggregateType.SUM, "age"));

        List<String> groupBy = Collections.singletonList("category");

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, groupBy);
        aggregate.open();

        // 验证有 3 个分组
        int groupCount = 0;
        while (aggregate.hasMore()) {
            Row row = aggregate.nextRow();
            groupCount++;
            // 第一列是 group by 列
            assertNotNull(row.getValue(0));
            // 后面是聚合结果
            assertNotNull(row.getValue(1)); // COUNT
            assertNotNull(row.getValue(2)); // SUM
        }

        assertEquals(3, groupCount);
    }

    @Test
    @DisplayName("测试空数据聚合")
    void testEmptyDataAggregate() throws IOException {
        List<Row> testData = new ArrayList<>();
        MockOperator mockOperator = new MockOperator(testData);

        List<AggregateExpr> aggregates = new ArrayList<>();
        aggregates.add(new AggregateExpr(AggregateType.COUNT, "*"));

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);
        aggregate.open();

        assertTrue(aggregate.hasMore());
        Row result = aggregate.nextRow();

        // COUNT(*) 在空数据时应该返回 0
        Object countValue = result.getValue(0);
        assertEquals(0, ((Number) countValue).longValue());

        assertFalse(aggregate.hasMore());
    }

    @Test
    @DisplayName("测试 close 操作")
    void testClose() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));

        MockOperator mockOperator = new MockOperator(testData);
        List<AggregateExpr> aggregates = new ArrayList<>();
        AggregateExpr expr = new AggregateExpr(AggregateType.COUNT, "*");
        aggregates.add(expr);

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);
        aggregate.open();

        // 验证有数据
        assertTrue(aggregate.hasMore());
        aggregate.nextRow();

        // close 后不再有数据
        aggregate.close();
        assertFalse(aggregate.hasMore());
    }

    @Test
    @DisplayName("测试 reset 操作")
    void testReset() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));

        MockOperator mockOperator = new MockOperator(testData);
        List<AggregateExpr> aggregates = new ArrayList<>();
        AggregateExpr expr = new AggregateExpr(AggregateType.COUNT, "*");
        aggregates.add(expr);

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);

        // 第一次执行
        aggregate.open();
        int count1 = 0;
        while (aggregate.hasMore()) {
            aggregate.nextRow();
            count1++;
        }

        // reset 后重新执行
        aggregate.reset();
        int count2 = 0;
        while (aggregate.hasMore()) {
            aggregate.nextRow();
            count2++;
        }

        assertEquals(count1, count2);
    }

    @Test
    @DisplayName("测试 getOutputColumns")
    void testGetOutputColumns() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));

        MockOperator mockOperator = new MockOperator(testData);
        List<AggregateExpr> aggregates = new ArrayList<>();
        AggregateExpr countExpr = new AggregateExpr(AggregateType.COUNT, "*");
        countExpr.setAlias("total");
        aggregates.add(countExpr);

        AggregateExpr sumExpr = new AggregateExpr(AggregateType.SUM, "age");
        sumExpr.setAlias("sum_age");
        aggregates.add(sumExpr);

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);

        String[] columns = aggregate.getOutputColumns();
        assertEquals(2, columns.length);
        assertEquals("total", columns[0]);
        assertEquals("sum_age", columns[1]);
    }

    @Test
    @DisplayName("测试带别名的聚合")
    void testAggregateWithAlias() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));

        MockOperator mockOperator = new MockOperator(testData);

        List<AggregateExpr> aggregates = new ArrayList<>();
        AggregateExpr countExpr = new AggregateExpr(AggregateType.COUNT, "*");
        countExpr.setAlias("user_count");
        aggregates.add(countExpr);

        AggregateExpr avgExpr = new AggregateExpr(AggregateType.AVG, "age");
        avgExpr.setAlias("avg_age");
        aggregates.add(avgExpr);

        AggregateOperator aggregate = new AggregateOperator(mockOperator, aggregates, null);
        aggregate.open();

        assertTrue(aggregate.hasMore());
        Row result = aggregate.nextRow();

        String[] columns = result.getColumns();
        assertNotNull(columns);
        assertEquals("user_count", columns[0]);
        assertEquals("avg_age", columns[1]);
    }

    // 辅助方法：创建测试行
    private Row createRow(int id, String category, int age) {
        return new Row(new String[]{"id", "category", "age"}, new Object[]{id, category, age});
    }

    // Mock Operator 用于测试
    private static class MockOperator extends Operator {
        private final List<Row> data;
        private int index = 0;
        private boolean opened = false;

        MockOperator(List<Row> data) {
            this.data = data;
        }

        @Override
        public void open() {
            opened = true;
            index = 0;
        }

        @Override
        public Row nextRow() {
            if (!opened || index >= data.size()) return null;
            return data.get(index++);
        }

        @Override
        public boolean hasMore() {
            return opened && index < data.size();
        }

        @Override
        public void close() {
            opened = false;
        }

        @Override
        public void reset() {
            index = 0;
        }

        @Override
        public String[] getOutputColumns() {
            return new String[]{"id", "category", "age"};
        }
    }
}
