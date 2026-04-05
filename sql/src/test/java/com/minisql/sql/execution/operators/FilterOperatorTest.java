package com.minisql.sql.execution.operators;

import com.minisql.sql.ast.SimpleCondition;
import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FilterOperator 单元测试
 */
@DisplayName("FilterOperator 单元测试")
class FilterOperatorTest {

    @Test
    @DisplayName("测试基本过滤功能")
    void testBasicFilter() throws IOException {
        // 创建测试数据
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));
        testData.add(createRow(3, "charlie", 35));

        MockOperator mockOperator = new MockOperator(testData);

        // 创建过滤条件：age > 25
        FilterOperator.Predicate predicate = row -> {
            Object age = row.getValue("age");
            if (age instanceof Number) {
                return ((Number) age).intValue() > 25;
            }
            return false;
        };

        FilterOperator filter = new FilterOperator(mockOperator, predicate);
        filter.open();

        // 验证过滤结果
        assertTrue(filter.hasMore());
        Row row1 = filter.nextRow();
        assertEquals(30, row1.getValue("age"));

        assertTrue(filter.hasMore());
        Row row2 = filter.nextRow();
        assertEquals(35, row2.getValue("age"));

        assertFalse(filter.hasMore());
    }

    @Test
    @DisplayName("测试过滤所有数据")
    void testFilterAll() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 20));
        testData.add(createRow(2, "bob", 25));

        MockOperator mockOperator = new MockOperator(testData);

        // 过滤条件：age > 100（没有匹配）
        FilterOperator.Predicate predicate = row -> {
            Object age = row.getValue("age");
            if (age instanceof Number) {
                return ((Number) age).intValue() > 100;
            }
            return false;
        };

        FilterOperator filter = new FilterOperator(mockOperator, predicate);
        filter.open();

        assertFalse(filter.hasMore());
    }

    @Test
    @DisplayName("测试过滤保留所有数据")
    void testFilterKeepAll() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 20));
        testData.add(createRow(2, "bob", 25));
        testData.add(createRow(3, "charlie", 30));

        MockOperator mockOperator = new MockOperator(testData);

        // 过滤条件：始终为 true
        FilterOperator.Predicate predicate = row -> true;

        FilterOperator filter = new FilterOperator(mockOperator, predicate);
        filter.open();

        int count = 0;
        while (filter.hasMore()) {
            filter.nextRow();
            count++;
        }

        assertEquals(3, count);
    }

    @Test
    @DisplayName("测试等于条件过滤")
    void testFilterEquals() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));
        testData.add(createRow(3, "charlie", 25));

        MockOperator mockOperator = new MockOperator(testData);

        // 过滤条件：age = 25
        FilterOperator.Predicate predicate = row -> {
            Object age = row.getValue("age");
            if (age instanceof Number) {
                return ((Number) age).intValue() == 25;
            }
            return false;
        };

        FilterOperator filter = new FilterOperator(mockOperator, predicate);
        filter.open();

        int count = 0;
        while (filter.hasMore()) {
            Row row = filter.nextRow();
            assertEquals(25, row.getValue("age"));
            count++;
        }

        assertEquals(2, count);
    }

    @Test
    @DisplayName("测试字符串条件过滤")
    void testFilterStringCondition() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));
        testData.add(createRow(3, "charlie", 35));

        MockOperator mockOperator = new MockOperator(testData);

        // 过滤条件：name starts with 'a'
        FilterOperator.Predicate predicate = row -> {
            Object name = row.getValue("name");
            if (name instanceof String) {
                return ((String) name).toLowerCase().startsWith("a");
            }
            return false;
        };

        FilterOperator filter = new FilterOperator(mockOperator, predicate);
        filter.open();

        assertTrue(filter.hasMore());
        Row row = filter.nextRow();
        assertEquals("alice", row.getValue("name"));

        assertFalse(filter.hasMore());
    }

    @Test
    @DisplayName("测试 close 操作")
    void testClose() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));

        MockOperator mockOperator = new MockOperator(testData);

        FilterOperator.Predicate predicate = row -> true;
        FilterOperator filter = new FilterOperator(mockOperator, predicate);
        filter.open();

        // 验证有数据
        assertTrue(filter.hasMore());
        filter.nextRow();

        // close 后不再有数据
        filter.close();
        assertFalse(filter.hasMore());
    }

    @Test
    @DisplayName("测试 reset 操作")
    void testReset() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));
        testData.add(createRow(2, "bob", 30));

        MockOperator mockOperator = new MockOperator(testData);
        FilterOperator.Predicate predicate = row -> true;
        FilterOperator filter = new FilterOperator(mockOperator, predicate);

        // 第一次遍历
        filter.open();
        int count1 = 0;
        while (filter.hasMore()) {
            filter.nextRow();
            count1++;
        }
        assertEquals(2, count1);

        // reset 后重新遍历
        filter.reset();
        int count2 = 0;
        while (filter.hasMore()) {
            filter.nextRow();
            count2++;
        }
        assertEquals(2, count2);
    }

    @Test
    @DisplayName("测试 getOutputColumns")
    void testGetOutputColumns() throws IOException {
        List<Row> testData = new ArrayList<>();
        testData.add(createRow(1, "alice", 25));

        MockOperator mockOperator = new MockOperator(testData);
        FilterOperator.Predicate predicate = row -> true;
        FilterOperator filter = new FilterOperator(mockOperator, predicate);

        String[] columns = filter.getOutputColumns();
        assertArrayEquals(new String[]{"id", "name", "age"}, columns);
    }

    @Test
    @DisplayName("测试空数据")
    void testEmptyData() throws IOException {
        List<Row> testData = new ArrayList<>();
        MockOperator mockOperator = new MockOperator(testData);

        FilterOperator.Predicate predicate = row -> true;
        FilterOperator filter = new FilterOperator(mockOperator, predicate);
        filter.open();

        assertFalse(filter.hasMore());
    }

    // 辅助方法：创建测试行
    private Row createRow(int id, String name, int age) {
        return new Row(new String[]{"id", "name", "age"}, new Object[]{id, name, age});
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
            return new String[]{"id", "name", "age"};
        }
    }
}
