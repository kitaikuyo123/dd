package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RowConverters 单元测试")
class RowConvertersTest {

    @Test
    @DisplayName("common.Row → sql.Row 基本转换")
    void toSqlRowBasic() {
        com.minisql.common.model.Row commonRow = new com.minisql.common.model.Row();
        commonRow.setColumn("name", "alice");
        commonRow.setColumn("age", 20);

        String[] columns = new String[]{"name", "age"};
        Row sqlRow = RowConverters.toSqlRow(commonRow, columns);

        assertNotNull(sqlRow);
        assertEquals("alice", sqlRow.getValue("name"));
        assertEquals(20, sqlRow.getValue("age"));
    }

    @Test
    @DisplayName("common.Row → sql.Row null 输入返回 null")
    void toSqlRowNull() {
        assertNull(RowConverters.toSqlRow(null, new String[]{"a"}));
    }

    @Test
    @DisplayName("common.Row → sql.Row 保留 rowKey")
    void toSqlRowPreservesRowKey() {
        com.minisql.common.model.Row commonRow = new com.minisql.common.model.Row();
        commonRow.setColumn("id", 1);
        commonRow.setRowKey("key123".getBytes());

        Row sqlRow = RowConverters.toSqlRow(commonRow, new String[]{"id"});
        assertNotNull(sqlRow.getRowKey());
        assertEquals("key123", new String(sqlRow.getRowKey()));
    }

    @Test
    @DisplayName("common.Row → sql.Row 缺失列值为 null")
    void toSqlRowMissingColumnIsNull() {
        com.minisql.common.model.Row commonRow = new com.minisql.common.model.Row();
        commonRow.setColumn("name", "alice");
        // age 未设置

        Row sqlRow = RowConverters.toSqlRow(commonRow, new String[]{"name", "age"});
        assertEquals("alice", sqlRow.getValue("name"));
        assertNull(sqlRow.getValue("age"));
    }

    @Test
    @DisplayName("toSqlRow 从算子获取列名")
    void toSqlRowFromOperator() {
        com.minisql.common.model.Row commonRow = new com.minisql.common.model.Row();
        commonRow.setColumn("x", 42);

        Operator mockOp = new Operator() {
            @Override public void open() {}
            @Override public void close() {}
            @Override public Row nextRow() { return null; }
            @Override public boolean hasMore() { return false; }
            @Override public void reset() {}
            @Override public String[] getOutputColumns() { return new String[]{"x"}; }
        };

        Row sqlRow = RowConverters.toSqlRow(commonRow, mockOp);
        assertNotNull(sqlRow);
        assertEquals(42, sqlRow.getValue("x"));
    }

    @Test
    @DisplayName("toSqlRow 从算子，null Row 返回 null")
    void toSqlRowFromOperatorNull() {
        Operator mockOp = new Operator() {
            @Override public void open() {}
            @Override public void close() {}
            @Override public Row nextRow() { return null; }
            @Override public boolean hasMore() { return false; }
            @Override public void reset() {}
            @Override public String[] getOutputColumns() { return new String[]{"a"}; }
        };
        assertNull(RowConverters.toSqlRow(null, mockOp));
    }
}
