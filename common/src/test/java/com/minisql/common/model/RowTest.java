package com.minisql.common.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Row 单元测试")
class RowTest {

    @Test
    @DisplayName("默认构造器")
    void defaultConstructor() {
        Row row = new Row();
        assertNull(row.getRowKey());
        assertNotNull(row.getValues());
        assertTrue(row.getValues().isEmpty());
        assertTrue(row.getTimestamp() > 0);
    }

    @Test
    @DisplayName("带 rowKey 构造器")
    void constructorWithRowKey() {
        byte[] key = "key1".getBytes();
        Row row = new Row(key);
        assertArrayEquals(key, row.getRowKey());
    }

    @Test
    @DisplayName("setColumn / getColumn")
    void setGetColumn() {
        Row row = new Row();
        row.setColumn("name", "alice");
        assertEquals("alice", row.getColumn("name"));
    }

    @Test
    @DisplayName("getColumn 不存在的列返回 null")
    void getColumnNonExistent() {
        Row row = new Row();
        assertNull(row.getColumn("nonexistent"));
    }

    @Test
    @DisplayName("setValue / getValue 是 getColumn 的别名")
    void setValueGetValueAlias() {
        Row row = new Row();
        row.setValue("age", 20);
        assertEquals(20, row.getValue("age"));
        assertEquals(20, row.getColumn("age"));
    }

    @Test
    @DisplayName("hasColumn")
    void hasColumn() {
        Row row = new Row();
        row.setColumn("name", "alice");
        assertTrue(row.hasColumn("name"));
        assertFalse(row.hasColumn("age"));
    }

    @Test
    @DisplayName("getColumnNames")
    void getColumnNames() {
        Row row = new Row();
        row.setColumn("id", 1);
        row.setColumn("name", "alice");

        Set<String> names = row.getColumnNames();
        assertEquals(2, names.size());
        assertTrue(names.contains("id"));
        assertTrue(names.contains("name"));
    }

    @Test
    @DisplayName("setColumn 覆盖已有值")
    void setColumnOverwrite() {
        Row row = new Row();
        row.setColumn("name", "alice");
        row.setColumn("name", "bob");
        assertEquals("bob", row.getColumn("name"));
    }

    @Test
    @DisplayName("null 值列")
    void nullColumnValue() {
        Row row = new Row();
        row.setColumn("name", null);
        assertTrue(row.hasColumn("name"));
        assertNull(row.getColumn("name"));
    }

    @Test
    @DisplayName("setRowKey / getRowKey")
    void setRowKey() {
        Row row = new Row();
        byte[] key = "newKey".getBytes();
        row.setRowKey(key);
        assertArrayEquals(key, row.getRowKey());
    }

    @Test
    @DisplayName("setTimestamp / getTimestamp")
    void setTimestamp() {
        Row row = new Row();
        row.setTimestamp(1234567890L);
        assertEquals(1234567890L, row.getTimestamp());
    }

    @Test
    @DisplayName("setValues 替换整个值映射")
    void setValues() {
        Row row = new Row();
        row.setColumn("old", 1);

        java.util.Map<String, Object> newValues = new java.util.HashMap<>();
        newValues.put("new", 2);
        row.setValues(newValues);

        assertNull(row.getColumn("old"));
        assertEquals(2, row.getColumn("new"));
    }

    @Test
    @DisplayName("getValues 返回可修改的 Map")
    void getValuesModifiable() {
        Row row = new Row();
        row.getValues().put("external", "value");
        assertEquals("value", row.getColumn("external"));
    }
}
