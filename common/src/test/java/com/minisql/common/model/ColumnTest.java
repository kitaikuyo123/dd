package com.minisql.common.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Column 单元测试")
class ColumnTest {

    @Test
    @DisplayName("默认构造器 nullable 为 true")
    void defaultConstructorNullableTrue() {
        Column col = new Column();
        assertTrue(col.isNullable());
        assertNull(col.getName());
        assertNull(col.getType());
        assertEquals(0, col.getLength());
        assertNull(col.getDefaultValue());
    }

    @Test
    @DisplayName("双参数构造器")
    void twoArgConstructor() {
        Column col = new Column("id", Column.ColumnType.INT);
        assertEquals("id", col.getName());
        assertEquals(Column.ColumnType.INT, col.getType());
        assertTrue(col.isNullable());
    }

    @Test
    @DisplayName("三参数构造器含长度")
    void threeArgConstructor() {
        Column col = new Column("name", Column.ColumnType.VARCHAR, 255);
        assertEquals("name", col.getName());
        assertEquals(Column.ColumnType.VARCHAR, col.getType());
        assertEquals(255, col.getLength());
    }

    @Test
    @DisplayName("setter 方法")
    void setters() {
        Column col = new Column();
        col.setName("age");
        col.setType(Column.ColumnType.BIGINT);
        col.setLength(10);
        col.setNullable(false);
        col.setDefaultValue(0);

        assertEquals("age", col.getName());
        assertEquals(Column.ColumnType.BIGINT, col.getType());
        assertEquals(10, col.getLength());
        assertFalse(col.isNullable());
        assertEquals(0, col.getDefaultValue());
    }

    @Test
    @DisplayName("toString 包含列名和类型")
    void toStringContainsNameAndType() {
        Column col = new Column("id", Column.ColumnType.INT);
        String str = col.toString();
        assertTrue(str.contains("id"));
        assertTrue(str.contains("INT"));
    }

    @Test
    @DisplayName("toString 含长度时显示长度")
    void toStringWithLength() {
        Column col = new Column("name", Column.ColumnType.VARCHAR, 100);
        assertTrue(col.toString().contains("100"));
    }

    @Test
    @DisplayName("ColumnType 枚举值完整")
    void columnTypeEnumValues() {
        Column.ColumnType[] types = Column.ColumnType.values();
        assertTrue(types.length >= 10);
        assertNotNull(Column.ColumnType.valueOf("INT"));
        assertNotNull(Column.ColumnType.valueOf("BIGINT"));
        assertNotNull(Column.ColumnType.valueOf("VARCHAR"));
        assertNotNull(Column.ColumnType.valueOf("BOOLEAN"));
        assertNotNull(Column.ColumnType.valueOf("TIMESTAMP"));
        assertNotNull(Column.ColumnType.valueOf("STRING"));
    }
}
