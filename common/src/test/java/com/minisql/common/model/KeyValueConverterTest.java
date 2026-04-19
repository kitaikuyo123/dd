package com.minisql.common.model;

import com.minisql.common.utils.RowKeySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("KeyValueConverter 单元测试")
class KeyValueConverterTest {

    private Table schema;

    @BeforeEach
    void setUp() {
        schema = new Table("users");
        schema.addColumn(new Column("id", Column.ColumnType.INT));
        schema.addColumn(new Column("name", Column.ColumnType.STRING));
        schema.addColumn(new Column("age", Column.ColumnType.INT));
        schema.setPrimaryKey("id");
    }

    private Row createRow(int id, String name, int age) {
        Row row = new Row();
        row.setColumn("id", id);
        row.setColumn("name", name);
        row.setColumn("age", age);
        row.setTimestamp(1000L);
        return row;
    }

    // ---- rowToKeyValues ----

    @Test
    @DisplayName("测试 Row → KeyValue[] 基本转换")
    void testRowToKeyValues() {
        Row row = createRow(1, "alice", 20);
        row.setRowKey(BytesUtilTestHelper.toBytes(1));

        KeyValue[] kvs = KeyValueConverter.rowToKeyValues(row, schema);

        // 主键列不生成 KeyValue，只有 name 和 age
        assertEquals(2, kvs.length);
        for (KeyValue kv : kvs) {
            assertEquals(KeyValue.Type.PUT, kv.getType());
            assertEquals(1000L, kv.getTimestamp());
            assertNotNull(kv.getQualifier());
        }
    }

    @Test
    @DisplayName("测试 Row → KeyValue[] 跳过主键列")
    void testRowToKeyValuesSkipsPrimaryKey() {
        Row row = createRow(1, "alice", 20);
        row.setRowKey(BytesUtilTestHelper.toBytes(1));

        KeyValue[] kvs = KeyValueConverter.rowToKeyValues(row, schema);

        for (KeyValue kv : kvs) {
            assertNotEquals("id", kv.getQualifier());
        }
    }

    @Test
    @DisplayName("测试 Row → KeyValue[] null Row 返回空数组")
    void testRowToKeyValuesNullRow() {
        KeyValue[] kvs = KeyValueConverter.rowToKeyValues(null, schema);
        assertEquals(0, kvs.length);
    }

    @Test
    @DisplayName("测试 Row → KeyValue[] null schema 返回空数组")
    void testRowToKeyValuesNullSchema() {
        Row row = createRow(1, "alice", 20);
        KeyValue[] kvs = KeyValueConverter.rowToKeyValues(row, null);
        assertEquals(0, kvs.length);
    }

    @Test
    @DisplayName("测试 Row → KeyValue[] null 值处理（可空列生成 DELETE 标记）")
    void testRowToKeyValuesNullValueNullableColumn() {
        Column nullableCol = new Column("bio", Column.ColumnType.STRING);
        nullableCol.setNullable(true);
        schema.addColumn(nullableCol);

        Row row = createRow(1, "alice", 20);
        row.setColumn("bio", null);
        row.setRowKey(BytesUtilTestHelper.toBytes(1));

        KeyValue[] kvs = KeyValueConverter.rowToKeyValues(row, schema);

        boolean foundDelete = false;
        for (KeyValue kv : kvs) {
            if ("bio".equals(kv.getQualifier())) {
                assertEquals(KeyValue.Type.DELETE, kv.getType());
                foundDelete = true;
            }
        }
        assertTrue(foundDelete, "nullable column with null value should produce DELETE marker");
    }

    @Test
    @DisplayName("测试 rowToKeyValuesList 返回 List")
    void testRowToKeyValuesList() {
        Row row = createRow(1, "alice", 20);
        row.setRowKey(BytesUtilTestHelper.toBytes(1));

        List<KeyValue> list = KeyValueConverter.rowToKeyValuesList(row, schema);
        assertEquals(2, list.size());
    }

    // ---- keyValuesToRow ----

    @Test
    @DisplayName("测试 KeyValue[] → Row 基本转换")
    void testKeyValuesToRow() {
        byte[] rowKey = BytesUtilTestHelper.toBytes(1);
        KeyValue[] kvs = new KeyValue[]{
            KeyValue.builder(rowKey).family("").qualifier("name").timestamp(1000)
                .value(RowKeySerializer.serialize("alice", Column.ColumnType.STRING)).build(),
            KeyValue.builder(rowKey).family("").qualifier("age").timestamp(1000)
                .value(RowKeySerializer.serialize(20, Column.ColumnType.INT)).build()
        };

        Row row = KeyValueConverter.keyValuesToRow(kvs, schema);

        assertNotNull(row);
        assertEquals("alice", row.getColumn("name"));
        assertEquals(20, row.getColumn("age"));
    }

    @Test
    @DisplayName("测试 KeyValue[] → Row 空数组返回 null")
    void testKeyValuesToRowEmpty() {
        assertNull(KeyValueConverter.keyValuesToRow(new KeyValue[0], schema));
    }

    @Test
    @DisplayName("测试 KeyValue[] → Row null 输入返回 null")
    void testKeyValuesToRowNull() {
        assertNull(KeyValueConverter.keyValuesToRow(null, schema));
    }

    // ---- createRowKey ----

    @Test
    @DisplayName("测试 createRowKey 单列主键")
    void testCreateRowKey() {
        byte[] rowKey = KeyValueConverter.createRowKey(42, schema);
        assertNotNull(rowKey);
        assertTrue(rowKey.length > 0);
    }

    @Test
    @DisplayName("测试 createRowKey 空主键名抛异常")
    void testCreateRowKeyEmptyPk() {
        Table t = new Table("t");
        t.addColumn(new Column("a", Column.ColumnType.INT));
        t.setPrimaryKey("");

        assertThrows(IllegalArgumentException.class, () -> KeyValueConverter.createRowKey(1, t));
    }

    @Test
    @DisplayName("测试 createRowKey 主键列不存在抛异常")
    void testCreateRowKeyMissingPkColumn() {
        Table t = new Table("t");
        t.setPrimaryKey("nonexistent");

        assertThrows(IllegalArgumentException.class, () -> KeyValueConverter.createRowKey(1, t));
    }

    // ---- createRowKeyFromRow ----

    @Test
    @DisplayName("测试 createRowKeyFromRow 单列主键")
    void testCreateRowKeyFromRow() {
        Row row = new Row();
        row.setColumn("id", 42);

        byte[] key = KeyValueConverter.createRowKeyFromRow(row, schema);
        assertNotNull(key);
        assertTrue(key.length > 0);
    }

    @Test
    @DisplayName("测试 createRowKeyFromRow 复合分区键")
    void testCreateRowKeyFromRowComposite() {
        schema.setPartitionKeys(List.of("id"));

        Row row = new Row();
        row.setColumn("id", 42);

        byte[] key = KeyValueConverter.createRowKeyFromRow(row, schema);
        assertNotNull(key);
    }

    // ---- extractPrimaryKeyValue / deserializePrimaryKey ----

    @Test
    @DisplayName("测试 extractPrimaryKeyValue")
    void testExtractPrimaryKeyValue() {
        Row row = createRow(42, "alice", 20);
        Object pk = KeyValueConverter.extractPrimaryKeyValue(row, schema);
        assertEquals(42, pk);
    }

    @Test
    @DisplayName("测试 extractPrimaryKeyValue 无主键返回 null")
    void testExtractPrimaryKeyValueNoPk() {
        Table t = new Table("t");
        t.addColumn(new Column("a", Column.ColumnType.INT));
        Row row = new Row();
        row.setColumn("a", 1);
        assertNull(KeyValueConverter.extractPrimaryKeyValue(row, t));
    }

    @Test
    @DisplayName("测试 deserializePrimaryKey 往返")
    void testDeserializePrimaryKeyRoundTrip() {
        byte[] rowKey = KeyValueConverter.createRowKey(42, schema);
        Object deserialized = KeyValueConverter.deserializePrimaryKey(rowKey, schema);
        assertEquals(42, deserialized);
    }

    // ---- prefixRowKey ----

    @Test
    @DisplayName("测试 prefixRowKey 添加前缀")
    void testPrefixRowKey() {
        byte[] key = "row1".getBytes();
        byte[] prefixed = KeyValueConverter.prefixRowKey(key, "prefix_");
        String result = new String(prefixed);
        assertTrue(result.startsWith("prefix_"));
        assertTrue(result.endsWith("row1"));
    }

    @Test
    @DisplayName("测试 prefixRowKey 空前缀返回原数组")
    void testPrefixRowKeyEmpty() {
        byte[] key = "row1".getBytes();
        assertArrayEquals(key, KeyValueConverter.prefixRowKey(key, ""));
    }

    @Test
    @DisplayName("测试 prefixRowKey null 前缀返回原数组")
    void testPrefixRowKeyNull() {
        byte[] key = "row1".getBytes();
        assertArrayEquals(key, KeyValueConverter.prefixRowKey(key, null));
    }

    // ---- matchesColumns / filterColumns ----

    @Test
    @DisplayName("测试 matchesColumns 匹配")
    void testMatchesColumnsTrue() {
        KeyValue kv = KeyValue.builder("row".getBytes()).qualifier("name").build();
        assertTrue(KeyValueConverter.matchesColumns(kv, List.of("name", "age")));
    }

    @Test
    @DisplayName("测试 matchesColumns 不匹配")
    void testMatchesColumnsFalse() {
        KeyValue kv = KeyValue.builder("row".getBytes()).qualifier("city").build();
        assertFalse(KeyValueConverter.matchesColumns(kv, List.of("name", "age")));
    }

    @Test
    @DisplayName("测试 matchesColumns null 列表返回 true")
    void testMatchesColumnsNullList() {
        KeyValue kv = KeyValue.builder("row".getBytes()).qualifier("name").build();
        assertTrue(KeyValueConverter.matchesColumns(kv, null));
    }

    @Test
    @DisplayName("测试 matchesColumns 空列表返回 true")
    void testMatchesColumnsEmptyList() {
        KeyValue kv = KeyValue.builder("row".getBytes()).qualifier("name").build();
        assertTrue(KeyValueConverter.matchesColumns(kv, List.of()));
    }

    @Test
    @DisplayName("测试 filterColumns 过滤")
    void testFilterColumns() {
        List<KeyValue> kvs = List.of(
            KeyValue.builder("r".getBytes()).qualifier("name").build(),
            KeyValue.builder("r".getBytes()).qualifier("age").build(),
            KeyValue.builder("r".getBytes()).qualifier("city").build()
        );

        List<KeyValue> filtered = KeyValueConverter.filterColumns(kvs, List.of("name", "age"));
        assertEquals(2, filtered.size());
    }

    @Test
    @DisplayName("测试 filterColumns null 列表返回原列表")
    void testFilterColumnsNullList() {
        List<KeyValue> kvs = List.of(KeyValue.builder("r".getBytes()).qualifier("name").build());
        assertSame(kvs, KeyValueConverter.filterColumns(kvs, null));
    }

    // ---- mapToRow / rowToMap ----

    @Test
    @DisplayName("测试 mapToRow")
    void testMapToRow() {
        Map<String, Object> map = Map.of("id", 1, "name", "alice", "age", 20);
        Row row = KeyValueConverter.mapToRow(map, schema);

        assertEquals(1, row.getColumn("id"));
        assertEquals("alice", row.getColumn("name"));
        assertEquals(20, row.getColumn("age"));
        assertNotNull(row.getRowKey());
    }

    @Test
    @DisplayName("测试 rowToMap")
    void testRowToMap() {
        Row row = createRow(1, "alice", 20);
        Map<String, Object> map = KeyValueConverter.rowToMap(row);

        assertEquals(1, map.get("id"));
        assertEquals("alice", map.get("name"));
        assertEquals(20, map.get("age"));
    }

    // ---- 往返一致性 ----

    @Test
    @DisplayName("测试 Row → KeyValue[] → Row 往返一致性")
    void testRoundTrip() {
        Row original = createRow(42, "alice", 25);
        original.setRowKey(KeyValueConverter.createRowKey(42, schema));

        KeyValue[] kvs = KeyValueConverter.rowToKeyValues(original, schema);
        Row restored = KeyValueConverter.keyValuesToRow(kvs, schema);

        assertNotNull(restored);
        assertEquals("alice", restored.getColumn("name"));
    }

    /**
     * 内部辅助类，避免依赖 BytesUtil 测试中的细节
     */
    private static class BytesUtilTestHelper {
        static byte[] toBytes(int val) {
            return com.minisql.common.utils.BytesUtil.toBytes(val);
        }
    }
}
