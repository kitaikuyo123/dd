package com.minisql.common.utils;

import com.minisql.common.model.Column;
import com.minisql.common.model.Table;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RowKeySerializer 单元测试
 */
@DisplayName("RowKeySerializer 单元测试")
class RowKeySerializerTest {

    @Test
    @DisplayName("测试 INT 类型序列化保序性")
    void testIntSerializationOrder() {
        int[] values = {-100, -1, 0, 1, 100};

        byte[][] serialized = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            serialized[i] = RowKeySerializer.serializeInt(values[i]);
        }

        // 验证保序性
        for (int i = 0; i < values.length - 1; i++) {
            assertTrue(BytesUtil.compareTo(serialized[i], serialized[i + 1]) < 0,
                "Order not preserved: " + values[i] + " < " + values[i + 1]);
        }
    }

    @Test
    @DisplayName("测试 BIGINT 类型序列化保序性")
    void testLongSerializationOrder() {
        long[] values = {-1000L, -1L, 0L, 1L, 1000L};

        byte[][] serialized = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            serialized[i] = RowKeySerializer.serializeLong(values[i]);
        }

        // 验证保序性
        for (int i = 0; i < values.length - 1; i++) {
            assertTrue(BytesUtil.compareTo(serialized[i], serialized[i + 1]) < 0,
                "Order not preserved: " + values[i] + " < " + values[i + 1]);
        }
    }

    @Test
    @DisplayName("测试 STRING 类型序列化保序性")
    void testStringSerializationOrder() {
        String[] values = {"a", "abc", "abcd", "b", "c"};

        byte[][] serialized = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            serialized[i] = RowKeySerializer.serializeString(values[i]);
        }

        // 验证保序性
        for (int i = 0; i < values.length - 1; i++) {
            assertTrue(BytesUtil.compareTo(serialized[i], serialized[i + 1]) < 0,
                "Order not preserved: " + values[i] + " < " + values[i + 1]);
        }
    }

    @Test
    @DisplayName("测试复合主键序列化")
    void testCompositeKeySerialization() {
        List<Object> values = Arrays.asList("user001", 100);
        List<Column.ColumnType> types = Arrays.asList(Column.ColumnType.VARCHAR, Column.ColumnType.INT);

        byte[] key1 = RowKeySerializer.serializeComposite(values, types);
        assertNotNull(key1);
        assertTrue(key1.length > 0);

        // 验证不同值的复合键
        List<Object> values2 = Arrays.asList("user002", 100);
        byte[] key2 = RowKeySerializer.serializeComposite(values2, types);

        // user001 应该小于 user002
        assertTrue(BytesUtil.compareTo(key1, key2) < 0);
    }

    @Test
    @DisplayName("测试复合主键序列化 - 相同前缀")
    void testCompositeKeySerializationSamePrefix() {
        List<Object> values1 = Arrays.asList("user001", 100);
        List<Object> values2 = Arrays.asList("user001", 200);
        List<Column.ColumnType> types = Arrays.asList(Column.ColumnType.VARCHAR, Column.ColumnType.INT);

        byte[] key1 = RowKeySerializer.serializeComposite(values1, types);
        byte[] key2 = RowKeySerializer.serializeComposite(values2, types);

        // 相同 user_id，不同的 bucket，100 < 200
        assertTrue(BytesUtil.compareTo(key1, key2) < 0);
    }

    @Test
    @DisplayName("测试反序列化 INT")
    void testDeserializeInt() {
        int original = 42;
        byte[] serialized = RowKeySerializer.serializeInt(original);
        int deserialized = RowKeySerializer.deserializeInt(serialized);
        assertEquals(original, deserialized);
    }

    @Test
    @DisplayName("测试反序列化 BIGINT")
    void testDeserializeLong() {
        long original = 1234567890L;
        byte[] serialized = RowKeySerializer.serializeLong(original);
        long deserialized = RowKeySerializer.deserializeLong(serialized);
        assertEquals(original, deserialized);
    }

    @Test
    @DisplayName("测试反序列化 STRING")
    void testDeserializeString() {
        String original = "test_string";
        byte[] serialized = RowKeySerializer.serializeString(original);
        String deserialized = RowKeySerializer.deserializeString(serialized);
        assertEquals(original, deserialized);
    }

    @Test
    @DisplayName("测试序列化反序列化复合主键")
    void testSerializeDeserializeComposite() {
        List<Object> values = Arrays.asList("user001", 100);
        List<Column.ColumnType> types = Arrays.asList(Column.ColumnType.VARCHAR, Column.ColumnType.INT);

        byte[] serialized = RowKeySerializer.serializeComposite(values, types);

        List<Object> deserialized = RowKeySerializer.deserializeComposite(serialized, types);

        assertEquals(2, deserialized.size());
        assertEquals("user001", deserialized.get(0));
        assertEquals(100, deserialized.get(1));
    }

    @Test
    @DisplayName("测试 BOOLEAN 序列化")
    void testBooleanSerialization() {
        byte[] trueBytes = RowKeySerializer.serializeBoolean(true);
        byte[] falseBytes = RowKeySerializer.serializeBoolean(false);

        assertEquals(1, trueBytes.length);
        assertEquals(1, trueBytes[0]); // true = 1
        assertEquals(1, falseBytes.length);
        assertEquals(0, falseBytes[0]); // false = 0

        assertTrue(RowKeySerializer.deserializeBoolean(trueBytes));
        assertFalse(RowKeySerializer.deserializeBoolean(falseBytes));
    }

    @Test
    @DisplayName("测试 TIMESTAMP 序列化")
    void testTimestampSerialization() {
        long timestamp = System.currentTimeMillis();
        byte[] serialized = RowKeySerializer.serializeTimestamp(timestamp);
        Long deserialized = RowKeySerializer.deserializeTimestamp(serialized);
        assertEquals(timestamp, deserialized);
    }

    @Test
    @DisplayName("测试 NULL 值处理")
    void testNullSerialization() {
        byte[] nullBytes = RowKeySerializer.serializeString(null);
        assertNotNull(nullBytes);
        assertEquals(1, nullBytes.length); // null marker is 1 byte (0x00)

        String deserialized = RowKeySerializer.deserializeString(nullBytes);
        assertNull(deserialized);
    }
}
