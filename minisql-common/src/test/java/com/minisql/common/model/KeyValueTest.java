package com.minisql.common.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * KeyValue 单元测试
 */
@DisplayName("KeyValue 单元测试")
class KeyValueTest {

    @Test
    @DisplayName("测试 KeyValue 基本构造")
    void testConstructor() {
        KeyValue kv = new KeyValue();

        assertNotNull(kv);
        assertEquals(KeyValue.Type.PUT, kv.getType());
        assertTrue(kv.getTimestamp() > 0);
        assertFalse(kv.isDelete());
    }

    @Test
    @DisplayName("测试带参数构造")
    void testConstructorWithParams() {
        byte[] rowKey = "row1".getBytes();
        KeyValue kv = new KeyValue(rowKey, "cf", "col", "value".getBytes());

        assertArrayEquals(rowKey, kv.getRowKey());
        assertEquals("cf", kv.getFamily());
        assertEquals("col", kv.getQualifier());
        assertArrayEquals("value".getBytes(), kv.getValue());
        assertEquals(KeyValue.Type.PUT, kv.getType());
        assertFalse(kv.isDelete());
    }

    @Test
    @DisplayName("测试带 timestamp 的构造")
    void testConstructorWithTimestamp() {
        byte[] rowKey = "row1".getBytes();
        long timestamp = 1234567890L;

        KeyValue kv = new KeyValue(rowKey, "cf", "col", timestamp, "value".getBytes());

        assertEquals(timestamp, kv.getTimestamp());
    }

    @Test
    @DisplayName("测试 Builder 模式创建")
    void testBuilderPattern() {
        byte[] rowKey = "row1".getBytes();

        KeyValue kv = KeyValue.builder(rowKey)
                .family("cf")
                .qualifier("col")
                .timestamp(1234567890L)
                .value("value".getBytes())
                .type(KeyValue.Type.PUT)
                .build();

        assertArrayEquals(rowKey, kv.getRowKey());
        assertEquals("cf", kv.getFamily());
        assertEquals("col", kv.getQualifier());
        assertEquals(1234567890L, kv.getTimestamp());
        assertArrayEquals("value".getBytes(), kv.getValue());
        assertEquals(KeyValue.Type.PUT, kv.getType());
    }

    @Test
    @DisplayName("测试 DELETE 类型")
    void testDeleteType() {
        KeyValue kv = new KeyValue();
        kv.setType(KeyValue.Type.DELETE);

        assertEquals(KeyValue.Type.DELETE, kv.getType());
        assertTrue(kv.isDelete());
    }

    @Test
    @DisplayName("测试 Type 枚举 code")
    void testTypeCode() {
        assertEquals(4, KeyValue.Type.PUT.getCode());
        assertEquals(8, KeyValue.Type.DELETE.getCode());
    }

    @Test
    @DisplayName("测试 Type fromCode 方法")
    void testTypeFromCode() {
        assertEquals(KeyValue.Type.PUT, KeyValue.Type.fromCode((byte) 4));
        assertEquals(KeyValue.Type.DELETE, KeyValue.Type.fromCode((byte) 8));

        assertThrows(IllegalArgumentException.class, () -> {
            KeyValue.Type.fromCode((byte) 99);
        });
    }

    @Test
    @DisplayName("测试 getColumn 获取完整列名")
    void testGetColumn() {
        KeyValue kv = new KeyValue();
        kv.setFamily("cf");
        kv.setQualifier("col");

        assertEquals("cf:col", kv.getColumn());
    }

    @Test
    @DisplayName("测试 equals 和 hashCode")
    void testEqualsAndHashCode() {
        byte[] rowKey = "row1".getBytes();
        long timestamp = 1234567890L;

        KeyValue kv1 = new KeyValue(rowKey, "cf", "col", timestamp, "value".getBytes());
        KeyValue kv2 = new KeyValue(rowKey, "cf", "col", timestamp, "value".getBytes());
        KeyValue kv3 = new KeyValue(rowKey, "cf", "col", timestamp + 1, "value".getBytes());

        assertEquals(kv1, kv2);
        assertNotEquals(kv1, kv3);  // timestamp 不同
        assertEquals(kv1.hashCode(), kv2.hashCode());
    }

    @Test
    @DisplayName("测试 compareTo 比较")
    void testCompareTo() {
        KeyValue kv1 = new KeyValue("a".getBytes(), "cf", "col", 1000, "v1".getBytes());
        KeyValue kv2 = new KeyValue("b".getBytes(), "cf", "col", 1000, "v2".getBytes());
        KeyValue kv3 = new KeyValue("a".getBytes(), "cf", "col", 2000, "v3".getBytes());

        assertTrue(kv1.compareTo(kv2) < 0);  // rowKey "a" < "b"
        assertTrue(kv2.compareTo(kv1) > 0);

        // 相同 rowKey，timestamp 大的在前（倒序）
        assertTrue(kv3.compareTo(kv1) < 0);  // timestamp 2000 > 1000，但 compareTo 返回负数表示 kv3 在前
    }

    @Test
    @DisplayName("测试 compareTo 相同 rowKey 不同 family")
    void testCompareToSameRowKeyDifferentFamily() {
        KeyValue kv1 = new KeyValue("row".getBytes(), "cf1", "col", 1000, "v1".getBytes());
        KeyValue kv2 = new KeyValue("row".getBytes(), "cf2", "col", 1000, "v2".getBytes());

        assertTrue(kv1.compareTo(kv2) < 0);  // cf1 < cf2
    }

    @Test
    @DisplayName("测试 compareTo 相同 rowKey 和 family 不同 qualifier")
    void testCompareToSameRowKeyAndFamilyDifferentQualifier() {
        KeyValue kv1 = new KeyValue("row".getBytes(), "cf", "col1", 1000, "v1".getBytes());
        KeyValue kv2 = new KeyValue("row".getBytes(), "cf", "col2", 1000, "v2".getBytes());

        assertTrue(kv1.compareTo(kv2) < 0);  // col1 < col2
    }

    @Test
    @DisplayName("测试 toString 方法")
    void testToString() {
        KeyValue kv = new KeyValue("row1".getBytes(), "cf", "col", 1000, "value".getBytes());
        String str = kv.toString();

        assertTrue(str.contains("KeyValue"));
        assertTrue(str.contains("rowKey"));
        assertTrue(str.contains("cf"));
        assertTrue(str.contains("col"));
        assertTrue(str.contains("value"));
    }

    @Test
    @DisplayName("测试 setter 方法")
    void testSetters() {
        KeyValue kv = new KeyValue();

        kv.setRowKey("row1".getBytes());
        assertArrayEquals("row1".getBytes(), kv.getRowKey());

        kv.setFamily("cf");
        assertEquals("cf", kv.getFamily());

        kv.setQualifier("col");
        assertEquals("col", kv.getQualifier());

        kv.setTimestamp(1234567890L);
        assertEquals(1234567890L, kv.getTimestamp());

        kv.setValue("value".getBytes());
        assertArrayEquals("value".getBytes(), kv.getValue());

        kv.setType(KeyValue.Type.DELETE);
        assertEquals(KeyValue.Type.DELETE, kv.getType());
        assertTrue(kv.isDelete());
    }

    @Test
    @DisplayName("测试 null 值处理")
    void testNullValue() {
        KeyValue kv = new KeyValue();
        kv.setRowKey("row1".getBytes());
        kv.setValue(null);

        assertNull(kv.getValue());
    }

    @Test
    @DisplayName("测试空字节数组 rowKey")
    void testEmptyRowKey() {
        KeyValue kv = new KeyValue(new byte[0], "cf", "col", "value".getBytes());

        assertArrayEquals(new byte[0], kv.getRowKey());
        assertEquals(0, kv.getRowKey().length);
    }

    @Test
    @DisplayName("测试 Builder 链式调用")
    void testBuilderChaining() {
        byte[] rowKey = "row1".getBytes();
        KeyValue.Builder builder = KeyValue.builder(rowKey);

        // 验证链式调用返回 builder 本身
        KeyValue.Builder result = builder.family("cf");
        assertSame(builder, result);

        result = builder.qualifier("col");
        assertSame(builder, result);

        result = builder.value("value".getBytes());
        assertSame(builder, result);
    }

    @Test
    @DisplayName("测试 Builder 多次 build")
    void testBuilderMultipleBuild() {
        // 注意：当前 Builder 实现返回同一个实例，所以多次 build 返回相同对象
        // 这是预期行为，因为 KeyValue 是可变对象
        KeyValue.Builder builder = KeyValue.builder("row1".getBytes())
                .family("cf")
                .qualifier("col");

        KeyValue kv1 = builder.value("value1".getBytes()).build();
        KeyValue kv2 = builder.value("value2".getBytes()).build();

        // 由于 Builder 返回同一个实例，kv1 和 kv2 是同一个对象
        assertSame(kv1, kv2);
        // 最后设置的值会覆盖之前的值
        assertArrayEquals("value2".getBytes(), kv1.getValue());
    }

    @Test
    @DisplayName("测试特殊字符 family 和 qualifier")
    void testSpecialCharactersInFamilyAndQualifier() {
        KeyValue kv = new KeyValue();
        kv.setFamily("cf!@#$%");
        kv.setQualifier("col_with_special_chars_123");

        assertEquals("cf!@#$%", kv.getFamily());
        assertEquals("col_with_special_chars_123", kv.getQualifier());
        assertEquals("cf!@#$%:col_with_special_chars_123", kv.getColumn());
    }

    @Test
    @DisplayName("测试大数据值")
    void testLargeValue() {
        byte[] rowKey = "row1".getBytes();
        byte[] largeValue = new byte[1024 * 1024];  // 1MB
        Arrays.fill(largeValue, (byte) 0x42);

        KeyValue kv = new KeyValue(rowKey, "cf", "col", largeValue);

        assertArrayEquals(largeValue, kv.getValue());
        assertEquals(1024 * 1024, kv.getValue().length);
    }
}
