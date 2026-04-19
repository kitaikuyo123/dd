package com.minisql.common.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("BytesUtil 单元测试")
class BytesUtilTest {

    // ---- long 往返 ----

    @Test
    @DisplayName("测试 long → byte[] → long 往返 - 正数")
    void testLongRoundTripPositive() {
        long val = 123456789L;
        assertEquals(val, BytesUtil.toLong(BytesUtil.toBytes(val)));
    }

    @Test
    @DisplayName("测试 long 往返 - 零")
    void testLongRoundTripZero() {
        assertEquals(0L, BytesUtil.toLong(BytesUtil.toBytes(0L)));
    }

    @Test
    @DisplayName("测试 long 往返 - 负数")
    void testLongRoundTripNegative() {
        long val = -987654321L;
        assertEquals(val, BytesUtil.toLong(BytesUtil.toBytes(val)));
    }

    @Test
    @DisplayName("测试 long 往返 - Long.MAX_VALUE")
    void testLongRoundTripMax() {
        assertEquals(Long.MAX_VALUE, BytesUtil.toLong(BytesUtil.toBytes(Long.MAX_VALUE)));
    }

    @Test
    @DisplayName("测试 long 往返 - Long.MIN_VALUE")
    void testLongRoundTripMin() {
        assertEquals(Long.MIN_VALUE, BytesUtil.toLong(BytesUtil.toBytes(Long.MIN_VALUE)));
    }

    // ---- int 往返 ----

    @Test
    @DisplayName("测试 int → byte[] → int 往返 - 正数")
    void testIntRoundTripPositive() {
        int val = 123456;
        assertEquals(val, BytesUtil.toInt(BytesUtil.toBytes(val)));
    }

    @Test
    @DisplayName("测试 int 往返 - 零")
    void testIntRoundTripZero() {
        assertEquals(0, BytesUtil.toInt(BytesUtil.toBytes(0)));
    }

    @Test
    @DisplayName("测试 int 往返 - 负数")
    void testIntRoundTripNegative() {
        int val = -99999;
        assertEquals(val, BytesUtil.toInt(BytesUtil.toBytes(val)));
    }

    @Test
    @DisplayName("测试 int 往返 - Integer.MAX_VALUE")
    void testIntRoundTripMax() {
        assertEquals(Integer.MAX_VALUE, BytesUtil.toInt(BytesUtil.toBytes(Integer.MAX_VALUE)));
    }

    @Test
    @DisplayName("测试 int 往返 - Integer.MIN_VALUE")
    void testIntRoundTripMin() {
        assertEquals(Integer.MIN_VALUE, BytesUtil.toInt(BytesUtil.toBytes(Integer.MIN_VALUE)));
    }

    // ---- String 往返 ----

    @Test
    @DisplayName("测试 String → byte[] → String 往返")
    void testStringRoundTrip() {
        String s = "hello world";
        assertEquals(s, BytesUtil.toString(BytesUtil.toBytes(s)));
    }

    @Test
    @DisplayName("测试 String 往返 - 中文")
    void testStringRoundTripChinese() {
        String s = "你好世界 MiniSQL";
        assertEquals(s, BytesUtil.toString(BytesUtil.toBytes(s)));
    }

    @Test
    @DisplayName("测试 String 往返 - 空字符串")
    void testStringRoundTripEmpty() {
        assertEquals("", BytesUtil.toString(BytesUtil.toBytes("")));
    }

    @Test
    @DisplayName("测试 toBytes(null) 返回 null")
    void testToBytesNull() {
        assertNull(BytesUtil.toBytes((String) null));
    }

    @Test
    @DisplayName("测试 toString(null) 返回 null")
    void testToStringNull() {
        assertNull(BytesUtil.toString(null));
    }

    // ---- toLong / toInt with offset ----

    @Test
    @DisplayName("测试 toLong 带偏移量")
    void testToLongWithOffset() {
        long val = 42L;
        byte[] bytes = new byte[16];
        byte[] encoded = BytesUtil.toBytes(val);
        System.arraycopy(encoded, 0, bytes, 4, 8);
        assertEquals(val, BytesUtil.toLong(bytes, 4));
    }

    @Test
    @DisplayName("测试 toInt 带偏移量")
    void testToIntWithOffset() {
        int val = 42;
        byte[] bytes = new byte[12];
        byte[] encoded = BytesUtil.toBytes(val);
        System.arraycopy(encoded, 0, bytes, 4, 4);
        assertEquals(val, BytesUtil.toInt(bytes, 4));
    }

    // ---- compareTo ----

    @Test
    @DisplayName("测试 compareTo - 相同数组")
    void testCompareToEqual() {
        byte[] a = "abc".getBytes();
        byte[] b = "abc".getBytes();
        assertEquals(0, BytesUtil.compareTo(a, b));
    }

    @Test
    @DisplayName("测试 compareTo - a < b")
    void testCompareToLess() {
        byte[] a = "abc".getBytes();
        byte[] b = "abd".getBytes();
        assertTrue(BytesUtil.compareTo(a, b) < 0);
    }

    @Test
    @DisplayName("测试 compareTo - a > b")
    void testCompareToGreater() {
        byte[] a = "abd".getBytes();
        byte[] b = "abc".getBytes();
        assertTrue(BytesUtil.compareTo(a, b) > 0);
    }

    @Test
    @DisplayName("测试 compareTo - 前缀较短的小于较长的")
    void testCompareToPrefix() {
        byte[] a = "ab".getBytes();
        byte[] b = "abc".getBytes();
        assertTrue(BytesUtil.compareTo(a, b) < 0);
    }

    @Test
    @DisplayName("测试 compareTo - 带偏移量和长度")
    void testCompareToWithOffsetAndLength() {
        byte[] buf1 = "xxabc".getBytes();
        byte[] buf2 = "yyabc".getBytes();
        assertEquals(0, BytesUtil.compareTo(buf1, 2, 3, buf2, 2, 3));
    }

    // ---- nextKey ----

    @Test
    @DisplayName("测试 nextKey - 正常追加零字节")
    void testNextKeyAppendsZero() {
        byte[] key = "row1".getBytes();
        byte[] next = BytesUtil.nextKey(key);
        assertEquals(key.length + 1, next.length);
        assertEquals(0, next[next.length - 1]);
        assertArrayEquals(key, java.util.Arrays.copyOf(next, key.length));
    }

    @Test
    @DisplayName("测试 nextKey - null 输入返回 {0}")
    void testNextKeyNull() {
        byte[] next = BytesUtil.nextKey(null);
        assertArrayEquals(new byte[]{0}, next);
    }

    @Test
    @DisplayName("测试 nextKey - 空数组返回 {0}")
    void testNextKeyEmpty() {
        byte[] next = BytesUtil.nextKey(new byte[0]);
        assertArrayEquals(new byte[]{0}, next);
    }

    // ---- hashCode / equals ----

    @Test
    @DisplayName("测试 hashCode - 相同内容相同哈希")
    void testHashCodeSameContent() {
        byte[] a = "hello".getBytes();
        byte[] b = "hello".getBytes();
        assertEquals(BytesUtil.hashCode(a), BytesUtil.hashCode(b));
    }

    @Test
    @DisplayName("测试 equals - 相同内容")
    void testEqualsSameContent() {
        assertTrue(BytesUtil.equals("abc".getBytes(), "abc".getBytes()));
    }

    @Test
    @DisplayName("测试 equals - 不同内容")
    void testEqualsDifferentContent() {
        assertFalse(BytesUtil.equals("abc".getBytes(), "abd".getBytes()));
    }

    @Test
    @DisplayName("测试 equals - 不同长度")
    void testEqualsDifferentLength() {
        assertFalse(BytesUtil.equals("ab".getBytes(), "abc".getBytes()));
    }

    @Test
    @DisplayName("测试 equals - 同一引用")
    void testEqualsSameReference() {
        byte[] a = "abc".getBytes();
        assertTrue(BytesUtil.equals(a, a));
    }

    @Test
    @DisplayName("测试 equals - null 参数")
    void testEqualsNull() {
        assertFalse(BytesUtil.equals(null, "abc".getBytes()));
        assertFalse(BytesUtil.equals("abc".getBytes(), null));
        assertTrue(BytesUtil.equals(null, null));
    }

    // ---- bytesToHex ----

    @Test
    @DisplayName("测试 bytesToHex - 正常输出")
    void testBytesToHex() {
        byte[] data = new byte[]{(byte) 0xFF, 0x00, 0x0A};
        String hex = BytesUtil.bytesToHex(data);
        assertEquals("FF 00 0A ", hex);
    }

    @Test
    @DisplayName("测试 bytesToHex - null 输入")
    void testBytesToHexNull() {
        assertEquals("null", BytesUtil.bytesToHex(null));
    }

    // ---- isKeyInRange ----

    @Test
    @DisplayName("测试 isKeyInRange - 在范围内")
    void testIsKeyInRangeInRange() {
        byte[] key = "b".getBytes();
        byte[] start = "a".getBytes();
        byte[] end = "c".getBytes();
        assertTrue(BytesUtil.isKeyInRange(key, start, end));
    }

    @Test
    @DisplayName("测试 isKeyInRange - 等于 start（包含）")
    void testIsKeyInRangeEqualStart() {
        byte[] key = "a".getBytes();
        byte[] start = "a".getBytes();
        byte[] end = "c".getBytes();
        assertTrue(BytesUtil.isKeyInRange(key, start, end));
    }

    @Test
    @DisplayName("测试 isKeyInRange - 等于 end（不包含）")
    void testIsKeyInRangeEqualEnd() {
        byte[] key = "c".getBytes();
        byte[] start = "a".getBytes();
        byte[] end = "c".getBytes();
        assertFalse(BytesUtil.isKeyInRange(key, start, end));
    }

    @Test
    @DisplayName("测试 isKeyInRange - 小于 start")
    void testIsKeyInRangeBelowStart() {
        byte[] key = "0".getBytes();
        byte[] start = "a".getBytes();
        byte[] end = "c".getBytes();
        assertFalse(BytesUtil.isKeyInRange(key, start, end));
    }

    @Test
    @DisplayName("测试 isKeyInRange - null start 表示无下界")
    void testIsKeyInRangeNullStart() {
        byte[] key = "a".getBytes();
        assertTrue(BytesUtil.isKeyInRange(key, null, "c".getBytes()));
    }

    @Test
    @DisplayName("测试 isKeyInRange - null end 表示无上界")
    void testIsKeyInRangeNullEnd() {
        byte[] key = "z".getBytes();
        assertTrue(BytesUtil.isKeyInRange(key, "a".getBytes(), null));
    }

    @Test
    @DisplayName("测试 isKeyInRange - null key 返回 false")
    void testIsKeyInRangeNullKey() {
        assertFalse(BytesUtil.isKeyInRange(null, "a".getBytes(), "c".getBytes()));
    }

    @Test
    @DisplayName("测试 isKeyInRange - 全 null 边界等同于全范围")
    void testIsKeyInRangeAllNullBounds() {
        byte[] key = "anything".getBytes();
        assertTrue(BytesUtil.isKeyInRange(key, null, null));
    }

    // ---- EMPTY_BYTE_ARRAY ----

    @Test
    @DisplayName("测试 EMPTY_BYTE_ARRAY 常量")
    void testEmptyByteArray() {
        assertNotNull(BytesUtil.EMPTY_BYTE_ARRAY);
        assertEquals(0, BytesUtil.EMPTY_BYTE_ARRAY.length);
    }
}
