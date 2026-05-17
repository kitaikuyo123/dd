package com.minisql.common.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ValueComparator 单元测试")
class ValueComparatorTest {

    // ==================== typedEquals ====================

    @Nested
    @DisplayName("typedEquals")
    class TypedEquals {

        @Test
        @DisplayName("null 参与比较 → false（SQL 语义）")
        void nullReturnsFalse() {
            assertFalse(ValueComparator.typedEquals(null, null));
            assertFalse(ValueComparator.typedEquals(1, null));
            assertFalse(ValueComparator.typedEquals(null, 1));
        }

        @Test
        @DisplayName("同类型 Integer 相等")
        void sameInt() {
            assertTrue(ValueComparator.typedEquals(42, 42));
            assertFalse(ValueComparator.typedEquals(1, 2));
        }

        @Test
        @DisplayName("同类型 String 相等")
        void sameString() {
            assertTrue(ValueComparator.typedEquals("abc", "abc"));
            assertFalse(ValueComparator.typedEquals("abc", "xyz"));
        }

        @Test
        @DisplayName("跨数值类型：Integer(5) == Long(5)")
        void crossNumericEqual() {
            assertTrue(ValueComparator.typedEquals(Integer.valueOf(5), Long.valueOf(5L)));
            assertTrue(ValueComparator.typedEquals(Integer.valueOf(5), Double.valueOf(5.0)));
            assertTrue(ValueComparator.typedEquals(Long.valueOf(5L), Double.valueOf(5.0)));
        }

        @Test
        @DisplayName("跨数值类型：不同值不相等")
        void crossNumericNotEqual() {
            assertFalse(ValueComparator.typedEquals(Integer.valueOf(5), Long.valueOf(6L)));
        }

        @Test
        @DisplayName("Float vs Double 相等（精确可表示值）")
        void floatDouble() {
            assertTrue(ValueComparator.typedEquals(3.5f, 3.5));
        }
    }

    // ==================== typedCompare ====================

    @Nested
    @DisplayName("typedCompare")
    class TypedCompare {

        @Test
        @DisplayName("nulls last：null > 非null")
        void nullsLast() {
            assertEquals(0, ValueComparator.typedCompare(null, null));
            assertTrue(ValueComparator.typedCompare(null, 1) > 0);
            assertTrue(ValueComparator.typedCompare(1, null) < 0);
        }

        @Test
        @DisplayName("同类型 Integer 排序")
        void sameInt() {
            assertTrue(ValueComparator.typedCompare(5, 10) < 0);
            assertTrue(ValueComparator.typedCompare(10, 5) > 0);
            assertEquals(0, ValueComparator.typedCompare(42, 42));
        }

        @Test
        @DisplayName("同类型 String 排序")
        void sameString() {
            assertTrue(ValueComparator.typedCompare("abc", "xyz") < 0);
            assertTrue(ValueComparator.typedCompare("xyz", "abc") > 0);
            assertEquals(0, ValueComparator.typedCompare("hello", "hello"));
        }

        @Test
        @DisplayName("跨数值类型：Integer(10) > Long(5)")
        void crossNumericOrdering() {
            assertTrue(ValueComparator.typedCompare(10, 5L) > 0);
            assertTrue(ValueComparator.typedCompare(5L, 10) < 0);
            assertEquals(0, ValueComparator.typedCompare(5, 5L));
        }

        @Test
        @DisplayName("跨数值类型：Integer vs Double")
        void intVsDouble() {
            assertTrue(ValueComparator.typedCompare(3, 2.5) > 0);
            assertTrue(ValueComparator.typedCompare(2.5, 3) < 0);
        }

        @Test
        @DisplayName("Long vs Double 精度安全")
        void longVsDouble() {
            assertEquals(0, ValueComparator.typedCompare(100L, 100.0));
        }

        @Test
        @DisplayName("异类型非数值 → toString 字典序兜底")
        void fallbackToString() {
            assertDoesNotThrow(() -> ValueComparator.typedCompare(new Object(), new Object()));
        }
    }

    // ==================== 遗留 API 兼容 ====================

    @Nested
    @DisplayName("遗留 compare（兼容）")
    class LegacyCompare {

        @Test
        @DisplayName("Integer vs Long — isInstance fallback")
        void intVsLong() {
            int cmp = ValueComparator.compare(Integer.valueOf(5), Long.valueOf(5L));
            assertEquals(0, cmp);
        }

        @Test
        @DisplayName("same type works")
        void sameType() {
            assertTrue(ValueComparator.compare(5, 10) < 0);
            assertTrue(ValueComparator.compare("abc", "xyz") < 0);
        }

        @Test
        @DisplayName("nulls last")
        void nullsLast() {
            assertTrue(ValueComparator.compare(null, "a") > 0);
            assertTrue(ValueComparator.compare("a", null) < 0);
        }
    }

    @Nested
    @DisplayName("遗留 compareWithNumericCoercion（兼容）")
    class LegacyCoercion {

        @Test
        @DisplayName("字符串数值解析")
        void stringParsing() {
            assertTrue(ValueComparator.compareWithNumericCoercion("10", "5") > 0);
            assertEquals(0, ValueComparator.compareWithNumericCoercion("3.14", "3.14"));
        }

        @Test
        @DisplayName("nulls last")
        void nullsLast() {
            assertTrue(ValueComparator.compareWithNumericCoercion(null, 1) > 0);
            assertTrue(ValueComparator.compareWithNumericCoercion(1, null) < 0);
        }
    }
}
