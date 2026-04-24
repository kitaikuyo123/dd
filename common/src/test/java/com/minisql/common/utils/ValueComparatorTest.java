package com.minisql.common.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ValueComparator 单元测试")
class ValueComparatorTest {

    @Test
    @DisplayName("both null → 0")
    void compareBothNull() {
        assertEquals(0, ValueComparator.compare(null, null));
    }

    @Test
    @DisplayName("left null → 1 (nulls last)")
    void compareLeftNull() {
        assertTrue(ValueComparator.compare(null, 1) > 0);
    }

    @Test
    @DisplayName("right null → -1 (nulls last)")
    void compareRightNull() {
        assertTrue(ValueComparator.compare(1, null) < 0);
    }

    @Test
    @DisplayName("same type Integer comparison")
    void compareSameTypeIntegers() {
        assertTrue(ValueComparator.compare(5, 10) < 0);
        assertTrue(ValueComparator.compare(10, 5) > 0);
        assertEquals(0, ValueComparator.compare(42, 42));
    }

    @Test
    @DisplayName("same type String comparison")
    void compareSameTypeStrings() {
        assertTrue(ValueComparator.compare("abc", "xyz") < 0);
        assertTrue(ValueComparator.compare("xyz", "abc") > 0);
        assertEquals(0, ValueComparator.compare("hello", "hello"));
    }

    @Test
    @DisplayName("Long values comparison")
    void compareLongValues() {
        assertTrue(ValueComparator.compare(100L, 200L) < 0);
        assertTrue(ValueComparator.compare(200L, 100L) > 0);
        assertEquals(0, ValueComparator.compare(42L, 42L));
    }

    @Test
    @DisplayName("Double values comparison")
    void compareDoubleValues() {
        assertTrue(ValueComparator.compare(1.5, 3.0) < 0);
        assertTrue(ValueComparator.compare(3.0, 1.5) > 0);
        assertEquals(0, ValueComparator.compare(2.5, 2.5));
    }

    @Test
    @DisplayName("different numeric types fall back to String comparison")
    void compareDifferentNumericTypes() {
        // Integer vs Long — isInstance returns false, so toString fallback
        int cmp = ValueComparator.compare(Integer.valueOf(5), Long.valueOf(5L));
        // "5" vs "5" → 0 (toString representations are identical)
        assertEquals(0, cmp);

        cmp = ValueComparator.compare(Integer.valueOf(10), Long.valueOf(5L));
        // "10" vs "5" → lexicographic, so '1' < '5' → negative
        assertTrue(cmp < 0);
    }

    @Test
    @DisplayName("non-Comparable types fall back to String comparison")
    void compareNonComparable() {
        // Use arrays as non-Comparable example (Object[] is not Comparable)
        int cmp = ValueComparator.compare(new Object(), new Object());
        // toString comparison of Object instances
        assertDoesNotThrow(() -> ValueComparator.compare(new Object(), new Object()));
    }

    @Test
    @DisplayName("null last ordering consistent with Collections.sort expectations")
    void nullLastConsistency() {
        assertTrue(ValueComparator.compare(null, "a") > 0, "null should be greater than non-null (nulls last)");
        assertTrue(ValueComparator.compare("a", null) < 0, "non-null should be less than null (nulls last)");
    }
}
