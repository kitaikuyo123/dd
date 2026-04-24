package com.minisql.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("StorageColumnPredicate tests")
class StorageColumnPredicateTest {

    @Nested
    @DisplayName("construction and getters")
    class Construction {

        @Test
        @DisplayName("stores qualifier, operator, and value")
        void testFields() {
            byte[] val = {1, 2, 3};
            StorageColumnPredicate pred = new StorageColumnPredicate("age", ">", val);
            assertEquals("age", pred.getQualifier());
            assertEquals(">", pred.getOperator());
            assertArrayEquals(val, pred.getValue());
        }

        @Test
        @DisplayName("defensive copy on value in constructor")
        void testDefensiveCopy() {
            byte[] val = {1, 2, 3};
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "=", val);
            val[0] = 99;
            assertEquals(1, pred.getValue()[0]);
        }

        @Test
        @DisplayName("defensive copy on value getter")
        void testGetterDefensiveCopy() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "=", new byte[]{5, 6});
            pred.getValue()[0] = 99;
            assertEquals(5, pred.getValue()[0]);
        }

        @Test
        @DisplayName("null value accepted")
        void testNullValue() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "=", null);
            assertNull(pred.getValue());
        }
    }

    @Nested
    @DisplayName("matchesQualifier")
    class MatchesQualifier {

        @Test
        @DisplayName("matches same qualifier")
        void testMatch() {
            StorageColumnPredicate pred = new StorageColumnPredicate("age", ">", new byte[]{0});
            assertTrue(pred.matchesQualifier("age"));
        }

        @Test
        @DisplayName("does not match different qualifier")
        void testNoMatch() {
            StorageColumnPredicate pred = new StorageColumnPredicate("age", ">", new byte[]{0});
            assertFalse(pred.matchesQualifier("name"));
        }

        @Test
        @DisplayName("null qualifier never matches")
        void testNullQualifier() {
            StorageColumnPredicate pred = new StorageColumnPredicate(null, ">", new byte[]{0});
            assertFalse(pred.matchesQualifier("age"));
            assertFalse(pred.matchesQualifier(null));
        }
    }
}
