package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicationLogEntry tests")
class ReplicationLogEntryTest {

    private KeyValue kv(String row, String value) {
        return new KeyValue(row.getBytes(), "cf", "q", value.getBytes());
    }

    @Nested
    @DisplayName("Construction")
    class Construction {

        @Test
        @DisplayName("stores sequenceId, timestamp, and mutations")
        void testBasicConstruction() {
            List<KeyValue> mutations = List.of(kv("r1", "v1"), kv("r2", "v2"));
            ReplicationLogEntry entry = new ReplicationLogEntry(10L, 1000L, mutations);

            assertEquals(10L, entry.getSequenceId());
            assertEquals(1000L, entry.getTimestamp());
            assertEquals(2, entry.getMutations().size());
            assertArrayEquals("r1".getBytes(), entry.getMutations().get(0).getRowKey());
            assertArrayEquals("r2".getBytes(), entry.getMutations().get(1).getRowKey());
        }

        @Test
        @DisplayName("null mutations list is treated as empty")
        void testNullMutations() {
            ReplicationLogEntry entry = new ReplicationLogEntry(1L, 1L, null);
            assertNotNull(entry.getMutations());
            assertTrue(entry.getMutations().isEmpty());
        }

        @Test
        @DisplayName("empty mutations list is preserved as empty")
        void testEmptyMutations() {
            ReplicationLogEntry entry = new ReplicationLogEntry(1L, 1L, Collections.emptyList());
            assertNotNull(entry.getMutations());
            assertTrue(entry.getMutations().isEmpty());
        }

        @Test
        @DisplayName("mutations list is defensively copied")
        void testDefensiveCopy() {
            List<KeyValue> original = new ArrayList<>(List.of(kv("r1", "v1")));
            ReplicationLogEntry entry = new ReplicationLogEntry(1L, 1L, original);

            original.add(kv("r2", "v2"));
            assertEquals(1, entry.getMutations().size());
        }

        @Test
        @DisplayName("mutations list is unmodifiable")
        void testUnmodifiableMutations() {
            List<KeyValue> mutations = new ArrayList<>(List.of(kv("r1", "v1")));
            ReplicationLogEntry entry = new ReplicationLogEntry(1L, 1L, mutations);

            assertThrows(UnsupportedOperationException.class, () ->
                entry.getMutations().add(kv("r2", "v2"))
            );
        }

        @Test
        @DisplayName("zero and negative sequenceId and timestamp are stored as-is")
        void testZeroAndNegativeValues() {
            ReplicationLogEntry entry = new ReplicationLogEntry(0L, -5L, null);
            assertEquals(0L, entry.getSequenceId());
            assertEquals(-5L, entry.getTimestamp());
        }

        @Test
        @DisplayName("large sequenceId values are preserved")
        void testLargeSequenceId() {
            long largeId = Long.MAX_VALUE;
            ReplicationLogEntry entry = new ReplicationLogEntry(largeId, 100L, null);
            assertEquals(largeId, entry.getSequenceId());
        }
    }

    @Nested
    @DisplayName("Getters")
    class Getters {

        @Test
        @DisplayName("getSequenceId returns the assigned value")
        void testGetSequenceId() {
            ReplicationLogEntry entry = new ReplicationLogEntry(42L, 0L, null);
            assertEquals(42L, entry.getSequenceId());
        }

        @Test
        @DisplayName("getTimestamp returns the assigned value")
        void testGetTimestamp() {
            ReplicationLogEntry entry = new ReplicationLogEntry(0L, 999L, null);
            assertEquals(999L, entry.getTimestamp());
        }

        @Test
        @DisplayName("getMutations returns the same immutable list on repeated calls")
        void testGetMutationsIdempotent() {
            ReplicationLogEntry entry = new ReplicationLogEntry(1L, 1L, List.of(kv("r1", "v1")));
            List<KeyValue> first = entry.getMutations();
            List<KeyValue> second = entry.getMutations();
            assertSame(first, second);
        }
    }
}
