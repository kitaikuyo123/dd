package com.minisql.replication;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicationConfig tests")
class ReplicationConfigTest {

    @Nested
    @DisplayName("Builder defaults")
    class BuilderDefaults {

        @Test
        @DisplayName("builder applies all documented defaults")
        void testBuilderDefaults() {
            ReplicationConfig config = ReplicationConfig.builder(3).build();

            assertEquals(3, config.getReplicationFactor());
            assertEquals(5000L, config.getReplicationTimeoutMs());
            assertEquals(3, config.getMaxRetryCount());
            assertEquals(10000L, config.getHealthCheckIntervalMs());
            assertEquals(3000L, config.getAckTimeoutMs());
            assertEquals(10000, config.getWalRetentionCount());
            assertFalse(config.isSyncReplicationEnabled());
            assertTrue(config.isQuorumAckEnabled());
            assertEquals(64, config.getMaxReplicationBatchSize());
        }
    }

    @Nested
    @DisplayName("Builder customisation")
    class BuilderCustomisation {

        @Test
        @DisplayName("all builder setters override defaults")
        void testBuilderOverrides() {
            ReplicationConfig config = ReplicationConfig.builder(5)
                .replicationTimeoutMs(9999L)
                .maxRetryCount(7)
                .healthCheckIntervalMs(8888L)
                .ackTimeoutMs(7777L)
                .walRetentionCount(500)
                .syncReplicationEnabled(true)
                .quorumAckEnabled(false)
                .build();

            assertEquals(5, config.getReplicationFactor());
            assertEquals(9999L, config.getReplicationTimeoutMs());
            assertEquals(7, config.getMaxRetryCount());
            assertEquals(8888L, config.getHealthCheckIntervalMs());
            assertEquals(7777L, config.getAckTimeoutMs());
            assertEquals(500, config.getWalRetentionCount());
            assertTrue(config.isSyncReplicationEnabled());
            assertFalse(config.isQuorumAckEnabled());
        }

        @Test
        @DisplayName("builder methods are chainable")
        void testBuilderChaining() {
            ReplicationConfig.Builder builder = ReplicationConfig.builder(1);
            assertSame(builder, builder.replicationTimeoutMs(100L));
            assertSame(builder, builder.maxRetryCount(1));
            assertSame(builder, builder.healthCheckIntervalMs(100L));
            assertSame(builder, builder.ackTimeoutMs(100L));
            assertSame(builder, builder.walRetentionCount(1));
            assertSame(builder, builder.syncReplicationEnabled(true));
            assertSame(builder, builder.quorumAckEnabled(true));
            assertSame(builder, builder.maxReplicationBatchSize(32));
        }
    }

    @Nested
    @DisplayName("getRequiredAcks calculation")
    class RequiredAcks {

        @Test
        @DisplayName("quorum ack computes majority: (n-1)/2 + 1")
        void testQuorumAcks() {
            ReplicationConfig config = ReplicationConfig.builder(3)
                .quorumAckEnabled(true)
                .build();

            // 1 replica -> (1-1)/2 + 1 = 1
            assertEquals(1, config.getRequiredAcks(1));
            // 3 replicas -> (3-1)/2 + 1 = 2
            assertEquals(2, config.getRequiredAcks(3));
            // 5 replicas -> (5-1)/2 + 1 = 3
            assertEquals(3, config.getRequiredAcks(5));
        }

        @Test
        @DisplayName("non-quorum ack requires all replicas")
        void testAllAcks() {
            ReplicationConfig config = ReplicationConfig.builder(3)
                .quorumAckEnabled(false)
                .build();

            assertEquals(1, config.getRequiredAcks(1));
            assertEquals(3, config.getRequiredAcks(3));
            assertEquals(5, config.getRequiredAcks(5));
        }

        @Test
        @DisplayName("quorum with even replica count still returns majority")
        void testQuorumEvenReplicas() {
            ReplicationConfig config = ReplicationConfig.builder(4)
                .quorumAckEnabled(true)
                .build();

            // 4 replicas -> (4-1)/2 + 1 = 2
            assertEquals(2, config.getRequiredAcks(4));
            // 2 replicas -> (2-1)/2 + 1 = 1
            assertEquals(1, config.getRequiredAcks(2));
        }
    }

    @Test
    @DisplayName("replication factor of 1 is accepted")
    void testSingleReplicationFactor() {
        ReplicationConfig config = ReplicationConfig.builder(1).build();
        assertEquals(1, config.getReplicationFactor());
    }

    @Test
    @DisplayName("multiple builds from same builder produce independent configs")
    void testMultipleBuilds() {
        ReplicationConfig.Builder builder = ReplicationConfig.builder(3)
            .replicationTimeoutMs(1000L);

        ReplicationConfig first = builder.build();
        builder.replicationTimeoutMs(2000L);
        ReplicationConfig second = builder.build();

        assertEquals(1000L, first.getReplicationTimeoutMs());
        assertEquals(2000L, second.getReplicationTimeoutMs());
    }
}
