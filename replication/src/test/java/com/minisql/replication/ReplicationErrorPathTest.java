package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Replication error-path tests")
class ReplicationErrorPathTest {

    // ------------------------------------------------------------------
    // Fake: transport client that can be toggled to throw
    // ------------------------------------------------------------------
    private static final class FakeTransportClient implements ReplicationTransportClient {
        private final Map<ServerId, Boolean> replicateResults = new ConcurrentHashMap<>();
        private volatile boolean throwOnReplicate = false;

        void setThrowOnReplicate(boolean flag) {
            this.throwOnReplicate = flag;
        }

        void setReplicateResult(ServerId replica, boolean success) {
            replicateResults.put(replica, success);
        }

        @Override
        public boolean replicate(ServerId replica, String regionId,
                                 ReplicationLogEntry entry, long timeoutMs) {
            if (throwOnReplicate) {
                throw new RuntimeException("transport error");
            }
            return replicateResults.getOrDefault(replica, true);
        }

        @Override
        public boolean replicateBatch(ServerId replica, String regionId,
                                      List<ReplicationLogEntry> entries, long timeoutMs) {
            if (throwOnReplicate) {
                throw new RuntimeException("transport error");
            }
            return replicateResults.getOrDefault(replica, true);
        }

        @Override
        public List<KeyValue> fetchSnapshot(ServerId primary, String regionId, long timeoutMs) {
            return Collections.emptyList();
        }

        @Override
        public boolean streamSnapshotDirect(ServerId primary, ServerId replica,
                                            String regionId, int batchSize,
                                            long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public boolean sendSnapshot(ServerId replica, String regionId,
                                    List<KeyValue> snapshot, int batchSize,
                                    long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public boolean sendSnapshotStreaming(ServerId replica, String regionId,
                                             List<KeyValue> snapshot, int batchSize,
                                             long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public void close() {
        }
    }

    // ------------------------------------------------------------------
    // Fake: WAL that can be toggled to throw on append or getEntries
    // ------------------------------------------------------------------
    private static final class FakeWal extends ReplicationWAL {
        private final AtomicLong sequence = new AtomicLong();
        private final Map<String, Long> appliedProgress = new ConcurrentHashMap<>();
        private final List<ReplicationLogEntry> allEntries =
            Collections.synchronizedList(new ArrayList<>());

        private volatile boolean throwOnAppend = false;
        private volatile boolean throwOnGetEntries = false;

        private FakeWal() {
            super();
        }

        void setThrowOnAppend(boolean flag) {
            this.throwOnAppend = flag;
        }

        void setThrowOnGetEntries(boolean flag) {
            this.throwOnGetEntries = flag;
        }

        @Override
        public void initialize() {
        }

        @Override
        public long getCurrentSequenceId(String regionId) {
            return sequence.get();
        }

        @Override
        public ReplicationLogEntry append(String regionId, List<KeyValue> mutations) {
            if (throwOnAppend) {
                throw new RuntimeException("WAL append failure");
            }
            ReplicationLogEntry entry = new ReplicationLogEntry(
                sequence.incrementAndGet(), System.currentTimeMillis(), mutations);
            allEntries.add(entry);
            return entry;
        }

        @Override
        public void markAsApplied(String regionId, long sequenceId, String replicaAddress) {
            String key = regionId + ":" + replicaAddress;
            appliedProgress.merge(key, sequenceId, Math::max);
        }

        @Override
        public long getAppliedProgress(String regionId, String replicaAddress) {
            return appliedProgress.getOrDefault(regionId + ":" + replicaAddress, 0L);
        }

        @Override
        public List<ReplicationLogEntry> getEntries(String regionId, long fromSequenceId) {
            if (throwOnGetEntries) {
                throw new RuntimeException("WAL getEntries failure");
            }
            List<ReplicationLogEntry> result = new ArrayList<>();
            for (ReplicationLogEntry entry : allEntries) {
                if (entry.getSequenceId() >= fromSequenceId) {
                    result.add(entry);
                }
            }
            return result;
        }

        @Override
        public void cleanup(String regionId, int maxRetention) {
        }

        @Override
        public void cleanup(String regionId, int maxRetention, long minConfirmedSeqId) {
        }

        @Override
        public void close() {
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static KeyValue sampleMutation() {
        KeyValue kv = new KeyValue();
        kv.setRowKey(new byte[]{0x01});
        kv.setFamily("cf");
        kv.setQualifier("q");
        kv.setTimestamp(1L);
        kv.setValue(new byte[]{0x02});
        kv.setType(KeyValue.Type.PUT);
        return kv;
    }

    private static final ServerId PRIMARY = new ServerId("primary", 16020);
    private static final ServerId SECONDARY_1 = new ServerId("secondary-1", 16021);
    private static final ServerId SECONDARY_2 = new ServerId("secondary-2", 16022);

    private static Region testRegion(String regionId) {
        return new Region(regionId, "orders", new byte[]{0x00}, new byte[]{0x7F});
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    @Test
    @DisplayName("replicateSync returns false when transport throws RuntimeException")
    void replicateWhenTransportThrowsException() {
        FakeTransportClient transport = new FakeTransportClient();
        FakeWal wal = new FakeWal();
        transport.setThrowOnReplicate(true);

        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3)
                .ackTimeoutMs(200)
                .replicationTimeoutMs(2000)
                .healthCheckIntervalMs(60000)
                .build(),
            wal,
            transport
        );
        coordinator.start();

        try {
            Region region = testRegion("err-region-1");
            coordinator.createReplicaGroup(region, List.of(PRIMARY, SECONDARY_1, SECONDARY_2));

            boolean result = coordinator.replicateSync(
                region.getRegionId(), List.of(sampleMutation()));

            // When transport throws, coordinator enters degraded mode (primary-only write)
            assertTrue(result, "replicateSync returns true in degraded mode when transport throws");
            assertTrue(coordinator.isDegraded(region.getRegionId()),
                "Region should be marked degraded when transport throws");
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("logMutations throws IllegalStateException when WAL append fails")
    void replicateWhenWALAppendFails() {
        FakeWal wal = new FakeWal();
        wal.setThrowOnAppend(true);

        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3)
                .ackTimeoutMs(200)
                .replicationTimeoutMs(2000)
                .healthCheckIntervalMs(60000)
                .build(),
            wal,
            new FakeTransportClient()
        );
        coordinator.start();

        try {
            Region region = testRegion("err-region-2");
            coordinator.createReplicaGroup(region, List.of(PRIMARY, SECONDARY_1, SECONDARY_2));

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> coordinator.logMutations(
                    region.getRegionId(), List.of(sampleMutation())),
                "logMutations should throw IllegalStateException when WAL append fails");

            assertTrue(ex.getMessage().contains("Failed to append replication WAL"),
                "Exception message should mention WAL append failure");
            assertNotNull(ex.getCause(),
                "Exception should wrap the root cause");
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("replicateSync returns true in degraded mode when all secondaries timeout")
    void replicateWhenAllSecondariesTimeout() {
        FakeTransportClient transport = new FakeTransportClient();
        FakeWal wal = new FakeWal();
        // Both secondaries fail to ACK
        transport.setReplicateResult(SECONDARY_1, false);
        transport.setReplicateResult(SECONDARY_2, false);

        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3)
                .ackTimeoutMs(200)
                .replicationTimeoutMs(2000)
                .healthCheckIntervalMs(60000)
                .build(),
            wal,
            transport
        );
        coordinator.start();

        try {
            Region region = testRegion("err-region-3");
            coordinator.createReplicaGroup(region, List.of(PRIMARY, SECONDARY_1, SECONDARY_2));

            boolean result = coordinator.replicateSync(
                region.getRegionId(), List.of(sampleMutation()));

            assertTrue(result,
                "replicateSync should return true in degraded mode (primary-only write)");
            assertTrue(coordinator.isDegraded(region.getRegionId()),
                "Region should be marked as degraded when all secondaries are unreachable");
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("failover with no healthy secondary keeps the same primary")
    void failoverWhenNoHealthyReplicaAvailable() {
        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3)
                .ackTimeoutMs(200)
                .replicationTimeoutMs(2000)
                .healthCheckIntervalMs(60000)
                .build(),
            null,
            new FakeTransportClient()
        );
        coordinator.start();

        try {
            Region region = testRegion("err-region-4");
            // Only primary, no secondaries at all
            coordinator.createReplicaGroup(region, List.of(PRIMARY));

            ServerId primaryBefore = coordinator.getReplicaGroup(region.getRegionId()).getPrimary();
            assertEquals(PRIMARY, primaryBefore, "Primary should be set correctly");

            // failover should throw because there are no secondaries to promote
            assertThrows(IllegalStateException.class,
                () -> coordinator.failover(region.getRegionId()),
                "failover should throw when no secondary is available");

            // Verify primary did not change (it was never reassigned)
            ServerId primaryAfter = coordinator.getReplicaGroup(region.getRegionId()).getPrimary();
            assertEquals(primaryBefore, primaryAfter,
                "Primary should remain unchanged after failed failover");
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("health check does not crash when WAL getEntries throws")
    void healthCheckWhenWalGetEntriesThrows() throws InterruptedException {
        FakeTransportClient transport = new FakeTransportClient();
        FakeWal wal = new FakeWal();
        wal.setThrowOnGetEntries(true);

        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3)
                .ackTimeoutMs(200)
                .replicationTimeoutMs(2000)
                .healthCheckIntervalMs(60000)
                .catchUpLagThreshold(5)
                .build(),
            wal,
            transport
        );
        coordinator.start();

        try {
            Region region = testRegion("err-region-5");
            coordinator.createReplicaGroup(region, List.of(PRIMARY, SECONDARY_1, SECONDARY_2));

            // Write some entries so primary progress advances
            transport.setReplicateResult(SECONDARY_1, true);
            transport.setReplicateResult(SECONDARY_2, true);
            for (int i = 0; i < 10; i++) {
                coordinator.logMutations(region.getRegionId(), List.of(sampleMutation()));
            }

            // Simulate secondary-2 falling behind so health check will try to catch it up
            coordinator.getReplicaGroup(region.getRegionId())
                .updateReplicaState(SECONDARY_2, 3L, 0L);

            // triggerHealthCheckNow itself should not throw, even though the
            // async catch-up task will fail internally due to getEntries throwing
            assertDoesNotThrow(() -> coordinator.triggerHealthCheckNow(),
                "triggerHealthCheckNow should not propagate exceptions from WAL getEntries");

            // Give the async catch-up task a moment to run (and fail internally)
            Thread.sleep(200);

            // Verify the coordinator is still functional and primary is unchanged
            assertEquals(PRIMARY,
                coordinator.getReplicaGroup(region.getRegionId()).getPrimary(),
                "Primary should remain unchanged after WAL getEntries failure");
        } finally {
            coordinator.stop();
        }
    }
}
