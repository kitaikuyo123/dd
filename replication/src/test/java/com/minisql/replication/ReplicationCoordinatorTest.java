package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicationCoordinator tests")
class ReplicationCoordinatorTest {

    @Test
    @DisplayName("quorum config is honored during replication")
    void testReplicationUsesConfigForAckPolicy() {
        FakeTransportClient transport = new FakeTransportClient();
        FakeWal wal = new FakeWal();
        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3)
                .ackTimeoutMs(200)
                .replicationTimeoutMs(500)
                .quorumAckEnabled(false)
                .build(),
            wal,
            transport
        );
        coordinator.start();

        try {
            Region region = new Region("region-1", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId secondary1 = new ServerId("secondary-1", 16021);
            ServerId secondary2 = new ServerId("secondary-2", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, secondary1, secondary2));

            transport.setReplicateResult(secondary1, true);
            transport.setReplicateResult(secondary2, false);

            assertFalse(coordinator.replicateSync(region.getRegionId(), List.of(sampleMutation())));
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("failover promotes healthiest most up-to-date secondary")
    void testFailoverSelectsBestReplica() {
        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3).build(),
            null,
            new FakeTransportClient()
        );
        coordinator.start();

        try {
            Region region = new Region("region-2", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId lagging = new ServerId("lagging", 16021);
            ServerId best = new ServerId("best", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, lagging, best));

            coordinator.getReplicaGroup(region.getRegionId()).updateReplicaState(lagging, 5L, 10L);
            coordinator.getReplicaGroup(region.getRegionId()).updateReplicaState(best, 8L, 0L);
            coordinator.failover(region.getRegionId());

            assertEquals(best, coordinator.getReplicaGroup(region.getRegionId()).getPrimary());
            assertEquals(ReplicaRole.PRIMARY, coordinator.getReplicaGroup(region.getRegionId()).getReplicaRole(best));
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("addReplicaSync completes full sync and marks replica secondary")
    void testAddReplicaSyncTransitionsRole() {
        FakeTransportClient transport = new FakeTransportClient();
        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3).build(),
            null,
            transport
        );
        coordinator.start();

        try {
            Region region = new Region("region-3", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId secondary = new ServerId("secondary", 16021);
            ServerId newReplica = new ServerId("new-replica", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, secondary));
            transport.setSnapshot(List.of(sampleMutation()));

            assertTrue(coordinator.addReplicaSync(region.getRegionId(), newReplica, 1000));
            assertEquals(ReplicaRole.SECONDARY,
                coordinator.getReplicaGroup(region.getRegionId()).getReplicaRole(newReplica));
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("idle system with no write traffic does not trigger primary stale")
    void testIdleSystemNotDetectedAsStale() throws InterruptedException {
        FakeTransportClient transport = new FakeTransportClient();
        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3)
                .healthCheckIntervalMs(100)   // very short interval so the check runs quickly
                .build(),
            null,
            transport
        );
        coordinator.start();

        try {
            Region region = new Region("region-idle", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId secondary1 = new ServerId("secondary-1", 16021);
            ServerId secondary2 = new ServerId("secondary-2", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, secondary1, secondary2));

            // Wait well past the stale threshold (healthCheckIntervalMs * 3 = 300ms)
            Thread.sleep(600);

            // Primary should still be the same — no failover triggered
            assertEquals(primary, coordinator.getReplicaGroup(region.getRegionId()).getPrimary());
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("health check refreshes primary lastUpdateTime as heartbeat")
    void testHealthCheckRefreshesPrimaryHeartbeat() throws InterruptedException {
        FakeTransportClient transport = new FakeTransportClient();
        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3)
                .healthCheckIntervalMs(100)
                .build(),
            null,
            transport
        );
        coordinator.start();

        try {
            Region region = new Region("region-hb", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId secondary1 = new ServerId("secondary-1", 16021);
            ServerId secondary2 = new ServerId("secondary-2", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, secondary1, secondary2));

            // Record the initial lastUpdateTime
            long initialTime = coordinator.getReplicaGroup(region.getRegionId())
                .getReplicaState(primary).getLastUpdateTime();

            // Wait for several health check cycles
            Thread.sleep(500);

            // The primary's lastUpdateTime should have been refreshed by health checks
            long afterTime = coordinator.getReplicaGroup(region.getRegionId())
                .getReplicaState(primary).getLastUpdateTime();
            assertTrue(afterTime > initialTime,
                "Primary lastUpdateTime should be refreshed by health check heartbeat");
            assertEquals(primary, coordinator.getReplicaGroup(region.getRegionId()).getPrimary());
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("auto catch-up sends WAL entries to stale secondary via health check")
    void testAutoCatchUpFromWal() throws InterruptedException {
        FakeTransportClient transport = new FakeTransportClient();
        FakeWal wal = new FakeWal();
        ReplicationCoordinator coordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3)
                .healthCheckIntervalMs(100)
                .catchUpLagThreshold(5)
                .build(),
            wal,
            transport
        );
        coordinator.start();

        try {
            Region region = new Region("region-catchup", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId secondary1 = new ServerId("secondary-1", 16021);
            ServerId secondary2 = new ServerId("secondary-2", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, secondary1, secondary2));

            transport.setReplicateResult(secondary1, true);
            transport.setReplicateResult(secondary2, true);

            // Write 10 entries — both secondaries ACK
            for (int i = 0; i < 10; i++) {
                coordinator.logMutations(region.getRegionId(), List.of(sampleMutation()));
            }
            // Simulate secondary2 falling behind: set its progress back
            coordinator.getReplicaGroup(region.getRegionId())
                .updateReplicaState(secondary2, 3L, 0L);

            // Wait for health check to trigger catch-up (lag = 10-3 = 7 > threshold 5)
            Thread.sleep(600);

            // secondary2 should have been caught up
            long progress = coordinator.getReplicaGroup(region.getRegionId())
                .getReplicaState(secondary2).getLastAppliedSequenceId();
            assertTrue(progress >= 10,
                "Secondary2 should have been caught up via health check, progress=" + progress);
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("WAL markAsApplied persists and restores progress")
    void testWalProgressPersistence() {
        FakeWal wal = new FakeWal();
        wal.initialize();

        // Simulate persisting progress for a replica
        wal.markAsApplied("region-1", 42L, "host1:1234");
        wal.markAsApplied("region-1", 45L, "host2:5678");
        wal.markAsApplied("region-1", 40L, "host1:1234"); // update host1 to lower (should not decrease)

        // Read back
        assertEquals(42L, wal.getAppliedProgress("region-1", "host1:1234"));
        assertEquals(45L, wal.getAppliedProgress("region-1", "host2:5678"));
        assertEquals(0L, wal.getAppliedProgress("region-1", "unknown:9999"));
    }

    private KeyValue sampleMutation() {
        KeyValue kv = new KeyValue();
        kv.setRowKey(new byte[]{0x01});
        kv.setFamily("cf");
        kv.setQualifier("q");
        kv.setTimestamp(1L);
        kv.setValue(new byte[]{0x02});
        kv.setType(KeyValue.Type.PUT);
        return kv;
    }

    private static final class FakeTransportClient implements ReplicationTransportClient {
        private final java.util.Map<ServerId, Boolean> replicateResults = new java.util.concurrent.ConcurrentHashMap<>();
        private List<KeyValue> snapshot = Collections.emptyList();

        void setReplicateResult(ServerId replica, boolean success) {
            replicateResults.put(replica, success);
        }

        void setSnapshot(List<KeyValue> snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public boolean replicate(ServerId replica, String regionId, ReplicationLogEntry entry, long timeoutMs) {
            return replicateResults.getOrDefault(replica, true);
        }

        @Override
        public boolean replicateBatch(ServerId replica, String regionId, List<ReplicationLogEntry> entries, long timeoutMs) {
            return replicateResults.getOrDefault(replica, true);
        }

        @Override
        public List<KeyValue> fetchSnapshot(ServerId primary, String regionId, long timeoutMs) {
            return snapshot;
        }

        @Override
        public boolean streamSnapshotDirect(ServerId primary, ServerId replica, String regionId, int batchSize, long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public boolean sendSnapshot(ServerId replica, String regionId, List<KeyValue> snapshot, int batchSize, long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public boolean sendSnapshotStreaming(ServerId replica, String regionId, List<KeyValue> snapshot, int batchSize, long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public void close() {
        }
    }

    private static final class FakeWal extends ReplicationWAL {
        private final AtomicLong sequence = new AtomicLong();
        private final java.util.Map<String, Long> appliedProgress = new java.util.concurrent.ConcurrentHashMap<>();
        private final List<ReplicationLogEntry> allEntries = Collections.synchronizedList(new ArrayList<>());

        private FakeWal() {
            super();
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
            ReplicationLogEntry entry = new ReplicationLogEntry(sequence.incrementAndGet(), System.currentTimeMillis(), mutations);
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
}
