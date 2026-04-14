package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.storage.MySQLConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
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
        public List<KeyValue> fetchSnapshot(ServerId primary, String regionId, long timeoutMs) {
            return snapshot;
        }

        @Override
        public boolean sendSnapshot(ServerId replica, String regionId, List<KeyValue> snapshot, int batchSize, long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public void close() {
        }
    }

    private static final class FakeWal extends ReplicationWAL {
        private final AtomicLong sequence = new AtomicLong();

        private FakeWal() {
            super(MySQLConfig.builder("jdbc:mysql://localhost:3306/test", "root", "root").build());
        }

        @Override
        public void initialize() {
        }

        @Override
        public long getCurrentSequenceId(String regionId) {
            return sequence.get();
        }

        @Override
        public ReplicationLogEntry append(String regionId, List<KeyValue> mutations) throws SQLException {
            return new ReplicationLogEntry(sequence.incrementAndGet(), System.currentTimeMillis(), mutations);
        }

        @Override
        public void markAsApplied(String regionId, long sequenceId, String replicaAddress) {
        }

        @Override
        public void close() {
        }
    }
}
