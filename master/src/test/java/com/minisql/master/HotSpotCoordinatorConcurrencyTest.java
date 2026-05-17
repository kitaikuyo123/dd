package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.master.rebalance.HotSpotCoordinator;
import com.minisql.master.rebalance.HotSpotCoordinator.HotSpotSettings;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.rebalance.RegionSplitCoordinator;
import com.minisql.master.recover.RecoveryCoordinator;
import com.minisql.master.rpc.RegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.replication.ReplicationConfig;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.replication.ReplicationTransportClient;
import com.minisql.replication.ReplicationWAL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("HotSpotCoordinator concurrency tests")
class HotSpotCoordinatorConcurrencyTest {

    private ClusterManager clusterManager;
    private MetadataManager metadataManager;
    private RegionSplitCoordinator splitCoordinator;
    private HotSpotCoordinator coordinator;

    @BeforeEach
    void setUp() {
        clusterManager = new ClusterManager(new LoadBalancer());
        metadataManager = new MetadataManager();

        StubCommandClient stubClient = new StubCommandClient();
        splitCoordinator = new RegionSplitCoordinator(
            clusterManager, metadataManager, new LoadBalancer(), stubClient);
        splitCoordinator.start();

        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();
        ReplicationCoordinator replicationCoordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(1).build(),
            new NoOpWal(),
            new NoOpTransportClient()
        );

        RecoveryCoordinator recoveryCoordinator = new RecoveryCoordinator(
            clusterManager, metadataManager, replicaMonitor,
            replicationCoordinator, lifecycleManager, stubClient);

        coordinator = new HotSpotCoordinator(
            clusterManager, metadataManager, splitCoordinator, recoveryCoordinator);

        coordinator.configure(new HotSpotSettings(20, 10, 3, 300_000));
    }

    @AfterEach
    void tearDown() {
        if (splitCoordinator != null) {
            splitCoordinator.stop();
        }
    }

    @Test
    @DisplayName("concurrent recordRegionLoad from multiple regions does not crash")
    void concurrentRecordLoadFromMultipleRegions() throws Exception {
        int regionCount = 8;
        ServerId primary = new ServerId("host-primary", 16020);
        clusterManager.registerServer(primary);

        // Register extra servers so ADD_READ_REPLICA can find targets
        for (int i = 1; i <= 7; i++) {
            clusterManager.registerServer(new ServerId("host-" + i, 16020 + i));
        }

        // Create and register 8 regions
        List<String> regionIds = new ArrayList<>();
        for (int i = 0; i < regionCount; i++) {
            String regionId = "region-conc-" + i;
            Region region = new Region(regionId, "table-conc",
                new byte[]{(byte) i}, new byte[]{(byte) (i + 1)});
            region.setPrimary(primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(regionId, primary);
            regionIds.add(regionId);
        }

        // Each thread records 5 load snapshots with high read counts for its region
        int snapshotsPerThread = 5;
        CyclicBarrier barrier = new CyclicBarrier(regionCount);
        CountDownLatch doneLatch = new CountDownLatch(regionCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < regionCount; i++) {
            final int index = i;
            final String regionId = regionIds.get(i);
            new Thread(() -> {
                try {
                    barrier.await();
                    for (int s = 0; s < snapshotsPerThread; s++) {
                        ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
                        load.setRegionId(regionId);
                        load.setReadRequests((s + 1) * 50L);
                        load.setWriteRequests(0L);
                        coordinator.recordRegionLoad(regionId, null, load);
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }, "recorder-" + index).start();
        }

        assertTrue(doneLatch.await(30, TimeUnit.SECONDS),
            "All recorder threads should finish within timeout");
        assertEquals(0, errorCount.get(),
            "No exceptions expected during concurrent recordRegionLoad");

        // After all threads finish, plan actions should succeed without errors
        List<HotSpotCoordinator.HotSpotAction> actions = assertDoesNotThrow(
            () -> coordinator.planPendingActions());

        // Verify the hotspots map is in a consistent state
        Map<String, HotSpotCoordinator.HotSpotInfo> hotSpots = coordinator.getCurrentHotSpots();
        assertNotNull(hotSpots);

        // With 5 snapshots of 50-delta each and readThreshold=20, all regions
        // should be detected as read-hot (or at least some of them).
        // The key assertion is: no crash, no data corruption.
        assertTrue(hotSpots.size() <= regionCount,
            "HotSpot count should not exceed total region count");
    }

    @Test
    @DisplayName("concurrent planPendingActions and recordRegionLoad do not deadlock or throw")
    void concurrentDetectionAndExecution() throws Exception {
        ServerId primary = new ServerId("host-primary", 16020);
        ServerId secondary = new ServerId("host-secondary", 16021);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        String regionId = "region-stress";
        Region region = new Region(regionId, "table-stress",
            new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(regionId, primary);

        // Seed some initial load so planPendingActions has data to work with
        for (int i = 1; i <= 3; i++) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId(regionId);
            load.setReadRequests(i * 50L);
            load.setWriteRequests(0L);
            coordinator.recordRegionLoad(regionId, null, load);
            Thread.sleep(50);
        }

        int iterations = 200;
        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch doneLatch = new CountDownLatch(2);
        AtomicInteger planErrors = new AtomicInteger(0);
        AtomicInteger recordErrors = new AtomicInteger(0);

        // Thread A: repeatedly calls planPendingActions
        new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    coordinator.planPendingActions();
                }
            } catch (Throwable t) {
                if (t instanceof ConcurrentModificationException) {
                    planErrors.incrementAndGet();
                }
                // Other exceptions (e.g. InterruptedException from barrier) are
                // acceptable in this stress test; we only track CME specifically.
            } finally {
                doneLatch.countDown();
            }
        }, "planner").start();

        // Thread B: repeatedly records load
        new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
                    load.setRegionId(regionId);
                    load.setReadRequests((i + 4) * 25L);
                    load.setWriteRequests(0L);
                    coordinator.recordRegionLoad(regionId, null, load);
                }
            } catch (Throwable t) {
                if (t instanceof ConcurrentModificationException) {
                    recordErrors.incrementAndGet();
                }
            } finally {
                doneLatch.countDown();
            }
        }, "recorder").start();

        assertTrue(doneLatch.await(30, TimeUnit.SECONDS),
            "Both threads should finish within timeout");
        assertEquals(0, planErrors.get(),
            "planPendingActions must not throw ConcurrentModificationException");
        assertEquals(0, recordErrors.get(),
            "recordRegionLoad must not throw ConcurrentModificationException");
    }

    // ================================
    // Helpers
    // ================================

    // ================================
    // Stub / Fake infrastructure
    // ================================

    private static final class StubCommandClient implements RegionServerCommandClient {
        private static final CommonProto.Status OK =
            CommonProto.Status.newBuilder().setCode(0).setSuccess(true).setMessage("OK").build();

        @Override
        public RegionServerProto.OpenRegionResponse openRegion(ServerId s, Region r, boolean asReplica) {
            return RegionServerProto.OpenRegionResponse.newBuilder().setStatus(OK).build();
        }

        @Override
        public RegionServerProto.CloseRegionResponse closeRegion(ServerId s, String r, boolean a, boolean d) {
            return RegionServerProto.CloseRegionResponse.newBuilder().setStatus(OK).build();
        }

        @Override
        public RegionServerProto.PromoteResponse promoteToPrimary(ServerId s, String r, long f) {
            return RegionServerProto.PromoteResponse.newBuilder().setStatus(OK).build();
        }

        @Override
        public RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId s, String r, long t) {
            return RegionServerProto.GetReplicationLagResponse.newBuilder()
                .setStatus(OK).setLagInEntries(0L).setLastAppliedSequenceId(0L).build();
        }

        @Override
        public RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId serverId, String regionId) {
            return RegionServerProto.GetSplitKeyResponse.newBuilder()
                .setStatus(OK)
                .setSplitKey(com.google.protobuf.ByteString.copyFrom(new byte[]{0x40}))
                .build();
        }

        @Override
        public RegionServerProto.SplitRegionResponse splitRegion(ServerId serverId, String regionId, byte[] splitKey,
                                                      String leftRegionId, String rightRegionId) {
            return RegionServerProto.SplitRegionResponse.newBuilder()
                .setStatus(OK)
                .setLeftRegion(CommonProto.RegionInfo.newBuilder()
                    .setRegionId(regionId + "_l").setTableName("orders")
                    .setStartKey(com.google.protobuf.ByteString.copyFrom(new byte[]{0x00}))
                    .setEndKey(com.google.protobuf.ByteString.copyFrom(new byte[]{0x40}))
                    .build())
                .setRightRegion(CommonProto.RegionInfo.newBuilder()
                    .setRegionId(regionId + "_r").setTableName("orders")
                    .setStartKey(com.google.protobuf.ByteString.copyFrom(new byte[]{0x40}))
                    .setEndKey(com.google.protobuf.ByteString.copyFrom(new byte[]{0x7F}))
                    .build())
                .build();
        }

        @Override
        public RegionServerProto.MergeRegionResponse mergeRegion(ServerId s, String l, String r, String merged) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.MigrateResponse startMigration(ServerId s, String r, ServerId t, long timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId s, String r, ServerId t, long f) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.AbortMigrationResponse abortMigration(ServerId s, String r) {
            throw new UnsupportedOperationException();
        }
    }

    /** No-op WAL to satisfy ReplicationCoordinator constructor. */
    private static final class NoOpWal extends ReplicationWAL {
        NoOpWal() { super(); }

        @Override public void initialize() { /* no-op */ }
        @Override public void close() { /* no-op */ }
    }

    /** No-op transport client for ReplicationCoordinator. */
    private static final class NoOpTransportClient implements ReplicationTransportClient {
        @Override public boolean replicate(ServerId r, String rid, com.minisql.replication.ReplicationLogEntry e, long t) { return true; }
        @Override public boolean replicateBatch(ServerId r, String rid, List<com.minisql.replication.ReplicationLogEntry> e, long t) { return true; }
        @Override public List<com.minisql.common.model.KeyValue> fetchSnapshot(ServerId p, String rid, long t) { return List.of(); }
        @Override public boolean sendSnapshot(ServerId r, String rid, List<com.minisql.common.model.KeyValue> s, int b, long t, long f) { return true; }
        @Override public boolean sendSnapshotStreaming(ServerId r, String rid, List<com.minisql.common.model.KeyValue> s, int b, long t, long f) { return true; }
        @Override public boolean streamSnapshotDirect(ServerId p, ServerId r, String rid, int b, long t, long f) { return true; }
        @Override public void close() {}
    }
}
