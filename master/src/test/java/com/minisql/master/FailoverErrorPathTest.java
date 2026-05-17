package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.recover.FailoverCoordinator;
import com.minisql.master.rpc.RegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FailoverCoordinator error path tests")
class FailoverErrorPathTest {

    private FailoverCoordinator coordinator;

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.shutdown();
        }
    }

    @Test
    @DisplayName("failover aborts when promote RPC fails")
    void failoverWhenPromoteRpcFails() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        Table table = new Table("fail_table");
        metadataManager.createTable(table);

        Region region = new Region("fail_r1", "fail_table",
            new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("fail_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondaryInfo = new ReplicaInfo("fail_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("fail_r1", primaryInfo);
        replicaMonitor.registerReplica("fail_r1", secondaryInfo);

        clusterManager.updateReplicaSequenceId("fail_r1", primary, 100L);
        clusterManager.updateReplicaSequenceId("fail_r1", secondary, 100L);

        FailingPromoteClient commandClient = new FailingPromoteClient();
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 100, 5000, 5000, 30000
        );

        coordinator.triggerEmergencyFailover("fail_r1");

        // Wait for async execution to complete
        assertTrue(commandClient.awaitCall(5, TimeUnit.SECONDS));

        // The promote RPC was called but returned failure
        assertEquals(1, commandClient.callCount);

        // Verify metadata primary was NOT changed (promotion failed)
        Region updated = metadataManager.getRegion("fail_r1");
        assertNotNull(updated);
        assertEquals(primary, updated.getPrimary(),
            "Primary should remain unchanged when promote RPC fails");
    }

    @Test
    @DisplayName("failover refuses candidate that is not caught up")
    void failoverWhenCandidateNotCaughtUp() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        Table table = new Table("lag_table");
        metadataManager.createTable(table);

        Region region = new Region("lag_r1", "lag_table",
            new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("lag_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondaryInfo = new ReplicaInfo("lag_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("lag_r1", primaryInfo);
        replicaMonitor.registerReplica("lag_r1", secondaryInfo);

        // Secondary is behind: primary at 100, secondary at 50
        clusterManager.updateReplicaSequenceId("lag_r1", primary, 100L);
        clusterManager.updateReplicaSequenceId("lag_r1", secondary, 50L);

        TrackingCommandClient commandClient = new TrackingCommandClient();
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 100, 5000, 5000, 30000
        );

        coordinator.triggerEmergencyFailover("lag_r1");

        Thread.sleep(500);

        // The candidate should be rejected because it's behind (seqId 50 < max 100)
        // No promotion should happen
        assertEquals(0, commandClient.promotionCount,
            "Lagging candidate should not be promoted");
    }

    @Test
    @DisplayName("normal failover blocked when max retries exhausted")
    void failoverWhenMaxRetriesExhausted() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        TrackingCommandClient commandClient = new TrackingCommandClient();
        // maxFailoverRetries = 0: any normal failover is immediately blocked
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 0, 1, 100, 5000, 30000
        );

        coordinator.triggerFailover("exhausted_r1");

        assertEquals(0, commandClient.promotionCount,
            "Failover should be blocked when max retries is zero");
    }

    // ================================
    // Fakes
    // ================================

    /** Returns failure status for promoteToPrimary */
    private static final class FailingPromoteClient implements RegionServerCommandClient {
        int callCount;
        private final CountDownLatch callLatch = new CountDownLatch(1);

        @Override
        public RegionServerProto.PromoteResponse promoteToPrimary(ServerId s, String r, long f) {
            callCount++;
            callLatch.countDown();
            return RegionServerProto.PromoteResponse.newBuilder()
                .setStatus(CommonProto.Status.newBuilder()
                    .setCode(1).setSuccess(false).setMessage("Promotion failed").build())
                .build();
        }

        @Override
        public RegionServerProto.OpenRegionResponse openRegion(ServerId s, Region r, boolean asReplica) {
            return RegionServerProto.OpenRegionResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override
        public RegionServerProto.CloseRegionResponse closeRegion(ServerId s, String r, boolean a, boolean d) {
            return RegionServerProto.CloseRegionResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override
        public RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId s, String r, long t) {
            return RegionServerProto.GetReplicationLagResponse.newBuilder()
                .setStatus(okStatus()).setLagInEntries(0L).setLastAppliedSequenceId(0L).build();
        }

        @Override public RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId s, String r) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.SplitRegionResponse splitRegion(ServerId s, String r, byte[] k, String l, String ri) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.MergeRegionResponse mergeRegion(ServerId s, String l, String r, String m) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.MigrateResponse startMigration(ServerId s, String r, ServerId t, long timeout) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId s, String r, ServerId t, long seq) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.AbortMigrationResponse abortMigration(ServerId s, String r) { throw new UnsupportedOperationException(); }

        boolean awaitCall(long timeout, TimeUnit unit) throws InterruptedException {
            return callLatch.await(timeout, unit);
        }

        private static CommonProto.Status okStatus() {
            return CommonProto.Status.newBuilder().setCode(0).setSuccess(true).setMessage("OK").build();
        }
    }

    /** Tracks promotion calls */
    private static final class TrackingCommandClient implements RegionServerCommandClient {
        volatile int promotionCount = 0;
        private final CountDownLatch promotionLatch = new CountDownLatch(1);

        @Override
        public RegionServerProto.PromoteResponse promoteToPrimary(ServerId s, String r, long f) {
            promotionCount++;
            promotionLatch.countDown();
            return RegionServerProto.PromoteResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override public RegionServerProto.OpenRegionResponse openRegion(ServerId s, Region r, boolean asReplica) { return RegionServerProto.OpenRegionResponse.newBuilder().setStatus(okStatus()).build(); }
        @Override public RegionServerProto.CloseRegionResponse closeRegion(ServerId s, String r, boolean a, boolean d) { return RegionServerProto.CloseRegionResponse.newBuilder().setStatus(okStatus()).build(); }
        @Override public RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId s, String r, long t) { return RegionServerProto.GetReplicationLagResponse.newBuilder().setStatus(okStatus()).setLagInEntries(0L).setLastAppliedSequenceId(0L).build(); }
        @Override public RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId s, String r) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.SplitRegionResponse splitRegion(ServerId s, String r, byte[] k, String l, String ri) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.MergeRegionResponse mergeRegion(ServerId s, String l, String r, String m) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.MigrateResponse startMigration(ServerId s, String r, ServerId t, long timeout) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId s, String r, ServerId t, long seq) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.AbortMigrationResponse abortMigration(ServerId s, String r) { throw new UnsupportedOperationException(); }

        private static CommonProto.Status okStatus() {
            return CommonProto.Status.newBuilder().setCode(0).setSuccess(true).setMessage("OK").build();
        }
    }
}
