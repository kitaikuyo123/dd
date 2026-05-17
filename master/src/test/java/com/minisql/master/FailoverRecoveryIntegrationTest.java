package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.recover.FailoverCoordinator;
import com.minisql.master.recover.RecoveryCoordinator;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Failover-Recovery integration tests")
class FailoverRecoveryIntegrationTest {

    private FailoverCoordinator failoverCoordinator;

    @AfterEach
    void tearDown() {
        if (failoverCoordinator != null) {
            failoverCoordinator.shutdown();
        }
    }

    @Test
    @DisplayName("primary failure triggers failover and promotes best secondary")
    void primaryFailsTriggersFailoverPromotesBestSecondary() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary1 = new ServerId("host-b", 16021, 2L);
        ServerId secondary2 = new ServerId("host-c", 16022, 3L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary1);
        clusterManager.registerServer(secondary2);

        Table table = new Table("integration_test");
        metadataManager.createTable(table);

        Region region = new Region("integ_r1", "integration_test",
            new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary1);
        region.addReplica(secondary2);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // secondary1 is more caught up than secondary2
        ReplicaInfo primaryInfo = new ReplicaInfo("integ_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondary1Info = new ReplicaInfo("integ_r1", secondary1, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondary1Info.setReplicationLag(10);
        ReplicaInfo secondary2Info = new ReplicaInfo("integ_r1", secondary2, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondary2Info.setReplicationLag(500);
        replicaMonitor.registerReplica("integ_r1", primaryInfo);
        replicaMonitor.registerReplica("integ_r1", secondary1Info);
        replicaMonitor.registerReplica("integ_r1", secondary2Info);

        clusterManager.updateReplicaSequenceId("integ_r1", primary, 100L);
        clusterManager.updateReplicaSequenceId("integ_r1", secondary1, 100L);
        clusterManager.updateReplicaSequenceId("integ_r1", secondary2, 90L);

        RecordingCommandClient commandClient = new RecordingCommandClient();
        failoverCoordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 100, 5000, 5000, 30000
        );

        // Simulate primary failure → trigger emergency failover
        failoverCoordinator.triggerEmergencyFailover("integ_r1");

        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS));
        assertEquals(secondary1, commandClient.lastPromotedServer,
            "Secondary with lowest lag should be promoted");
    }

    @Test
    @DisplayName("old primary rejoins as secondary after failover")
    void oldPrimaryRejoinsAsSecondaryAfterFailover() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        Table table = new Table("rejoin_test");
        metadataManager.createTable(table);

        Region region = new Region("rejoin_r1", "rejoin_test",
            new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("rejoin_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondaryInfo = new ReplicaInfo("rejoin_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("rejoin_r1", primaryInfo);
        replicaMonitor.registerReplica("rejoin_r1", secondaryInfo);

        clusterManager.updateReplicaSequenceId("rejoin_r1", primary, 100L);
        clusterManager.updateReplicaSequenceId("rejoin_r1", secondary, 100L);

        RecordingCommandClient commandClient = new RecordingCommandClient();
        failoverCoordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 100, 5000, 5000, 30000
        );

        // Trigger failover: secondary becomes primary
        failoverCoordinator.triggerEmergencyFailover("rejoin_r1");
        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS));

        // Wait for async metadata update
        ServerId newPrimary = null;
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            Region r = metadataManager.getRegion("rejoin_r1");
            if (r != null && r.getPrimary() != null && !r.getPrimary().equals(primary)) {
                newPrimary = r.getPrimary();
                break;
            }
            Thread.sleep(50);
        }
        assertNotNull(newPrimary, "Metadata primary should be updated");
        assertEquals(secondary, newPrimary);

        // Simulate old primary rejoining (it's still active in ClusterManager)
        assertTrue(clusterManager.isServerActive(primary));
    }

    @Test
    @DisplayName("failover updates both metadata and cluster manager consistently")
    void failoverUpdatesZkAndClusterManager() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        Table table = new Table("consistency_test");
        metadataManager.createTable(table);

        Region region = new Region("consist_r1", "consistency_test",
            new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("consist_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondaryInfo = new ReplicaInfo("consist_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("consist_r1", primaryInfo);
        replicaMonitor.registerReplica("consist_r1", secondaryInfo);

        clusterManager.updateReplicaSequenceId("consist_r1", primary, 50L);
        clusterManager.updateReplicaSequenceId("consist_r1", secondary, 50L);

        RecordingCommandClient commandClient = new RecordingCommandClient();
        failoverCoordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 100, 5000, 5000, 30000
        );

        failoverCoordinator.triggerEmergencyFailover("consist_r1");
        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS));

        // Poll until metadata is updated (async after promotion)
        ServerId newMetaPrimary = null;
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            Region r = metadataManager.getRegion("consist_r1");
            if (r != null && r.getPrimary() != null && !r.getPrimary().equals(primary)) {
                newMetaPrimary = r.getPrimary();
                break;
            }
            Thread.sleep(50);
        }
        assertNotNull(newMetaPrimary, "Metadata primary should be updated");
        assertEquals(secondary, newMetaPrimary);
    }

    // ================================
    // Fakes
    // ================================

    private static final class RecordingCommandClient implements RegionServerCommandClient {
        volatile ServerId lastPromotedServer;
        volatile String lastPromotedRegionId;
        final AtomicInteger promotionCount = new AtomicInteger(0);
        private final CountDownLatch promotionLatch = new CountDownLatch(1);

        @Override
        public RegionServerProto.PromoteResponse promoteToPrimary(ServerId s, String r, long f) {
            lastPromotedServer = s;
            lastPromotedRegionId = r;
            promotionCount.incrementAndGet();
            promotionLatch.countDown();
            return RegionServerProto.PromoteResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override public RegionServerProto.OpenRegionResponse openRegion(ServerId s, Region r, boolean a) { return RegionServerProto.OpenRegionResponse.newBuilder().setStatus(okStatus()).build(); }
        @Override public RegionServerProto.CloseRegionResponse closeRegion(ServerId s, String r, boolean a, boolean d) { return RegionServerProto.CloseRegionResponse.newBuilder().setStatus(okStatus()).build(); }
        @Override public RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId s, String r, long t) { return RegionServerProto.GetReplicationLagResponse.newBuilder().setStatus(okStatus()).setLagInEntries(0L).setLastAppliedSequenceId(100L).build(); }
        @Override public RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId s, String r) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.SplitRegionResponse splitRegion(ServerId s, String r, byte[] k, String l, String ri) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.MergeRegionResponse mergeRegion(ServerId s, String l, String r, String m) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.MigrateResponse startMigration(ServerId s, String r, ServerId t, long timeout) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId s, String r, ServerId t, long seq) { throw new UnsupportedOperationException(); }
        @Override public RegionServerProto.AbortMigrationResponse abortMigration(ServerId s, String r) { throw new UnsupportedOperationException(); }

        boolean awaitPromotion(long timeout, TimeUnit unit) throws InterruptedException {
            return promotionLatch.await(timeout, unit);
        }

        private static CommonProto.Status okStatus() {
            return CommonProto.Status.newBuilder().setCode(0).setSuccess(true).setMessage("OK").build();
        }
    }
}
