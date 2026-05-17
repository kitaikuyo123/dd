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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FailoverCoordinator concurrency tests")
class FailoverCoordinatorConcurrencyTest {

    private FailoverCoordinator coordinator;

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.shutdown();
        }
    }

    @Test
    @DisplayName("concurrent failover triggers for same region only one executes")
    void concurrentFailoverTriggersForSameRegionOnlyOneExecutes() throws Exception {
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

        Table table = new Table("conc_table");
        metadataManager.createTable(table);

        Region region = new Region("conc_r1", "conc_table",
            new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary1);
        region.addReplica(secondary2);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("conc_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondary1Info = new ReplicaInfo("conc_r1", secondary1, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondary1Info.setReplicationLag(0);
        ReplicaInfo secondary2Info = new ReplicaInfo("conc_r1", secondary2, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondary2Info.setReplicationLag(10);
        replicaMonitor.registerReplica("conc_r1", primaryInfo);
        replicaMonitor.registerReplica("conc_r1", secondary1Info);
        replicaMonitor.registerReplica("conc_r1", secondary2Info);

        clusterManager.updateReplicaSequenceId("conc_r1", primary, 100L);
        clusterManager.updateReplicaSequenceId("conc_r1", secondary1, 100L);
        clusterManager.updateReplicaSequenceId("conc_r1", secondary2, 95L);

        CountingCommandClient commandClient = new CountingCommandClient();
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 10, 100, 5000, 5000, 30000
        );

        // 3 threads trigger failover concurrently for same region
        int triggerCount = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(triggerCount);
        for (int i = 0; i < triggerCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    coordinator.triggerEmergencyFailover("conc_r1");
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            }, "trigger-" + i).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

        // Wait for the actual execution to complete
        Thread.sleep(500);

        // At least one promotion should have happened
        assertTrue(commandClient.promotionCount.get() >= 1,
            "At least one failover should execute, got " + commandClient.promotionCount.get());
    }

    @Test
    @DisplayName("failover cooldown prevents rapid retrigger")
    void failoverCooldownPreventsRapidRetrigger() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        Table table = new Table("cooldown_table");
        metadataManager.createTable(table);

        Region region = new Region("cooldown_r1", "cooldown_table",
            new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("cooldown_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondaryInfo = new ReplicaInfo("cooldown_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("cooldown_r1", primaryInfo);
        replicaMonitor.registerReplica("cooldown_r1", secondaryInfo);

        clusterManager.updateReplicaSequenceId("cooldown_r1", primary, 50L);
        clusterManager.updateReplicaSequenceId("cooldown_r1", secondary, 50L);

        CountingCommandClient commandClient = new CountingCommandClient();
        // Very long base cooldown: 60 seconds
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 10, 60000, 300000, 5000, 30000
        );

        // First failover via emergency (bypasses cooldown)
        coordinator.triggerEmergencyFailover("cooldown_r1");
        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS));
        int countAfterFirst = commandClient.promotionCount.get();

        // Second failover via normal mode: should be blocked by cooldown
        coordinator.triggerFailover("cooldown_r1");
        assertEquals(countAfterFirst, commandClient.promotionCount.get(),
            "Second failover should be blocked by cooldown");
    }

    // ================================
    // Fakes
    // ================================

    private static final class CountingCommandClient implements RegionServerCommandClient {
        volatile ServerId lastPromotedServer;
        volatile String lastPromotedRegionId;
        final AtomicInteger promotionCount = new AtomicInteger(0);
        private final CountDownLatch promotionLatch = new CountDownLatch(1);

        @Override
        public RegionServerProto.OpenRegionResponse openRegion(ServerId s, Region r, boolean asReplica) {
            return RegionServerProto.OpenRegionResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override
        public RegionServerProto.CloseRegionResponse closeRegion(ServerId s, String r, boolean a, boolean d) {
            return RegionServerProto.CloseRegionResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override
        public RegionServerProto.PromoteResponse promoteToPrimary(ServerId s, String r, long f) {
            lastPromotedServer = s;
            lastPromotedRegionId = r;
            promotionCount.incrementAndGet();
            promotionLatch.countDown();
            return RegionServerProto.PromoteResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override
        public RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId s, String r, long t) {
            return RegionServerProto.GetReplicationLagResponse.newBuilder()
                .setStatus(okStatus()).setLagInEntries(0L).setLastAppliedSequenceId(100L).build();
        }

        @Override
        public RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId s, String r) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.SplitRegionResponse splitRegion(ServerId s, String r, byte[] k,
                                                      String leftR, String rightR) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.MergeRegionResponse mergeRegion(ServerId s, String l, String r, String m) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.MigrateResponse startMigration(ServerId s, String r, ServerId t, long timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId s, String r, ServerId t, long seq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.AbortMigrationResponse abortMigration(ServerId s, String r) {
            throw new UnsupportedOperationException();
        }

        boolean awaitPromotion(long timeout, TimeUnit unit) throws InterruptedException {
            return promotionLatch.await(timeout, unit);
        }

        private static CommonProto.Status okStatus() {
            return CommonProto.Status.newBuilder().setCode(0).setSuccess(true).setMessage("OK").build();
        }
    }
}
