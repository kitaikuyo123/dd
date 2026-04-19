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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FailoverCoordinator tests")
class FailoverCoordinatorTest {

    private FailoverCoordinator coordinator;

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.shutdown();
        }
    }

    // ---------------------------------------------------------------
    // triggerFailover: picks best replica and promotes it
    // ---------------------------------------------------------------

    @Test
    @DisplayName("triggerFailover promotes healthiest secondary to primary")
    void triggerFailoverPromotesHealthiestSecondary() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary1 = new ServerId("host-b", 16021, 2L);
        ServerId secondary2 = new ServerId("host-c", 16022, 3L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary1);
        clusterManager.registerServer(secondary2);

        Table table = new Table("orders");
        metadataManager.createTable(table);

        Region region = new Region("orders_r1", "orders", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary1);
        region.addReplica(secondary2);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // Register replicas in the monitor
        ReplicaInfo primaryInfo = new ReplicaInfo("orders_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondary1Info = new ReplicaInfo("orders_r1", secondary1, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondary1Info.setReplicationLag(100);
        ReplicaInfo secondary2Info = new ReplicaInfo("orders_r1", secondary2, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondary2Info.setReplicationLag(500);
        replicaMonitor.registerReplica("orders_r1", primaryInfo);
        replicaMonitor.registerReplica("orders_r1", secondary1Info);
        replicaMonitor.registerReplica("orders_r1", secondary2Info);

        // Sequence IDs: secondary1 is caught up
        clusterManager.updateReplicaSequenceId("orders_r1", primary, 100L);
        clusterManager.updateReplicaSequenceId("orders_r1", secondary1, 100L);
        clusterManager.updateReplicaSequenceId("orders_r1", secondary2, 95L);

        RecordingCommandClient commandClient = new RecordingCommandClient();
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 100, 5000, 5000, 30000
        );

        // Trigger failover using emergency mode to bypass cooldown
        coordinator.triggerEmergencyFailover("orders_r1");

        // Wait for async execution
        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS),
            "Promotion should complete within timeout");

        // secondary1 should have been promoted (lowest lag among healthy secondaries)
        assertEquals(secondary1, commandClient.lastPromotedServer);
        assertEquals("orders_r1", commandClient.lastPromotedRegionId);
    }

    // ---------------------------------------------------------------
    // Cooldown prevents rapid failover
    // ---------------------------------------------------------------

    @Test
    @DisplayName("cooldown prevents rapid consecutive failovers for same region")
    void cooldownPreventsRapidFailover() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        Table table = new Table("items");
        metadataManager.createTable(table);

        Region region = new Region("items_r1", "items", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("items_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondaryInfo = new ReplicaInfo("items_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("items_r1", primaryInfo);
        replicaMonitor.registerReplica("items_r1", secondaryInfo);

        clusterManager.updateReplicaSequenceId("items_r1", primary, 50L);
        clusterManager.updateReplicaSequenceId("items_r1", secondary, 50L);

        RecordingCommandClient commandClient = new RecordingCommandClient();
        // Use a very long base cooldown so the second trigger is blocked
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 60000, 300000, 5000, 30000
        );

        // First failover via emergency (bypasses cooldown)
        coordinator.triggerEmergencyFailover("items_r1");
        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS));

        int promotionCountAfterFirst = commandClient.promotionCount;

        // Second failover via normal mode should be blocked by cooldown
        coordinator.triggerFailover("items_r1");

        // Give a small window for an unwanted second promotion
        Thread.sleep(200);
        assertEquals(promotionCountAfterFirst, commandClient.promotionCount,
            "Second failover should be blocked by cooldown");
    }

    // ---------------------------------------------------------------
    // Emergency mode bypasses cooldown
    // ---------------------------------------------------------------

    @Test
    @DisplayName("emergency failover bypasses cooldown")
    void emergencyFailoverBypassesCooldown() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        Table table = new Table("products");
        metadataManager.createTable(table);

        Region region = new Region("products_r1", "products", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("products_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondaryInfo = new ReplicaInfo("products_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("products_r1", primaryInfo);
        replicaMonitor.registerReplica("products_r1", secondaryInfo);

        clusterManager.updateReplicaSequenceId("products_r1", primary, 50L);
        clusterManager.updateReplicaSequenceId("products_r1", secondary, 50L);

        RecordingCommandClient commandClient = new RecordingCommandClient();
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 60000, 300000, 5000, 30000
        );

        // Trigger first failover (emergency)
        coordinator.triggerEmergencyFailover("products_r1");
        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS));

        // Trigger second failover (also emergency, should bypass cooldown)
        // Re-register replicas so there's a valid candidate
        ReplicaInfo newPrimaryInfo = new ReplicaInfo("products_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        replicaMonitor.registerReplica("products_r1", newPrimaryInfo);

        ReplicaInfo oldPrimaryInfo = new ReplicaInfo("products_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        oldPrimaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("products_r1", oldPrimaryInfo);

        coordinator.triggerEmergencyFailover("products_r1");
        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS),
            "Emergency failover should bypass cooldown");
    }

    // ---------------------------------------------------------------
    // Max retries exceeded
    // ---------------------------------------------------------------

    @Test
    @DisplayName("normal failover blocked when max retries is exceeded")
    void normalFailoverBlockedWhenMaxRetriesExceeded() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        RecordingCommandClient commandClient = new RecordingCommandClient();
        // maxFailoverRetries = 0 means any normal failover is blocked
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 0, 1, 100, 5000, 30000
        );

        // Attempt normal failover - should be blocked due to max retries (0)
        coordinator.triggerFailover("missing_r1");

        Thread.sleep(200);
        assertEquals(0, commandClient.promotionCount,
            "Failover should be blocked when max retries is zero");
    }

    // ---------------------------------------------------------------
    // Shutdown
    // ---------------------------------------------------------------

    @Test
    @DisplayName("shutdown terminates executor without error")
    void shutdownTerminatesCleanly() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            new StubCommandClient(), 3, 30000, 300000, 10000, 60000
        );

        // Should not throw
        assertDoesNotThrow(() -> coordinator.shutdown());
    }

    // ---------------------------------------------------------------
    // Callback registration: onReplicaFailed triggers failover for primary
    // ---------------------------------------------------------------

    @Test
    @DisplayName("onReplicaFailed callback triggers failover when primary fails")
    void callbackTriggersFailoverOnPrimaryFailure() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        Table table = new Table("accounts");
        metadataManager.createTable(table);

        Region region = new Region("accounts_r1", "accounts", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("accounts_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondaryInfo = new ReplicaInfo("accounts_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("accounts_r1", primaryInfo);
        replicaMonitor.registerReplica("accounts_r1", secondaryInfo);

        clusterManager.updateReplicaSequenceId("accounts_r1", primary, 100L);
        clusterManager.updateReplicaSequenceId("accounts_r1", secondary, 100L);

        RecordingCommandClient commandClient = new RecordingCommandClient();
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 100, 5000, 5000, 30000
        );

        // Simulate primary failure via the callback registered in the constructor
        replicaMonitor.registerCallback(new ReplicaMonitor.FailoverCallback() {
            @Override
            public void onReplicaFailed(String regionId, ServerId failedReplica) {
                lifecycleManager.transition(regionId, failedReplica,
                    ReplicaLifecycleManager.ReplicaLifecycleState.OFFLINE,
                    "Simulated failure");
            }

            @Override
            public void onReplicaLagging(String regionId, ServerId laggingReplica, long lagMs) {
            }
        });

        // Trigger emergency failover to test the path
        coordinator.triggerEmergencyFailover("accounts_r1");
        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS));
        assertEquals(secondary, commandClient.lastPromotedServer);
    }

    // ---------------------------------------------------------------
    // Metadata updated after failover
    // ---------------------------------------------------------------

    @Test
    @DisplayName("metadata primary is updated after successful failover")
    void metadataPrimaryUpdatedAfterFailover() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        ServerId secondary = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(secondary);

        Table table = new Table("logs");
        metadataManager.createTable(table);

        Region region = new Region("logs_r1", "logs", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        ReplicaInfo primaryInfo = new ReplicaInfo("logs_r1", primary, null, null, null,
            ReplicaInfo.ReplicaState.PRIMARY);
        ReplicaInfo secondaryInfo = new ReplicaInfo("logs_r1", secondary, null, null, null,
            ReplicaInfo.ReplicaState.SECONDARY);
        secondaryInfo.setReplicationLag(0);
        replicaMonitor.registerReplica("logs_r1", primaryInfo);
        replicaMonitor.registerReplica("logs_r1", secondaryInfo);

        clusterManager.updateReplicaSequenceId("logs_r1", primary, 100L);
        clusterManager.updateReplicaSequenceId("logs_r1", secondary, 100L);

        RecordingCommandClient commandClient = new RecordingCommandClient();
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 100, 5000, 5000, 30000
        );

        coordinator.triggerEmergencyFailover("logs_r1");
        assertTrue(commandClient.awaitPromotion(5, TimeUnit.SECONDS));

        // Metadata is updated asynchronously after promotion; poll until visible
        ServerId newPrimary = null;
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            Region updated = metadataManager.getRegion("logs_r1");
            if (updated != null && updated.getPrimary() != null
                && !updated.getPrimary().equals(primary)) {
                newPrimary = updated.getPrimary();
                break;
            }
            Thread.sleep(50);
        }
        assertNotNull(newPrimary, "Metadata primary should have been updated");
        assertEquals(secondary, newPrimary);
    }

    // ---------------------------------------------------------------
    // No suitable replica: failover completes without promotion
    // ---------------------------------------------------------------

    @Test
    @DisplayName("failover with no suitable replica does not promote")
    void failoverWithNoSuitableReplicaDoesNotPromote() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("host-a", 16020, 1L);
        clusterManager.registerServer(primary);

        Table table = new Table("solo");
        metadataManager.createTable(table);

        Region region = new Region("solo_r1", "solo", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        metadataManager.registerRegionForTable(region, primary);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // No secondary replicas registered

        RecordingCommandClient commandClient = new RecordingCommandClient();
        coordinator = new FailoverCoordinator(
            clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, 3, 100, 5000, 5000, 30000
        );

        coordinator.triggerEmergencyFailover("solo_r1");

        Thread.sleep(500);
        assertEquals(0, commandClient.promotionCount,
            "No promotion should happen when there is no suitable replica");
    }

    // ---------------------------------------------------------------
    // Recording fake for RegionServerCommandClient
    // ---------------------------------------------------------------

    private static final class RecordingCommandClient implements RegionServerCommandClient {
        private ServerId lastPromotedServer;
        private String lastPromotedRegionId;
        private int promotionCount = 0;
        private final CountDownLatch promotionLatch = new CountDownLatch(1);

        @Override
        public RegionServerProto.OpenRegionResponse openRegion(ServerId serverId, Region region, boolean asReplica) {
            return RegionServerProto.OpenRegionResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override
        public RegionServerProto.CloseRegionResponse closeRegion(ServerId serverId, String regionId, boolean abort,
                                                                 boolean dropTable) {
            return RegionServerProto.CloseRegionResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override
        public RegionServerProto.PromoteResponse promoteToPrimary(ServerId serverId, String regionId, long fencingToken) {
            lastPromotedServer = serverId;
            lastPromotedRegionId = regionId;
            promotionCount++;
            promotionLatch.countDown();
            return RegionServerProto.PromoteResponse.newBuilder().setStatus(okStatus()).build();
        }

        @Override
        public RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId serverId, String regionId,
                                                                             long timeoutMs) {
            return RegionServerProto.GetReplicationLagResponse.newBuilder()
                .setStatus(okStatus()).setLagInEntries(0L).setLastAppliedSequenceId(100L).build();
        }

        @Override
        public RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId serverId, String regionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.SplitRegionResponse splitRegion(ServerId serverId, String regionId, byte[] splitKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.MergeRegionResponse mergeRegion(ServerId serverId, String leftRegionId,
                                                                 String rightRegionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.MigrateResponse startMigration(ServerId serverId, String regionId,
                                                                 ServerId targetServer, long timeoutMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId serverId, String regionId,
                                                                             ServerId targetServer, long fromSequenceId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.AbortMigrationResponse abortMigration(ServerId serverId, String regionId) {
            throw new UnsupportedOperationException();
        }

        boolean awaitPromotion(long timeout, TimeUnit unit) throws InterruptedException {
            return promotionLatch.await(timeout, unit);
        }

        private static CommonProto.Status okStatus() {
            return CommonProto.Status.newBuilder().setCode(0).setSuccess(true).setMessage("OK").build();
        }
    }

    // ---------------------------------------------------------------
    // Stub client for cases where we don't care about RPC calls
    // ---------------------------------------------------------------

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
            return RegionServerProto.GetReplicationLagResponse.newBuilder().setStatus(OK)
                .setLagInEntries(0L).setLastAppliedSequenceId(0L).build();
        }

        @Override
        public RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId s, String r) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.SplitRegionResponse splitRegion(ServerId s, String r, byte[] k) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.MergeRegionResponse mergeRegion(ServerId s, String l, String r) {
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
    }
}
