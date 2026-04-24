package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.master.rebalance.HotSpotCoordinator;
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
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("HotSpotCoordinator integration tests")
class HotSpotCoordinatorIntegrationTest {

    private ClusterManager clusterManager;
    private MetadataManager metadataManager;
    private HotSpotCoordinator coordinator;
    private RegionSplitCoordinator splitCoordinator;
    private RecoveryCoordinator recoveryCoordinator;
    private StubCommandClient stubClient;

    @BeforeEach
    void setUp() {
        clusterManager = new ClusterManager(new LoadBalancer());
        metadataManager = new MetadataManager();
        stubClient = new StubCommandClient();

        splitCoordinator = new RegionSplitCoordinator(
            clusterManager, metadataManager, new LoadBalancer(), stubClient);
        splitCoordinator.start();

        // Build RecoveryCoordinator with minimal real dependencies
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();
        ReplicationCoordinator replicationCoordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(1).build(),
            new NoOpWal(),
            new NoOpTransportClient()
        );

        recoveryCoordinator = new RecoveryCoordinator(
            clusterManager, metadataManager, replicaMonitor,
            replicationCoordinator, lifecycleManager, stubClient);

        coordinator = new HotSpotCoordinator(
            clusterManager, metadataManager, splitCoordinator, recoveryCoordinator);

        // Use thresholds suitable for fast test intervals
        coordinator.configure(new HotSpotCoordinator.HotSpotSettings(
            20, 10, 3, 300_000
        ));
    }

    @AfterEach
    void tearDown() {
        if (splitCoordinator != null) {
            splitCoordinator.stop();
        }
    }

    // ================================
    // executeAction — SPLIT_REGION
    // ================================

    @Nested
    @DisplayName("executeAction SPLIT_REGION")
    class ExecuteSplit {

        @Test
        @DisplayName("execute split delegates to RegionSplitCoordinator")
        void testExecuteSplitRegion() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);

            Region region = createRegion("region-split", "orders", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            // Set low split threshold so checkAndSplitRegion will accept
            splitCoordinator.setSplitThresholdSize(0);

            // Trigger write hotspot
            recordWriteHistory(coordinator, region.getRegionId(), 0, 15, 30);
            invokeDetection(coordinator);

            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            assertEquals(1, actions.size());
            assertEquals(HotSpotCoordinator.HotSpotActionType.SPLIT_REGION, actions.get(0).getType());

            // Execute — should not throw
            assertDoesNotThrow(() -> coordinator.executeAction(actions.get(0)));
        }

        @Test
        @DisplayName("cooldown applied after split execution")
        void testCooldownAfterSplitExecution() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);

            Region region = createRegion("region-split-cd", "orders", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);
            splitCoordinator.setSplitThresholdSize(0);

            recordWriteHistory(coordinator, region.getRegionId(), 0, 15, 30);
            invokeDetection(coordinator);

            List<HotSpotCoordinator.HotSpotAction> first = coordinator.planPendingActions();
            assertEquals(1, first.size());
            coordinator.executeAction(first.get(0));

            // Immediately re-detect — should be in cooldown
            recordWriteHistory(coordinator, region.getRegionId(), 30, 45, 60);
            invokeDetection(coordinator);
            List<HotSpotCoordinator.HotSpotAction> second = coordinator.planPendingActions();
            assertTrue(second.isEmpty(), "No new action during cooldown");
        }
    }

    // ================================
    // executeAction — ADD_READ_REPLICA
    // ================================

    @Nested
    @DisplayName("executeAction ADD_READ_REPLICA")
    class ExecuteAddReplica {

        @Test
        @DisplayName("execute add-read-replica registers replica in ClusterManager")
        void testExecuteAddReadReplica() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            ServerId target = new ServerId("target-host", 16021);
            clusterManager.registerServer(primary);
            clusterManager.registerServer(target);

            Region region = createRegion("region-read-repl", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            // Trigger read hotspot
            recordReadHistory(coordinator, region.getRegionId(), 0, 25, 50);
            invokeDetection(coordinator);

            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            assertEquals(1, actions.size());
            assertEquals(HotSpotCoordinator.HotSpotActionType.ADD_READ_REPLICA, actions.get(0).getType());
            assertEquals(target, actions.get(0).getTargetServer());

            coordinator.executeAction(actions.get(0));

            // Verify replica was added to ClusterManager
            List<ServerId> replicas = clusterManager.getReplicaServers(region.getRegionId());
            assertTrue(replicas.contains(target), "Target server should be in replica list");
        }

        @Test
        @DisplayName("execute with null action does not throw")
        void testExecuteNullAction() {
            assertDoesNotThrow(() -> coordinator.executeAction(null));
        }

        @Test
        @DisplayName("execute with null target server does not add replica")
        void testExecuteNullTargetServer() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);

            Region region = createRegion("region-null-target", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            // Create action manually with null target
            HotSpotCoordinator.HotSpotAction action = new HotSpotCoordinator.HotSpotAction(
                region.getRegionId(),
                HotSpotCoordinator.HotSpotActionType.ADD_READ_REPLICA,
                primary, null,
                HotSpotCoordinator.HotSpotType.READ
            );

            assertDoesNotThrow(() -> coordinator.executeAction(action));

            // No replica should be added
            List<ServerId> replicas = clusterManager.getReplicaServers(region.getRegionId());
            assertTrue(replicas.isEmpty());
        }
    }

    // ================================
    // calculateDisplayScore
    // ================================

    @Nested
    @DisplayName("calculateDisplayScore")
    class DisplayScore {

        @Test
        @DisplayName("score is 0 when no load history")
        void testScoreNoHistory() {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);
            Region region = createRegion("region-score-empty", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            double score = coordinator.calculateDisplayScore(region.getRegionId(), 0);
            assertEquals(0.0, score, 0.01);
        }

        @Test
        @DisplayName("score below 50 when load is below threshold")
        void testScoreBelowThreshold() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);
            Region region = createRegion("region-score-low", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            // Low load: delta of 5 per 50ms interval, well below threshold of 20
            recordReadHistory(coordinator, region.getRegionId(), 0, 5, 10, 15);

            double score = coordinator.calculateDisplayScore(region.getRegionId(), 0);
            assertTrue(score > 0 && score < 50,
                "Score should be between 0 and 50 for below-threshold load, got " + score);
        }

        @Test
        @DisplayName("score above 50 when load exceeds threshold (hotspot)")
        void testScoreAboveThreshold() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);
            Region region = createRegion("region-score-high", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            // High load: delta of 25 per 50ms, well above threshold of 20
            recordReadHistory(coordinator, region.getRegionId(), 0, 25, 50, 75);

            double score = coordinator.calculateDisplayScore(region.getRegionId(), 0);
            assertTrue(score >= 50,
                "Score should be >= 50 for hotspot load, got " + score);
            assertTrue(score <= 100,
                "Score should be <= 100, got " + score);
        }

        @Test
        @DisplayName("replication lag adds to score")
        void testScoreWithReplicationLag() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);
            Region region = createRegion("region-score-lag", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            // Moderate load
            recordReadHistory(coordinator, region.getRegionId(), 0, 10, 20, 30);

            double scoreNoLag = coordinator.calculateDisplayScore(region.getRegionId(), 0);
            double scoreWithLag = coordinator.calculateDisplayScore(region.getRegionId(), 500);

            assertTrue(scoreWithLag > scoreNoLag,
                "Score with lag should be higher: noLag=" + scoreNoLag + " withLag=" + scoreWithLag);
        }

        @Test
        @DisplayName("score capped at 100")
        void testScoreCapped() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);
            Region region = createRegion("region-score-cap", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            // Very high load + high lag
            recordReadHistory(coordinator, region.getRegionId(), 0, 200, 400, 600);

            double score = coordinator.calculateDisplayScore(region.getRegionId(), 50000);
            assertTrue(score <= 100.0, "Score should be capped at 100, got " + score);
        }
    }

    // ================================
    // Full lifecycle
    // ================================

    @Nested
    @DisplayName("Full lifecycle")
    class FullLifecycle {

        @Test
        @DisplayName("detect → plan → execute → cooldown → expire → clear")
        void testFullLifecycle() throws Exception {
            // Short cooldown for test
            coordinator.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 50));

            ServerId primary = new ServerId("primary-host", 16020);
            ServerId target = new ServerId("target-host", 16021);
            clusterManager.registerServer(primary);
            clusterManager.registerServer(target);

            Region region = createRegion("region-lifecycle", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            // Phase 1: Detect hotspot
            recordReadHistory(coordinator, region.getRegionId(), 0, 25, 50);
            invokeDetection(coordinator);

            Map<String, HotSpotCoordinator.HotSpotInfo> hotSpots = coordinator.getCurrentHotSpots();
            assertTrue(hotSpots.containsKey(region.getRegionId()));
            assertEquals(HotSpotCoordinator.HotSpotType.READ, hotSpots.get(region.getRegionId()).getType());

            // Phase 2: Plan action
            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            assertEquals(1, actions.size());

            // Phase 3: Execute
            coordinator.executeAction(actions.get(0));
            assertTrue(clusterManager.getReplicaServers(region.getRegionId()).contains(target));

            // Phase 4: Cooldown — no new actions
            recordReadHistory(coordinator, region.getRegionId(), 50, 75, 100);
            invokeDetection(coordinator);
            assertTrue(coordinator.planPendingActions().isEmpty());

            // Phase 5: Wait for cooldown to expire, then load drops
            Thread.sleep(100);
            recordReadHistory(coordinator, region.getRegionId(), 100, 101, 102, 103, 104);
            invokeDetection(coordinator);

            // Hotspot should be cleared
            assertFalse(coordinator.getCurrentHotSpots().containsKey(region.getRegionId()),
                "Hotspot should be cleared after load drops and cooldown expires");
        }
    }

    // ================================
    // Multi-region
    // ================================

    @Nested
    @DisplayName("Multi-region concurrent detection")
    class MultiRegion {

        @Test
        @DisplayName("two hot regions detected simultaneously")
        void testTwoHotRegionsSimultaneously() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            ServerId target1 = new ServerId("target1-host", 16021);
            ServerId target2 = new ServerId("target2-host", 16022);
            clusterManager.registerServer(primary);
            clusterManager.registerServer(target1);
            clusterManager.registerServer(target2);

            Region regionA = createRegion("region-a", "users", primary);
            Region regionB = createRegion("region-b", "orders", primary);
            metadataManager.registerRegion(regionA);
            metadataManager.registerRegion(regionB);
            clusterManager.assignRegionToServer(regionA.getRegionId(), primary);
            clusterManager.assignRegionToServer(regionB.getRegionId(), primary);

            // Both regions are read-hot
            recordReadHistory(coordinator, regionA.getRegionId(), 0, 25, 50);
            recordReadHistory(coordinator, regionB.getRegionId(), 0, 25, 50);
            invokeDetection(coordinator);

            Map<String, HotSpotCoordinator.HotSpotInfo> hotSpots = coordinator.getCurrentHotSpots();
            assertTrue(hotSpots.containsKey(regionA.getRegionId()));
            assertTrue(hotSpots.containsKey(regionB.getRegionId()));

            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            assertEquals(2, actions.size());
        }

        @Test
        @DisplayName("only hot region triggers action, normal region ignored")
        void testMixedHotAndNormal() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            ServerId target = new ServerId("target-host", 16021);
            clusterManager.registerServer(primary);
            clusterManager.registerServer(target);

            Region hotRegion = createRegion("region-hot", "users", primary);
            Region coolRegion = createRegion("region-cool", "orders", primary);
            metadataManager.registerRegion(hotRegion);
            metadataManager.registerRegion(coolRegion);
            clusterManager.assignRegionToServer(hotRegion.getRegionId(), primary);
            clusterManager.assignRegionToServer(coolRegion.getRegionId(), primary);

            // Hot region: delta 25 per interval (above threshold 20)
            recordReadHistory(coordinator, hotRegion.getRegionId(), 0, 25, 50);
            // Cool region: delta 5 per interval (below threshold 20)
            recordReadHistory(coordinator, coolRegion.getRegionId(), 0, 5, 10);

            invokeDetection(coordinator);

            Map<String, HotSpotCoordinator.HotSpotInfo> hotSpots = coordinator.getCurrentHotSpots();
            assertTrue(hotSpots.containsKey(hotRegion.getRegionId()));
            assertFalse(hotSpots.containsKey(coolRegion.getRegionId()));

            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            assertEquals(1, actions.size());
            assertEquals(hotRegion.getRegionId(), actions.get(0).getRegionId());
        }

        @Test
        @DisplayName("three regions with mixed read/write hotspots")
        void testMixedHotSpotTypes() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            ServerId target = new ServerId("target-host", 16021);
            clusterManager.registerServer(primary);
            clusterManager.registerServer(target);

            Region readHot = createRegion("region-readhot", "users", primary);
            Region writeHot = createRegion("region-writehot", "orders", primary);
            Region cool = createRegion("region-cool2", "products", primary);
            metadataManager.registerRegion(readHot);
            metadataManager.registerRegion(writeHot);
            metadataManager.registerRegion(cool);
            clusterManager.assignRegionToServer(readHot.getRegionId(), primary);
            clusterManager.assignRegionToServer(writeHot.getRegionId(), primary);
            clusterManager.assignRegionToServer(cool.getRegionId(), primary);

            // Read-hot: delta 25 (above readThreshold=20)
            recordReadHistory(coordinator, readHot.getRegionId(), 0, 25, 50);
            // Write-hot: delta 15 (above writeThreshold=10)
            recordWriteHistory(coordinator, writeHot.getRegionId(), 0, 15, 30);
            // Cool: low load
            recordReadHistory(coordinator, cool.getRegionId(), 0, 5, 10);

            invokeDetection(coordinator);

            Map<String, HotSpotCoordinator.HotSpotInfo> hotSpots = coordinator.getCurrentHotSpots();
            assertEquals(2, hotSpots.size());
            assertEquals(HotSpotCoordinator.HotSpotType.READ, hotSpots.get(readHot.getRegionId()).getType());
            assertEquals(HotSpotCoordinator.HotSpotType.WRITE, hotSpots.get(writeHot.getRegionId()).getType());
            assertFalse(hotSpots.containsKey(cool.getRegionId()));

            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            assertEquals(2, actions.size());
        }
    }

    // ================================
    // executeAction recovery path
    // ================================

    @Nested
    @DisplayName("executeAction recovery coordinator interactions")
    class RecoveryInteractions {

        @Test
        @DisplayName("add-read-replica sets desiredReplicaCount on RecoveryCoordinator")
        void testDesiredReplicaCountSet() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            ServerId target = new ServerId("target-host", 16021);
            clusterManager.registerServer(primary);
            clusterManager.registerServer(target);

            Region region = createRegion("region-desired", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            recordReadHistory(coordinator, region.getRegionId(), 0, 25, 50);
            invokeDetection(coordinator);

            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            coordinator.executeAction(actions.get(0));

            // Verify observable effect: replica was added to ClusterManager
            List<ServerId> replicas = clusterManager.getReplicaServers(region.getRegionId());
            assertTrue(replicas.contains(target), "Replica should be registered in ClusterManager");
        }

        @Test
        @DisplayName("hotspot clear removes desired replica count")
        void testDesiredReplicaCountCleared() throws Exception {
            coordinator.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 50));

            ServerId primary = new ServerId("primary-host", 16020);
            ServerId target = new ServerId("target-host", 16021);
            clusterManager.registerServer(primary);
            clusterManager.registerServer(target);

            Region region = createRegion("region-clear-desired", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            // Trigger and execute
            recordReadHistory(coordinator, region.getRegionId(), 0, 25, 50);
            invokeDetection(coordinator);
            coordinator.executeAction(coordinator.planPendingActions().get(0));
            assertTrue(clusterManager.getReplicaServers(region.getRegionId()).contains(target));

            // Wait for cooldown + load drops
            Thread.sleep(100);
            recordReadHistory(coordinator, region.getRegionId(), 50, 51, 52, 53, 54);
            invokeDetection(coordinator);

            // Hotspot should be cleared
            assertFalse(coordinator.getCurrentHotSpots().containsKey(region.getRegionId()));
        }
    }

    // ================================
    // Edge cases
    // ================================

    @Nested
    @DisplayName("Edge cases")
    class EdgeCases {

        @Test
        @DisplayName("region not in metadata produces no action")
        void testUnknownRegionNoAction() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);

            // Record load for a region that doesn't exist in metadata
            recordReadHistory(coordinator, "phantom-region", 0, 25, 50);
            invokeDetection(coordinator);

            // Should not crash, and should not produce actions
            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            assertTrue(actions.isEmpty());
        }

        @Test
        @DisplayName("no servers available produces no add-replica action")
        void testNoTargetServerNoReplicaAction() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            clusterManager.registerServer(primary);
            // Only one server, no secondaries available

            Region region = createRegion("region-no-target", "users", primary);
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);

            recordReadHistory(coordinator, region.getRegionId(), 0, 25, 50);
            invokeDetection(coordinator);

            // Hotspot should be detected but no executable action
            assertTrue(coordinator.getCurrentHotSpots().containsKey(region.getRegionId()));
            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            assertTrue(actions.isEmpty(), "No action when no target server available");
        }

        @Test
        @DisplayName("replica count already at target produces no add-replica action")
        void testReplicaCountAlreadyMet() throws Exception {
            ServerId primary = new ServerId("primary-host", 16020);
            ServerId replica1 = new ServerId("replica1-host", 16021);
            ServerId replica2 = new ServerId("replica2-host", 16022);
            ServerId extra = new ServerId("extra-host", 16023);
            clusterManager.registerServer(primary);
            clusterManager.registerServer(replica1);
            clusterManager.registerServer(replica2);
            clusterManager.registerServer(extra);

            Region region = createRegion("region-enough-replicas", "users", primary);
            region.setReplicas(List.of(primary, replica1, replica2));
            metadataManager.registerRegion(region);
            clusterManager.assignRegionToServer(region.getRegionId(), primary);
            clusterManager.addReplica(region.getRegionId(), replica1);
            clusterManager.addReplica(region.getRegionId(), replica2);

            recordReadHistory(coordinator, region.getRegionId(), 0, 25, 50);
            invokeDetection(coordinator);

            // Already has 3 replicas (targetReadReplicaCount=3), no action needed
            assertTrue(coordinator.getCurrentHotSpots().containsKey(region.getRegionId()));
            List<HotSpotCoordinator.HotSpotAction> actions = coordinator.planPendingActions();
            assertTrue(actions.isEmpty(), "No action when replica count already met");
        }
    }

    // ================================
    // Helpers
    // ================================

    private Region createRegion(String regionId, String tableName, ServerId primary) {
        Region region = new Region(regionId, tableName, "a".getBytes(), "z".getBytes());
        region.setPrimary(primary);
        return region;
    }

    private void recordReadHistory(HotSpotCoordinator mgr, String regionId, long... readRequests)
        throws Exception {
        for (long readRequest : readRequests) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId(regionId);
            load.setReadRequests(readRequest);
            load.setWriteRequests(0L);
            mgr.recordRegionLoad(regionId, null, load);
            Thread.sleep(50);
        }
    }

    private void recordWriteHistory(HotSpotCoordinator mgr, String regionId, long... writeRequests)
        throws Exception {
        for (long writeRequest : writeRequests) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId(regionId);
            load.setReadRequests(0L);
            load.setWriteRequests(writeRequest);
            mgr.recordRegionLoad(regionId, null, load);
            Thread.sleep(50);
        }
    }

    private void invokeDetection(HotSpotCoordinator mgr) throws Exception {
        Method method = HotSpotCoordinator.class.getDeclaredMethod("detectAndPlanHotSpots");
        method.setAccessible(true);
        method.invoke(mgr);
    }

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
        public RegionServerProto.SplitRegionResponse splitRegion(ServerId serverId, String regionId, byte[] splitKey) {
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
        public RegionServerProto.MergeRegionResponse mergeRegion(ServerId s, String l, String r) {
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
        @Override public void close() {}
    }
}
