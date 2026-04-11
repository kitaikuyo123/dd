package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.HotSpotCoordinator;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.rebalance.RegionSplitCoordinator;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("HotSpotCoordinator tests")
class HotSpotCoordinatorTest {

    @Test
    @DisplayName("can create manager and read empty hotspot view")
    void testCreateHotSpotCoordinator() {
        HotSpotCoordinator manager = new HotSpotCoordinator(
            createClusterManager(),
            new MetadataManager(),
            createRegionSplitCoordinator(),
            null
        );

        assertNotNull(manager);
        assertTrue(manager.getCurrentHotSpots().isEmpty());
    }

    @Test
    @DisplayName("read hotspot prefers adding replica when replica count is low")
    void testDetectReadHotSpotPlansReplicaAddition() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager, createRegionSplitCoordinator(), null);

        // Configure thresholds suitable for test intervals (~10ms)
        manager.configure(new HotSpotCoordinator.HotSpotSettings(
            20, 10, 3, 300000
        ));

        ServerId primary = new ServerId("primary-host", 16020);
        ServerId target = new ServerId("target-host", 16021);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(target);

        Region region = createRegion("region-read", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // Deltas: 25, 25 per ~10ms interval → above readThreshold of 20
        recordReadHistory(manager, region.getRegionId(), 0, 25, 50);
        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.planPendingActions();
        assertEquals(1, actions.size());
        assertEquals(HotSpotCoordinator.HotSpotActionType.ADD_READ_REPLICA, actions.get(0).getType());
        assertEquals(target, actions.get(0).getTargetServer());

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
        assertEquals(HotSpotCoordinator.HotSpotType.READ, currentHotSpots.get(region.getRegionId()).getType());
    }

    @Test
    @DisplayName("write hotspot plans split action")
    void testDetectWriteHotSpotPlansSplit() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-write", "orders", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // Deltas: 15, 15 per ~10ms → constant, above writeThreshold of 10, not growing
        recordWriteHistory(manager, region.getRegionId(), 0, 15, 30);

        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.planPendingActions();
        assertEquals(1, actions.size());
        assertEquals(HotSpotCoordinator.HotSpotActionType.SPLIT_REGION, actions.get(0).getType());

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
        assertEquals(HotSpotCoordinator.HotSpotType.WRITE, currentHotSpots.get(region.getRegionId()).getType());
    }

    @Test
    @DisplayName("write growing hotspot plans split action")
    void testDetectWriteGrowingHotSpotPlansSplit() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-write-growing", "orders", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // Deltas: 15, 35 per ~10ms → growing (ratio 2.33 > 1.2), above writeThreshold of 10
        recordWriteHistory(manager, region.getRegionId(), 0, 15, 50);

        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.planPendingActions();
        assertEquals(1, actions.size());
        assertEquals(HotSpotCoordinator.HotSpotActionType.SPLIT_REGION, actions.get(0).getType());

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
        assertEquals(HotSpotCoordinator.HotSpotType.WRITE, currentHotSpots.get(region.getRegionId()).getType());
    }

    @Test
    @DisplayName("cooldown prevents duplicate actions but allows status update")
    void testCooldownPreventsDuplicateActionsButAllowsStatusUpdate() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        ServerId target = new ServerId("target-host", 16021);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(target);

        Region region = createRegion("region-cooldown", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // First detection
        recordReadHistory(manager, region.getRegionId(), 0, 25, 50);
        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> firstActions = manager.planPendingActions();
        assertEquals(1, firstActions.size());

        // Second detection (during cooldown)
        recordReadHistory(manager, region.getRegionId(), 60, 85, 110);
        invokeHotSpotDetection(manager);

        // No new actions during cooldown
        List<HotSpotCoordinator.HotSpotAction> secondActions = manager.planPendingActions();
        assertTrue(secondActions.isEmpty());

        // But hotspot status should still be updated
        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
    }

    @Test
    @DisplayName("stale hotspot is removed from map when no longer hot")
    void testStaleHotSpotIsRemoved() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        // Short cooldown so it expires between detections
        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 50));

        ServerId primary = new ServerId("primary-host", 16020);
        ServerId target = new ServerId("target-host", 16021);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(target);

        Region region = createRegion("region-stale", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // First: trigger hotspot
        recordReadHistory(manager, region.getRegionId(), 0, 25, 50);
        invokeHotSpotDetection(manager);
        assertTrue(manager.getCurrentHotSpots().containsKey(region.getRegionId()));

        // Wait for cooldown to expire
        Thread.sleep(100);

        // Load returns to normal (delta of 1 per interval, below threshold of 20)
        // Adding more low-load snapshots dilutes the old high-delta average below threshold
        recordReadHistory(manager, region.getRegionId(), 50, 51, 52, 53, 54);
        invokeHotSpotDetection(manager);

        // Hotspot should be removed
        assertFalse(manager.getCurrentHotSpots().containsKey(region.getRegionId()));
    }

    @Test
    @DisplayName("stale server is excluded from replica targets")
    void testStaleServerExcludedFromReplicaTargets() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        ServerId staleServer = new ServerId("stale-host", 16021);
        ServerId healthyServer = new ServerId("healthy-host", 16022);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(staleServer);
        clusterManager.registerServer(healthyServer);

        // Simulate stale server heartbeat expiry
        ClusterManager.ServerInfo staleInfo = clusterManager.getActiveServersList().stream()
            .filter(s -> s.getServerId().equals(staleServer))
            .findFirst()
            .orElse(null);
        if (staleInfo != null) {
            staleInfo.setLastHeartbeat(System.currentTimeMillis() - 120000);
        }

        Region region = createRegion("region-stale-test", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        recordReadHistory(manager, region.getRegionId(), 0, 25, 50);
        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.planPendingActions();
        assertEquals(1, actions.size());
        assertEquals(healthyServer, actions.get(0).getTargetServer());
    }

    @Test
    @DisplayName("constant load should not be classified as growing")
    void testConstantLoadNotGrowing() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-constant", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // Deltas: 25, 25, 25 → constant. Growth ratio = 1.0 < 1.2
        recordReadHistory(manager, region.getRegionId(), 0, 25, 50, 75);

        invokeHotSpotDetection(manager);

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
        assertEquals(HotSpotCoordinator.HotSpotType.READ, currentHotSpots.get(region.getRegionId()).getType());
    }

    @Test
    @DisplayName("write hotspot takes priority over read when both exceed thresholds and write is more severe")
    void testWritePriorityOverRead() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        // readThreshold=20, writeThreshold=10, growthThreshold=2.0 for margin
        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-both-hot", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // Constant raw deltas: readDelta=30 (severity 1.5), writeDelta=25 (severity 2.5) → write wins
        // Use 6 snapshots for stable average despite timing jitter
        recordMixedHistory(manager, region.getRegionId(),
            new long[][]{{0, 0}, {30, 25}, {60, 50}, {90, 75}, {120, 100}, {150, 125}});

        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.planPendingActions();
        assertEquals(1, actions.size());
        // Write hotspot → SPLIT_REGION
        assertEquals(HotSpotCoordinator.HotSpotActionType.SPLIT_REGION, actions.get(0).getType());

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
        assertEquals(HotSpotCoordinator.HotSpotType.WRITE, currentHotSpots.get(region.getRegionId()).getType());
    }

    @Test
    @DisplayName("combined read+write pressure triggers hotspot even when neither alone exceeds threshold")
    void testCombinedReadWriteHotSpot() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        // readThreshold=20, writeThreshold=10
        // combined threshold = 0.7 * (20+10) = 21 per interval
        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-combined", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // readDelta=15 (below 20), writeDelta=8 (below 10), combined=23 (above 21)
        recordMixedHistory(manager, region.getRegionId(),
            new long[][]{{0, 0}, {15, 8}, {30, 16}});

        invokeHotSpotDetection(manager);

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
        // Combined hotspot is treated as write-type
        assertEquals(HotSpotCoordinator.HotSpotType.WRITE, currentHotSpots.get(region.getRegionId()).getType());
    }

    @Test
    @DisplayName("detection works correctly with irregular snapshot intervals")
    void testIrregularIntervals() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        // Threshold: 100 per interval (assume ~10ms interval)
        // With a 50ms interval, per-second threshold = 100 / 0.05 = 2000/s
        // Delta of 150 over 50ms = 3000/s > 2000/s → hotspot
        manager.configure(new HotSpotCoordinator.HotSpotSettings(100, 50, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-irregular", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // Create snapshots with varying intervals
        // Snapshot 0→1: ~10ms, delta=120 → 12000/s
        // Snapshot 1→2: ~50ms, delta=150 → 3000/s
        // Average per-sec: 7500/s. Threshold per-sec (avg interval ~30ms): 100/0.03 = 3333/s
        recordReadHistory(manager, region.getRegionId(), 0, 120);
        Thread.sleep(50);
        recordReadHistory(manager, region.getRegionId(), 270);

        invokeHotSpotDetection(manager);

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
        assertEquals(HotSpotCoordinator.HotSpotType.READ, currentHotSpots.get(region.getRegionId()).getType());
    }

    @Test
    @DisplayName("counter reset does not cause false positive")
    void testCounterResetNoFalsePositive() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-reset", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // Counter reset in the middle: 0→5→2→4
        // Delta 1: 5 per ~50ms ≈ 100/s (well below threshold 400/s)
        // Delta 2: max(0, 2-5)=0 (counter reset, clamped)
        // Delta 3: 4-2=2 per ~50ms ≈ 40/s
        // Average: (100+0+40)/3 ≈ 47/s << 400/s → NOT a hotspot
        recordReadHistory(manager, region.getRegionId(), 0, 5, 2, 4);

        invokeHotSpotDetection(manager);

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertFalse(currentHotSpots.containsKey(region.getRegionId()));
    }

    @Test
    @DisplayName("zero load followed by activity should not crash")
    void testZeroDeltaHandling() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        manager.configure(new HotSpotCoordinator.HotSpotSettings(20, 10, 3, 300000));

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-zero", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // 0→0→50: first delta is 0, second delta is 50 per ~50ms = 1000/s
        // Average per-sec: 500/s > threshold 400/s → hotspot
        recordReadHistory(manager, region.getRegionId(), 0, 0, 50);

        invokeHotSpotDetection(manager);

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
    }

    // ==================== Helper methods ====================

    private ClusterManager createClusterManager() {
        return new ClusterManager(new LoadBalancer());
    }

    private RegionSplitCoordinator createRegionSplitCoordinator() {
        return new RegionSplitCoordinator(createClusterManager(), new MetadataManager(), new LoadBalancer());
    }

    private Region createRegion(String regionId, String tableName, ServerId primary) {
        Region region = new Region(regionId, tableName, "a".getBytes(), "z".getBytes());
        region.setPrimary(primary);
        return region;
    }

    private void recordReadHistory(HotSpotCoordinator manager, String regionId, long... readRequests)
        throws Exception {
        for (long readRequest : readRequests) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId(regionId);
            load.setReadRequests(readRequest);
            load.setWriteRequests(0L);
            manager.recordRegionLoad(regionId, load);
            Thread.sleep(50);
        }
    }

    private void recordWriteHistory(HotSpotCoordinator manager, String regionId, long... writeRequests)
        throws Exception {
        for (long writeRequest : writeRequests) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId(regionId);
            load.setReadRequests(0L);
            load.setWriteRequests(writeRequest);
            manager.recordRegionLoad(regionId, load);
            Thread.sleep(50);
        }
    }

    private void recordMixedHistory(HotSpotCoordinator manager, String regionId, long[][] readWritePairs)
        throws Exception {
        for (long[] pair : readWritePairs) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId(regionId);
            load.setReadRequests(pair[0]);
            load.setWriteRequests(pair[1]);
            manager.recordRegionLoad(regionId, load);
            Thread.sleep(50);
        }
    }

    private void invokeHotSpotDetection(HotSpotCoordinator manager) throws Exception {
        Method method = HotSpotCoordinator.class.getDeclaredMethod("detectAndPlanHotSpots");
        method.setAccessible(true);
        method.invoke(manager);
    }
}
