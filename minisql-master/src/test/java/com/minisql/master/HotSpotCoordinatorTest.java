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

        ServerId primary = new ServerId("primary-host", 16020);
        ServerId target = new ServerId("target-host", 16021);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(target);

        Region region = createRegion("region-read", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        recordReadHistory(manager, region.getRegionId(), 0, 12000, 25000);
        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.drainPendingActions();
        assertEquals(1, actions.size());
        assertEquals(HotSpotCoordinator.HotSpotActionType.ADD_READ_REPLICA, actions.get(0).getType());
        assertEquals(target, actions.get(0).getTargetServer());

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
        assertEquals(HotSpotCoordinator.HotSpotType.READ, currentHotSpots.get(region.getRegionId()).getType());
    }

    @Test
    @DisplayName("read hotspot can prefer region move when enough replicas already exist")
    void testDetectReadHotSpotPlansMoveWhenReplicaCountIsAlreadyHigh() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager, createRegionSplitCoordinator(), null);

        ServerId primary = new ServerId("primary-host", 16020);
        ServerId replica1 = new ServerId("replica-1", 16021);
        ServerId replica2 = new ServerId("replica-2", 16022);
        ServerId replica3 = new ServerId("replica-3", 16023);
        ServerId freeTarget = new ServerId("free-target", 16024);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(replica1);
        clusterManager.registerServer(replica2);
        clusterManager.registerServer(replica3);
        clusterManager.registerServer(freeTarget);

        Region region = createRegion("region-move", "orders", primary);
        region.addReplica(replica1);
        region.addReplica(replica2);
        region.addReplica(replica3);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);
        clusterManager.addReplica(region.getRegionId(), replica1);
        clusterManager.addReplica(region.getRegionId(), replica2);
        clusterManager.addReplica(region.getRegionId(), replica3);

        recordReadHistory(manager, region.getRegionId(), 0, 18000, 42000);
        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.drainPendingActions();
        assertEquals(1, actions.size());
        assertEquals(HotSpotCoordinator.HotSpotActionType.MOVE_REGION, actions.get(0).getType());
        assertEquals(primary, actions.get(0).getSourceServer());
        assertEquals(freeTarget, actions.get(0).getTargetServer());
    }

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

    private void recordReadHistory(HotSpotCoordinator manager, String regionId, long... readRequests) {
        for (long readRequest : readRequests) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId(regionId);
            load.setReadRequests(readRequest);
            load.setWriteRequests(0L);
            manager.recordRegionLoad(regionId, load);
        }
    }

    private void invokeHotSpotDetection(HotSpotCoordinator manager) throws Exception {
        Method method = HotSpotCoordinator.class.getDeclaredMethod("detectAndPlanHotSpots");
        method.setAccessible(true);
        method.invoke(manager);
    }
}
