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
    @DisplayName("write hotspot plans split action")
    void testDetectWriteHotSpotPlansSplit() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-write", "orders", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // 记录写请求历史（超过阈值 100，但增长平稳以避免触发 GROWING 类型）
        // 使用递增但增长比例较小的数据
        recordWriteHistory(manager, region.getRegionId(), 1000, 1050, 1180);

        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.drainPendingActions();
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

        ServerId primary = new ServerId("primary-host", 16020);
        clusterManager.registerServer(primary);

        Region region = createRegion("region-write-growing", "orders", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // 记录写请求历史（快速增长触发 GROWING 类型）
        recordWriteHistory(manager, region.getRegionId(), 0, 50, 180);

        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.drainPendingActions();
        assertEquals(1, actions.size());
        assertEquals(HotSpotCoordinator.HotSpotActionType.SPLIT_REGION, actions.get(0).getType());

        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = manager.getCurrentHotSpots();
        assertTrue(currentHotSpots.containsKey(region.getRegionId()));
        assertEquals(HotSpotCoordinator.HotSpotType.WRITE_GROWING, currentHotSpots.get(region.getRegionId()).getType());
    }

    @Test
    @DisplayName("cooldown prevents duplicate actions but allows status update")
    void testCooldownPreventsDuplicateActionsButAllowsStatusUpdate() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        ServerId primary = new ServerId("primary-host", 16020);
        ServerId target = new ServerId("target-host", 16021);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(target);

        Region region = createRegion("region-cooldown", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // 第一次检测
        recordReadHistory(manager, region.getRegionId(), 0, 12000, 25000);
        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> firstActions = manager.drainPendingActions();
        assertEquals(1, firstActions.size());

        // 第二次检测（冷却期内）
        recordReadHistory(manager, region.getRegionId(), 30000, 40000, 55000);
        invokeHotSpotDetection(manager);

        // 冷却期内不应产生新动作
        List<HotSpotCoordinator.HotSpotAction> secondActions = manager.drainPendingActions();
        assertTrue(secondActions.isEmpty());

        // 但热点状态应该更新
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

        ServerId primary = new ServerId("primary-host", 16020);
        ServerId target = new ServerId("target-host", 16021);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(target);

        Region region = createRegion("region-stale", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        // 先产生热点
        recordReadHistory(manager, region.getRegionId(), 0, 12000, 25000);
        invokeHotSpotDetection(manager);
        assertTrue(manager.getCurrentHotSpots().containsKey(region.getRegionId()));

        // 模拟冷却期过期
        Thread.sleep(100);
        manager.configure(new HotSpotCoordinator.HotSpotSettings(
            200, 100, 1.2, 3, 50)); // 设置很短的冷却期 50ms
        Thread.sleep(100);

        // 负载恢复正常（低于阈值）
        recordReadHistory(manager, region.getRegionId(), 25000, 25010, 25020);
        invokeHotSpotDetection(manager);

        // 热点应该被移除
        assertFalse(manager.getCurrentHotSpots().containsKey(region.getRegionId()));
    }

    @Test
    @DisplayName("stale server is excluded from replica targets")
    void testStaleServerExcludedFromReplicaTargets() throws Exception {
        ClusterManager clusterManager = createClusterManager();
        MetadataManager metadataManager = new MetadataManager();
        HotSpotCoordinator manager = new HotSpotCoordinator(clusterManager, metadataManager,
            createRegionSplitCoordinator(), null);

        ServerId primary = new ServerId("primary-host", 16020);
        ServerId staleServer = new ServerId("stale-host", 16021);
        ServerId healthyServer = new ServerId("healthy-host", 16022);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(staleServer);
        clusterManager.registerServer(healthyServer);

        // 模拟 staleServer 心跳过期
        ClusterManager.ServerInfo staleInfo = clusterManager.getActiveServersList().stream()
            .filter(s -> s.getServerId().equals(staleServer))
            .findFirst()
            .orElse(null);
        if (staleInfo != null) {
            staleInfo.setLastHeartbeat(System.currentTimeMillis() - 120000); // 2 分钟前
        }

        Region region = createRegion("region-stale-test", "users", primary);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), primary);

        recordReadHistory(manager, region.getRegionId(), 0, 12000, 25000);
        invokeHotSpotDetection(manager);

        List<HotSpotCoordinator.HotSpotAction> actions = manager.drainPendingActions();
        assertEquals(1, actions.size());
        // 应该选择健康服务器，而不是过期服务器
        assertEquals(healthyServer, actions.get(0).getTargetServer());
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

    private void recordWriteHistory(HotSpotCoordinator manager, String regionId, long... writeRequests) {
        for (long writeRequest : writeRequests) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId(regionId);
            load.setReadRequests(0L);
            load.setWriteRequests(writeRequest);
            manager.recordRegionLoad(regionId, load);
        }
    }

    private void invokeHotSpotDetection(HotSpotCoordinator manager) throws Exception {
        Method method = HotSpotCoordinator.class.getDeclaredMethod("detectAndPlanHotSpots");
        method.setAccessible(true);
        method.invoke(manager);
    }
}
