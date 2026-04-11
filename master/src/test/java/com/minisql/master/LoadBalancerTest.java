package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.state.ClusterManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LoadBalancer tests")
class LoadBalancerTest {

    @Test
    @DisplayName("can create load balancer")
    void testCreateLoadBalancer() {
        assertNotNull(new LoadBalancer());
    }

    @Test
    @DisplayName("load based strategy prefers lighter server")
    void testSelectServerForRegionLoadBased() {
        LoadBalancer balancer = new LoadBalancer();
        balancer.setStrategy(LoadBalancer.Strategy.LOAD_BASED);

        ServerId server1 = new ServerId("host1", 8080);
        ServerId server2 = new ServerId("host2", 8080);

        ClusterManager.ServerInfo info1 = createServerInfo(server1, 30.0);
        ClusterManager.ServerInfo info2 = createServerInfo(server2, 70.0);

        Region region = new Region("region-1", "table1", "a".getBytes(), "z".getBytes());
        ServerId selected = balancer.selectServerForRegion(region, Arrays.asList(info1, info2));

        assertEquals(server1, selected);
    }

    @Test
    @DisplayName("round robin strategy cycles through candidates")
    void testRoundRobinSelection() {
        LoadBalancer balancer = new LoadBalancer();
        balancer.setStrategy(LoadBalancer.Strategy.ROUND_ROBIN);

        ServerId server1 = new ServerId("host1", 8080);
        ServerId server2 = new ServerId("host2", 8081);
        ClusterManager.ServerInfo info1 = createServerInfo(server1, 20.0);
        ClusterManager.ServerInfo info2 = createServerInfo(server2, 20.0);
        Region region = new Region("region-rr", "users", "a".getBytes(), "z".getBytes());
        List<ClusterManager.ServerInfo> servers = Arrays.asList(info1, info2);

        assertEquals(server1, balancer.selectServerForRegion(region, servers));
        assertEquals(server2, balancer.selectServerForRegion(region, servers));
        assertEquals(server1, balancer.selectServerForRegion(region, servers));
    }

    @Test
    @DisplayName("random strategy still returns the only viable target")
    void testRandomSelectionWithSingleHealthyCandidate() {
        LoadBalancer balancer = new LoadBalancer();
        balancer.setStrategy(LoadBalancer.Strategy.RANDOM);

        ServerId overloaded = new ServerId("overloaded", 8080);
        ServerId healthy = new ServerId("healthy", 8081);
        ClusterManager.ServerInfo overloadedInfo = createServerInfo(overloaded, 95.0);
        ClusterManager.ServerInfo healthyInfo = createServerInfo(healthy, 20.0);
        for (int i = 0; i < 100; i++) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId("hot-region-" + i);
            overloadedInfo.getRegionLoads().put(load.getRegionId(), load);
        }
        Region region = new Region("region-random", "users", "a".getBytes(), "z".getBytes());

        ServerId selected = balancer.selectServerForRegion(region, Arrays.asList(overloadedInfo, healthyInfo));
        assertEquals(healthy, selected);
    }

    @Test
    @DisplayName("load based balancing computes move action for uneven cluster")
    void testComputeBalanceActions() {
        LoadBalancer balancer = new LoadBalancer();
        balancer.setStrategy(LoadBalancer.Strategy.LOAD_BASED);

        ServerId source = new ServerId("host1", 8080);
        ServerId target = new ServerId("host2", 8080);

        ClusterManager.ServerInfo sourceInfo = createServerInfo(source, 95.0);
        ClusterManager.ServerInfo targetInfo = createServerInfo(target, 5.0);

        ClusterManager.RegionLoad regionLoad = new ClusterManager.RegionLoad();
        regionLoad.setRegionId("region-1");
        regionLoad.setStoreFileSize(512L * 1024 * 1024);
        regionLoad.setReadRequests(100);
        regionLoad.setWriteRequests(100);
        sourceInfo.getRegionLoads().put("region-1", regionLoad);

        List<LoadBalancer.BalanceAction> actions = balancer.computeBalanceActions(Arrays.asList(sourceInfo, targetInfo));

        assertFalse(actions.isEmpty());
        assertEquals("region-1", actions.get(0).getRegionId());
        assertEquals(source, actions.get(0).getSource());
        assertEquals(target, actions.get(0).getTarget());
    }


    private ClusterManager.ServerInfo createServerInfo(ServerId serverId, double loadPercent) {
        ClusterManager.ServerInfo info = new ClusterManager.ServerInfo(serverId, System.currentTimeMillis());
        ClusterManager.ServerMetrics metrics = new ClusterManager.ServerMetrics();
        metrics.setCpuUsage(loadPercent);
        metrics.setMemoryUsage(loadPercent);
        metrics.setTotalSpace(1_000_000L);
        metrics.setAvailableSpace((long) ((100.0 - loadPercent) / 100.0 * 1_000_000L));
        info.setMetrics(metrics);
        return info;
    }
}
