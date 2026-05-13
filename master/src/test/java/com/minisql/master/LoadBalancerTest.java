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

        ClusterManager.ServerInfo info1 = createServerInfo(server1, 0);
        ClusterManager.ServerInfo info2 = createServerInfo(server2, 1);

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
        ClusterManager.ServerInfo info1 = createServerInfo(server1, 0);
        ClusterManager.ServerInfo info2 = createServerInfo(server2, 0);
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
        ClusterManager.ServerInfo overloadedInfo = createServerInfo(overloaded, 8);
        ClusterManager.ServerInfo healthyInfo = createServerInfo(healthy, 0);
        Region region = new Region("region-random", "users", "a".getBytes(), "z".getBytes());

        ServerId selected = balancer.selectServerForRegion(region, Arrays.asList(overloadedInfo, healthyInfo));
        assertEquals(healthy, selected);
    }

    @Test
    @DisplayName("load based balancing computes move action for uneven cluster")
    void testComputeBalanceActions() {
        LoadBalancer balancer = new LoadBalancer();
        balancer.setStrategy(LoadBalancer.Strategy.LOAD_BASED);
        balancer.setBalanceThreshold(1.0);

        ServerId source = new ServerId("host1", 8080);
        ServerId target = new ServerId("host2", 8080);

        ClusterManager.ServerInfo sourceInfo = createServerInfo(source, 1);
        ClusterManager.ServerInfo targetInfo = createServerInfo(target, 0);

        List<LoadBalancer.BalanceAction> actions = balancer.computeBalanceActions(Arrays.asList(sourceInfo, targetInfo));

        assertFalse(actions.isEmpty());
        assertEquals(source, actions.get(0).getSource());
        assertEquals(target, actions.get(0).getTarget());
    }


    private ClusterManager.ServerInfo createServerInfo(ServerId serverId, int regionCount) {
        ClusterManager.ServerInfo info = new ClusterManager.ServerInfo(serverId, System.currentTimeMillis());
        for (int i = 0; i < regionCount; i++) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId("region-" + i);
            info.updateRegionLoad(load.getRegionId(), load);
        }
        return info;
    }
}
