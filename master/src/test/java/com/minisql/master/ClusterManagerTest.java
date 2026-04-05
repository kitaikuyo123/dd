package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.state.ClusterManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ClusterManager tests")
class ClusterManagerTest {

    private LoadBalancer createMockLoadBalancer() {
        return new LoadBalancer();
    }

    @Test
    @DisplayName("registerServer adds active server")
    void testRegisterServer() {
        ClusterManager manager = new ClusterManager(createMockLoadBalancer());
        ServerId server = new ServerId("host1", 8080);

        manager.registerServer(server);

        List<ClusterManager.ServerInfo> servers = manager.getActiveServersList();
        assertEquals(1, servers.size());
        assertEquals(server, servers.get(0).getServerId());
    }

    @Test
    @DisplayName("assignRegion creates table region mapping")
    void testAssignRegion() {
        ClusterManager manager = new ClusterManager(createMockLoadBalancer());
        ServerId server = new ServerId("host1", 8080);
        Region region = new Region("region-1", "users", "a".getBytes(), "z".getBytes());

        manager.registerServer(server);
        manager.assignRegion(region);

        assertEquals(server, manager.getPrimaryServerForRegion("region-1"));
        assertTrue(manager.getTableRegions("users").contains("region-1"));
    }

    @Test
    @DisplayName("removeRegionMetadata clears assignments replicas sequence ids and fencing")
    void testRemoveRegionMetadata() {
        ClusterManager manager = new ClusterManager(createMockLoadBalancer());
        ServerId server = new ServerId("host1", 8080);
        Region region = new Region("region-1", "users", "a".getBytes(), "z".getBytes());

        manager.registerServer(server);
        manager.assignRegion(region);
        manager.addReplica(region.getRegionId(), server);
        manager.updateReplicaSequenceId(region.getRegionId(), server, 123L);
        manager.updateFencingToken(region.getRegionId(), 9L);

        manager.removeRegionMetadata(region.getTableName(), region.getRegionId());

        assertNull(manager.getPrimaryServerForRegion(region.getRegionId()));
        assertTrue(manager.getReplicaServers(region.getRegionId()).isEmpty());
        assertEquals(0L, manager.getReplicaSequenceId(region.getRegionId(), server));
        assertEquals(0L, manager.getFencingToken(region.getRegionId()));
        assertTrue(manager.getTableRegions(region.getTableName()).isEmpty());
    }

    @Test
    @DisplayName("getRegionAssignments returns tracked assignments")
    void testGetRegionAssignments() {
        ClusterManager manager = new ClusterManager(createMockLoadBalancer());
        ServerId server = new ServerId("host1", 8080);
        Region region = new Region("region-1", "users", "a".getBytes(), "z".getBytes());

        manager.registerServer(server);
        manager.assignRegion(region);

        Map<String, ClusterManager.RegionAssignment> assignments = manager.getRegionAssignments();
        assertEquals(1, assignments.size());
        assertNotNull(assignments.get("region-1"));
    }
}
