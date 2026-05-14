package com.minisql.master.monitoring;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MonitoringServiceTest {

    @Test
    void buildsSnapshotsFromClusterState() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();
        MonitoringService service = new MonitoringService(clusterManager, metadataManager, replicaMonitor, lifecycleManager);

        ServerId server = new ServerId("localhost", 16020);
        clusterManager.registerServer(server, System.currentTimeMillis());
        ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
        load.setRegionId("region-1");
        load.setReadRequests(11);
        load.setWriteRequests(7);
        load.setStoreFileSize(99);
        clusterManager.updateRegionLoad(server, "region-1", load);
        clusterManager.updateRegionState("region-1", Region.State.OPEN);
        clusterManager.assignRegionToServer("region-1", server);
        clusterManager.addReplica("region-1", server);

        Table table = new Table("users");
        metadataManager.createTable(table);
        Region region = new Region("region-1", "users", new byte[]{0}, new byte[]{127});
        region.setPrimary(server);
        region.addReplica(server);
        metadataManager.registerRegion(region);

        service.recordSqlMetric("SELECT", "users", true, 12, List.of("region-1"), null, "client");

        List<Map<String, Object>> regions = service.regions();
        List<Map<String, Object>> tables = service.tables();

        assertEquals(1, service.servers().size());
        assertFalse(regions.isEmpty());
        assertEquals("users", regions.get(0).get("tableName"));
        assertFalse(tables.isEmpty());
        assertEquals(1L, ((Number) service.sqlSummary("5m").get("requestCount")).longValue());
    }
}
