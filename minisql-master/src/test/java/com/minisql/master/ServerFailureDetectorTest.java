package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.master.detect.ServerFailedEvent;
import com.minisql.master.detect.ServerFailureDetector;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ServerFailureDetector integration-style tests")
class ServerFailureDetectorTest {

    @Test
    @DisplayName("server failure publishes recovery event and marks replicas offline")
    void handleServerFailurePublishesRecoveryEvent() throws Exception {
        LoadBalancer loadBalancer = new LoadBalancer();
        ClusterManager clusterManager = new ClusterManager(loadBalancer);
        MetadataManager metadataManager = new MetadataManager();
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId failedPrimary = new ServerId("localhost", 16020, 1000L);
        ServerId secondary = new ServerId("localhost", 16021, 1001L);

        clusterManager.registerServer(failedPrimary);
        clusterManager.registerServer(secondary);

        Table table = new Table("products");
        metadataManager.createTable(table);

        Region region = new Region("products_r1", "products", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(failedPrimary);
        region.addReplica(failedPrimary);
        region.addReplica(secondary);
        metadataManager.registerRegionForTable(region, failedPrimary);

        clusterManager.assignRegionToServer(region.getRegionId(), failedPrimary);
        clusterManager.updateRegionState(region.getRegionId(), Region.State.OPEN);
        clusterManager.addReplica(region.getRegionId(), failedPrimary);
        clusterManager.addReplica(region.getRegionId(), secondary);

        ServerFailureDetector detector = new ServerFailureDetector(clusterManager, metadataManager, loadBalancer);
        detector.setLifecycleManager(lifecycleManager);
        List<ServerFailedEvent> events = new ArrayList<>();
        detector.setEventSink(event -> {
            if (event instanceof ServerFailedEvent) {
                events.add((ServerFailedEvent) event);
            }
        });

        invokeHandleServerFailure(detector, failedPrimary);

        ReplicaLifecycleManager.ReplicaLifecycleStatus failedStatus =
            lifecycleManager.getStatus(region.getRegionId(), failedPrimary);
        assertNotNull(failedStatus);
        assertEquals(ReplicaLifecycleManager.ReplicaLifecycleState.OFFLINE, failedStatus.getState());
        assertFalse(clusterManager.isServerActive(failedPrimary));
        assertEquals(1, events.size());
        assertEquals(failedPrimary, events.get(0).getFailedServer());
        assertEquals(List.of(region.getRegionId()), events.get(0).getAffectedRegionIds());
    }

    @Test
    @DisplayName("handleServerFailure ignores servers without assigned regions")
    void handleServerFailureSkipsUnassignedServer() throws Exception {
        LoadBalancer loadBalancer = new LoadBalancer();
        ClusterManager clusterManager = new ClusterManager(loadBalancer);
        MetadataManager metadataManager = new MetadataManager();

        ServerId idleServer = new ServerId("localhost", 16030, 2000L);
        clusterManager.registerServer(idleServer);

        ServerFailureDetector detector = new ServerFailureDetector(clusterManager, metadataManager, loadBalancer);
        List<ServerFailedEvent> events = new ArrayList<>();
        detector.setEventSink(event -> {
            if (event instanceof ServerFailedEvent) {
                events.add((ServerFailedEvent) event);
            }
        });

        invokeHandleServerFailure(detector, idleServer);

        assertFalse(clusterManager.isServerActive(idleServer));
        assertTrue(clusterManager.getRegionAssignments().isEmpty());
        assertTrue(events.isEmpty());
    }

    private static void invokeHandleServerFailure(ServerFailureDetector detector, ServerId failedServer) throws Exception {
        Method method = ServerFailureDetector.class.getDeclaredMethod("handleServerFailure", ServerId.class);
        method.setAccessible(true);
        method.invoke(detector, failedServer);
    }
}
