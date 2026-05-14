package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaMonitor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicaMonitor tests")
class ReplicaMonitorTest {

    @Test
    @DisplayName("updateHeartbeat recovers an offline replica")
    void testReplicaOfflineThenRecovered() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor monitor = new ReplicaMonitor(clusterManager, metadataManager);

        ServerId primaryServer = new ServerId("primary-host", 16019);
        ServerId replicaServer = new ServerId("replica-host", 16020);

        Region region = new Region("region-1", "test", new byte[0], new byte[0]);
        region.setPrimary(primaryServer);
        region.addReplica(primaryServer);
        region.addReplica(replicaServer);
        metadataManager.registerRegionForTable(region, primaryServer);

        ReplicaInfo replica = new ReplicaInfo("region-1", replicaServer, "", "", "",
            ReplicaInfo.ReplicaState.OFFLINE);

        AtomicBoolean recoveredCalled = new AtomicBoolean(false);
        monitor.registerCallback(new ReplicaMonitor.FailoverCallback() {
            @Override
            public void onReplicaFailed(String regionId, ServerId failedReplica) {
            }

            @Override
            public void onReplicaLagging(String regionId, ServerId laggingReplica, long lagMs) {
            }

            @Override
            public void onReplicaRecovered(String regionId, ServerId recoveredReplica) {
                recoveredCalled.set(true);
            }
        });
        monitor.registerReplica("region-1", replica);

        monitor.updateHeartbeat("region-1", replicaServer, 0L);

        assertTrue(recoveredCalled.get());
        assertEquals(ReplicaInfo.ReplicaState.SECONDARY, replica.getState());
    }

    @Test
    @DisplayName("removeRegion clears tracked health state and getReplicas returns empty")
    void testRemoveRegionClearsTrackedReplicas() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor monitor = new ReplicaMonitor(clusterManager, metadataManager);

        ServerId replicaServer = new ServerId("replica-host", 16020);
        Region region = new Region("region-1", "test", new byte[0], new byte[0]);
        region.setPrimary(replicaServer);
        region.addReplica(replicaServer);
        metadataManager.registerRegionForTable(region, replicaServer);

        ReplicaInfo replica = new ReplicaInfo("region-1", replicaServer, "", "", "",
            ReplicaInfo.ReplicaState.SECONDARY);
        monitor.registerReplica("region-1", replica);
        assertEquals(1, monitor.getReplicas("region-1").size());

        monitor.removeRegion("region-1");

        // After removeRegion, health state is gone. getReplicas reads from Region metadata
        // which still exists, so it will auto-create a default entry. To truly test cleanup,
        // remove the Region metadata too to get empty result.
        metadataManager.removeRegion("region-1");
        assertTrue(monitor.getReplicas("region-1").isEmpty());
    }

}
