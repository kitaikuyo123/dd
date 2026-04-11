package com.minisql.master;

import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.state.ClusterManager;
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
        ReplicaMonitor monitor = new ReplicaMonitor(new ClusterManager(new LoadBalancer()));
        ServerId replicaServer = new ServerId("replica-host", 16020);
        // Replica starts OFFLINE (as would be set by ZooKeeper-driven failure detection)
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

        // Heartbeat recovers the OFFLINE replica to SECONDARY
        monitor.updateHeartbeat("region-1", replicaServer, 0L);

        assertTrue(recoveredCalled.get());
        assertEquals(ReplicaInfo.ReplicaState.SECONDARY, replica.getState());
    }

    @Test
    @DisplayName("removeRegion clears tracked replicas")
    void testRemoveRegionClearsTrackedReplicas() {
        ReplicaMonitor monitor = new ReplicaMonitor(new ClusterManager(new LoadBalancer()));
        ServerId replicaServer = new ServerId("replica-host", 16020);
        ReplicaInfo replica = new ReplicaInfo("region-1", replicaServer, "", "", "",
            ReplicaInfo.ReplicaState.SECONDARY);

        monitor.registerReplica("region-1", replica);
        assertEquals(1, monitor.getReplicas("region-1").size());

        monitor.removeRegion("region-1");

        assertTrue(monitor.getReplicas("region-1").isEmpty());
    }

}
