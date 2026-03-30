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
    @DisplayName("health check marks timeout and later recovery")
    void testReplicaOfflineThenRecovered() throws Exception {
        ReplicaMonitor monitor = new ReplicaMonitor(new ClusterManager(new LoadBalancer()),
            10L, 30L, 100L, 10L);
        ServerId replicaServer = new ServerId("replica-host", 16020);
        ReplicaInfo replica = new ReplicaInfo("region-1", replicaServer, "", "", "",
            ReplicaInfo.ReplicaState.SECONDARY);

        AtomicBoolean failedCalled = new AtomicBoolean(false);
        AtomicBoolean recoveredCalled = new AtomicBoolean(false);
        monitor.registerCallback(new ReplicaMonitor.FailoverCallback() {
            @Override
            public void onReplicaFailed(String regionId, ServerId failedReplica) {
                failedCalled.set(true);
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
        monitor.start();

        try {
            waitFor(() -> failedCalled.get()
                && replica.getState() == ReplicaInfo.ReplicaState.OFFLINE, 1000L);

            monitor.updateHeartbeat("region-1", replicaServer, 0L);

            waitFor(() -> recoveredCalled.get()
                && replica.getState() == ReplicaInfo.ReplicaState.SECONDARY, 1000L);
        } finally {
            monitor.stop();
        }

        assertTrue(failedCalled.get());
        assertTrue(recoveredCalled.get());
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

    private void waitFor(Check check, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (check.done()) {
                return;
            }
            Thread.sleep(10L);
        }
        fail("Condition was not satisfied within " + timeoutMs + "ms");
    }

    @FunctionalInterface
    private interface Check {
        boolean done();
    }
}
