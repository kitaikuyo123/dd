package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.MasterProto;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.recover.FailoverCoordinator;
import com.minisql.master.recover.RecoveryCoordinator;
import com.minisql.master.rpc.MasterServiceImpl;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.replication.ReplicationConfig;
import com.minisql.replication.ReplicationCoordinator;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MasterServiceImpl component tests")
class MasterServiceImplTest {

    @Test
    @DisplayName("reportPrimaryChange updates metadata cluster state and region lookup")
    void testReportPrimaryChangeUpdatesPrimaryEverywhere() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicationCoordinator replicationCoordinator = new ReplicationCoordinator(ReplicationConfig.builder(3).build());
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();
        MasterServiceImpl service = new MasterServiceImpl(
            clusterManager,
            metadataManager,
            new LoadBalancer(),
            replicationCoordinator,
            replicaMonitor,
            null,
            null,
            lifecycleManager,
            null
        );

        Table table = new Table("products");
        metadataManager.createTable(table);

        ServerId oldPrimary = new ServerId("old-primary", 16020);
        ServerId newPrimary = new ServerId("new-primary", 16021);
        Region region = new Region("region-1", "products", "a".getBytes(), "z".getBytes());
        region.setPrimary(oldPrimary);
        region.setReplicas(List.of(oldPrimary, newPrimary));
        metadataManager.registerRegionForTable(region, oldPrimary);
        clusterManager.assignRegionToServer(region.getRegionId(), oldPrimary);
        clusterManager.addReplica(region.getRegionId(), oldPrimary);
        clusterManager.addReplica(region.getRegionId(), newPrimary);
        replicaMonitor.registerReplica(region.getRegionId(),
            new ReplicaInfo(region.getRegionId(), oldPrimary, "", "", "", ReplicaInfo.ReplicaState.PRIMARY));
        replicaMonitor.registerReplica(region.getRegionId(),
            new ReplicaInfo(region.getRegionId(), newPrimary, "", "", "", ReplicaInfo.ReplicaState.SECONDARY));

        CapturingObserver<MasterProto.PrimaryChangeResponse> responseObserver = new CapturingObserver<>();
        service.reportPrimaryChange(
            MasterProto.PrimaryChangeRequest.newBuilder()
                .setRegionId(region.getRegionId())
                .setOldPrimary(toProtoServer(oldPrimary))
                .setNewPrimary(toProtoServer(newPrimary))
                .build(),
            responseObserver
        );

        assertTrue(responseObserver.value.getStatus().getSuccess());
        assertEquals(newPrimary, metadataManager.getRegion(region.getRegionId()).getPrimary());
        assertEquals(newPrimary, clusterManager.getPrimaryServerForRegion(region.getRegionId()));
        assertEquals(ReplicaLifecycleManager.ReplicaLifecycleState.PRIMARY_READY,
            lifecycleManager.getStatus(region.getRegionId(), newPrimary).getState());
        assertEquals(ReplicaLifecycleManager.ReplicaLifecycleState.OFFLINE,
            lifecycleManager.getStatus(region.getRegionId(), oldPrimary).getState());

        CapturingObserver<MasterProto.GetTableRegionsResponse> regionsObserver = new CapturingObserver<>();
        service.getTableRegions(
            MasterProto.GetTableRegionsRequest.newBuilder().setTableName("products").build(),
            regionsObserver
        );

        assertTrue(regionsObserver.value.getStatus().getSuccess());
        assertEquals(1, regionsObserver.value.getRegionsCount());
        assertEquals("new-primary", regionsObserver.value.getRegions(0).getPrimary().getHost());
        assertEquals(16021, regionsObserver.value.getRegions(0).getPrimary().getPort());
    }

    @Test
    @DisplayName("deleteTable clears metadata monitor lifecycle and replication groups")
    void testDeleteTableCleansRegionState() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicationCoordinator replicationCoordinator = new ReplicationCoordinator(ReplicationConfig.builder(3).build());
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();
        MasterServiceImpl service = new MasterServiceImpl(
            clusterManager,
            metadataManager,
            new LoadBalancer(),
            replicationCoordinator,
            replicaMonitor,
            null,
            null,
            lifecycleManager,
            null
        );

        Table table = new Table("products");
        metadataManager.createTable(table);

        Region region = new Region("region-1", "products", "a".getBytes(), "z".getBytes());
        ServerId replica = new ServerId("replica", 16020);
        metadataManager.registerRegion(region);
        clusterManager.assignRegionToServer(region.getRegionId(), replica);
        clusterManager.addReplica(region.getRegionId(), replica);
        clusterManager.updateReplicaSequenceId(region.getRegionId(), replica, 11L);
        clusterManager.updateFencingToken(region.getRegionId(), 3L);
        replicaMonitor.registerReplica(region.getRegionId(),
            new ReplicaInfo(region.getRegionId(), replica, "", "", "", ReplicaInfo.ReplicaState.SECONDARY));
        lifecycleManager.transition(region.getRegionId(), replica,
            ReplicaLifecycleManager.ReplicaLifecycleState.SECONDARY_READY, "ready");
        replicationCoordinator.createReplicaGroup(region, List.of(replica));

        CapturingObserver<MasterProto.DeleteTableResponse> responseObserver = new CapturingObserver<>();
        service.deleteTable(
            MasterProto.DeleteTableRequest.newBuilder().setTableName("products").build(),
            responseObserver
        );

        assertTrue(responseObserver.value.getStatus().getSuccess());
        assertFalse(metadataManager.tableExists("products"));
        assertNull(metadataManager.getRegion(region.getRegionId()));
        assertNull(clusterManager.getPrimaryServerForRegion(region.getRegionId()));
        assertTrue(clusterManager.getReplicaServers(region.getRegionId()).isEmpty());
        assertTrue(replicaMonitor.getReplicas(region.getRegionId()).isEmpty());
        assertNull(lifecycleManager.getStatus(region.getRegionId(), replica));
        assertNull(replicationCoordinator.getReplicaGroup(region.getRegionId()));
    }

    @Test
    @DisplayName("reportRegionStatus delegates failures to failover manager")
    void testReportRegionStatusDelegatesFailureHandling() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicationCoordinator replicationCoordinator = new ReplicationCoordinator(ReplicationConfig.builder(3).build());
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();
        RecordingFailoverCoordinator failoverCoordinator =
            new RecordingFailoverCoordinator(clusterManager, metadataManager, replicaMonitor, lifecycleManager);
        MasterServiceImpl service = new MasterServiceImpl(
            clusterManager,
            metadataManager,
            new LoadBalancer(),
            replicationCoordinator,
            replicaMonitor,
            failoverCoordinator,
            null,
            lifecycleManager,
            null
        );

        CapturingObserver<MasterProto.RegionStatusResponse> responseObserver = new CapturingObserver<>();
        service.reportRegionStatus(
            MasterProto.RegionStatusRequest.newBuilder()
                .setRegionId("region-1")
                .setState(CommonProto.RegionState.CLOSED)
                .setErrorMessage("disk failure")
                .build(),
            responseObserver
        );

        assertTrue(responseObserver.value.getStatus().getSuccess());
        assertEquals("region-1", failoverCoordinator.triggeredRegionId);
    }

    @Test
    @DisplayName("server failure events delegate failover and replacement bootstrap")
    void testHandleServerFailureEventDelegatesRecovery() throws Exception {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicationCoordinator replicationCoordinator = new ReplicationCoordinator(ReplicationConfig.builder(3).build());
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();
        RecordingFailoverCoordinator failoverCoordinator =
            new RecordingFailoverCoordinator(clusterManager, metadataManager, replicaMonitor, lifecycleManager);
        RecordingRecoveryCoordinator recoveryCoordinator =
            new RecordingRecoveryCoordinator(clusterManager, metadataManager, replicaMonitor, lifecycleManager);

        ServerId failedPrimary = new ServerId("failed-primary", 16020, 1L);
        ServerId secondary = new ServerId("secondary", 16021, 2L);
        ServerId spare = new ServerId("spare", 16022, 3L);
        clusterManager.registerServer(failedPrimary);
        clusterManager.registerServer(secondary);
        clusterManager.registerServer(spare);

        Table table = new Table("orders");
        metadataManager.createTable(table);
        Region region = new Region("orders_r1", "orders", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(failedPrimary);
        region.setReplicas(List.of(failedPrimary, secondary));
        metadataManager.registerRegionForTable(region, failedPrimary);
        clusterManager.assignRegionToServer(region.getRegionId(), failedPrimary);
        clusterManager.addReplica(region.getRegionId(), failedPrimary);
        clusterManager.addReplica(region.getRegionId(), secondary);

        MasterServiceImpl service = new MasterServiceImpl(
            clusterManager,
            metadataManager,
            new LoadBalancer(),
            replicationCoordinator,
            replicaMonitor,
            failoverCoordinator,
            recoveryCoordinator,
            lifecycleManager,
            null
        );

        service.recoverRegionAfterServerFailure(region.getRegionId(), failedPrimary, true);

        waitFor(() -> failoverCoordinator.triggeredRegionId != null
            && recoveryCoordinator.bootstrappedRegionId != null, 1000L);

        assertEquals(region.getRegionId(), failoverCoordinator.triggeredRegionId);
        assertEquals(region.getRegionId(), recoveryCoordinator.bootstrappedRegionId);
        assertEquals(spare, recoveryCoordinator.bootstrappedServer);
    }

    private CommonProto.ServerId toProtoServer(ServerId serverId) {
        return CommonProto.ServerId.newBuilder()
            .setHost(serverId.getHost())
            .setPort(serverId.getPort())
            .build();
    }

    private static final class CapturingObserver<T> implements StreamObserver<T> {
        private T value;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }
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

    private static final class RecordingFailoverCoordinator extends FailoverCoordinator {
        private String triggeredRegionId;

        private RecordingFailoverCoordinator(ClusterManager clusterManager,
                                             MetadataManager metadataManager,
                                             ReplicaMonitor replicaMonitor,
                                             ReplicaLifecycleManager lifecycleManager) {
            super(clusterManager, metadataManager, replicaMonitor, lifecycleManager);
        }

        @Override
        public void triggerEmergencyFailover(String regionId) {
            this.triggeredRegionId = regionId;
        }
    }

    private static final class RecordingRecoveryCoordinator extends RecoveryCoordinator {
        private final ClusterManager clusterManager;
        private String bootstrappedRegionId;
        private ServerId bootstrappedServer;

        private RecordingRecoveryCoordinator(ClusterManager clusterManager,
                                             MetadataManager metadataManager,
                                             ReplicaMonitor replicaMonitor,
                                             ReplicaLifecycleManager lifecycleManager) {
            super(clusterManager, metadataManager, replicaMonitor,
                new ReplicationCoordinator(ReplicationConfig.builder(3).build()), lifecycleManager);
            this.clusterManager = clusterManager;
        }

        @Override
        public void bootstrapReplica(String regionId, ServerId replica) {
            this.bootstrappedRegionId = regionId;
            this.bootstrappedServer = replica;
            clusterManager.addReplica(regionId, replica);
        }
    }
}
