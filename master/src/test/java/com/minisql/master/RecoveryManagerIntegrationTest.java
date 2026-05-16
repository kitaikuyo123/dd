package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.recover.RecoveryCoordinator;
import com.minisql.master.rpc.RegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.replication.ReplicationConfig;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RecoveryCoordinator integration-style tests")
class RecoveryManagerIntegrationTest {

    @Test
    @DisplayName("reconcileRecoveredServer recovers an existing replica member")
    void reconcileRecoveredServerRecoversExistingReplicaMember() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("localhost", 16022, 1000L);
        ServerId recovered = new ServerId("localhost", 16020, 1001L);

        clusterManager.registerServer(primary);
        clusterManager.registerServer(recovered);

        Table table = new Table("products");
        metadataManager.createTable(table);

        Region region = new Region("products_r1", "products", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(recovered);
        metadataManager.registerRegionForTable(region, primary);

        RecordingRecoveryManager recoveryManager =
            new RecordingRecoveryManager(clusterManager, metadataManager, replicaMonitor, lifecycleManager);

        recoveryManager.reconcileRecoveredServer(recovered);

        assertEquals(region.getRegionId(), recoveryManager.recoveredRegionId);
        assertEquals(recovered, recoveryManager.recoveredServer);
        assertNull(recoveryManager.bootstrappedRegionId);
    }

    @Test
    @DisplayName("reconcileRecoveredServer bootstraps when active replicas are below target factor")
    void reconcileRecoveredServerBootstrapsWhenReplicaCountIsLow() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaMonitor replicaMonitor = new ReplicaMonitor(clusterManager, metadataManager);
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId primary = new ServerId("localhost", 16022, 1000L);
        ServerId existingSecondary = new ServerId("localhost", 16021, 1001L);
        ServerId recovered = new ServerId("localhost", 16020, 1002L);

        clusterManager.registerServer(primary);
        clusterManager.registerServer(existingSecondary);
        clusterManager.registerServer(recovered);

        Table table = new Table("products");
        metadataManager.createTable(table);

        Region region = new Region("products_r2", "products", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(existingSecondary);
        metadataManager.registerRegionForTable(region, primary);

        RecordingRecoveryManager recoveryManager =
            new RecordingRecoveryManager(clusterManager, metadataManager, replicaMonitor, lifecycleManager);

        recoveryManager.reconcileRecoveredServer(recovered);

        assertEquals(region.getRegionId(), recoveryManager.bootstrappedRegionId);
        assertEquals(recovered, recoveryManager.bootstrappedServer);
        assertNull(recoveryManager.recoveredRegionId);
    }

    private static final class RecordingRecoveryManager extends RecoveryCoordinator {
        private String recoveredRegionId;
        private ServerId recoveredServer;
        private String bootstrappedRegionId;
        private ServerId bootstrappedServer;

        private RecordingRecoveryManager(ClusterManager clusterManager,
                                         MetadataManager metadataManager,
                                         ReplicaMonitor replicaMonitor,
                                         ReplicaLifecycleManager lifecycleManager) {
            super(clusterManager, metadataManager, replicaMonitor,
                new ReplicationCoordinator(ReplicationConfig.builder(3).build()), lifecycleManager,
                new ReadyCommandClient());
        }

        @Override
        public void bootstrapReplica(String regionId, ServerId replica) {
            this.bootstrappedRegionId = regionId;
            this.bootstrappedServer = replica;
        }

        @Override
        public void recoverReplica(String regionId, ServerId replica) {
            this.recoveredRegionId = regionId;
            this.recoveredServer = replica;
        }
    }

    private static final class ReadyCommandClient implements RegionServerCommandClient {
        private static final CommonProto.Status OK = CommonProto.Status.newBuilder()
            .setCode(0)
            .setSuccess(true)
            .setMessage("OK")
            .build();

        @Override
        public RegionServerProto.OpenRegionResponse openRegion(ServerId serverId, Region region, boolean asReplica) {
            return RegionServerProto.OpenRegionResponse.newBuilder().setStatus(OK).build();
        }

        @Override
        public RegionServerProto.CloseRegionResponse closeRegion(ServerId serverId, String regionId, boolean abort, boolean dropTable) {
            return RegionServerProto.CloseRegionResponse.newBuilder().setStatus(OK).build();
        }

        @Override
        public RegionServerProto.PromoteResponse promoteToPrimary(ServerId serverId, String regionId, long fencingToken) {
            return RegionServerProto.PromoteResponse.newBuilder().setStatus(OK).build();
        }

        @Override
        public RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId serverId, String regionId, long timeoutMs) {
            return RegionServerProto.GetReplicationLagResponse.newBuilder().setStatus(OK).build();
        }

        @Override
        public RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId serverId, String regionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.SplitRegionResponse splitRegion(ServerId serverId, String regionId, byte[] splitKey,
                                                      String leftRegionId, String rightRegionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.MergeRegionResponse mergeRegion(ServerId serverId, String leftRegionId, String rightRegionId,
                                                      String mergedRegionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.MigrateResponse startMigration(ServerId serverId, String regionId, ServerId targetServer, long timeoutMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId serverId, String regionId, ServerId targetServer, long fromSequenceId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.AbortMigrationResponse abortMigration(ServerId serverId, String regionId) {
            throw new UnsupportedOperationException();
        }
    }
}
