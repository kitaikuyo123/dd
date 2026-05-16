package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.rebalance.RegionMigrationCoordinator;
import com.minisql.master.rpc.RegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("RegionMigrationCoordinator tests")
class RegionMigrationOrchestratorTest {

    @Test
    @DisplayName("migration executes remote commands before committing metadata")
    void migrationExecutesRemoteCommandsBeforeCommittingMetadata() {
        ClusterManager clusterManager = new ClusterManager(new LoadBalancer());
        MetadataManager metadataManager = new MetadataManager();
        ReplicaLifecycleManager lifecycleManager = new ReplicaLifecycleManager();

        ServerId source = new ServerId("source-host", 16020, 1L);
        ServerId target = new ServerId("target-host", 16021, 2L);
        clusterManager.registerServer(source);
        clusterManager.registerServer(target);

        Table table = new Table("orders");
        metadataManager.createTable(table);

        Region region = new Region("orders_r1", "orders", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(source);
        region.addReplica(source);
        region.addReplica(target);
        metadataManager.registerRegionForTable(region, source);
        clusterManager.assignRegionToServer(region.getRegionId(), source);

        RecordingCommandClient commandClient = new RecordingCommandClient(clusterManager);
        RegionMigrationCoordinator orchestrator =
            new RegionMigrationCoordinator(clusterManager, metadataManager, new LoadBalancer(), commandClient, lifecycleManager);

        orchestrator.execute(new LoadBalancer.BalanceAction(region.getRegionId(), source, target));

        assertEquals(List.of(
            "open:" + target.getServerName(),
            "start:" + source.getServerName() + "->" + target.getServerName(),
            "lag:" + target.getServerName(),
            "finalize:" + source.getServerName(),
            "lag:" + target.getServerName(),
            "promote:" + target.getServerName(),
            "close:" + source.getServerName()
        ), commandClient.operations);
        assertEquals(source, commandClient.primaryAssignmentWhenClosingSource);
        assertEquals(target, clusterManager.getPrimaryServerForRegion(region.getRegionId()));
        assertEquals(1L, clusterManager.getFencingToken(region.getRegionId()));
        assertEquals(ReplicaLifecycleManager.ReplicaLifecycleState.PRIMARY_READY,
            lifecycleManager.getStatus(region.getRegionId(), target).getState());
        assertEquals(ReplicaLifecycleManager.ReplicaLifecycleState.REMOVED,
            lifecycleManager.getStatus(region.getRegionId(), source).getState());
    }

    private static final class RecordingCommandClient implements RegionServerCommandClient {
        private final ClusterManager clusterManager;
        private final List<String> operations = new ArrayList<>();
        private ServerId primaryAssignmentWhenClosingSource;

        private RecordingCommandClient(ClusterManager clusterManager) {
            this.clusterManager = clusterManager;
        }

        @Override
        public RegionServerProto.OpenRegionResponse openRegion(ServerId serverId, Region region, boolean asReplica) {
            operations.add("open:" + serverId.getServerName());
            return RegionServerProto.OpenRegionResponse.newBuilder().setStatus(successStatus()).build();
        }

        @Override
        public RegionServerProto.CloseRegionResponse closeRegion(ServerId serverId, String regionId, boolean abort,
                                                                 boolean dropTable) {
            operations.add("close:" + serverId.getServerName());
            primaryAssignmentWhenClosingSource = clusterManager.getPrimaryServerForRegion(regionId);
            return RegionServerProto.CloseRegionResponse.newBuilder().setStatus(successStatus()).build();
        }

        @Override
        public RegionServerProto.PromoteResponse promoteToPrimary(ServerId serverId, String regionId, long fencingToken) {
            operations.add("promote:" + serverId.getServerName());
            return RegionServerProto.PromoteResponse.newBuilder().setStatus(successStatus()).build();
        }

        @Override
        public RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId serverId, String regionId,
                                                                             long timeoutMs) {
            operations.add("lag:" + serverId.getServerName());
            return RegionServerProto.GetReplicationLagResponse.newBuilder()
                .setStatus(successStatus())
                .setLagInEntries(0L)
                .setLastAppliedSequenceId(100L)
                .build();
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
        public RegionServerProto.MergeRegionResponse mergeRegion(ServerId serverId, String leftRegionId,
                                                                 String rightRegionId, String mergedRegionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RegionServerProto.MigrateResponse startMigration(ServerId serverId, String regionId, ServerId targetServer,
                                                                long timeoutMs) {
            operations.add("start:" + serverId.getServerName() + "->" + targetServer.getServerName());
            return RegionServerProto.MigrateResponse.newBuilder()
                .setStatus(successStatus())
                .setSourceSequenceId(10L)
                .build();
        }

        @Override
        public RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId serverId, String regionId,
                                                                             ServerId targetServer, long fromSequenceId) {
            operations.add("finalize:" + serverId.getServerName());
            return RegionServerProto.FinalizeMigrationResponse.newBuilder()
                .setStatus(successStatus())
                .setSourceSequenceId(20L)
                .build();
        }

        @Override
        public RegionServerProto.AbortMigrationResponse abortMigration(ServerId serverId, String regionId) {
            throw new UnsupportedOperationException();
        }

        private CommonProto.Status successStatus() {
            return CommonProto.Status.newBuilder()
                .setSuccess(true)
                .build();
        }
    }
}
