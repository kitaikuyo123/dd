package com.minisql.master;

import com.google.protobuf.ByteString;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.rpc.GrpcRegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("GrpcRegionServerCommandClient request assembly tests")
class GrpcRegionServerCommandClientTest {

    @Test
    @DisplayName("open region request carries topology info")
    void buildOpenRegionRequestCarriesTopology() {
        LoadBalancer loadBalancer = new LoadBalancer();
        ClusterManager clusterManager = new ClusterManager(loadBalancer);
        GrpcRegionServerCommandClient commandClient = new GrpcRegionServerCommandClient(clusterManager);

        ServerId primary = new ServerId("primary-host", 16020, 1L);
        ServerId replica = new ServerId("replica-host", 16021, 2L);
        clusterManager.registerServer(primary);
        clusterManager.registerServer(replica);

        Region region = new Region("orders_r1", "orders", new byte[]{0x01}, new byte[]{0x7F});
        region.setPrimary(primary);
        region.addReplica(primary);
        region.addReplica(replica);

        RegionServerProto.OpenRegionRequest request = commandClient.buildOpenRegionRequest(primary, region, false);
        CommonProto.RegionInfo regionInfo = request.getRegion();

        assertFalse(request.getAsReplica());
        assertEquals(region.getRegionId(), regionInfo.getRegionId());
        assertEquals(region.getTableName(), regionInfo.getTableName());
        assertArrayEquals(region.getStartKey(), regionInfo.getStartKey().toByteArray());
        assertArrayEquals(region.getEndKey(), regionInfo.getEndKey().toByteArray());
        assertEquals(primary.getHost(), regionInfo.getPrimary().getHost());
        assertEquals(primary.getPort(), regionInfo.getPrimary().getPort());
        assertEquals(2, regionInfo.getReplicasCount());
        assertEquals(primary.getHost(), regionInfo.getReplicas(0).getHost());
        assertEquals(replica.getHost(), regionInfo.getReplicas(1).getHost());
    }

    @Test
    @DisplayName("control request builders preserve command arguments")
    void buildControlRequestsPreserveArguments() {
        LoadBalancer loadBalancer = new LoadBalancer();
        ClusterManager clusterManager = new ClusterManager(loadBalancer);
        GrpcRegionServerCommandClient commandClient = new GrpcRegionServerCommandClient(clusterManager);

        ServerId target = new ServerId("target-host", 17000, 3L);
        byte[] splitKey = new byte[]{0x10, 0x20};

        assertEquals("orders_r1", commandClient.buildCloseRegionRequest("orders_r1", true, false).getRegionId());
        assertTrue(commandClient.buildCloseRegionRequest("orders_r1", true, false).getAbort());
        assertFalse(commandClient.buildCloseRegionRequest("orders_r1", true, false).getDropTable());

        RegionServerProto.PromoteRequest promoteRequest = commandClient.buildPromoteRequest("orders_r1", 99L);
        assertEquals("orders_r1", promoteRequest.getRegionId());
        assertEquals(99L, promoteRequest.getFencingToken());

        assertEquals("orders_r1",
            commandClient.buildGetReplicationLagRequest("orders_r1").getRegionId());
        assertEquals("orders_r1",
            commandClient.buildGetSplitKeyRequest("orders_r1").getRegionId());

        RegionServerProto.SplitRegionRequest splitRequest = commandClient.buildSplitRegionRequest("orders_r1", splitKey, null, null);
        assertEquals("orders_r1", splitRequest.getRegionId());
        assertEquals(ByteString.copyFrom(splitKey), splitRequest.getSplitKey());

        RegionServerProto.MergeRegionRequest mergeRequest =
            commandClient.buildMergeRegionRequest("orders_r1_left", "orders_r1_right", null);
        assertEquals("orders_r1_left", mergeRequest.getLeftRegionId());
        assertEquals("orders_r1_right", mergeRequest.getRightRegionId());

        RegionServerProto.MigrateRequest migrateRequest =
            commandClient.buildStartMigrationRequest("orders_r1", target);
        assertEquals("orders_r1", migrateRequest.getRegionId());
        assertEquals(target.getHost(), migrateRequest.getTargetServer().getHost());
        assertEquals(target.getPort(), migrateRequest.getTargetServer().getPort());

        RegionServerProto.FinalizeMigrationRequest finalizeRequest =
            commandClient.buildFinalizeMigrationRequest("orders_r1", target, 1234L);
        assertEquals("orders_r1", finalizeRequest.getRegionId());
        assertEquals(target.getHost(), finalizeRequest.getTargetServer().getHost());
        assertEquals(target.getPort(), finalizeRequest.getTargetServer().getPort());
        assertEquals(1234L, finalizeRequest.getFromSequenceId());

        assertEquals("orders_r1", commandClient.buildAbortMigrationRequest("orders_r1").getRegionId());
    }
}
