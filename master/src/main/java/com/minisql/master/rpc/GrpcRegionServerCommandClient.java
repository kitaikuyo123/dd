package com.minisql.master.rpc;

import com.google.protobuf.ByteString;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.common.proto.RegionServerServiceGrpc;
import com.minisql.common.rpc.GrpcChannelFactory;
import com.minisql.master.state.ClusterManager;
import io.grpc.ManagedChannel;

import java.util.concurrent.TimeUnit;

/**
 * Centralizes master-to-regionserver gRPC command wiring so managers no longer
 * duplicate request assembly and channel handling.
 */
public class GrpcRegionServerCommandClient implements RegionServerCommandClient {

    private final ClusterManager clusterManager;

    public GrpcRegionServerCommandClient(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public RegionServerProto.OpenRegionResponse openRegion(ServerId serverId, Region region, boolean asReplica) {
        try (CommandSession session = openSession(serverId, 30000)) {
            return session.stub.openRegion(buildOpenRegionRequest(serverId, region, asReplica));
        }
    }

    @Override
    public RegionServerProto.CloseRegionResponse closeRegion(ServerId serverId, String regionId, boolean abort,
                                                             boolean dropTable) {
        try (CommandSession session = openSession(serverId, 30000)) {
            return session.stub.closeRegion(buildCloseRegionRequest(regionId, abort, dropTable));
        }
    }

    @Override
    public RegionServerProto.PromoteResponse promoteToPrimary(ServerId serverId, String regionId, long fencingToken) {
        try (CommandSession session = openSession(serverId, 10000)) {
            return session.stub.promoteToPrimary(buildPromoteRequest(regionId, fencingToken));
        }
    }

    @Override
    public RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId serverId, String regionId,
                                                                         long timeoutMs) {
        try (CommandSession session = openSession(serverId, timeoutMs)) {
            return session.stub.getReplicationLag(buildGetReplicationLagRequest(regionId));
        }
    }

    @Override
    public RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId serverId, String regionId) {
        try (CommandSession session = openSession(serverId, 10000)) {
            return session.stub.getSplitKey(buildGetSplitKeyRequest(regionId));
        }
    }

    @Override
    public RegionServerProto.SplitRegionResponse splitRegion(ServerId serverId, String regionId, byte[] splitKey) {
        try (CommandSession session = openSession(serverId, 30000)) {
            return session.stub.splitRegion(buildSplitRegionRequest(regionId, splitKey));
        }
    }

    @Override
    public RegionServerProto.MergeRegionResponse mergeRegion(ServerId serverId, String leftRegionId,
                                                             String rightRegionId) {
        try (CommandSession session = openSession(serverId, 30000)) {
            return session.stub.mergeRegion(buildMergeRegionRequest(leftRegionId, rightRegionId));
        }
    }

    @Override
    public RegionServerProto.MigrateResponse startMigration(ServerId serverId, String regionId, ServerId targetServer,
                                                            long timeoutMs) {
        try (CommandSession session = openSession(serverId, timeoutMs)) {
            return session.stub.startMigration(buildStartMigrationRequest(regionId, targetServer));
        }
    }

    @Override
    public RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId serverId, String regionId,
                                                                         ServerId targetServer, long fromSequenceId) {
        try (CommandSession session = openSession(serverId, 30000)) {
            return session.stub.finalizeMigration(
                buildFinalizeMigrationRequest(regionId, targetServer, fromSequenceId));
        }
    }

    @Override
    public RegionServerProto.AbortMigrationResponse abortMigration(ServerId serverId, String regionId) {
        try (CommandSession session = openSession(serverId, 10000)) {
            return session.stub.abortMigration(buildAbortMigrationRequest(regionId));
        }
    }

    public RegionServerProto.OpenRegionRequest buildOpenRegionRequest(ServerId targetServer, Region region, boolean asReplica) {
        return RegionServerProto.OpenRegionRequest.newBuilder()
            .setRegion(buildRegionInfo(targetServer, region))
            .setAsReplica(asReplica)
            .build();
    }

    public RegionServerProto.CloseRegionRequest buildCloseRegionRequest(String regionId, boolean abort, boolean dropTable) {
        return RegionServerProto.CloseRegionRequest.newBuilder()
            .setRegionId(regionId)
            .setAbort(abort)
            .setDropTable(dropTable)
            .build();
    }

    public RegionServerProto.PromoteRequest buildPromoteRequest(String regionId, long fencingToken) {
        return RegionServerProto.PromoteRequest.newBuilder()
            .setRegionId(regionId)
            .setFencingToken(fencingToken)
            .build();
    }

    public RegionServerProto.GetReplicationLagRequest buildGetReplicationLagRequest(String regionId) {
        return RegionServerProto.GetReplicationLagRequest.newBuilder()
            .setRegionId(regionId)
            .build();
    }

    public RegionServerProto.GetSplitKeyRequest buildGetSplitKeyRequest(String regionId) {
        return RegionServerProto.GetSplitKeyRequest.newBuilder()
            .setRegionId(regionId)
            .build();
    }

    public RegionServerProto.SplitRegionRequest buildSplitRegionRequest(String regionId, byte[] splitKey) {
        return RegionServerProto.SplitRegionRequest.newBuilder()
            .setRegionId(regionId)
            .setSplitKey(ByteString.copyFrom(splitKey))
            .build();
    }

    public RegionServerProto.MergeRegionRequest buildMergeRegionRequest(String leftRegionId, String rightRegionId) {
        return RegionServerProto.MergeRegionRequest.newBuilder()
            .setLeftRegionId(leftRegionId)
            .setRightRegionId(rightRegionId)
            .build();
    }

    public RegionServerProto.MigrateRequest buildStartMigrationRequest(String regionId, ServerId targetServer) {
        return RegionServerProto.MigrateRequest.newBuilder()
            .setRegionId(regionId)
            .setTargetServer(toProtoServerId(targetServer))
            .build();
    }

    public RegionServerProto.FinalizeMigrationRequest buildFinalizeMigrationRequest(String regionId, ServerId targetServer,
                                                                                    long fromSequenceId) {
        return RegionServerProto.FinalizeMigrationRequest.newBuilder()
            .setRegionId(regionId)
            .setTargetServer(toProtoServerId(targetServer))
            .setFromSequenceId(fromSequenceId)
            .build();
    }

    public RegionServerProto.AbortMigrationRequest buildAbortMigrationRequest(String regionId) {
        return RegionServerProto.AbortMigrationRequest.newBuilder()
            .setRegionId(regionId)
            .build();
    }

    public CommonProto.RegionInfo buildRegionInfo(ServerId targetServer, Region region) {
        CommonProto.RegionInfo.Builder builder = CommonProto.RegionInfo.newBuilder()
            .setRegionId(region.getRegionId())
            .setTableName(region.getTableName());
        if (region.getStartKey() != null) {
            builder.setStartKey(ByteString.copyFrom(region.getStartKey()));
        }
        if (region.getEndKey() != null) {
            builder.setEndKey(ByteString.copyFrom(region.getEndKey()));
        }
        if (region.getPrimary() != null) {
            builder.setPrimary(toProtoServerId(region.getPrimary()));
        }
        if (region.getReplicas() != null) {
            for (ServerId replica : region.getReplicas()) {
                builder.addReplicas(toProtoServerId(replica));
            }
        }

        return builder.build();
    }

    private CommonProto.ServerId toProtoServerId(ServerId serverId) {
        return CommonProto.ServerId.newBuilder()
            .setHost(serverId.getHost())
            .setPort(serverId.getPort())
            .build();
    }

    /**
     * Open a short-lived session backed by a cached channel.
     * The channel is reused across commands to avoid the overhead of
     * TCP + HTTP/2 negotiation per command.
     */
    private CommandSession openSession(ServerId serverId, long timeoutMs) {
        ManagedChannel channel = GrpcChannelFactory.forAddress(serverId.getHost(), serverId.getPort());
        RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
            RegionServerServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
        return new CommandSession(stub);
    }

    private static final class CommandSession implements AutoCloseable {
        private final RegionServerServiceGrpc.RegionServerServiceBlockingStub stub;

        private CommandSession(RegionServerServiceGrpc.RegionServerServiceBlockingStub stub) {
            this.stub = stub;
        }

        @Override
        public void close() {
            // Channel is pooled — do NOT shut down here.
        }
    }
}
