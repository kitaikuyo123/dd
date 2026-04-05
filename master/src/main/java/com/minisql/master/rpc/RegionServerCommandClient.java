package com.minisql.master.rpc;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.RegionServerProto;

public interface RegionServerCommandClient {

    RegionServerProto.OpenRegionResponse openRegion(ServerId serverId, Region region, boolean asReplica);

    RegionServerProto.CloseRegionResponse closeRegion(ServerId serverId, String regionId, boolean abort, boolean dropTable);

    RegionServerProto.PromoteResponse promoteToPrimary(ServerId serverId, String regionId, long fencingToken);

    RegionServerProto.GetReplicationLagResponse getReplicationLag(ServerId serverId, String regionId, long timeoutMs);

    RegionServerProto.GetSplitKeyResponse getSplitKey(ServerId serverId, String regionId);

    RegionServerProto.SplitRegionResponse splitRegion(ServerId serverId, String regionId, byte[] splitKey);

    RegionServerProto.MergeRegionResponse mergeRegion(ServerId serverId, String leftRegionId, String rightRegionId);

    RegionServerProto.MigrateResponse startMigration(ServerId serverId, String regionId, ServerId targetServer, long timeoutMs);

    RegionServerProto.FinalizeMigrationResponse finalizeMigration(ServerId serverId, String regionId, ServerId targetServer,
                                                                 long fromSequenceId);

    RegionServerProto.AbortMigrationResponse abortMigration(ServerId serverId, String regionId);
}
