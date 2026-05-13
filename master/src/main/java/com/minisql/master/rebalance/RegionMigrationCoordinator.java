package com.minisql.master.rebalance;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.rpc.RegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.zookeeper.DistributedLock;
import com.minisql.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Region 迁移协调器
 *
 * 管理负载均衡中 Region 在 RegionServer 间迁移的完整状态机，
 * 使 MasterServiceImpl 只需关注 RPC 入口和高层协调。
 *
 * 迁移状态机:
 *   OPENING_TARGET -> INITIAL_SYNC -> FINALIZING_SOURCE -> WAITING_FINAL_SYNC
 *   -> PROMOTING_TARGET -> CLOSING_SOURCE -> COMMITTING_METADATA -> COMPLETED
 *
 * 关键步骤:
 *   1. 在目标服务器打开 Region 副本
 *   2. 初始数据同步（通过复制通道追赶）
 *   3. 暂停源服务器写入，排空最后的 WAL
 *   4. 等待目标服务器完成最终同步
 *   5. 提升目标服务器为主副本
 *   6. 关闭源服务器上的 Region
 *   7. 提交元数据切换
 *
 * 失败时自动回滚，但目标提升后的失败需要人工干预。
 */
public class RegionMigrationCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(RegionMigrationCoordinator.class);

    private final RebalanceSupport support;
    private final RegionServerCommandClient commandClient;
    private final ReplicaLifecycleManager lifecycleManager;
    private final Map<String, MigrationStatus> migrationStatuses = new ConcurrentHashMap<>();

    public RegionMigrationCoordinator(ClusterManager clusterManager,
                                      MetadataManager metadataManager,
                                      LoadBalancer loadBalancer,
                                      RegionServerCommandClient commandClient,
                                      ReplicaLifecycleManager lifecycleManager) {
        this.support = new RebalanceSupport(clusterManager, metadataManager, loadBalancer);
        this.commandClient = commandClient;
        this.lifecycleManager = lifecycleManager;
    }

    public void setMonitoringService(MonitoringService monitoringService) {
        this.support.monitoringService = monitoringService;
    }

    public void setZkClient(ZkClient zkClient) {
        this.support.zkClient = zkClient;
    }

    public void execute(LoadBalancer.BalanceAction action) {
        DistributedLock lock = null;
        logger.info("Executing balance action: move {} from {} to {}",
            action.getRegionId(), action.getSource(), action.getTarget());

        MigrationStatus migrationStatus = new MigrationStatus(
            action.getRegionId(), action.getSource(), action.getTarget());
        migrationStatuses.put(action.getRegionId(), migrationStatus);

        try {
            lock = support.acquireRegionLock(action.getRegionId());
            String regionId = action.getRegionId();
            ServerId sourceServer = action.getSource();
            ServerId targetServer = action.getTarget();
            boolean targetOpened = false;
            boolean targetAlreadyHosted = false;
            boolean sourceFinalized = false;
            boolean targetPromoted = false;

            Region region = support.metadataManager.getRegion(regionId);
            if (region == null) {
                failMigration(migrationStatus, "Region not found");
                return;
            }
            targetAlreadyHosted = region.getReplicas() != null && region.getReplicas().contains(targetServer);

            updateMigrationState(migrationStatus, MigrationState.OPENING_TARGET, "Opening target region");
            transition(regionId, targetServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.BOOTSTRAPPING,
                "Opening target region for balance");
            targetOpened = notifyServerOpenRegionSync(targetServer, region);
            if (!targetOpened) {
                failMigration(migrationStatus, "Failed to open target region for migration");
                transition(regionId, targetServer,
                    ReplicaLifecycleManager.ReplicaLifecycleState.FAILED,
                    "Failed to open target region");
                return;
            }

            updateMigrationState(migrationStatus, MigrationState.INITIAL_SYNC, "Starting initial catch-up");
            transition(regionId, targetServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.CATCHING_UP,
                "Starting initial catch-up");
            long expectedSequenceId = notifyServerStartMigration(sourceServer, regionId, targetServer);
            if (expectedSequenceId < 0) {
                rollbackMigration(migrationStatus, targetOpened && !targetAlreadyHosted, sourceFinalized, targetPromoted);
                return;
            }

            boolean syncCompleted = waitForReplicationSync(regionId, targetServer, expectedSequenceId);
            if (!syncCompleted) {
                rollbackMigration(migrationStatus, targetOpened && !targetAlreadyHosted, sourceFinalized, targetPromoted);
                return;
            }

            updateMigrationState(migrationStatus, MigrationState.FINALIZING_SOURCE,
                "Blocking source writes and draining final WAL");
            transition(regionId, sourceServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.FINALIZING,
                "Blocking source writes for migration");
            long finalSequenceId = notifyServerFinalizeMigration(sourceServer, regionId, targetServer, expectedSequenceId);
            if (finalSequenceId < 0) {
                rollbackMigration(migrationStatus, targetOpened && !targetAlreadyHosted, sourceFinalized, targetPromoted);
                return;
            }
            sourceFinalized = true;

            updateMigrationState(migrationStatus, MigrationState.WAITING_FINAL_SYNC, "Waiting for final target catch-up");
            boolean finalSyncCompleted = waitForReplicationSync(regionId, targetServer, finalSequenceId);
            if (!finalSyncCompleted) {
                rollbackMigration(migrationStatus, targetOpened && !targetAlreadyHosted, sourceFinalized, targetPromoted);
                return;
            }

            updateMigrationState(migrationStatus, MigrationState.PROMOTING_TARGET, "Promoting target to primary");
            transition(regionId, targetServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.PROMOTING,
                "Promoting target after catch-up");
            boolean promoted = notifyServerPromoteToPrimary(targetServer, regionId);
            if (!promoted) {
                rollbackMigration(migrationStatus, targetOpened && !targetAlreadyHosted, sourceFinalized, targetPromoted);
                return;
            }
            targetPromoted = true;

            updateMigrationState(migrationStatus, MigrationState.CLOSING_SOURCE, "Closing source region");
            boolean closed = notifyServerCloseRegionSync(sourceServer, regionId, false);
            if (!closed) {
                failMigrationRequiresManualIntervention(
                    migrationStatus,
                    "Source close failed after target promotion; source remains write-blocked");
                return;
            }

            updateMigrationState(migrationStatus, MigrationState.COMMITTING_METADATA, "Committing metadata switch");
            support.clusterManager.unassignRegion(regionId);
            support.clusterManager.assignRegionToServer(regionId, targetServer);
            region.setPrimary(targetServer);
            region.addReplica(targetServer);
            region.removeReplica(sourceServer);
            support.clusterManager.addReplica(regionId, targetServer);
            support.clusterManager.removeReplica(regionId, sourceServer);
            support.clusterManager.removeRegionLoad(sourceServer, regionId);
            support.metadataManager.registerRegionForTable(region, targetServer);
            transition(regionId, targetServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.PRIMARY_READY,
                "Balanced region now primary on target");
            transition(regionId, sourceServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.REMOVED,
                "Source replica closed after balance");

            support.syncReplicaMonitor(regionId, sourceServer);

            migrationStatus.setState(MigrationState.COMPLETED);
            updateMigrationState(migrationStatus, MigrationState.COMPLETED, "Migration completed");
            logger.info("Migration completed for region: {}", regionId);
        } catch (Exception e) {
            failMigration(migrationStatus, "Failed to execute balance action: " + e.getMessage());
        } finally {
            support.releaseLock(lock);
        }
    }

    /**
     * Query the current migration state for a region, or null if no migration is in progress.
     */
    public MigrationState getMigrationState(String regionId) {
        MigrationStatus status = migrationStatuses.get(regionId);
        return status != null ? status.getState() : null;
    }

    /**
     * 获取当前正在进行中的迁移数量（非终态）
     */
    public int getOngoingMigrationCount() {
        int count = 0;
        for (MigrationStatus status : migrationStatuses.values()) {
            MigrationState state = status.getState();
            if (state != null
                && state != MigrationState.COMPLETED
                && state != MigrationState.ROLLED_BACK
                && state != MigrationState.FAILED_REQUIRES_MANUAL_INTERVENTION) {
                count++;
            }
        }
        return count;
    }

    private long notifyServerStartMigration(ServerId sourceServer, String regionId, ServerId targetServer) {
        try {
            RegionServerProto.MigrateResponse response =
                commandClient.startMigration(sourceServer, regionId, targetServer, TimeUnit.MINUTES.toMillis(1));
            if (response.getStatus().getSuccess()) {
                return response.getSourceSequenceId();
            }
            logger.warn("startMigration rejected for region {} from {} to {}: {}",
                regionId, sourceServer, targetServer, response.getStatus().getMessage());
            return -1L;
        } catch (Exception e) {
            logger.error("Failed to notify server start migration: {}", e.getMessage(), e);
            return -1L;
        }
    }

    private long notifyServerFinalizeMigration(ServerId sourceServer, String regionId, ServerId targetServer,
                                               long fromSequenceId) {
        try {
            RegionServerProto.FinalizeMigrationResponse response =
                commandClient.finalizeMigration(sourceServer, regionId, targetServer, fromSequenceId);
            if (response.getStatus().getSuccess()) {
                return response.getSourceSequenceId();
            }
            logger.warn("finalizeMigration rejected for region {} from {} to {}: {}",
                regionId, sourceServer, targetServer, response.getStatus().getMessage());
            return -1L;
        } catch (Exception e) {
            logger.error("Failed to finalize migration on source server: {}", e.getMessage(), e);
            return -1L;
        }
    }

    private boolean notifyServerAbortMigration(ServerId sourceServer, String regionId) {
        try {
            return commandClient.abortMigration(sourceServer, regionId).getStatus().getSuccess();
        } catch (Exception e) {
            logger.error("Failed to abort migration on source server: {}", e.getMessage(), e);
            return false;
        }
    }

    private boolean waitForReplicationSync(String regionId, ServerId targetServer, long expectedSequenceId) {
        long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);

        while (System.currentTimeMillis() < deadlineMs) {
            try {
                RegionServerProto.GetReplicationLagResponse response =
                    commandClient.getReplicationLag(targetServer, regionId, TimeUnit.SECONDS.toMillis(5));
                if (response.getStatus().getSuccess()) {
                    long lastAppliedSequenceId = response.getLastAppliedSequenceId();
                    support.clusterManager.updateReplicaSequenceId(regionId, targetServer, lastAppliedSequenceId);
                    if (lastAppliedSequenceId >= expectedSequenceId) {
                        return true;
                    }
                } else {
                    logger.warn("Replication lag query failed for region {} on target {}: {}",
                        regionId, targetServer, response.getStatus().getMessage());
                }

                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                logger.warn("Failed to verify replication sync for region {} on target {}: {}",
                    regionId, targetServer, e.getMessage(), e);
            }
        }

        return false;
    }

    private void rollbackMigration(MigrationStatus status, boolean targetOpenedByMigration, boolean sourceFinalized,
                                   boolean targetPromoted) {
        updateMigrationState(status, MigrationState.ROLLING_BACK, "Rolling back failed migration");

        if (!targetPromoted) {
            if (sourceFinalized) {
                notifyServerAbortMigration(status.sourceServer, status.regionId);
            }
            if (targetOpenedByMigration) {
                notifyServerCloseRegionSync(status.targetServer, status.regionId, false);
            }
            status.setState(MigrationState.ROLLED_BACK);
            updateMigrationState(status, MigrationState.ROLLED_BACK, "Rollback completed");
            return;
        }

        failMigrationRequiresManualIntervention(
            status,
            "Rollback is unsafe after target promotion; manual intervention required");
    }

    private void updateMigrationState(MigrationStatus status, MigrationState state, String detail) {
        logger.info("[MIGRATION] region={} source={} target={} state={}{}",
                status.regionId, status.sourceServer, status.targetServer, state,
                (detail != null && !detail.isEmpty() ? " detail=" + detail : ""));
        if (state == MigrationState.OPENING_TARGET || state == MigrationState.INITIAL_SYNC) {
            support.recordEvent("REGION_MIGRATION_STARTED", "INFO", status.regionId, status.sourceServer,
                status.targetServer, "Region migration started", detail);
        } else if (state == MigrationState.COMPLETED) {
            support.recordEvent("REGION_MIGRATION_COMPLETED", "INFO", status.regionId, status.sourceServer,
                status.targetServer, "Region migration completed", detail);
        }
    }

    private void failMigration(MigrationStatus status, String detail) {
        status.setState(MigrationState.ROLLED_BACK);
        updateMigrationState(status, MigrationState.ROLLED_BACK, detail);
        logger.warn("[MIGRATION] region={} failed: {}", status.regionId, detail);
    }

    private void failMigrationRequiresManualIntervention(MigrationStatus status, String detail) {
        status.setState(MigrationState.FAILED_REQUIRES_MANUAL_INTERVENTION);
        updateMigrationState(status, MigrationState.FAILED_REQUIRES_MANUAL_INTERVENTION, detail);
        logger.warn("[MIGRATION] region={} requires manual intervention: {}", status.regionId, detail);
    }

    private boolean notifyServerPromoteToPrimary(ServerId serverId, String regionId) {
        try {
            long fencingToken = support.clusterManager.getFencingToken(regionId) + 1;
            RegionServerProto.PromoteResponse response =
                commandClient.promoteToPrimary(serverId, regionId, fencingToken);
            if (response.getStatus().getSuccess()) {
                support.clusterManager.updateFencingToken(regionId, fencingToken);
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.error("Failed to notify server promote to primary: {}", e.getMessage(), e);
            return false;
        }
    }

    private boolean notifyServerOpenRegionSync(ServerId serverId, Region region) {
        logger.info("Synchronously notifying {} to open region {}", serverId, region.getRegionId());

        try {
            RegionServerProto.OpenRegionResponse response = commandClient.openRegion(serverId, region, false);
            if (response.getStatus().getSuccess()) {
                logger.info("Region {} opened successfully on {}", region.getRegionId(), serverId);
                return true;
            }
            logger.warn("Failed to open region {}: {}", region.getRegionId(), response.getStatus().getMessage());
            return false;
        } catch (Exception e) {
            logger.error("Failed to notify server open region: {}", e.getMessage(), e);
            return false;
        }
    }

    private boolean notifyServerCloseRegionSync(ServerId serverId, String regionId, boolean dropTable) {
        logger.info("Synchronously notifying {} to close region {}{}",
            serverId, regionId, (dropTable ? " and drop table" : ""));

        try {
            RegionServerProto.CloseRegionResponse response =
                commandClient.closeRegion(serverId, regionId, false, dropTable);
            if (response.getStatus().getSuccess()) {
                logger.info("Region {} closed successfully on {}{}",
                    regionId, serverId, (dropTable ? " and table dropped" : ""));
                return true;
            }
            logger.warn("Failed to close region {}: {}", regionId, response.getStatus().getMessage());
            return false;
        } catch (Exception e) {
            logger.error("Failed to synchronously close region {} on {}: {}", regionId, serverId, e.getMessage(), e);
            return false;
        }
    }

    private void transition(String regionId, ServerId serverId,
                            ReplicaLifecycleManager.ReplicaLifecycleState state, String detail) {
        lifecycleManager.transition(regionId, serverId, state, detail);
    }

    // --- Enums and inner classes ---

    enum MigrationState {
        OPENING_TARGET,
        INITIAL_SYNC,
        FINALIZING_SOURCE,
        WAITING_FINAL_SYNC,
        PROMOTING_TARGET,
        CLOSING_SOURCE,
        COMMITTING_METADATA,
        COMPLETED,
        ROLLING_BACK,
        ROLLED_BACK,
        FAILED_REQUIRES_MANUAL_INTERVENTION
    }

    static final class MigrationStatus {
        private final String regionId;
        private final ServerId sourceServer;
        private final ServerId targetServer;
        private volatile MigrationState state;

        MigrationStatus(String regionId, ServerId sourceServer, ServerId targetServer) {
            this.regionId = regionId;
            this.sourceServer = sourceServer;
            this.targetServer = targetServer;
            this.state = MigrationState.OPENING_TARGET;
        }

        MigrationState getState() { return state; }
        void setState(MigrationState state) { this.state = state; }
    }
}
