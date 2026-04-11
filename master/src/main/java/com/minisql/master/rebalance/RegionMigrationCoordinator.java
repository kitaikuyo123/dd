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
 * Owns the region migration state machine so MasterServiceImpl can stay focused
 * on RPC entrypoints and high-level coordination.
 */
public class RegionMigrationCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(RegionMigrationCoordinator.class);

    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final RegionServerCommandClient commandClient;
    private final ReplicaLifecycleManager lifecycleManager;
    private final Map<String, MigrationStatus> migrationStatuses = new ConcurrentHashMap<>();

    private MonitoringService monitoringService;
    private volatile ZkClient zkClient;

    public RegionMigrationCoordinator(ClusterManager clusterManager,
                                      MetadataManager metadataManager,
                                      RegionServerCommandClient commandClient,
                                      ReplicaLifecycleManager lifecycleManager) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.commandClient = commandClient;
        this.lifecycleManager = lifecycleManager;
    }

    public void setMonitoringService(MonitoringService monitoringService) {
        this.monitoringService = monitoringService;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void execute(LoadBalancer.BalanceAction action) {
        DistributedLock lock = null;
        logger.info("Executing balance action: move {} from {} to {}",
            action.getRegionId(), action.getSource(), action.getTarget());

        MigrationStatus migrationStatus = new MigrationStatus(
            action.getRegionId(), action.getSource(), action.getTarget(), MigrationState.OPENING_TARGET);
        migrationStatuses.put(action.getRegionId(), migrationStatus);

        try {
            lock = acquireRegionLock(action.getRegionId());
            String regionId = action.getRegionId();
            ServerId sourceServer = action.getSource();
            ServerId targetServer = action.getTarget();
            boolean targetOpened = false;
            boolean sourceFinalized = false;
            boolean targetPromoted = false;

            Region region = metadataManager.getRegion(regionId);
            if (region == null) {
                failMigration(migrationStatus, "Region not found");
                return;
            }

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
                rollbackMigration(migrationStatus, targetOpened, sourceFinalized, targetPromoted);
                return;
            }

            boolean syncCompleted = waitForReplicationSync(regionId, targetServer, expectedSequenceId);
            if (!syncCompleted) {
                rollbackMigration(migrationStatus, targetOpened, sourceFinalized, targetPromoted);
                return;
            }

            updateMigrationState(migrationStatus, MigrationState.FINALIZING_SOURCE,
                "Blocking source writes and draining final WAL");
            transition(regionId, sourceServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.FINALIZING,
                "Blocking source writes for migration");
            long finalSequenceId = notifyServerFinalizeMigration(sourceServer, regionId, targetServer, expectedSequenceId);
            if (finalSequenceId < 0) {
                rollbackMigration(migrationStatus, targetOpened, sourceFinalized, targetPromoted);
                return;
            }
            sourceFinalized = true;

            updateMigrationState(migrationStatus, MigrationState.WAITING_FINAL_SYNC, "Waiting for final target catch-up");
            boolean finalSyncCompleted = waitForReplicationSync(regionId, targetServer, finalSequenceId);
            if (!finalSyncCompleted) {
                rollbackMigration(migrationStatus, targetOpened, sourceFinalized, targetPromoted);
                return;
            }

            updateMigrationState(migrationStatus, MigrationState.PROMOTING_TARGET, "Promoting target to primary");
            transition(regionId, targetServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.PROMOTING,
                "Promoting target after catch-up");
            boolean promoted = notifyServerPromoteToPrimary(targetServer, regionId);
            if (!promoted) {
                rollbackMigration(migrationStatus, targetOpened, sourceFinalized, targetPromoted);
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
            clusterManager.unassignRegion(regionId);
            clusterManager.assignRegionToServer(regionId, targetServer);
            region.setPrimary(targetServer);
            region.addReplica(targetServer);
            metadataManager.registerRegionForTable(region, targetServer);
            transition(regionId, targetServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.PRIMARY_READY,
                "Balanced region now primary on target");
            transition(regionId, sourceServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.REMOVED,
                "Source replica closed after balance");

            updateMigrationState(migrationStatus, MigrationState.COMPLETED, "Migration completed");
            logger.info("Migration completed for region: {}", regionId);
        } catch (Exception e) {
            failMigration(migrationStatus, "Failed to execute balance action: " + e.getMessage());
        } finally {
            releaseLock(lock);
        }
    }

    private DistributedLock acquireRegionLock(String regionId) throws Exception {
        if (zkClient == null) {
            return null;
        }
        DistributedLock lock = new DistributedLock(zkClient.getClient(),
            "/minisql/locks/regions/" + regionId);
        lock.acquire();
        return lock;
    }

    private void releaseLock(DistributedLock lock) {
        if (lock == null) {
            return;
        }
        try {
            if (lock.isAcquiredInThisProcess()) {
                lock.release();
            }
        } catch (Exception e) {
            logger.warn("Failed to release migration lock: {}", e.getMessage(), e);
        }
    }

    private long notifyServerStartMigration(ServerId sourceServer, String regionId, ServerId targetServer) {
        try {
            RegionServerProto.MigrateResponse response =
                commandClient.startMigration(sourceServer, regionId, targetServer, TimeUnit.MINUTES.toMillis(1));
            return response.getStatus().getSuccess() ? response.getSourceSequenceId() : -1L;
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
            return response.getStatus().getSuccess() ? response.getSourceSequenceId() : -1L;
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
                    clusterManager.updateReplicaSequenceId(regionId, targetServer, lastAppliedSequenceId);
                    if (lastAppliedSequenceId >= expectedSequenceId) {
                        return true;
                    }
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

    private void rollbackMigration(MigrationStatus status, boolean targetOpened, boolean sourceFinalized,
                                   boolean targetPromoted) {
        updateMigrationState(status, MigrationState.ROLLING_BACK, "Rolling back failed migration");

        if (!targetPromoted) {
            if (sourceFinalized) {
                notifyServerAbortMigration(status.sourceServer, status.regionId);
            }
            if (targetOpened) {
                notifyServerCloseRegionSync(status.targetServer, status.regionId, false);
            }
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
            recordEvent("REGION_MIGRATION_STARTED", "INFO", status.regionId, status.sourceServer,
                status.targetServer, "Region migration started", detail);
        } else if (state == MigrationState.COMPLETED) {
            recordEvent("REGION_MIGRATION_COMPLETED", "INFO", status.regionId, status.sourceServer,
                status.targetServer, "Region migration completed", detail);
        }
    }

    private void failMigration(MigrationStatus status, String detail) {
        updateMigrationState(status, MigrationState.ROLLED_BACK, detail);
        logger.warn("[MIGRATION] region={} failed: {}", status.regionId, detail);
    }

    private void failMigrationRequiresManualIntervention(MigrationStatus status, String detail) {
        updateMigrationState(status, MigrationState.FAILED_REQUIRES_MANUAL_INTERVENTION, detail);
        logger.warn("[MIGRATION] region={} requires manual intervention: {}", status.regionId, detail);
    }

    private boolean notifyServerPromoteToPrimary(ServerId serverId, String regionId) {
        try {
            long fencingToken = clusterManager.getFencingToken(regionId) + 1;
            RegionServerProto.PromoteResponse response =
                commandClient.promoteToPrimary(serverId, regionId, fencingToken);
            if (response.getStatus().getSuccess()) {
                clusterManager.updateFencingToken(regionId, fencingToken);
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

    private void recordEvent(String type, String severity, String regionId, ServerId sourceServer,
                             ServerId targetServer, String message, String details) {
        if (monitoringService != null) {
            monitoringService.recordEvent(type, severity, regionId, null,
                sourceServer == null ? null : sourceServer.getHost() + ":" + sourceServer.getPort(),
                targetServer == null ? null : targetServer.getHost() + ":" + targetServer.getPort(),
                message, details);
        }
    }

    private enum MigrationState {
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

    private static final class MigrationStatus {
        private final String regionId;
        private final ServerId sourceServer;
        private final ServerId targetServer;

        private MigrationStatus(String regionId, ServerId sourceServer, ServerId targetServer, MigrationState state) {
            this.regionId = regionId;
            this.sourceServer = sourceServer;
            this.targetServer = targetServer;
        }
    }
}
