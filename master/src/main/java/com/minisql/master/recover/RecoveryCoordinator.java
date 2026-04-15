package com.minisql.master.recover;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.rpc.GrpcRegionServerCommandClient;
import com.minisql.master.rpc.RegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.replication.ReplicaGroup;
import com.minisql.replication.ReplicationCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Coordinates replica bootstrap and recovery catch-up so recovered or newly
 * added replicas rejoin the serving path only after metadata and replication
 * state are refreshed together.
 */
public class RecoveryCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryCoordinator.class);
    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final ReplicaMonitor replicaMonitor;
    private final ReplicationCoordinator replicationCoordinator;
    private final ReplicaLifecycleManager lifecycleManager;
    private final RegionServerCommandClient commandClient;
    private final ExecutorService recoveryExecutor;
    private final ConcurrentHashMap<String, Boolean> inFlightRecoveries = new ConcurrentHashMap<>();
    private MonitoringService monitoringService;

    public RecoveryCoordinator(ClusterManager clusterManager,
                               MetadataManager metadataManager,
                               ReplicaMonitor replicaMonitor,
                               ReplicationCoordinator replicationCoordinator,
                               ReplicaLifecycleManager lifecycleManager) {
        this(clusterManager, metadataManager, replicaMonitor, replicationCoordinator, lifecycleManager,
            new GrpcRegionServerCommandClient(clusterManager));
    }

    public RecoveryCoordinator(ClusterManager clusterManager,
                               MetadataManager metadataManager,
                               ReplicaMonitor replicaMonitor,
                               ReplicationCoordinator replicationCoordinator,
                               ReplicaLifecycleManager lifecycleManager,
                               RegionServerCommandClient commandClient) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.replicaMonitor = replicaMonitor;
        this.replicationCoordinator = replicationCoordinator;
        this.lifecycleManager = lifecycleManager;
        this.commandClient = commandClient;
        this.recoveryExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "Replica-Recovery");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        replicaMonitor.registerCallback(new ReplicaMonitor.FailoverCallback() {
            @Override
            public void onReplicaFailed(String regionId, ServerId failedReplica) {
                // no-op
            }

            @Override
            public void onReplicaLagging(String regionId, ServerId laggingReplica, long lagMs) {
                // no-op
            }

            @Override
            public void onReplicaRecovered(String regionId, ServerId recoveredReplica) {
                recoverReplica(regionId, recoveredReplica);
            }
        });
    }

    public void stop() {
        recoveryExecutor.shutdownNow();
    }

    public void setMonitoringService(MonitoringService monitoringService) {
        this.monitoringService = monitoringService;
    }

    public void bootstrapReplica(String regionId, ServerId replica) {
        scheduleRecovery(regionId, replica, true);
    }

    public void bootstrapReplicaSync(String regionId, ServerId replica) {
        executeRecovery(regionId, replica, true);
    }

    public void recoverReplica(String regionId, ServerId replica) {
        scheduleRecovery(regionId, replica, false);
    }

    /**
     * Tracks which (region, server) pairs have already been scheduled for recovery
     * in the current reconciliation pass, preventing duplicate scheduling when
     * multiple RegionServers register concurrently.
     */
    private final ConcurrentHashMap<String, Boolean> reconciledPairs = new ConcurrentHashMap<>();

    public void reconcileRecoveredServer(ServerId recoveredServer) {
        for (Region region : metadataManager.getAllRegions()) {
            if (region == null) {
                continue;
            }

            String regionId = region.getRegionId();
            ServerId primary = region.getPrimary();
            if (primary == null) {
                continue;
            }

            if (recoveredServer.equals(primary)) {
                reconcileRegionAfterPrimaryRecovery(region);
                continue;
            }

            if (!isPrimaryReady(region)) {
                continue;
            }

            String pairKey = buildTaskKey(regionId, recoveredServer);
            if (reconciledPairs.putIfAbsent(pairKey, Boolean.TRUE) != null) {
                // Already scheduled for this region+server in a previous pass
                continue;
            }

            if (region.getReplicas().contains(recoveredServer)) {
                recoverReplica(regionId, recoveredServer);
                continue;
            }

            int targetReplicationFactor = 3;
            com.minisql.common.model.Table table = metadataManager.getTable(region.getTableName());
            if (table != null && table.getProperties() != null) {
                targetReplicationFactor = table.getProperties().getReplicationFactor();
            }

            long activeReplicaCount = region.getReplicas().stream()
                .filter(clusterManager::isServerActive)
                .count();

            if (activeReplicaCount < targetReplicationFactor) {
                bootstrapReplica(regionId, recoveredServer);
            }
        }
    }

    private void reconcileRegionAfterPrimaryRecovery(Region region) {
        ensurePrimaryRegionOpen(region);
        waitForPrimaryReady(region);

        int targetReplicationFactor = 3;
        com.minisql.common.model.Table table = metadataManager.getTable(region.getTableName());
        if (table != null && table.getProperties() != null) {
            targetReplicationFactor = table.getProperties().getReplicationFactor();
        }

        List<ServerId> activeServers = new ArrayList<>();
        for (ClusterManager.ServerInfo serverInfo : clusterManager.getActiveServersList()) {
            if (serverInfo != null && serverInfo.getServerId() != null) {
                activeServers.add(serverInfo.getServerId());
            }
        }

        String regionId = region.getRegionId();
        for (ServerId replica : new ArrayList<>(region.getReplicas())) {
            if (replica.equals(region.getPrimary())) {
                continue;
            }
            if (clusterManager.isServerActive(replica)) {
                String pairKey = buildTaskKey(regionId, replica);
                if (reconciledPairs.putIfAbsent(pairKey, Boolean.TRUE) != null) {
                    continue;
                }
                recoverReplica(regionId, replica);
            }
        }

        long activeReplicaCount = region.getReplicas().stream()
            .filter(clusterManager::isServerActive)
            .count();

        if (activeReplicaCount >= targetReplicationFactor) {
            return;
        }

        for (ServerId candidate : activeServers) {
            if (candidate.equals(region.getPrimary()) || region.getReplicas().contains(candidate)) {
                continue;
            }
            String pairKey = buildTaskKey(regionId, candidate);
            if (reconciledPairs.putIfAbsent(pairKey, Boolean.TRUE) != null) {
                continue;
            }
            bootstrapReplica(regionId, candidate);
            activeReplicaCount++;
            if (activeReplicaCount >= targetReplicationFactor) {
                break;
            }
        }
    }

    private void ensurePrimaryRegionOpen(Region region) {
        ServerId primary = region.getPrimary();
        if (primary == null || !clusterManager.isServerActive(primary)) {
            return;
        }

        clusterManager.assignRegionToServer(region.getRegionId(), primary);
        if (!commandClient.openRegion(primary, region, false).getStatus().getSuccess()) {
            throw new IllegalStateException("Failed to reopen primary region on " + primary +
                " for region " + region.getRegionId());
        }
    }

    private boolean isPrimaryReady(Region region) {
        ServerId primary = region.getPrimary();
        if (primary == null || !clusterManager.isServerActive(primary)) {
            return false;
        }
        try {
            return commandClient.getReplicationLag(primary, region.getRegionId(), 5000)
                .getStatus()
                .getSuccess();
        } catch (Exception e) {
            return false;
        }
    }

    private void waitForPrimaryReady(Region region) {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            if (isPrimaryReady(region)) {
                return;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for primary readiness for region " +
                    region.getRegionId(), e);
            }
        }
        throw new IllegalStateException("Primary replica did not become ready for region " + region.getRegionId() +
            " on " + region.getPrimary());
    }

    private void scheduleRecovery(String regionId, ServerId replica, boolean initializing) {
        String taskKey = buildTaskKey(regionId, replica);
        if (inFlightRecoveries.putIfAbsent(taskKey, Boolean.TRUE) != null) {
            return;
        }

        recoveryExecutor.submit(() -> {
            try {
                performRecovery(regionId, replica, initializing);
            } catch (Exception e) {
                recordEvent("RECOVERY_FAILED", "ERROR", regionId, replica, "Replica recovery failed", e.getMessage());
                lifecycleManager.transition(regionId, replica,
                    ReplicaLifecycleManager.ReplicaLifecycleState.FAILED,
                    "Replica recovery failed: " + e.getMessage());
                logger.error("Replica recovery failed for region {} on {}: {}", regionId, replica, e.getMessage());
            } finally {
                inFlightRecoveries.remove(taskKey);
                reconciledPairs.remove(taskKey);
            }
        });
    }

    private void executeRecovery(String regionId, ServerId replica, boolean initializing) {
        String taskKey = buildTaskKey(regionId, replica);
        if (inFlightRecoveries.putIfAbsent(taskKey, Boolean.TRUE) != null) {
            waitForReplicaReady(regionId, replica);
            return;
        }

        try {
            performRecovery(regionId, replica, initializing);
        } finally {
            inFlightRecoveries.remove(taskKey);
        }
    }

    private void performRecovery(String regionId, ServerId replica, boolean initializing) {
        recordEvent("RECOVERY_STARTED", "INFO", regionId, replica,
            initializing ? "Replica bootstrap started" : "Replica recovery started", null);
        lifecycleManager.transition(regionId, replica,
            initializing ? ReplicaLifecycleManager.ReplicaLifecycleState.BOOTSTRAPPING
                : ReplicaLifecycleManager.ReplicaLifecycleState.REBUILDING,
            initializing ? "Starting replica bootstrap" : "Starting replica recovery");
        ensureReplicaRegistered(regionId, replica, initializing);
        openReplicaRegion(regionId, replica);
        ensureReplicationCatchUp(regionId, replica);
        markReplicaReady(regionId, replica);
        recordEvent("RECOVERY_COMPLETED", "INFO", regionId, replica,
            initializing ? "Replica bootstrap completed" : "Replica recovery completed", null);
    }

    private void waitForReplicaReady(String regionId, ServerId replica) {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            ReplicaLifecycleManager.ReplicaLifecycleStatus status = lifecycleManager.getStatus(regionId, replica);
            if (status != null) {
                if (status.getState() == ReplicaLifecycleManager.ReplicaLifecycleState.SECONDARY_READY
                    || status.getState() == ReplicaLifecycleManager.ReplicaLifecycleState.PRIMARY_READY) {
                    return;
                }
                if (status.getState() == ReplicaLifecycleManager.ReplicaLifecycleState.FAILED) {
                    throw new IllegalStateException("Replica bootstrap failed for " + regionId + " on " + replica +
                        ": " + status.getDetail());
                }
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for replica bootstrap", e);
            }
        }

        throw new IllegalStateException("Timed out waiting for replica bootstrap: " + regionId + " on " + replica);
    }

    private void ensureReplicaRegistered(String regionId, ServerId replica, boolean initializing) {
        clusterManager.addReplica(regionId, replica);

        Region region = metadataManager.getRegion(regionId);
        if (region != null) {
            region.addReplica(replica);
            metadataManager.registerRegionForTable(region, region.getPrimary());
        }

        boolean exists = false;
        for (ReplicaInfo info : replicaMonitor.getReplicas(regionId)) {
            if (info.getServerId().equals(replica)) {
                exists = true;
                info.setServerId(replica);
                info.setState(initializing ? ReplicaInfo.ReplicaState.INITIALIZING : info.getState());
                info.heartbeat();
                break;
            }
        }
        if (!exists) {
            ReplicaInfo replicaInfo = new ReplicaInfo();
            replicaInfo.setRegionId(regionId);
            replicaInfo.setServerId(replica);
            replicaInfo.setState(initializing ? ReplicaInfo.ReplicaState.INITIALIZING : ReplicaInfo.ReplicaState.SECONDARY);
            replicaMonitor.registerReplica(regionId, replicaInfo);
        }
    }

    private void ensureReplicationCatchUp(String regionId, ServerId replica) {
        lifecycleManager.transition(regionId, replica,
            ReplicaLifecycleManager.ReplicaLifecycleState.CATCHING_UP,
            "Waiting for replica catch-up");
        ReplicaGroup group = replicationCoordinator.getReplicaGroup(regionId);
        if (group == null) {
            throw new IllegalStateException("Replica group not found for region " + regionId);
        }

        List<ServerId> replicas = group.getReplicas();
        if (!replicas.contains(replica)) {
            if (!replicationCoordinator.addReplicaSync(regionId, replica, 60000)) {
                throw new IllegalStateException("Full sync failed while adding replica " + replica);
            }
            return;
        }

        if (!replicationCoordinator.resyncReplicaSync(regionId, replica, 60000)) {
            throw new IllegalStateException("Full sync failed while resyncing replica " + replica);
        }
    }

    private void markReplicaReady(String regionId, ServerId replica) {
        for (ReplicaInfo info : replicaMonitor.getReplicas(regionId)) {
            if (info.getServerId().equals(replica) && !info.isPrimary()) {
                info.setServerId(replica);
                info.setState(ReplicaInfo.ReplicaState.SECONDARY);
                info.setReplicationLag(0);
                info.heartbeat();
                break;
            }
        }

        Region region = metadataManager.getRegion(regionId);
        if (region != null && !region.getReplicas().contains(replica)) {
            region.addReplica(replica);
            metadataManager.registerRegionForTable(region, region.getPrimary());
        }

        lifecycleManager.transition(regionId, replica,
            ReplicaLifecycleManager.ReplicaLifecycleState.SECONDARY_READY,
            "Replica ready for serving");
    }

    private String buildTaskKey(String regionId, ServerId replica) {
        return regionId + "|" + replica.getHost() + ":" + replica.getPort();
    }

    private void openReplicaRegion(String regionId, ServerId replica) {
        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            throw new IllegalStateException("Region metadata not found: " + regionId);
        }

        if (!commandClient.openRegion(replica, region, true).getStatus().getSuccess()) {
            throw new IllegalStateException("Failed to open replica region on " + replica);
        }
    }

    private void recordEvent(String type, String severity, String regionId, ServerId replica,
                             String message, String details) {
        if (monitoringService != null) {
            monitoringService.recordEvent(type, severity, regionId, null,
                replica == null ? null : replica.getHost() + ":" + replica.getPort(),
                null, message, details);
        }
    }

}
