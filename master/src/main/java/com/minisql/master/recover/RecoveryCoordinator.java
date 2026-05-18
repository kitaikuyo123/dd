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
import com.minisql.master.RegionTopologyProvider;
import com.minisql.replication.ReplicaGroup;
import com.minisql.replication.ReplicationCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 副本恢复协调器
 *
 * 负责新副本引导（Bootstrap）和故障副本恢复（Recovery）的全流程编排。
 * 恢复流程确保副本在元数据和复制状态完全同步后才重新加入服务路径。
 *
 * 核心流程（performRecovery）:
 *   1. 注册副本到集群元数据
 *   2. 在目标 RegionServer 上打开 Region
 *   3. 执行复制追赶（全量同步或增量同步）
 *   4. 标记副本为就绪状态
 *   5. 检查并修剪多余副本至目标副本数
 *
 * 支持异步调度（scheduleRecovery）和同步执行（executeRecovery）两种模式。
 * 当 RegionServer 恢复上线时，通过 reconcileRecoveredServer 进行全量对账。
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
    private final ConcurrentHashMap<String, Object> regionLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> desiredReplicaCounts = new ConcurrentHashMap<>();
    private MonitoringService monitoringService;

    public RecoveryCoordinator(ClusterManager clusterManager,
                               MetadataManager metadataManager,
                               ReplicaMonitor replicaMonitor,
                               ReplicationCoordinator replicationCoordinator,
                               ReplicaLifecycleManager lifecycleManager) {
        this(clusterManager, metadataManager, replicaMonitor, replicationCoordinator, lifecycleManager,
            new GrpcRegionServerCommandClient(clusterManager), 2);
    }

    public RecoveryCoordinator(ClusterManager clusterManager,
                               MetadataManager metadataManager,
                               ReplicaMonitor replicaMonitor,
                               ReplicationCoordinator replicationCoordinator,
                               ReplicaLifecycleManager lifecycleManager,
                               RegionServerCommandClient commandClient) {
        this(clusterManager, metadataManager, replicaMonitor, replicationCoordinator, lifecycleManager,
            commandClient, 2);
    }

    public RecoveryCoordinator(ClusterManager clusterManager,
                               MetadataManager metadataManager,
                               ReplicaMonitor replicaMonitor,
                               ReplicationCoordinator replicationCoordinator,
                               ReplicaLifecycleManager lifecycleManager,
                               RegionServerCommandClient commandClient,
                               int threadPoolSize) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.replicaMonitor = replicaMonitor;
        this.replicationCoordinator = replicationCoordinator;
        this.lifecycleManager = lifecycleManager;
        this.commandClient = commandClient;
        int poolSize = Math.max(1, threadPoolSize);
        this.recoveryExecutor = Executors.newFixedThreadPool(poolSize, r -> {
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

    public void setDesiredReplicaCount(String regionId, int desiredReplicaCount) {
        if (regionId == null || regionId.isBlank()) {
            return;
        }
        int normalized = Math.max(1, desiredReplicaCount);
        desiredReplicaCounts.put(regionId, normalized);
        logger.info("[RECOVERY-TARGET] region={} desiredReplicaCount={}", regionId, normalized);
    }

    public void clearDesiredReplicaCount(String regionId) {
        if (regionId == null || regionId.isBlank()) {
            return;
        }
        Integer removed = desiredReplicaCounts.remove(regionId);
        if (removed != null) {
            logger.info("[RECOVERY-TARGET] region={} clearedDesiredReplicaCount(previous={})", regionId, removed);
        }
    }

    public void reconcileReplicaTarget(String regionId) {
        if (regionId == null || regionId.isBlank()) {
            return;
        }
        enforceReplicaTarget(regionId);
    }

    /**
     * Clean up per-region resources (locks, desired counts) when a region is
     * permanently removed, to prevent unbounded growth of these maps.
     */
    public void cleanupRegion(String regionId) {
        if (regionId == null || regionId.isBlank()) {
            return;
        }
        regionLocks.remove(regionId);
        desiredReplicaCounts.remove(regionId);
        logger.debug("Cleaned up recovery resources for region {}", regionId);
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
        logger.info("[RECOVERY-RECONCILE] Start reconcile for recovered server {}", recoveredServer);
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

            enforceReplicaTarget(regionId);

            String pairKey = buildTaskKey(regionId, recoveredServer);
            if (reconciledPairs.putIfAbsent(pairKey, Boolean.TRUE) != null) {
                // Already scheduled for this region+server in a previous pass
                continue;
            }

            if (region.getReplicas().contains(recoveredServer)) {
                logger.info("[RECOVERY-RECONCILE] Region {} already contains recovered server {}, schedule recoverReplica",
                    regionId, recoveredServer);
                recoverReplica(regionId, recoveredServer);
                continue;
            }

            int targetReplicationFactor = resolveTargetReplicationFactor(region);

            long activeReplicaCount = region.getReplicas().stream()
                .filter(clusterManager::isServerActive)
                .count();

            logger.info("[RECOVERY-RECONCILE] Region={} table={} primary={} replicas={} activeReplicaCount={} targetReplicationFactor={}",
                region.getRegionId(), region.getTableName(), primary, region.getReplicas(),
                activeReplicaCount, targetReplicationFactor);

            if (activeReplicaCount < targetReplicationFactor) {
                logger.info("[RECOVERY-BOOTSTRAP] Region {} activeReplicaCount {} < target {}, bootstrap on recovered server {}",
                    regionId, activeReplicaCount, targetReplicationFactor, recoveredServer);
                bootstrapReplica(regionId, recoveredServer);
            } else {
                logger.info("[RECOVERY-RECONCILE] Region {} does not need bootstrap (activeReplicaCount {} >= target {})",
                    regionId, activeReplicaCount, targetReplicationFactor);
            }
        }
    }

    private void reconcileRegionAfterPrimaryRecovery(Region region) {
        ensurePrimaryRegionOpen(region);
        waitForPrimaryReady(region);

        int targetReplicationFactor = resolveTargetReplicationFactor(region);

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
            enforceReplicaTarget(region.getRegionId());
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

        enforceReplicaTarget(region.getRegionId());
    }

    private void ensurePrimaryRegionOpen(Region region) {
        ServerId primary = region.getPrimary();
        if (primary == null || !clusterManager.isServerActive(primary)) {
            return;
        }

        clusterManager.assignRegionToServer(region.getRegionId(), primary);
        metadataManager.registerRegionForTable(region, primary);

        IOException lastException = null;
        for (int attempt = 1; attempt <= 5; attempt++) {
            try {
                if (commandClient.openRegion(primary, region, false).getStatus().getSuccess()) {
                    return;
                }
                lastException = new IOException("openRegion returned failure status");
            } catch (Exception e) {
                lastException = new IOException("openRegion call failed: " + e.getMessage(), e);
            }
            if (attempt < 5) {
                try {
                    Thread.sleep(200L * attempt);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        throw new IllegalStateException("Failed to reopen primary region on " + primary +
            " for region " + region.getRegionId(), lastException);
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
        logger.info("[RECOVERY-BOOTSTRAP] Begin performRecovery region={} replica={} initializing={}",
            regionId, replica, initializing);
        recordEvent("RECOVERY_STARTED", "INFO", regionId, replica,
            initializing ? "Replica bootstrap started" : "Replica recovery started", null);
        lifecycleManager.transition(regionId, replica,
            initializing ? ReplicaLifecycleManager.ReplicaLifecycleState.BOOTSTRAPPING
                : ReplicaLifecycleManager.ReplicaLifecycleState.REBUILDING,
            initializing ? "Starting replica bootstrap" : "Starting replica recovery");
        // 先打开 region，再写入拓扑 —— 避免 failover 选到未就绪的节点
        openReplicaRegion(regionId, replica);
        ensureReplicaRegistered(regionId, replica, initializing);
        ensureReplicationCatchUp(regionId, replica);
        markReplicaReady(regionId, replica);
        recordEvent("RECOVERY_COMPLETED", "INFO", regionId, replica,
            initializing ? "Replica bootstrap completed" : "Replica recovery completed", null);
        enforceReplicaTarget(regionId);
        logger.info("[RECOVERY-BOOTSTRAP] Completed performRecovery region={} replica={} initializing={}",
            regionId, replica, initializing);
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
        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            logger.warn("[RECOVERY-BOOTSTRAP] Region {} not found in metadata, cannot register replica", regionId);
            return;
        }
        region.addReplica(replica);
        metadataManager.registerRegionForTable(region, region.getPrimary());
        clusterManager.addReplica(regionId, replica);
        logger.info("[RECOVERY-BOOTSTRAP] ensureReplicaRegistered region={} replica={} initializing={} replicasAfterRegister={}",
            regionId, replica, initializing, region.getReplicas());

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
        ReplicaGroup group = ensureReplicaGroup(regionId);

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

    private ReplicaGroup ensureReplicaGroup(String regionId) {
        ReplicaGroup existing = replicationCoordinator.getReplicaGroup(regionId);
        if (existing != null) {
            return existing;
        }

        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            throw new IllegalStateException("Replica group not found and region metadata missing: " + regionId);
        }

        List<ServerId> orderedReplicas = new ArrayList<>();
        if (region.getPrimary() != null) {
            orderedReplicas.add(region.getPrimary());
        }
        for (ServerId replica : region.getReplicas()) {
            if (replica != null && !orderedReplicas.contains(replica)) {
                orderedReplicas.add(replica);
            }
        }

        if (orderedReplicas.isEmpty()) {
            throw new IllegalStateException("Replica group not found and no replicas in metadata for region " + regionId);
        }

        replicationCoordinator.createReplicaGroup(region, orderedReplicas,
                new RegionTopologyProvider(metadataManager, regionId));
        logger.warn("[RECOVERY-RECONCILE] Rebuilt missing replica group for region {} from metadata replicas={}",
            regionId, orderedReplicas);

        ReplicaGroup rebuilt = replicationCoordinator.getReplicaGroup(regionId);
        if (rebuilt == null) {
            throw new IllegalStateException("Failed to rebuild replica group for region " + regionId);
        }
        return rebuilt;
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

        if (region != null) {
            logger.info("[RECOVERY-BOOTSTRAP] markReplicaReady region={} replica={} replicasNow={}",
                regionId, replica, region.getReplicas());
        }

        lifecycleManager.transition(regionId, replica,
            ReplicaLifecycleManager.ReplicaLifecycleState.SECONDARY_READY,
            "Replica ready for serving");
    }

    private void enforceReplicaTarget(String regionId) {
        Object regionLock = regionLocks.computeIfAbsent(regionId, ignored -> new Object());
        synchronized (regionLock) {
            Region region = metadataManager.getRegion(regionId);
            if (region == null || region.getPrimary() == null) {
                // Region no longer exists — clean up the lock to prevent unbounded growth
                regionLocks.remove(regionId);
                return;
            }

            int targetReplicationFactor = resolveTargetReplicationFactor(region);
            targetReplicationFactor = Math.max(1, targetReplicationFactor);

            List<ServerId> currentReplicas = new ArrayList<>(region.getReplicas());
            if (currentReplicas.size() <= targetReplicationFactor) {
                return;
            }

            ServerId primary = region.getPrimary();
            List<ServerId> secondaries = new ArrayList<>();
            for (ServerId replica : currentReplicas) {
                if (replica != null && !replica.equals(primary)) {
                    secondaries.add(replica);
                }
            }

            secondaries.sort(Comparator
                .comparing((ServerId sid) -> !clusterManager.isServerActive(sid))
                .thenComparing(ServerId::getHost)
                .thenComparingInt(ServerId::getPort));

            int keepSecondaryCount = Math.max(0, targetReplicationFactor - 1);
            Set<ServerId> keepReplicas = new HashSet<>();
            keepReplicas.add(primary);
            for (int i = 0; i < Math.min(keepSecondaryCount, secondaries.size()); i++) {
                keepReplicas.add(secondaries.get(i));
            }

            List<ServerId> removed = new ArrayList<>();
            for (ServerId replica : currentReplicas) {
                if (replica == null || replica.equals(primary) || keepReplicas.contains(replica)) {
                    continue;
                }
                removed.add(replica);
            }

            if (removed.isEmpty()) {
                return;
            }

            for (ServerId replica : removed) {
                try {
                    if (!commandClient.closeRegion(replica, regionId, false, false).getStatus().getSuccess()) {
                        logger.warn("Trim closeRegion returned failure for region {} on {} - keeping replica in metadata",
                            regionId, replica);
                        continue;
                    }
                } catch (Exception e) {
                    logger.warn("Failed to close trimmed replica region {} on {} - keeping replica in metadata: {}",
                        regionId, replica, e.getMessage());
                    continue;
                }
                region.removeReplica(replica);
                clusterManager.removeReplica(regionId, replica);
                clusterManager.removeRegionLoad(replica, regionId);
                replicaMonitor.removeReplica(regionId, replica);
                lifecycleManager.transition(regionId, replica,
                    ReplicaLifecycleManager.ReplicaLifecycleState.REMOVED,
                    "Trimmed excess secondary replica to match target replica count");
                recordEvent("REPLICA_TRIMMED", "INFO", regionId, replica,
                    "Trimmed excess secondary replica",
                    "targetReplicationFactor=" + targetReplicationFactor);
            }

            metadataManager.registerRegionForTable(region, primary);
            logger.info("[RECOVERY-TRIM] region={} target={} removed={} replicasNow={}",
                regionId, targetReplicationFactor, removed, region.getReplicas());
        }
    }

    private int resolveTargetReplicationFactor(Region region) {
        int tableReplicationFactor = 3;
        com.minisql.common.model.Table table = metadataManager.getTable(region.getTableName());
        if (table != null && table.getProperties() != null) {
            tableReplicationFactor = table.getProperties().getReplicationFactor();
        }
        tableReplicationFactor = Math.max(1, tableReplicationFactor);

        int desiredReplicaCount = desiredReplicaCounts.getOrDefault(region.getRegionId(), tableReplicationFactor);
        desiredReplicaCount = Math.max(1, desiredReplicaCount);
        return Math.max(tableReplicationFactor, desiredReplicaCount);
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
