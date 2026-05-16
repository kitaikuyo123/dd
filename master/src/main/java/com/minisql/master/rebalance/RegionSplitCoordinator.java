package com.minisql.master.rebalance;

import com.minisql.common.Constants;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.*;
import com.minisql.common.utils.BytesUtil;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.recover.RecoveryCoordinator;
import com.minisql.master.rpc.GrpcRegionServerCommandClient;
import com.minisql.master.rpc.RegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.zookeeper.DistributedLock;
import com.minisql.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Region split coordinator. Monitors Region sizes, triggers splits, and
 * delegates to {@link RebalanceSupport} for shared replica-topology and
 * runtime-cleanup logic.
 */
public class RegionSplitCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(RegionSplitCoordinator.class);

    private static final long DEFAULT_TABLE_SPLIT_COOLDOWN_MS = TimeUnit.MINUTES.toMillis(2);

    private volatile long splitThresholdSize = Constants.DEFAULT_SPLIT_THRESHOLD;
    private volatile long tableSplitCooldownMs = DEFAULT_TABLE_SPLIT_COOLDOWN_MS;

    private final BlockingQueue<SplitTask> splitQueue = new LinkedBlockingQueue<>();
    private final Set<String> splittingRegions = ConcurrentHashMap.newKeySet();
    private final Set<String> queuedRegions = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> tableSplitCooldownUntilMs = new ConcurrentHashMap<>();

    private final ExecutorService splitExecutor;
    private final RebalanceSupport support;
    private final RegionServerCommandClient commandClient;
    private RegionMergeCoordinator mergeCoordinator;
    private volatile boolean running = false;

    public RegionSplitCoordinator(ClusterManager clusterManager,
                                  MetadataManager metadataManager,
                                  LoadBalancer loadBalancer) {
        this(clusterManager, metadataManager, loadBalancer, new GrpcRegionServerCommandClient(clusterManager));
    }

    public RegionSplitCoordinator(ClusterManager clusterManager,
                                  MetadataManager metadataManager,
                                  LoadBalancer loadBalancer,
                                  RegionServerCommandClient commandClient) {
        this.support = new RebalanceSupport(clusterManager, metadataManager, loadBalancer);
        this.commandClient = commandClient;

        this.splitExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "RegionSplit-Worker");
            t.setDaemon(true);
            return t;
        });
    }

    // --- Setters for shared dependencies ---

    public void setMonitoringService(MonitoringService monitoringService) {
        this.support.monitoringService = monitoringService;
    }

    public void setZkClient(ZkClient zkClient) {
        this.support.zkClient = zkClient;
    }

    public void setMergeCoordinator(RegionMergeCoordinator mergeCoordinator) {
        this.mergeCoordinator = mergeCoordinator;
    }

    public void setRecoveryCoordinator(RecoveryCoordinator recoveryCoordinator) {
        this.support.recoveryCoordinator = recoveryCoordinator;
    }

    public void setReplicationCoordinator(ReplicationCoordinator replicationCoordinator) {
        this.support.replicationCoordinator = replicationCoordinator;
    }

    public void setReplicaMonitor(ReplicaMonitor replicaMonitor) {
        this.support.replicaMonitor = replicaMonitor;
    }

    public void setLifecycleManager(ReplicaLifecycleManager lifecycleManager) {
        this.support.lifecycleManager = lifecycleManager;
    }

    public void setSplitThresholdSize(long splitThresholdSize) {
        this.splitThresholdSize = splitThresholdSize;
    }

    public void setTableSplitCooldownMs(long tableSplitCooldownMs) {
        this.tableSplitCooldownMs = Math.max(0L, tableSplitCooldownMs);
    }

    // --- Lifecycle ---

    public void start() {
        if (running) {
            return;
        }
        running = true;
        splitExecutor.submit(this::processSplitTasks);
        logger.info("RegionSplitCoordinator started");
    }

    public void stop() {
        running = false;
        splitExecutor.shutdown();
        splitQueue.clear();
        queuedRegions.clear();
        tableSplitCooldownUntilMs.clear();
        logger.info("RegionSplitCoordinator stopped");
    }

    // --- Public API ---

    public boolean shouldSplit(ClusterManager.RegionLoad load) {
        long totalSize = load.getStoreFileSize() + load.getMemStoreSize();
        return totalSize >= splitThresholdSize;
    }

    public boolean scheduleSplit(String regionId, String tableName, ServerId serverId, ClusterManager.RegionLoad load) {
        if (!running) {
            logger.warn("RegionSplitCoordinator is not running, reject split schedule for region: {}", regionId);
            return false;
        }

        clearExpiredTableSplitCooldowns();
        if (isTableInSplitCooldown(tableName)) {
            long remainingMs = tableSplitCooldownUntilMs.getOrDefault(tableName, 0L) - System.currentTimeMillis();
            logger.info("Skip split scheduling for region {} in table {} due to table split cooldown (remaining={}ms)",
                regionId, tableName, Math.max(0L, remainingMs));
            return false;
        }

        if (splittingRegions.contains(regionId) || queuedRegions.contains(regionId)) {
            logger.info("Skip split scheduling for region {} (already splitting or queued)", regionId);
            return false;
        }

        if (!queuedRegions.add(regionId)) {
            logger.info("Skip split scheduling for region {} (queue add race detected)", regionId);
            return false;
        }

        SplitTask task = new SplitTask(regionId, tableName, serverId);
        boolean offered = splitQueue.offer(task);
        if (offered) {
            logger.info("Scheduled split for region: {} (size: {})",
                regionId, formatSize(load.getStoreFileSize() + load.getMemStoreSize()));
        } else {
            queuedRegions.remove(regionId);
            logger.warn("Failed to enqueue split task for region: {}", regionId);
        }
        return offered;
    }

    public boolean checkAndSplitRegion(String regionId) {
        Region region = support.metadataManager.getRegion(regionId);
        if (region == null) {
            logger.warn("Region not found: {}", regionId);
            return false;
        }

        if (splittingRegions.contains(regionId)) {
            return false;
        }

        ServerId serverId = support.clusterManager.getPrimaryServerForRegion(regionId);
        if (serverId == null) {
            logger.warn("No server assigned to region: {}", regionId);
            return false;
        }

        ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
        load.setRegionId(regionId);
        load.setStoreFileSize(splitThresholdSize + 1);

        boolean scheduled = scheduleSplit(regionId, region.getTableName(), serverId, load);
        if (scheduled) {
            logger.info("Hotspot split request accepted for region: {}", regionId);
        } else {
            logger.info("Hotspot split request not scheduled for region: {} (already splitting/queued or coordinator not running)",
                regionId);
        }
        return scheduled;
    }

    public Set<String> getSplittingRegions() {
        return new HashSet<>(splittingRegions);
    }

    // --- Internal ---

    private void processSplitTasks() {
        while (running) {
            try {
                SplitTask task = splitQueue.poll(1, TimeUnit.SECONDS);
                if (task != null) {
                    queuedRegions.remove(task.getRegionId());
                    executeSplit(task);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error processing split task: {}", e.getMessage(), e);
            }
        }
    }

    private void executeSplit(SplitTask task) {
        String regionId = task.getRegionId();
        DistributedLock lock = null;

        if (!splittingRegions.add(regionId)) {
            logger.warn("Region {} is already splitting, skip", regionId);
            return;
        }

        try {
            lock = support.acquireRegionLock(regionId);
            logger.info("Starting split for region: {}", regionId);

            byte[] splitKey = getSplitKeyFromServer(task.getServerId(), regionId);
            if (splitKey == null) {
                logger.warn("Failed to get split key for region: {} (server={})", regionId, task.getServerId());
                return;
            }

            logger.info("Split key for region {}: {}", regionId, BytesUtil.bytesToHex(splitKey));

            support.recordEvent("REGION_SPLIT_STARTED", "INFO", regionId, task.getServerId(),
                "Region split started", null);
            String leftRegionId = support.metadataManager.allocateRegionId(task.getTableName());
            String rightRegionId = support.metadataManager.allocateRegionId(task.getTableName());
            SplitResult result = notifyServerSplitRegion(task.getServerId(), regionId, splitKey,
                leftRegionId, rightRegionId);
            if (result == null) {
                logger.warn("Split failed for region: {} (server={})", regionId, task.getServerId());
                return;
            }

            updateMetadataAfterSplit(regionId, task.getServerId(), result);

            ServerId leftServer = task.getServerId();
            ServerId rightServer = selectServerForNewRegion(result.getRightRegion());

            if (rightServer == null) {
                rightServer = leftServer;
            }
            if (!rightServer.equals(leftServer)) {
                migrateRegion(result.getRightRegion().getRegionId(), leftServer, rightServer);
            }

            support.ensureReplicaTopology(result.getLeftRegion().getRegionId());
            support.ensureReplicaTopology(result.getRightRegion().getRegionId());

            logger.info("Split completed for region: {} -> {} (on {}), {} (on {})",
                    regionId, result.getLeftRegion().getRegionId(), leftServer,
                    result.getRightRegion().getRegionId(), rightServer);
            recordTableSplit(task.getTableName());
            support.recordEvent("REGION_SPLIT_COMPLETED", "INFO", regionId, task.getServerId(),
                "Region split completed",
                result.getLeftRegion().getRegionId() + "," + result.getRightRegion().getRegionId());

            if (mergeCoordinator != null) {
                mergeCoordinator.recordRegionSplit(result.getLeftRegion().getRegionId());
                mergeCoordinator.recordRegionSplit(result.getRightRegion().getRegionId());
            }

        } catch (Exception e) {
            logger.error("Error splitting region {}: {}", regionId, e.getMessage(), e);
        } finally {
            support.releaseLock(lock);
            splittingRegions.remove(regionId);
        }
    }

    private byte[] getSplitKeyFromServer(ServerId serverId, String regionId) {
        try {
            RegionServerProto.GetSplitKeyResponse response = commandClient.getSplitKey(serverId, regionId);
            if (response.getStatus().getSuccess()) {
                return response.getSplitKey().toByteArray();
            }
            logger.warn("RegionServer returned failed split key response: region={} server={} message={}",
                regionId, serverId, response.getStatus().getMessage());
        } catch (Exception e) {
            logger.error("Failed to get split key from server: {}", e.getMessage(), e);
        }
        return null;
    }

    private SplitResult notifyServerSplitRegion(ServerId serverId, String regionId, byte[] splitKey,
                                                String leftRegionId, String rightRegionId) {
        try {
            RegionServerProto.SplitRegionResponse response = commandClient.splitRegion(serverId, regionId, splitKey,
                leftRegionId, rightRegionId);
            if (response.getStatus().getSuccess()) {
                return new SplitResult(
                        RebalanceSupport.convertProtoToRegion(response.getLeftRegion()),
                        RebalanceSupport.convertProtoToRegion(response.getRightRegion())
                );
            }
            logger.warn("RegionServer rejected split request: region={} server={} message={}",
                regionId, serverId, response.getStatus().getMessage());
        } catch (Exception e) {
            logger.error("Failed to split region on server: {}", e.getMessage(), e);
        }
        return null;
    }

    private void updateMetadataAfterSplit(String oldRegionId, ServerId primaryServer, SplitResult result) {
        Region oldRegion = support.metadataManager.getRegion(oldRegionId);
        String tableName = oldRegion != null ? oldRegion.getTableName() : null;
        support.cleanupRegionRuntime(oldRegionId, tableName);
        support.metadataManager.removeRegion(oldRegionId);
        support.metadataManager.removeRegion(result.getLeftRegion().getRegionId());
        support.metadataManager.removeRegion(result.getRightRegion().getRegionId());

        registerSplitRegion(result.getLeftRegion(), primaryServer);
        registerSplitRegion(result.getRightRegion(), primaryServer);

        logger.info("ZooKeeper metadata updated for split regions: {} and {}",
            result.getLeftRegion().getRegionId(), result.getRightRegion().getRegionId());
    }

    private void registerSplitRegion(Region region, ServerId primaryServer) {
        support.metadataManager.registerRegionForTable(region, primaryServer);
        if (primaryServer != null) {
            support.clusterManager.assignRegionToServer(region.getRegionId(), primaryServer);
            support.clusterManager.addReplica(region.getRegionId(), primaryServer);
            support.clusterManager.updateRegionState(region.getRegionId(), Region.State.OPEN);
        }
    }

    private ServerId selectServerForNewRegion(Region region) {
        List<ClusterManager.ServerInfo> servers =
                new ArrayList<>(support.clusterManager.getActiveServers());
        return support.loadBalancer.selectServerForRegion(region, servers);
    }

    /**
     * Migrate a newly-split region to the target server.
     * If any step fails mid-way, rolls back to keep the region on the source.
     */
    private void migrateRegion(String regionId, ServerId sourceServer, ServerId targetServer) {
        logger.info("Migrating region {} from {} to {}", regionId, sourceServer, targetServer);

        Region region = support.metadataManager.getRegion(regionId);
        if (region == null) {
            logger.error("Region not found: {}", regionId);
            return;
        }

        // Phase 1: open on target
        boolean targetOpened = false;
        try {
            RegionServerProto.OpenRegionResponse openResp = commandClient.openRegion(targetServer, region, false);
            if (!openResp.getStatus().getSuccess()) {
                logger.warn("Failed to open region {} on target {}, keeping on source: {}",
                    regionId, targetServer, openResp.getStatus().getMessage());
                return;
            }
            targetOpened = true;
        } catch (Exception e) {
            logger.error("Failed to open region {} on target {}, keeping on source: {}",
                regionId, targetServer, e.getMessage());
            return;
        }

        // Phase 2: close on source
        try {
            RegionServerProto.CloseRegionResponse closeResp =
                commandClient.closeRegion(sourceServer, regionId, true, false);
            if (!closeResp.getStatus().getSuccess()) {
                logger.warn("Failed to close region {} on source {} after target opened; " +
                    "region now exists on both servers, manual cleanup may be needed: {}",
                    regionId, sourceServer, closeResp.getStatus().getMessage());
            }
        } catch (Exception e) {
            logger.error("Failed to close region {} on source {} after target opened: {}",
                regionId, sourceServer, e.getMessage());
        }

        // Phase 3: update metadata (always proceed to keep metadata consistent with target)
        region.removeReplica(sourceServer);
        region.setPrimary(targetServer);
        region.addReplica(targetServer);

        support.metadataManager.registerRegionForTable(region, targetServer);
        support.clusterManager.unassignRegion(regionId);
        support.clusterManager.assignRegionToServer(regionId, targetServer);
        support.clusterManager.removeReplica(regionId, sourceServer);
        support.clusterManager.addReplica(regionId, targetServer);
        support.clusterManager.removeRegionLoad(sourceServer, regionId);
        support.clusterManager.updateRegionState(regionId, Region.State.OPEN);

        logger.info("Migration completed for region: {}", regionId);
    }

    // --- Table split cooldown ---

    private void recordTableSplit(String tableName) {
        if (tableName == null || tableName.isBlank() || tableSplitCooldownMs <= 0) {
            return;
        }
        tableSplitCooldownUntilMs.put(tableName, System.currentTimeMillis() + tableSplitCooldownMs);
    }

    private boolean isTableInSplitCooldown(String tableName) {
        if (tableName == null || tableName.isBlank() || tableSplitCooldownMs <= 0) {
            return false;
        }
        return tableSplitCooldownUntilMs.getOrDefault(tableName, 0L) > System.currentTimeMillis();
    }

    private void clearExpiredTableSplitCooldowns() {
        if (tableSplitCooldownUntilMs.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        tableSplitCooldownUntilMs.entrySet().removeIf(entry -> entry.getValue() <= now);
    }

    // --- Utility ---

    private String formatSize(long size) {
        if (size >= 1024 * 1024 * 1024) {
            return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
        } else if (size >= 1024 * 1024) {
            return String.format("%.2f MB", size / (1024.0 * 1024));
        } else if (size >= 1024) {
            return String.format("%.2f KB", size / 1024.0);
        }
        return size + " B";
    }

    // --- Inner classes ---

    private static class SplitTask {
        private final String regionId;
        private final String tableName;
        private final ServerId serverId;

        SplitTask(String regionId, String tableName, ServerId serverId) {
            this.regionId = regionId;
            this.tableName = tableName;
            this.serverId = serverId;
        }

        String getRegionId() { return regionId; }
        String getTableName() { return tableName; }
        ServerId getServerId() { return serverId; }
    }

    private static class SplitResult {
        private final Region leftRegion;
        private final Region rightRegion;

        SplitResult(Region leftRegion, Region rightRegion) {
            this.leftRegion = leftRegion;
            this.rightRegion = rightRegion;
        }

        Region getLeftRegion() { return leftRegion; }
        Region getRightRegion() { return rightRegion; }
    }
}
