package com.minisql.master.rebalance;

import com.minisql.common.Constants;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.utils.BytesUtil;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.recover.RecoveryCoordinator;
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
 * Region merge coordinator. Monitors Region sizes, triggers merges to
 * prevent fragmentation, and delegates shared logic to {@link RebalanceSupport}.
 */
public class RegionMergeCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(RegionMergeCoordinator.class);

    private static final long DEFAULT_MERGE_COOLDOWN_MS = 60 * 60 * 1000;

    private volatile long mergeThresholdSize = Constants.DEFAULT_MERGE_THRESHOLD;
    private volatile long maxMergeSize = Constants.DEFAULT_MERGE_MAX_SIZE;
    private volatile long minMergeSize = Constants.DEFAULT_MERGE_MIN_SIZE;
    private volatile long mergeCooldownMs = DEFAULT_MERGE_COOLDOWN_MS;

    private final BlockingQueue<MergeTask> mergeQueue = new LinkedBlockingQueue<>();
    private final Set<String> mergingRegions = ConcurrentHashMap.newKeySet();
    private final Set<String> queuedMergingRegions = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> recentSplitRegions = new ConcurrentHashMap<>();

    private final ExecutorService mergeExecutor;
    private final RebalanceSupport support;
    private final RegionServerCommandClient commandClient;

    private volatile RegionMigrationCoordinator migrationCoordinator;

    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    public RegionMergeCoordinator(ClusterManager clusterManager,
                                  MetadataManager metadataManager,
                                  LoadBalancer loadBalancer,
                                  RegionServerCommandClient commandClient) {
        this.support = new RebalanceSupport(clusterManager, metadataManager, loadBalancer);
        this.commandClient = commandClient;

        this.mergeExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "RegionMerge-Worker");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Backward-compatible constructor that creates a no-op command client.
     * Prefer the 4-argument constructor.
     */
    public RegionMergeCoordinator(ClusterManager clusterManager,
                                  MetadataManager metadataManager,
                                  LoadBalancer loadBalancer) {
        this(clusterManager, metadataManager, loadBalancer, null);
    }

    /**
     * Legacy 2-argument constructor kept for existing callers.
     * The caller must call setters for commandClient before merge operations are used.
     */
    public RegionMergeCoordinator(ClusterManager clusterManager,
                                  MetadataManager metadataManager) {
        this(clusterManager, metadataManager, null, null);
    }

    // --- Setters ---

    public void setMonitoringService(MonitoringService monitoringService) {
        this.support.monitoringService = monitoringService;
    }

    public void setZkClient(ZkClient zkClient) {
        this.support.zkClient = zkClient;
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

    public void setCommandClient(RegionServerCommandClient commandClient) {
        // For legacy callers that used the 2-arg constructor
        try {
            java.lang.reflect.Field field = RegionMergeCoordinator.class.getDeclaredField("commandClient");
            field.setAccessible(true);
            field.set(this, commandClient);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set commandClient", e);
        }
    }

    public void setMergeThresholdSize(long mergeThresholdSize) {
        this.mergeThresholdSize = mergeThresholdSize;
    }

    public void setMaxMergeSize(long maxMergeSize) {
        this.maxMergeSize = maxMergeSize;
    }

    public void setMigrationCoordinator(RegionMigrationCoordinator migrationCoordinator) {
        this.migrationCoordinator = migrationCoordinator;
    }

    public void setMinMergeSize(long minMergeSize) {
        this.minMergeSize = minMergeSize;
    }

    public void setMergeCooldownMs(long mergeCooldownMs) {
        this.mergeCooldownMs = mergeCooldownMs;
    }

    // --- Lifecycle ---

    public void start() {
        if (running) {
            return;
        }
        running = true;

        scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "RegionMerge-Scheduler");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleWithFixedDelay(this::checkAndScheduleMerges, 60, 60, TimeUnit.SECONDS);
        mergeExecutor.submit(this::processMergeTasks);

        logger.info("RegionMergeCoordinator started");
    }

    public void stop() {
        running = false;
        mergeQueue.clear();
        queuedMergingRegions.clear();
        mergingRegions.clear();

        if (scheduler != null) {
            scheduler.shutdown();
        }
        mergeExecutor.shutdown();

        logger.info("RegionMergeCoordinator stopped");
    }

    // --- Public API ---

    public void recordRegionSplit(String regionId) {
        recentSplitRegions.put(regionId, System.currentTimeMillis());
        cleanupRecentSplits();
    }

    /**
     * Trigger a synchronous merge check (for tests; production uses the scheduler).
     */
    public void triggerCheckNow() {
        checkAndScheduleMerges();
    }

    /**
     * 强制合并指定表的相邻 region（绕过大小检查和冷却期）。
     */
    public boolean forceMerge(String tableName) {
        List<Region> regions = new ArrayList<>(support.metadataManager.getRegionsForTable(tableName));
        if (regions.size() < 2) {
            logger.info("Table {} has fewer than 2 regions, nothing to merge", tableName);
            return false;
        }
        regions.sort(Comparator.comparing(Region::getStartKey, BytesUtil::compareTo));
        for (int i = 0; i < regions.size() - 1; i++) {
            Region left = regions.get(i);
            Region right = regions.get(i + 1);
            if (mergingRegions.contains(left.getRegionId()) || mergingRegions.contains(right.getRegionId())
                || queuedMergingRegions.contains(left.getRegionId()) || queuedMergingRegions.contains(right.getRegionId())) {
                continue;
            }

            // 确保两个 region 在同一台 RS 上，否则迁到一起
            ServerId leftServer = support.clusterManager.getPrimaryServerForRegion(left.getRegionId());
            ServerId rightServer = support.clusterManager.getPrimaryServerForRegion(right.getRegionId());
            if (leftServer != null && rightServer != null && !leftServer.equals(rightServer)) {
                logger.info("Force merge: migrating {} from {} to {} before merge",
                    right.getRegionId(), rightServer, leftServer);
                try {
                    if (migrationCoordinator != null) {
                        migrationCoordinator.execute(new LoadBalancer.BalanceAction(
                            right.getRegionId(), rightServer, leftServer));
                    } else {
                        logger.warn("MigrationCoordinator not set, cannot colocate regions for merge");
                        return false;
                    }
                } catch (Exception e) {
                    logger.error("Failed to migrate {} to {}: {}", right.getRegionId(), leftServer, e.getMessage());
                    return false;
                }
            }

            boolean scheduled = scheduleMerge(left, right);
            logger.info("Force merge {} for {} + {}: {}", scheduled ? "scheduled" : "failed",
                left.getRegionId(), right.getRegionId(), tableName);
            return scheduled;
        }
        logger.info("No adjacent region pair available for merge in table: {}", tableName);
        return false;
    }

    // --- Internal ---

    private void cleanupRecentSplits() {
        long now = System.currentTimeMillis();
        recentSplitRegions.entrySet().removeIf(entry ->
            now - entry.getValue() > mergeCooldownMs);
    }

    private void checkAndScheduleMerges() {
        try {
            Collection<String> tableNames = getAllTableNames();

            for (String tableName : tableNames) {
                List<Region> regions = new ArrayList<>(support.metadataManager.getRegionsForTable(tableName));
                regions.sort(Comparator.comparing(Region::getStartKey, BytesUtil::compareTo));

                for (int i = 0; i < regions.size() - 1; i++) {
                    Region left = regions.get(i);
                    Region right = regions.get(i + 1);

                    if (shouldMerge(left, right) && !scheduleMerge(left, right)) {
                        logger.info("Skip merge scheduling for regions {} and {} due to queued overlap",
                            left.getRegionId(), right.getRegionId());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error checking merges: {}", e.getMessage(), e);
        }
    }

    private boolean shouldMerge(Region left, Region right) {
        if (!isAdjacent(left, right)) {
            return false;
        }

        if (mergingRegions.contains(left.getRegionId()) ||
            mergingRegions.contains(right.getRegionId()) ||
            queuedMergingRegions.contains(left.getRegionId()) ||
            queuedMergingRegions.contains(right.getRegionId())) {
            return false;
        }

        if (isInCooldown(left.getRegionId()) || isInCooldown(right.getRegionId())) {
            return false;
        }

        ServerId leftServer = support.clusterManager.getPrimaryServerForRegion(left.getRegionId());
        ServerId rightServer = support.clusterManager.getPrimaryServerForRegion(right.getRegionId());
        if (leftServer == null || rightServer == null || !leftServer.equals(rightServer)) {
            return false;
        }

        long leftSize = getRegionSize(left.getRegionId());
        long rightSize = getRegionSize(right.getRegionId());
        long totalSize = leftSize + rightSize;

        if (totalSize > maxMergeSize) {
            return false;
        }

        if (leftSize < mergeThresholdSize && rightSize < mergeThresholdSize) {
            return true;
        }

        if (leftSize < minMergeSize || rightSize < minMergeSize) {
            return true;
        }

        return false;
    }

    private boolean isInCooldown(String regionId) {
        Long splitTime = recentSplitRegions.get(regionId);
        if (splitTime == null) {
            return false;
        }
        return System.currentTimeMillis() - splitTime < mergeCooldownMs;
    }

    private boolean isAdjacent(Region left, Region right) {
        return Arrays.equals(left.getEndKey(), right.getStartKey());
    }

    private long getRegionSize(String regionId) {
        ServerId serverId = support.clusterManager.getPrimaryServerForRegion(regionId);
        if (serverId == null) {
            return 0;
        }

        ClusterManager.ServerInfo serverInfo = getServerInfo(serverId);
        if (serverInfo == null) {
            return 0;
        }

        ClusterManager.RegionLoad load = serverInfo.getRegionLoads().get(regionId);
        if (load == null) {
            return 0;
        }

        return load.getStoreFileSize() + load.getMemStoreSize();
    }

    private ClusterManager.ServerInfo getServerInfo(ServerId serverId) {
        for (ClusterManager.ServerInfo info : support.clusterManager.getActiveServers()) {
            if (info.getServerId().equals(serverId)) {
                return info;
            }
        }
        return null;
    }

    private boolean scheduleMerge(Region left, Region right) {
        String leftId = left.getRegionId();
        String rightId = right.getRegionId();
        if (!queuedMergingRegions.add(leftId)) {
            return false;
        }
        if (!queuedMergingRegions.add(rightId)) {
            queuedMergingRegions.remove(leftId);
            return false;
        }

        MergeTask task = new MergeTask(
            leftId,
            rightId,
            left.getTableName(),
            support.clusterManager.getPrimaryServerForRegion(leftId)
        );

        if (mergeQueue.offer(task)) {
            logger.info("Scheduled merge for regions: {} and {} (table: {})",
                leftId, rightId, left.getTableName());
            return true;
        }
        queuedMergingRegions.remove(leftId);
        queuedMergingRegions.remove(rightId);
        return false;
    }

    private void processMergeTasks() {
        while (running) {
            try {
                MergeTask task = mergeQueue.poll(1, TimeUnit.SECONDS);
                if (task != null) {
                    executeMerge(task);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error processing merge task: {}", e.getMessage(), e);
            }
        }
    }

    private void executeMerge(MergeTask task) {
        String leftRegionId = task.getLeftRegionId();
        String rightRegionId = task.getRightRegionId();
        DistributedLock leftLock = null;
        DistributedLock rightLock = null;

        if (!mergingRegions.add(leftRegionId) || !mergingRegions.add(rightRegionId)) {
            logger.warn("Regions {} or {} are already merging, skip", leftRegionId, rightRegionId);
            releaseQueuedReservation(task);
            mergingRegions.remove(leftRegionId);
            mergingRegions.remove(rightRegionId);
            return;
        }
        releaseQueuedReservation(task);

        try {
            String firstLockRegion = leftRegionId.compareTo(rightRegionId) <= 0 ? leftRegionId : rightRegionId;
            String secondLockRegion = leftRegionId.compareTo(rightRegionId) <= 0 ? rightRegionId : leftRegionId;
            leftLock = support.acquireRegionLock(firstLockRegion);
            rightLock = support.acquireRegionLock(secondLockRegion);
            MergeTask validatedTask = revalidateMergeTask(task);
            if (validatedTask == null) {
                logger.info("Skip stale merge task for regions {} and {}", leftRegionId, rightRegionId);
                return;
            }
            logger.info("Starting merge for regions: {} and {}", leftRegionId, rightRegionId);
            support.recordEvent("REGION_MERGE_STARTED", "INFO", leftRegionId, validatedTask.getServerId(),
                "Region merge started", rightRegionId);

            String mergedRegionId = support.metadataManager.allocateRegionId(validatedTask.getTableName());
            MergeResult result = notifyServerMergeRegions(
                validatedTask.getServerId(), leftRegionId, rightRegionId, mergedRegionId);

            if (result == null) {
                logger.warn("Merge failed for regions: {} and {}", leftRegionId, rightRegionId);
                return;
            }

            updateMetadataAfterMerge(leftRegionId, rightRegionId, result, validatedTask.getServerId());

            logger.info("Merge completed: {} + {} -> {}", leftRegionId, rightRegionId,
                result.getMergedRegion().getRegionId());
            support.recordEvent("REGION_MERGE_COMPLETED", "INFO", leftRegionId, validatedTask.getServerId(),
                "Region merge completed", result.getMergedRegion().getRegionId());

        } catch (Exception e) {
            logger.error("Error merging regions {} and {}: {}", leftRegionId, rightRegionId, e.getMessage(), e);
        } finally {
            support.releaseLock(rightLock);
            support.releaseLock(leftLock);
            mergingRegions.remove(leftRegionId);
            mergingRegions.remove(rightRegionId);
        }
    }

    private MergeResult notifyServerMergeRegions(ServerId serverId, String leftRegionId, String rightRegionId,
                                                 String mergedRegionId) {
        if (commandClient == null) {
            logger.error("commandClient not set, cannot merge regions");
            return null;
        }
        try {
            RegionServerProto.MergeRegionResponse response =
                commandClient.mergeRegion(serverId, leftRegionId, rightRegionId, mergedRegionId);

            if (response.getStatus().getSuccess()) {
                return new MergeResult(RebalanceSupport.convertProtoToRegion(response.getMergedRegion()));
            }
            logger.warn("Merge rejected by server: region={} left={} right={} message={}",
                serverId, leftRegionId, rightRegionId, response.getStatus().getMessage());
        } catch (Exception e) {
            logger.error("Failed to merge regions on server: {}", e.getMessage(), e);
        }
        return null;
    }

    private void updateMetadataAfterMerge(String leftRegionId, String rightRegionId, MergeResult result,
                                          ServerId primaryServer) {
        Region leftRegion = support.metadataManager.getRegion(leftRegionId);
        Region rightRegion = support.metadataManager.getRegion(rightRegionId);
        support.cleanupRegionRuntime(leftRegionId, leftRegion == null ? null : leftRegion.getTableName());
        support.cleanupRegionRuntime(rightRegionId, rightRegion == null ? null : rightRegion.getTableName());
        support.metadataManager.removeRegion(leftRegionId);
        support.metadataManager.removeRegion(rightRegionId);

        Region mergedRegion = result.getMergedRegion();
        support.metadataManager.removeRegion(mergedRegion.getRegionId());
        mergedRegion.setPrimary(primaryServer);
        mergedRegion.setReplicas(new ArrayList<>());
        if (primaryServer != null) {
            mergedRegion.addReplica(primaryServer);
        }
        support.metadataManager.registerRegionForTable(mergedRegion, primaryServer);

        if (primaryServer != null) {
            support.clusterManager.assignRegionToServer(mergedRegion.getRegionId(), primaryServer);
            support.clusterManager.addReplica(mergedRegion.getRegionId(), primaryServer);
            support.clusterManager.updateRegionState(mergedRegion.getRegionId(), Region.State.OPEN);
        }

        support.ensureReplicaTopology(mergedRegion.getRegionId());
        recordRegionSplit(mergedRegion.getRegionId());
    }

    private void releaseQueuedReservation(MergeTask task) {
        if (task == null) {
            return;
        }
        queuedMergingRegions.remove(task.getLeftRegionId());
        queuedMergingRegions.remove(task.getRightRegionId());
    }

    private MergeTask revalidateMergeTask(MergeTask task) {
        Region left = support.metadataManager.getRegion(task.getLeftRegionId());
        Region right = support.metadataManager.getRegion(task.getRightRegionId());
        if (left == null || right == null) {
            return null;
        }
        if (!Objects.equals(left.getTableName(), right.getTableName())) {
            return null;
        }
        if (!isAdjacent(left, right)) {
            return null;
        }

        ServerId leftServer = support.clusterManager.getPrimaryServerForRegion(left.getRegionId());
        ServerId rightServer = support.clusterManager.getPrimaryServerForRegion(right.getRegionId());
        if (leftServer == null || rightServer == null || !leftServer.equals(rightServer)) {
            return null;
        }
        if (!support.clusterManager.isServerActive(leftServer)) {
            return null;
        }
        return new MergeTask(left.getRegionId(), right.getRegionId(), left.getTableName(), leftServer);
    }

    private Collection<String> getAllTableNames() {
        Set<String> tableNames = new HashSet<>();
        for (Region region : support.metadataManager.getAllRegions()) {
            tableNames.add(region.getTableName());
        }
        return tableNames;
    }

    // --- Inner classes ---

    private static class MergeTask {
        private final String leftRegionId;
        private final String rightRegionId;
        private final String tableName;
        private final ServerId serverId;

        MergeTask(String leftRegionId, String rightRegionId,
                  String tableName, ServerId serverId) {
            this.leftRegionId = leftRegionId;
            this.rightRegionId = rightRegionId;
            this.tableName = tableName;
            this.serverId = serverId;
        }

        String getLeftRegionId() { return leftRegionId; }
        String getRightRegionId() { return rightRegionId; }
        String getTableName() { return tableName; }
        ServerId getServerId() { return serverId; }
    }

    private static class MergeResult {
        private final Region mergedRegion;

        MergeResult(Region mergedRegion) {
            this.mergedRegion = mergedRegion;
        }

        Region getMergedRegion() { return mergedRegion; }
    }
}
