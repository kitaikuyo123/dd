package com.minisql.master.rebalance;

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
 * Region 分裂管理器
 * 负责监控 Region 大小，自动触发分裂
 */
public class RegionSplitCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(RegionSplitCoordinator.class);

    // 分裂阈值：单个 Region 最大大小（默认 10GB）
    private static final long DEFAULT_splitThresholdSize = 10L * 1024 * 1024 * 1024;
    private static final long DEFAULT_TABLE_SPLIT_COOLDOWN_MS = TimeUnit.MINUTES.toMillis(2);
    private volatile long splitThresholdSize = DEFAULT_splitThresholdSize;
    private volatile long tableSplitCooldownMs = DEFAULT_TABLE_SPLIT_COOLDOWN_MS;

    // 待分裂的 Region 队列
    private final BlockingQueue<SplitTask> splitQueue = new LinkedBlockingQueue<>();

    // 正在分裂的 Region（防止重复分裂）
    private final Set<String> splittingRegions = ConcurrentHashMap.newKeySet();
    // 已加入分裂队列但尚未开始执行的 Region（防止重复入队）
    private final Set<String> queuedRegions = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> tableSplitCooldownUntilMs = new ConcurrentHashMap<>();

    // 分裂线程池
    private final ExecutorService splitExecutor;

    // 依赖的管理器
    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final LoadBalancer loadBalancer;
    private final RegionServerCommandClient commandClient;
    private MonitoringService monitoringService;
    private volatile ZkClient zkClient;
    private RegionMergeCoordinator mergeCoordinator;
    private volatile RecoveryCoordinator recoveryCoordinator;
    private volatile ReplicationCoordinator replicationCoordinator;
    private volatile ReplicaMonitor replicaMonitor;
    private volatile ReplicaLifecycleManager lifecycleManager;

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
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.loadBalancer = loadBalancer;
        this.commandClient = commandClient;

        // 创建分裂线程池（单线程，顺序处理）
        this.splitExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "RegionSplit-Worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void setMonitoringService(MonitoringService monitoringService) {
        this.monitoringService = monitoringService;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void setMergeCoordinator(RegionMergeCoordinator mergeCoordinator) {
        this.mergeCoordinator = mergeCoordinator;
    }

    public void setRecoveryCoordinator(RecoveryCoordinator recoveryCoordinator) {
        this.recoveryCoordinator = recoveryCoordinator;
    }

    public void setReplicationCoordinator(ReplicationCoordinator replicationCoordinator) {
        this.replicationCoordinator = replicationCoordinator;
    }

    public void setReplicaMonitor(ReplicaMonitor replicaMonitor) {
        this.replicaMonitor = replicaMonitor;
    }

    public void setLifecycleManager(ReplicaLifecycleManager lifecycleManager) {
        this.lifecycleManager = lifecycleManager;
    }

    public void setSplitThresholdSize(long splitThresholdSize) {
        this.splitThresholdSize = splitThresholdSize;
    }

    public void setTableSplitCooldownMs(long tableSplitCooldownMs) {
        this.tableSplitCooldownMs = Math.max(0L, tableSplitCooldownMs);
    }

    /**
     * 启动分裂管理器
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;

        // 启动分裂处理器
        splitExecutor.submit(this::processSplitTasks);

        logger.info("RegionSplitCoordinator started");
    }

    /**
     * 停止分裂管理器
     */
    public void stop() {
        running = false;
        splitExecutor.shutdown();
        splitQueue.clear();
        queuedRegions.clear();
        tableSplitCooldownUntilMs.clear();

        logger.info("RegionSplitCoordinator stopped");
    }

    /**
     * 判断 Region 是否需要分裂
     */
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

        SplitTask task = new SplitTask(regionId, tableName, serverId, load);
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

    /**
     * 处理分裂任务队列
     */
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

    /**
     * 执行 Region 分裂
     */
    private void executeSplit(SplitTask task) {
        String regionId = task.getRegionId();
        DistributedLock lock = null;

        // 标记为正在分裂
        if (!splittingRegions.add(regionId)) {
            logger.warn("Region {} is already splitting, skip", regionId);
            return;
        }

        try {
            lock = acquireRegionLock(regionId);
            logger.info("Starting split for region: {}", regionId);

            // 1. 获取分裂点（从 RegionServer 获取）
            byte[] splitKey = getSplitKeyFromServer(task.getServerId(), regionId);
            if (splitKey == null) {
                logger.warn("Failed to get split key for region: {} (server={})", regionId, task.getServerId());
                return;
            }

            logger.info("Split key for region {}: {}", regionId, BytesUtil.bytesToHex(splitKey));

            // 2. 通知 RegionServer 执行分裂
            recordEvent("REGION_SPLIT_STARTED", "INFO", regionId, task.getServerId(),
                "Region split started", null);
            SplitResult result = notifyServerSplitRegion(task.getServerId(), regionId, splitKey);
            if (result == null) {
                logger.warn("Split failed for region: {} (server={})", regionId, task.getServerId());
                return;
            }

            // 3. 更新元数据
            updateMetadataAfterSplit(regionId, task.getServerId(), result);

            // 4. 为新 Region 分配服务器
            ServerId leftServer = task.getServerId();  // 左半部分留在原服务器
            ServerId rightServer = selectServerForNewRegion(result.getRightRegion());  // 右半部分可能迁移

            // 5. 如果右半部分分配到新服务器，执行迁移
            if (rightServer == null) {
                rightServer = leftServer;
            }
            if (!rightServer.equals(leftServer)) {
                migrateRegion(result.getRightRegion().getRegionId(), leftServer, rightServer);
            }

            ensureReplicaTopology(result.getLeftRegion().getRegionId());
            ensureReplicaTopology(result.getRightRegion().getRegionId());

            logger.info("Split completed for region: {} -> {} (on {}), {} (on {})",
                    regionId, result.getLeftRegion().getRegionId(), leftServer,
                    result.getRightRegion().getRegionId(), rightServer);
            recordTableSplit(task.getTableName());
            recordEvent("REGION_SPLIT_COMPLETED", "INFO", regionId, task.getServerId(),
                "Region split completed",
                result.getLeftRegion().getRegionId() + "," + result.getRightRegion().getRegionId());

            // 6. 记录分裂事件到合并冷却期，防止刚分裂的 Region 被立即合并
            if (mergeCoordinator != null) {
                mergeCoordinator.recordRegionSplit(result.getLeftRegion().getRegionId());
                mergeCoordinator.recordRegionSplit(result.getRightRegion().getRegionId());
            }

        } catch (Exception e) {
            logger.error("Error splitting region {}: {}", regionId, e.getMessage(), e);
        } finally {
            releaseLock(lock);
            splittingRegions.remove(regionId);
        }
    }

    /**
     * 从 RegionServer 获取分裂点
     */
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

    /**
     * 通知 RegionServer 执行分裂
     */
    private SplitResult notifyServerSplitRegion(ServerId serverId, String regionId, byte[] splitKey) {
        try {
            RegionServerProto.SplitRegionResponse response = commandClient.splitRegion(serverId, regionId, splitKey);
            if (response.getStatus().getSuccess()) {
                return new SplitResult(
                        convertProtoToRegion(response.getLeftRegion()),
                        convertProtoToRegion(response.getRightRegion())
                );
            }
            logger.warn("RegionServer rejected split request: region={} server={} message={}",
                regionId, serverId, response.getStatus().getMessage());
        } catch (Exception e) {
            logger.error("Failed to split region on server: {}", e.getMessage(), e);
        }
        return null;
    }

    /**
     * 分裂后更新元数据
     */
    private void updateMetadataAfterSplit(String oldRegionId, ServerId primaryServer, SplitResult result) {
        // 1. 移除旧的 Region
        Region oldRegion = metadataManager.getRegion(oldRegionId);
        String tableName = oldRegion != null ? oldRegion.getTableName() : null;
        cleanupRegionRuntime(oldRegionId, tableName);
        metadataManager.removeRegion(oldRegionId);
        metadataManager.removeRegion(result.getLeftRegion().getRegionId());
        metadataManager.removeRegion(result.getRightRegion().getRegionId());

        // 2. 注册新的 Region
        registerSplitRegion(result.getLeftRegion(), primaryServer);
        registerSplitRegion(result.getRightRegion(), primaryServer);

        logger.info("ZooKeeper metadata updated for split regions: {} and {}",
            result.getLeftRegion().getRegionId(), result.getRightRegion().getRegionId());
    }

    private void registerSplitRegion(Region region, ServerId primaryServer) {
        metadataManager.registerRegionForTable(region, primaryServer);
        if (primaryServer != null) {
            clusterManager.assignRegionToServer(region.getRegionId(), primaryServer);
            clusterManager.addReplica(region.getRegionId(), primaryServer);
            clusterManager.updateRegionState(region.getRegionId(), Region.State.OPEN);
        }
    }

    /**
     * 为新 Region 选择服务器
     */
    private ServerId selectServerForNewRegion(Region region) {
        // 使用负载均衡器选择服务器
        List<ClusterManager.ServerInfo> servers =
                new ArrayList<>(clusterManager.getActiveServers());
        return loadBalancer.selectServerForRegion(region, servers);
    }

    /**
     * 迁移 Region 到目标服务器
     */
    private void migrateRegion(String regionId, ServerId sourceServer, ServerId targetServer) {
        logger.info("Migrating region {} from {} to {}", regionId, sourceServer, targetServer);

        // 1. 获取 Region 信息
        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            logger.error("Region not found: {}", regionId);
            return;
        }

        // 2. 先执行命令，再提交 assignment，避免半迁移状态写入元数据
        region.removeReplica(sourceServer);
        region.setPrimary(targetServer);
        region.addReplica(targetServer);

        notifyServerCloseRegion(sourceServer, regionId);
        notifyServerOpenRegion(targetServer, region);
        clusterManager.unassignRegion(regionId);
        clusterManager.assignRegionToServer(regionId, targetServer);
        clusterManager.removeReplica(regionId, sourceServer);
        clusterManager.addReplica(regionId, targetServer);
        clusterManager.removeRegionLoad(sourceServer, regionId);
        clusterManager.updateRegionState(regionId, Region.State.OPEN);
        metadataManager.registerRegionForTable(region, targetServer);

        logger.info("Migration completed for region: {}", regionId);
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
            logger.warn("Failed to release split lock for region", e);
        }
    }

    /**
     * 通知服务器关闭 Region
     * @param serverId 服务器 ID
     * @param regionId Region ID
     * @param dropTable 是否删除表（用于 DROP TABLE 场景）
     */
    private void notifyServerCloseRegion(ServerId serverId, String regionId, boolean dropTable) {
        try {
            RegionServerProto.CloseRegionResponse response =
                commandClient.closeRegion(serverId, regionId, true, dropTable);

            if (response.getStatus().getSuccess()) {
                logger.info("Notified {} to close region: {}{}", serverId, regionId, dropTable ? " and drop table" : "");
            } else {
                logger.warn("Failed to close region on {}: {}", serverId, response.getStatus().getMessage());
            }
        } catch (Exception e) {
            logger.error("Failed to notify server close region: {}", serverId, e);
        }
    }

    /**
     * 通知服务器关闭 Region（向后兼容，不删除表）
     */
    private void notifyServerCloseRegion(ServerId serverId, String regionId) {
        notifyServerCloseRegion(serverId, regionId, false);
    }

    /**
     * 通知服务器打开 Region
     */
    private void notifyServerOpenRegion(ServerId serverId, Region region) {
        try {
            RegionServerProto.OpenRegionResponse response = commandClient.openRegion(serverId, region, false);

            if (response.getStatus().getSuccess()) {
                logger.info("Notified {} to open region: {}", serverId, region.getRegionId());
            } else {
                logger.warn("Failed to open region on {}: {}", serverId, response.getStatus().getMessage());
            }
        } catch (Exception e) {
            logger.error("Failed to notify server open region: {}", serverId, e);
        }
    }

    /**
     * 检查并触发 Region 分裂（供 HotSpotCoordinator 调用）
     */
    public boolean checkAndSplitRegion(String regionId) {
        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            logger.warn("Region not found: {}", regionId);
            return false;
        }

        // 检查是否已经在分裂中
        if (splittingRegions.contains(regionId)) {
            return false;
        }

        ServerId serverId = clusterManager.getPrimaryServerForRegion(regionId);
        if (serverId == null) {
            logger.warn("No server assigned to region: {}", regionId);
            return false;
        }

        // 创建分裂任务
        ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
        load.setRegionId(regionId);
        load.setStoreFileSize(splitThresholdSize + 1);  // 强制超过阈值触发分裂

        boolean scheduled = scheduleSplit(regionId, region.getTableName(), serverId, load);
        if (scheduled) {
            logger.info("Hotspot split request accepted for region: {}", regionId);
        } else {
            logger.info("Hotspot split request not scheduled for region: {} (already splitting/queued or coordinator not running)",
                regionId);
        }
        return scheduled;
    }

    /**
     * 获取正在分裂的 Region 列表
     */
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

    public Set<String> getSplittingRegions() {
        return new HashSet<>(splittingRegions);
    }

    private void ensureReplicaTopology(String regionId) {
        Region region = metadataManager.getRegion(regionId);
        if (region == null || region.getPrimary() == null) {
            return;
        }

        int targetReplicationFactor = resolveReplicationFactor(region);
        List<ServerId> selectedServers = selectServersForReplication(region, targetReplicationFactor);
        if (selectedServers.isEmpty()) {
            selectedServers.add(region.getPrimary());
        }

        region.setPrimary(selectedServers.get(0));
        region.setReplicas(new ArrayList<>(selectedServers));
        metadataManager.registerRegionForTable(region, region.getPrimary());
        clusterManager.assignRegionToServer(regionId, region.getPrimary());
        clusterManager.updateRegionState(regionId, Region.State.OPEN);
        for (ServerId server : selectedServers) {
            clusterManager.addReplica(regionId, server);
        }

        if (replicationCoordinator != null) {
            replicationCoordinator.removeReplicaGroup(regionId);
            replicationCoordinator.createReplicaGroup(region, selectedServers);
        }

        if (recoveryCoordinator != null) {
            for (int i = 1; i < selectedServers.size(); i++) {
                recoveryCoordinator.bootstrapReplica(regionId, selectedServers.get(i));
            }
        }
    }

    private List<ServerId> selectServersForReplication(Region region, int replicationFactor) {
        int normalizedFactor = Math.max(1, replicationFactor);
        LinkedHashSet<ServerId> selected = new LinkedHashSet<>();
        if (region.getPrimary() != null) {
            selected.add(region.getPrimary());
        }
        if (region.getReplicas() != null) {
            selected.addAll(region.getReplicas());
        }

        List<ClusterManager.ServerInfo> candidates = new ArrayList<>(clusterManager.getActiveServersList());
        candidates.removeIf(info -> info == null || info.getServerId() == null || selected.contains(info.getServerId()));
        while (selected.size() < normalizedFactor && !candidates.isEmpty()) {
            ServerId serverId = loadBalancer.selectServerForRegion(region, candidates);
            if (serverId == null) {
                break;
            }
            selected.add(serverId);
            candidates.removeIf(info -> serverId.equals(info.getServerId()));
        }
        return new ArrayList<>(selected);
    }

    private int resolveReplicationFactor(Region region) {
        com.minisql.common.model.Table table = metadataManager.getTable(region.getTableName());
        if (table != null && table.getProperties() != null) {
            return Math.max(1, table.getProperties().getReplicationFactor());
        }
        return 3;
    }

    private void cleanupRegionRuntime(String regionId, String tableName) {
        clusterManager.removeRegionMetadata(tableName, regionId);
        if (replicaMonitor != null) {
            replicaMonitor.removeRegion(regionId);
        }
        if (lifecycleManager != null) {
            lifecycleManager.removeRegion(regionId);
        }
        if (recoveryCoordinator != null) {
            recoveryCoordinator.clearDesiredReplicaCount(regionId);
        }
        if (replicationCoordinator != null) {
            replicationCoordinator.removeReplicaGroup(regionId);
        }
    }

    private void recordEvent(String type, String severity, String regionId, ServerId serverId,
                             String message, String details) {
        if (monitoringService != null) {
            monitoringService.recordEvent(type, severity, regionId, null,
                serverId == null ? null : serverId.getHost() + ":" + serverId.getPort(),
                null, message, details);
        }
    }

    /**
     * Protobuf Region 转换为模型
     */
    private Region convertProtoToRegion(CommonProto.RegionInfo proto) {
        Region region = new Region();
        region.setRegionId(proto.getRegionId());
        region.setTableName(proto.getTableName());
        region.setStartKey(proto.getStartKey().toByteArray());
        region.setEndKey(proto.getEndKey().toByteArray());
        return region;
    }

    /**
     * 格式化大小显示
     */
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

    /**
     * 分裂任务
     */
    private static class SplitTask {
        private final String regionId;
        private final String tableName;
        private final ServerId serverId;

        public SplitTask(String regionId, String tableName,
                         ServerId serverId, ClusterManager.RegionLoad load) {
            this.regionId = regionId;
            this.tableName = tableName;
            this.serverId = serverId;
        }

        public String getRegionId() { return regionId; }
        public String getTableName() { return tableName; }
        public ServerId getServerId() { return serverId; }

    }

    /**
     * 分裂结果
     */
    private static class SplitResult {
        private final Region leftRegion;
        private final Region rightRegion;

        public SplitResult(Region leftRegion, Region rightRegion) {
            this.leftRegion = leftRegion;
            this.rightRegion = rightRegion;
        }

        public Region getLeftRegion() { return leftRegion; }
        public Region getRightRegion() { return rightRegion; }
    }
}
