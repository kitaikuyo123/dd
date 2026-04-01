package com.minisql.master.rebalance;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.*;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.rpc.GrpcRegionServerCommandClient;
import com.minisql.master.rpc.RegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
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
    private static final long SPLIT_THRESHOLD_SIZE = 10L * 1024 * 1024 * 1024;

    // 最小分裂大小：小于此值不分裂（避免过度分裂，默认 1GB）
    private static final long MIN_SPLIT_SIZE = 1L * 1024 * 1024 * 1024;

    // 待分裂的 Region 队列
    private final BlockingQueue<SplitTask> splitQueue = new LinkedBlockingQueue<>();

    // 正在分裂的 Region（防止重复分裂）
    private final Set<String> splittingRegions = ConcurrentHashMap.newKeySet();

    // 分裂线程池
    private final ExecutorService splitExecutor;

    // 依赖的管理器
    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final LoadBalancer loadBalancer;
    private final RegionServerCommandClient commandClient;
    private MonitoringService monitoringService;
    private volatile ZkClient zkClient;

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

        System.out.println("RegionSplitCoordinator started");
    }

    /**
     * 停止分裂管理器
     */
    public void stop() {
        running = false;
        splitExecutor.shutdown();

        System.out.println("RegionSplitCoordinator stopped");
    }

    /**
     * 判断 Region 是否需要分裂
     */
    public boolean shouldSplit(ClusterManager.RegionLoad load) {
        long totalSize = load.getStoreFileSize() + load.getMemStoreSize();
        return totalSize >= SPLIT_THRESHOLD_SIZE;
    }

    public boolean scheduleSplit(String regionId, String tableName, ServerId serverId, ClusterManager.RegionLoad load) {
        if (splittingRegions.contains(regionId)) {
            return false;
        }
        SplitTask task = new SplitTask(regionId, tableName, serverId, load);
        boolean offered = splitQueue.offer(task);
        if (offered) {
            System.out.println("Scheduled split for region: " + regionId +
                " (size: " + formatSize(load.getStoreFileSize() + load.getMemStoreSize()) + ")");
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
                    executeSplit(task);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("Error processing split task: " + e.getMessage());
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
            System.out.println("Region " + regionId + " is already splitting, skip");
            return;
        }

        try {
            lock = acquireRegionLock(regionId);
            System.out.println("Starting split for region: " + regionId);

            // 1. 获取分裂点（从 RegionServer 获取）
            byte[] splitKey = getSplitKeyFromServer(task.getServerId(), regionId);
            if (splitKey == null) {
                System.err.println("Failed to get split key for region: " + regionId);
                return;
            }

            System.out.println("Split key for region " + regionId + ": " + bytesToHex(splitKey));

            // 2. 通知 RegionServer 执行分裂
            recordEvent("REGION_SPLIT_STARTED", "INFO", regionId, task.getServerId(),
                "Region split started", null);
            SplitResult result = notifyServerSplitRegion(task.getServerId(), regionId, splitKey);
            if (result == null) {
                System.err.println("Split failed for region: " + regionId);
                return;
            }

            // 3. 更新元数据
            updateMetadataAfterSplit(regionId, task.getServerId(), result);

            // 4. 为新 Region 分配服务器
            ServerId leftServer = task.getServerId();  // 左半部分留在原服务器
            ServerId rightServer = selectServerForNewRegion(result.getRightRegion());  // 右半部分可能迁移

            // 5. 如果右半部分分配到新服务器，执行迁移
            if (!rightServer.equals(leftServer)) {
                migrateRegion(result.getRightRegion().getRegionId(), leftServer, rightServer);
            }

            System.out.println("Split completed for region: " + regionId +
                    " -> " + result.getLeftRegion().getRegionId() + " (on " + leftServer + "), " +
                    result.getRightRegion().getRegionId() + " (on " + rightServer + ")");
            recordEvent("REGION_SPLIT_COMPLETED", "INFO", regionId, task.getServerId(),
                "Region split completed",
                result.getLeftRegion().getRegionId() + "," + result.getRightRegion().getRegionId());

        } catch (Exception e) {
            System.err.println("Error splitting region " + regionId + ": " + e.getMessage());
            e.printStackTrace();
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
        } catch (Exception e) {
            System.err.println("Failed to get split key from server: " + e.getMessage());
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
        } catch (Exception e) {
            System.err.println("Failed to split region on server: " + e.getMessage());
        }
        return null;
    }

    /**
     * 分裂后更新元数据
     */
    private void updateMetadataAfterSplit(String oldRegionId, ServerId primaryServer, SplitResult result) {
        // 1. 移除旧的 Region
        metadataManager.removeRegion(oldRegionId);
        clusterManager.unassignRegion(oldRegionId);

        // 2. 注册新的 Region
        metadataManager.registerRegionForTable(result.getLeftRegion(), primaryServer);
        metadataManager.registerRegionForTable(result.getRightRegion(), primaryServer);

        logger.info("ZooKeeper metadata updated for split regions: {} and {}",
            result.getLeftRegion().getRegionId(), result.getRightRegion().getRegionId());
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
        notifyServerCloseRegion(sourceServer, regionId);
        notifyServerOpenRegion(targetServer, region);
        clusterManager.unassignRegion(regionId);
        clusterManager.assignRegionToServer(regionId, targetServer);
        region.setPrimary(targetServer);
        region.addReplica(targetServer);
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
            System.err.println("Region not found: " + regionId);
            return false;
        }

        // 检查是否已经在分裂中
        if (splittingRegions.contains(regionId)) {
            return false;
        }

        ServerId serverId = clusterManager.getPrimaryServerForRegion(regionId);
        if (serverId == null) {
            System.err.println("No server assigned to region: " + regionId);
            return false;
        }

        // 创建分裂任务
        ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
        load.setRegionId(regionId);
        load.setStoreFileSize(SPLIT_THRESHOLD_SIZE + 1);  // 强制超过阈值触发分裂

        return scheduleSplit(regionId, region.getTableName(), serverId, load);
    }

    /**
     * 手动触发分裂（用于测试或管理命令）
     */
    public boolean triggerManualSplit(String regionId) {
        return checkAndSplitRegion(regionId);
    }

    /**
     * 获取正在分裂的 Region 列表
     */
    public Set<String> getSplittingRegions() {
        return new HashSet<>(splittingRegions);
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

    private String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * 分裂任务
     */
    private static class SplitTask {
        private final String regionId;
        private final String tableName;
        private final ServerId serverId;
        private final ClusterManager.RegionLoad load;

        public SplitTask(String regionId, String tableName,
                         ServerId serverId, ClusterManager.RegionLoad load) {
            this.regionId = regionId;
            this.tableName = tableName;
            this.serverId = serverId;
            this.load = load;
        }

        public String getRegionId() { return regionId; }
        public String getTableName() { return tableName; }
        public ServerId getServerId() { return serverId; }
        public ClusterManager.RegionLoad getLoad() { return load; }
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
