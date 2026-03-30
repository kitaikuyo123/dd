package com.minisql.master.rebalance;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.*;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;
import java.util.concurrent.*;

/**
 * Region 合并管理器
 * 负责监控 Region 大小，自动触发合并，防止 Region 碎片化
 */
public class RegionMergeCoordinator {

    // 合并阈值：单个 Region 小于此值可考虑合并（默认 100MB）
    private static final long MERGE_THRESHOLD_SIZE = 100L * 1024 * 1024;

    // 最大合并阈值：两个 Region 加起来不能超过此值（默认 8GB，小于分裂阈值）
    private static final long MAX_MERGE_SIZE = 8L * 1024 * 1024 * 1024;

    // 最小合并大小：小于此值强制合并（默认 10MB）
    private static final long MIN_MERGE_SIZE = 10L * 1024 * 1024;

    // 合并冷却期：刚分裂的 Region 多久内不合并（默认 1小时）
    private static final long MERGE_COOLDOWN_MS = 60 * 60 * 1000;

    // 待合并的 Region 队列
    private final BlockingQueue<MergeTask> mergeQueue = new LinkedBlockingQueue<>();

    // 正在合并的 Region（防止重复合并）
    private final Set<String> mergingRegions = ConcurrentHashMap.newKeySet();

    // 最近分裂的 Region 及其时间（用于冷却期判断）
    private final Map<String, Long> recentSplitRegions = new ConcurrentHashMap<>();

    // 合并线程池
    private final ExecutorService mergeExecutor;

    // 依赖的管理器
    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private MonitoringService monitoringService;

    // 调度器
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    public RegionMergeCoordinator(ClusterManager clusterManager,
                                  MetadataManager metadataManager) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;

        // 创建合并线程池（单线程，顺序处理）
        this.mergeExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "RegionMerge-Worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void setMonitoringService(MonitoringService monitoringService) {
        this.monitoringService = monitoringService;
    }

    /**
     * 启动合并管理器
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;

        // 启动调度器，定期检查是否需要合并
        scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "RegionMerge-Scheduler");
            t.setDaemon(true);
            return t;
        });

        // 每 60 秒检查一次（合并检查频率比分裂低）
        scheduler.scheduleWithFixedDelay(this::checkAndScheduleMerges, 60, 60, TimeUnit.SECONDS);

        // 启动合并处理器
        mergeExecutor.submit(this::processMergeTasks);

        System.out.println("RegionMergeCoordinator started");
    }

    /**
     * 停止合并管理器
     */
    public void stop() {
        running = false;

        if (scheduler != null) {
            scheduler.shutdown();
        }
        mergeExecutor.shutdown();

        System.out.println("RegionMergeCoordinator stopped");
    }

    /**
     * 记录 Region 分裂事件（用于冷却期）
     */
    public void recordRegionSplit(String regionId) {
        recentSplitRegions.put(regionId, System.currentTimeMillis());
        // 清理过期的记录（超过冷却期的）
        cleanupRecentSplits();
    }

    /**
     * 清理过期的分裂记录
     */
    private void cleanupRecentSplits() {
        long now = System.currentTimeMillis();
        recentSplitRegions.entrySet().removeIf(entry ->
            now - entry.getValue() > MERGE_COOLDOWN_MS);
    }

    /**
     * 检查所有表，将满足合并条件的 Region 加入队列
     */
    private void checkAndScheduleMerges() {
        try {
            // 获取所有表
            Collection<String> tableNames = getAllTableNames();

            for (String tableName : tableNames) {
                // 获取该表的所有 Region
                List<Region> regions = new ArrayList<>(metadataManager.getRegionsForTable(tableName));

                // 按 startKey 排序
                regions.sort(Comparator.comparing(Region::getStartKey, this::compareBytes));

                // 检查相邻的 Region 是否可以合并
                for (int i = 0; i < regions.size() - 1; i++) {
                    Region left = regions.get(i);
                    Region right = regions.get(i + 1);

                    if (shouldMerge(left, right)) {
                        scheduleMerge(left, right);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error checking merges: " + e.getMessage());
        }
    }

    /**
     * 判断两个 Region 是否应该合并
     */
    private boolean shouldMerge(Region left, Region right) {
        // 检查是否相邻
        if (!isAdjacent(left, right)) {
            return false;
        }

        // 检查是否正在合并中
        if (mergingRegions.contains(left.getRegionId()) ||
            mergingRegions.contains(right.getRegionId())) {
            return false;
        }

        // 检查冷却期
        if (isInCooldown(left.getRegionId()) || isInCooldown(right.getRegionId())) {
            return false;
        }

        // 检查是否在同一个服务器上（简化处理，跨服务器合并不支持）
        ServerId leftServer = clusterManager.getPrimaryServerForRegion(left.getRegionId());
        ServerId rightServer = clusterManager.getPrimaryServerForRegion(right.getRegionId());
        if (leftServer == null || rightServer == null || !leftServer.equals(rightServer)) {
            return false;
        }

        // 获取 Region 大小
        long leftSize = getRegionSize(left.getRegionId());
        long rightSize = getRegionSize(right.getRegionId());
        long totalSize = leftSize + rightSize;

        // 如果总大小超过最大合并阈值，不能合并
        if (totalSize > MAX_MERGE_SIZE) {
            return false;
        }

        // 如果两个都很小（小于 MERGE_THRESHOLD），可以合并
        if (leftSize < MERGE_THRESHOLD_SIZE && rightSize < MERGE_THRESHOLD_SIZE) {
            return true;
        }

        // 如果其中一个非常小（小于 MIN_MERGE_SIZE），强制合并
        if (leftSize < MIN_MERGE_SIZE || rightSize < MIN_MERGE_SIZE) {
            return true;
        }

        return false;
    }

    /**
     * 检查 Region 是否在冷却期内
     */
    private boolean isInCooldown(String regionId) {
        Long splitTime = recentSplitRegions.get(regionId);
        if (splitTime == null) {
            return false;
        }
        return System.currentTimeMillis() - splitTime < MERGE_COOLDOWN_MS;
    }

    /**
     * 检查两个 Region 是否相邻
     */
    private boolean isAdjacent(Region left, Region right) {
        return Arrays.equals(left.getEndKey(), right.getStartKey());
    }

    /**
     * 获取 Region 大小
     */
    private long getRegionSize(String regionId) {
        ServerId serverId = clusterManager.getPrimaryServerForRegion(regionId);
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

    /**
     * 获取服务器信息
     */
    private ClusterManager.ServerInfo getServerInfo(ServerId serverId) {
        for (ClusterManager.ServerInfo info : clusterManager.getActiveServers()) {
            if (info.getServerId().equals(serverId)) {
                return info;
            }
        }
        return null;
    }

    /**
     * 将合并任务加入队列
     */
    private void scheduleMerge(Region left, Region right) {
        String mergeKey = left.getRegionId() + "_" + right.getRegionId();

        MergeTask task = new MergeTask(
            left.getRegionId(),
            right.getRegionId(),
            left.getTableName(),
            clusterManager.getPrimaryServerForRegion(left.getRegionId())
        );

        if (mergeQueue.offer(task)) {
            System.out.println("Scheduled merge for regions: " + left.getRegionId() +
                " and " + right.getRegionId() + " (table: " + left.getTableName() + ")");
        }
    }

    /**
     * 处理合并任务队列
     */
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
                System.err.println("Error processing merge task: " + e.getMessage());
            }
        }
    }

    /**
     * 执行 Region 合并
     */
    private void executeMerge(MergeTask task) {
        String leftRegionId = task.getLeftRegionId();
        String rightRegionId = task.getRightRegionId();

        // 标记为正在合并
        if (!mergingRegions.add(leftRegionId) || !mergingRegions.add(rightRegionId)) {
            System.out.println("Regions " + leftRegionId + " or " + rightRegionId + " are already merging, skip");
            mergingRegions.remove(leftRegionId);
            mergingRegions.remove(rightRegionId);
            return;
        }

        try {
            System.out.println("Starting merge for regions: " + leftRegionId + " and " + rightRegionId);
            recordEvent("REGION_MERGE_STARTED", "INFO", leftRegionId, task.getServerId(),
                "Region merge started", rightRegionId);

            // 1. 通知 RegionServer 执行合并
            MergeResult result = notifyServerMergeRegions(
                task.getServerId(), leftRegionId, rightRegionId);

            if (result == null) {
                System.err.println("Merge failed for regions: " + leftRegionId + " and " + rightRegionId);
                return;
            }

            // 2. 更新元数据
            updateMetadataAfterMerge(leftRegionId, rightRegionId, result);

            System.out.println("Merge completed: " + leftRegionId + " + " + rightRegionId +
                " -> " + result.getMergedRegion().getRegionId());
            recordEvent("REGION_MERGE_COMPLETED", "INFO", leftRegionId, task.getServerId(),
                "Region merge completed", result.getMergedRegion().getRegionId());

        } catch (Exception e) {
            System.err.println("Error merging regions " + leftRegionId + " and " + rightRegionId + ": " + e.getMessage());
            e.printStackTrace();
        } finally {
            mergingRegions.remove(leftRegionId);
            mergingRegions.remove(rightRegionId);
        }
    }

    /**
     * 通知 RegionServer 执行合并
     */
    private MergeResult notifyServerMergeRegions(ServerId serverId, String leftRegionId, String rightRegionId) {
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(serverId.getHost(), serverId.getPort())
                    .usePlaintext()
                    .build();

            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                    RegionServerServiceGrpc.newBlockingStub(channel);

            RegionServerProto.MergeRegionRequest request = RegionServerProto.MergeRegionRequest.newBuilder()
                    .setLeftRegionId(leftRegionId)
                    .setRightRegionId(rightRegionId)
                    .build();

            RegionServerProto.MergeRegionResponse response = stub.mergeRegion(request);

            if (response.getStatus().getSuccess()) {
                return new MergeResult(convertProtoToRegion(response.getMergedRegion()));
            }
        } catch (Exception e) {
            System.err.println("Failed to merge regions on server: " + e.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
        return null;
    }

    /**
     * 合并后更新元数据
     */
    private void updateMetadataAfterMerge(String leftRegionId, String rightRegionId, MergeResult result) {
        // 1. 移除旧的 Region
        metadataManager.removeRegion(leftRegionId);
        metadataManager.removeRegion(rightRegionId);
        clusterManager.unassignRegion(leftRegionId);
        clusterManager.unassignRegion(rightRegionId);

        // 2. 注册新的 Region
        metadataManager.registerRegion(result.getMergedRegion());

        // 3. 分配服务器（复用左 Region 的服务器）
        ServerId serverId = clusterManager.getPrimaryServerForRegion(leftRegionId);
        if (serverId != null) {
            clusterManager.assignRegionToServer(result.getMergedRegion().getRegionId(), serverId);
        }

        // 4. 记录分裂历史（用于冷却期）
        recordRegionSplit(result.getMergedRegion().getRegionId());
    }

    /**
     * 手动触发合并（用于测试或管理命令）
     */
    public boolean triggerManualMerge(String leftRegionId, String rightRegionId) {
        Region left = metadataManager.getRegion(leftRegionId);
        Region right = metadataManager.getRegion(rightRegionId);

        if (left == null || right == null) {
            System.err.println("Region not found: " + leftRegionId + " or " + rightRegionId);
            return false;
        }

        if (!left.getTableName().equals(right.getTableName())) {
            System.err.println("Regions are not in the same table");
            return false;
        }

        ServerId serverId = clusterManager.getPrimaryServerForRegion(leftRegionId);
        if (serverId == null) {
            System.err.println("No server assigned to region: " + leftRegionId);
            return false;
        }

        MergeTask task = new MergeTask(leftRegionId, rightRegionId, left.getTableName(), serverId);
        return mergeQueue.offer(task);
    }

    /**
     * 获取正在合并的 Region 列表
     */
    public Set<String> getMergingRegions() {
        return new HashSet<>(mergingRegions);
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
     * 获取所有表名
     */
    private Collection<String> getAllTableNames() {
        Set<String> tableNames = new HashSet<>();
        for (Region region : metadataManager.getAllRegions()) {
            tableNames.add(region.getTableName());
        }
        return tableNames;
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
     * 比较字节数组
     */
    private int compareBytes(byte[] a, byte[] b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    /**
     * 合并任务
     */
    private static class MergeTask {
        private final String leftRegionId;
        private final String rightRegionId;
        private final String tableName;
        private final ServerId serverId;

        public MergeTask(String leftRegionId, String rightRegionId,
                         String tableName, ServerId serverId) {
            this.leftRegionId = leftRegionId;
            this.rightRegionId = rightRegionId;
            this.tableName = tableName;
            this.serverId = serverId;
        }

        public String getLeftRegionId() { return leftRegionId; }
        public String getRightRegionId() { return rightRegionId; }
        public String getTableName() { return tableName; }
        public ServerId getServerId() { return serverId; }
    }

    /**
     * 合并结果
     */
    private static class MergeResult {
        private final Region mergedRegion;

        public MergeResult(Region mergedRegion) {
            this.mergedRegion = mergedRegion;
        }

        public Region getMergedRegion() { return mergedRegion; }
    }
}
