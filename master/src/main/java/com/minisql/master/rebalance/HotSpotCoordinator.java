package com.minisql.master.rebalance;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.recover.RecoveryCoordinator;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * 热点检测与缓解协调器
 *
 * 通过收集 Region 负载快照，检测读写热点 Region，并生成标准化的缓解动作。
 * 热点检测算法基于时间窗口内的平均 QPS 与阈值的比较:
 *   - 读热点: 平均读 QPS 超过读阈值
 *   - 写热点: 平均写 QPS 超过写阈值
 *   - 混合热点: 读写总 QPS 超过综合阈值（0.7 * 读阈值 + 写阈值）
 *
 * 缓解动作:
 *   - ADD_READ_REPLICA: 增加读副本分散读压力
 *   - SPLIT_REGION: 分裂 Region 减小热点范围
 *
 * 内置冷却机制，同一 Region 在冷却期内不会重复触发动作。
 */
public class HotSpotCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(HotSpotCoordinator.class);

    public static final long DEFAULT_READ_THRESHOLD_PER_INTERVAL = 200;
    public static final long DEFAULT_WRITE_THRESHOLD_PER_INTERVAL = 100;
    public static final int DEFAULT_TARGET_READ_REPLICA_COUNT = 3;
    public static final long DEFAULT_COOLDOWN_MS = TimeUnit.MINUTES.toMillis(5);

    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final RegionSplitCoordinator splitCoordinator;
    private final RecoveryCoordinator recoveryCoordinator;

    private final Map<String, HotSpotInfo> hotSpots = new ConcurrentHashMap<>();
    private final Map<String, Queue<RegionLoadSnapshot>> loadHistory = new ConcurrentHashMap<>();
    private final Queue<HotSpotAction> pendingActions = new ConcurrentLinkedQueue<>();
    private final Set<String> pendingRegionIds = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> cooldownUntilMs = new ConcurrentHashMap<>();
    private volatile long readThresholdPerInterval = DEFAULT_READ_THRESHOLD_PER_INTERVAL;
    private volatile long writeThresholdPerInterval = DEFAULT_WRITE_THRESHOLD_PER_INTERVAL;
    private volatile int targetReadReplicaCount = DEFAULT_TARGET_READ_REPLICA_COUNT;
    private volatile long cooldownMs = DEFAULT_COOLDOWN_MS;
    private volatile long historyWindowMs = TimeUnit.MINUTES.toMillis(2);
    private volatile int minSnapshotCount = 3;

    public HotSpotCoordinator(ClusterManager clusterManager,
                              MetadataManager metadataManager,
                              RegionSplitCoordinator splitCoordinator,
                              RecoveryCoordinator recoveryCoordinator) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.splitCoordinator = splitCoordinator;
        this.recoveryCoordinator = recoveryCoordinator;
    }

    public void configure(HotSpotSettings settings) {
        if (settings == null) {
            return;
        }
        this.readThresholdPerInterval = Math.max(1L, settings.getReadThresholdPerInterval());
        this.writeThresholdPerInterval = Math.max(1L, settings.getWriteThresholdPerInterval());
        this.targetReadReplicaCount = Math.max(1, settings.getTargetReadReplicaCount());
        this.cooldownMs = Math.max(0L, settings.getCooldownMs());
    }

    /**
     * 注入热点注册表，用于将检测到的热点信息同步给负载均衡器
     */
    public void setHotSpotRegistry(LoadBalancer.HotSpotRegistry hotSpotRegistry) {
        this.hotSpotRegistry = hotSpotRegistry;
    }

    // 热点注册表（可选注入）
    private volatile LoadBalancer.HotSpotRegistry hotSpotRegistry;

    public long getReadThresholdPerInterval() {
        return readThresholdPerInterval;
    }

    public long getWriteThresholdPerInterval() {
        return writeThresholdPerInterval;
    }

    public int getTargetReadReplicaCount() {
        return targetReadReplicaCount;
    }

    public long getCooldownMs() {
        return cooldownMs;
    }

    public List<HotSpotAction> planPendingActions() {
        detectAndPlanHotSpots();
        return drainPendingActions();
    }

    public void recordRegionLoad(String regionId, ServerId reporter, ClusterManager.RegionLoad load) {
        if (!isPrimaryReporter(regionId, reporter)) {
            return;
        }

        Queue<RegionLoadSnapshot> history = loadHistory.computeIfAbsent(
            regionId, ignored -> new ConcurrentLinkedQueue<>());
        history.offer(new RegionLoadSnapshot(System.currentTimeMillis(), load));

        // 使用可配置的时间窗口
        long cutoff = System.currentTimeMillis() - historyWindowMs;
        while (!history.isEmpty() && history.peek().timestamp < cutoff) {
            history.poll();
        }
    }


    private List<HotSpotAction> drainPendingActions() {
        List<HotSpotAction> actions = new ArrayList<>();
        HotSpotAction action;
        while ((action = pendingActions.poll()) != null) {
            actions.add(action);
        }
        return actions;
    }

    public void executeAction(HotSpotAction action) {
        if (action == null) {
            return;
        }

        switch (action.getType()) {
            case ADD_READ_REPLICA:
                if (action.getTargetServer() != null) {
                    addReadReplica(action.getRegionId(), action.getTargetServer());
                }
                break;
            case SPLIT_REGION:
                splitHotRegion(action.getRegionId());
                break;
            default:
                break;
        }

        // Apply cooldown only after action is attempted (successful or not),
        // so that the same region won't be hammered in a tight loop.
        cooldownUntilMs.put(action.getRegionId(), System.currentTimeMillis() + cooldownMs);
        pendingRegionIds.remove(action.getRegionId());
    }

    public Map<String, HotSpotInfo> getCurrentHotSpots() {
        return new HashMap<>(hotSpots);
    }

    private void detectAndPlanHotSpots() {
        clearExpiredCooldowns();

        for (Map.Entry<String, Queue<RegionLoadSnapshot>> entry : loadHistory.entrySet()) {
            String regionId = entry.getKey();
            Queue<RegionLoadSnapshot> history = entry.getValue();
            if (history == null || history.size() < minSnapshotCount) {
                clearHotSpotIfNeeded(regionId, "insufficient snapshots");
                continue;
            }

            HotSpotType hotSpotType = analyzeHotSpot(history);

            // 始终更新热点状态（用于监控显示）
            if (hotSpotType != null) {
                long requestCount = estimateRequestDelta(history);
                hotSpots.put(regionId, new HotSpotInfo(regionId, hotSpotType, requestCount));
            } else {
                clearHotSpotIfNeeded(regionId, "load dropped below threshold");
                continue;
            }

            // 冷却期只阻止动作规划
            long now = System.currentTimeMillis();
            if (cooldownUntilMs.getOrDefault(regionId, 0L) > now) {
                continue;
            }

            HotSpotAction action = planHotSpotAction(regionId, hotSpotType);
            if (action != null && !pendingRegionIds.contains(regionId)) {
                logger.info("Hot spot action planned: region={} type={} action={} target={}",
                    regionId, hotSpotType, action.getType(), action.getTargetServer());
                pendingActions.offer(action);
                pendingRegionIds.add(regionId);
            } else {
                logger.info("Hot spot detected but no executable action: region={} type={} (likely no eligible target replica server or incomplete metadata)",
                    regionId, hotSpotType);
            }
        }

        // 将当前热点信息同步到注册表，供负载均衡器放置决策使用
        syncHotSpotsToRegistry();
    }

    /**
     * 将检测到的热点信息同步到 HotSpotRegistry
     */
    private void syncHotSpotsToRegistry() {
        if (hotSpotRegistry == null) {
            return;
        }
        hotSpotRegistry.updateHotSpots(hotSpots, regionId -> {
            Region region = metadataManager.getRegion(regionId);
            return region != null ? region.getTableName() : null;
        });
    }

    private boolean isPrimaryReporter(String regionId, ServerId reporter) {
        if (regionId == null || reporter == null) {
            return true;
        }
        ServerId primary = clusterManager.getPrimaryServerForRegion(regionId);
        if (primary == null) {
            return true;
        }
        return sameEndpoint(primary, reporter);
    }

    private boolean sameEndpoint(ServerId left, ServerId right) {
        if (left == null || right == null) {
            return false;
        }
        return left.getPort() == right.getPort() && left.getHost().equals(right.getHost());
    }

    private void clearHotSpotIfNeeded(String regionId, String reason) {
        HotSpotInfo removed = hotSpots.remove(regionId);
        if (removed == null) {
            return;
        }
        cooldownUntilMs.remove(regionId);
        if (hotSpotRegistry != null) {
            hotSpotRegistry.clearHotSpot(regionId);
        }
        if (recoveryCoordinator != null) {
            recoveryCoordinator.clearDesiredReplicaCount(regionId);
            recoveryCoordinator.reconcileReplicaTarget(regionId);
        }
        logger.info("Hot spot cleared: region={} reason={}", regionId, reason);
    }

    private void clearExpiredCooldowns() {
        long now = System.currentTimeMillis();
        cooldownUntilMs.entrySet().removeIf(entry -> entry.getValue() <= now);
    }

    /**
     * Converts raw cumulative-counter snapshots into time-normalized per-second deltas.
     */
    private List<IntervalDelta> computePerIntervalDeltas(List<RegionLoadSnapshot> snapshots) {
        List<IntervalDelta> deltas = new ArrayList<>();
        for (int i = 1; i < snapshots.size(); i++) {
            RegionLoadSnapshot prev = snapshots.get(i - 1);
            RegionLoadSnapshot curr = snapshots.get(i);
            long intervalMs = curr.timestamp - prev.timestamp;
            if (intervalMs <= 0) {
                continue;
            }
            long readDelta = Math.max(0, curr.load.getReadRequests() - prev.load.getReadRequests());
            long writeDelta = Math.max(0, curr.load.getWriteRequests() - prev.load.getWriteRequests());
            double readPerSec = readDelta * 1000.0 / intervalMs;
            double writePerSec = writeDelta * 1000.0 / intervalMs;
            deltas.add(new IntervalDelta(readPerSec, writePerSec, intervalMs));
        }
        return deltas;
    }

    private HotSpotType analyzeHotSpot(Queue<RegionLoadSnapshot> history) {
        List<RegionLoadSnapshot> snapshots = new ArrayList<>(history);
        if (snapshots.size() < minSnapshotCount) {
            return null;
        }

        List<IntervalDelta> deltas = computePerIntervalDeltas(snapshots);
        if (deltas.isEmpty()) {
            return null;
        }

        double avgReadPerSec = 0, avgWritePerSec = 0, avgIntervalMs = 0;
        for (IntervalDelta d : deltas) {
            avgReadPerSec += d.readPerSec;
            avgWritePerSec += d.writePerSec;
            avgIntervalMs += d.intervalMs;
        }
        avgReadPerSec /= deltas.size();
        avgWritePerSec /= deltas.size();
        avgIntervalMs /= deltas.size();

        double avgIntervalSec = avgIntervalMs / 1000.0;
        if (avgIntervalSec <= 0) {
            return null;
        }
        double readThresholdPerSec = readThresholdPerInterval / avgIntervalSec;
        double writeThresholdPerSec = writeThresholdPerInterval / avgIntervalSec;
        double combinedThresholdPerSec = 0.7 * (readThresholdPerSec + writeThresholdPerSec);

        boolean readHot = avgReadPerSec > readThresholdPerSec;
        boolean writeHot = avgWritePerSec > writeThresholdPerSec;
        double combinedPerSec = avgReadPerSec + avgWritePerSec;
        boolean combinedHot = !readHot && !writeHot && combinedPerSec > combinedThresholdPerSec;

        if (readHot && writeHot) {
            double readSeverity = avgReadPerSec / readThresholdPerSec;
            double writeSeverity = avgWritePerSec / writeThresholdPerSec;
            return writeSeverity >= readSeverity ? HotSpotType.WRITE : HotSpotType.READ;
        }
        if (readHot) {
            return HotSpotType.READ;
        }
        if (writeHot) {
            return HotSpotType.WRITE;
        }
        if (combinedHot) {
            return HotSpotType.WRITE;
        }
        return null;
    }

    private long estimateRequestDelta(Queue<RegionLoadSnapshot> history) {
        List<RegionLoadSnapshot> snapshots = new ArrayList<>(history);
        if (snapshots.size() < 2) {
            return 0;
        }

        List<IntervalDelta> deltas = computePerIntervalDeltas(snapshots);
        if (deltas.isEmpty()) {
            return 0;
        }

        double avgPerSec = 0, avgIntervalMs = 0;
        for (IntervalDelta d : deltas) {
            avgPerSec += d.combinedPerSec();
            avgIntervalMs += d.intervalMs;
        }
        avgPerSec /= deltas.size();
        avgIntervalMs /= deltas.size();
        return Math.round(avgPerSec * avgIntervalMs / 1000.0);
    }

    /**
     * 计算前端展示用的热点分数（0-100）
     * <ul>
     *   <li>非热点 region：基于当前 QPS 相对于阈值的占比，线性映射到 0-50</li>
     *   <li>热点 region：阈值占比 50 分 + 超出部分映射到 50-100</li>
     *   <li>复制延迟额外加 0-10 分</li>
     * </ul>
     */
    public double calculateDisplayScore(String regionId, long replicationLag) {
        // 从历史快照中计算当前 QPS
        Queue<RegionLoadSnapshot> history = loadHistory.get(regionId);
        double readQps = 0, writeQps = 0;
        if (history != null && history.size() >= 2) {
            List<RegionLoadSnapshot> snapshots = new ArrayList<>(history);
            List<IntervalDelta> deltas = computePerIntervalDeltas(snapshots);
            if (!deltas.isEmpty()) {
                for (IntervalDelta d : deltas) {
                    readQps += d.readPerSec;
                    writeQps += d.writePerSec;
                }
                readQps /= deltas.size();
                writeQps /= deltas.size();
            }
        }

        // 将阈值转换为 per-second（与检测算法一致）
        double avgIntervalSec = estimateAvgIntervalSec(history);
        double readThresholdPerSec = avgIntervalSec > 0 ? readThresholdPerInterval / avgIntervalSec : 0;
        double writeThresholdPerSec = avgIntervalSec > 0 ? writeThresholdPerInterval / avgIntervalSec : 0;
        double maxThreshold = Math.max(readThresholdPerSec, writeThresholdPerSec);

        double score;
        if (maxThreshold <= 0) {
            score = 0;
        } else {
            // 综合热度：读写加权（写权重更高，与检测一致）
            double combinedQps = readQps + writeQps * 2.0;
            double ratio = combinedQps / maxThreshold; // 0.0 = 无流量, 1.0 = 刚好到阈值
            if (ratio <= 1.0) {
                // 非热点：0 → 50
                score = ratio * 50.0;
            } else {
                // 热点：50 → 100（超出阈值部分映射，超过阈值 3 倍封顶）
                double overRatio = Math.min(2.0, ratio - 1.0);
                score = 50.0 + overRatio * 25.0;
            }
        }

        // 复制延迟惩罚：0-10 分
        double lagPenalty = replicationLag > 0 ? Math.min(10.0, replicationLag / 100.0) : 0.0;

        return Math.min(100.0, score + lagPenalty);
    }

    private double estimateAvgIntervalSec(Queue<RegionLoadSnapshot> history) {
        if (history == null || history.size() < 2) {
            return 0;
        }
        List<RegionLoadSnapshot> snapshots = new ArrayList<>(history);
        List<IntervalDelta> deltas = computePerIntervalDeltas(snapshots);
        if (deltas.isEmpty()) {
            return 0;
        }
        double avgMs = 0;
        for (IntervalDelta d : deltas) {
            avgMs += d.intervalMs;
        }
        return (avgMs / deltas.size()) / 1000.0;
    }

    private HotSpotAction planHotSpotAction(String regionId, HotSpotType type) {
        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            return null;
        }

        ServerId primaryServer = clusterManager.getPrimaryServerForRegion(regionId);

        switch (type) {
            case READ:
                return planReadReplica(regionId, type);
            case WRITE:
                return new HotSpotAction(regionId, HotSpotActionType.SPLIT_REGION, primaryServer, null, type);
            default:
                return null;
        }
    }

    private HotSpotAction planReadReplica(String regionId, HotSpotType hotSpotType) {
        ServerId primaryServer = clusterManager.getPrimaryServerForRegion(regionId);
        if (primaryServer == null) {
            return null;
        }

        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            return null;
        }

        List<ClusterManager.ServerInfo> servers = new ArrayList<>(clusterManager.getActiveServers());
        List<ServerId> currentReplicas = new ArrayList<>(region.getReplicas());
        if (!currentReplicas.contains(primaryServer)) {
            currentReplicas.add(primaryServer);
        }

        int targetReplicaCount = Math.max(1, targetReadReplicaCount);
        if (currentReplicas.size() >= targetReplicaCount) {
            logger.info("Hot read detected but target replica count already reached: region={} current={} target={}",
                regionId, currentReplicas.size(), targetReplicaCount);
            return null;
        }

        // 过滤掉主服务器、现有副本、以及已下线的服务器（由 ZK 临时节点驱动）
        List<ClusterManager.ServerInfo> nonReplicaTargets = new ArrayList<>(servers);
        nonReplicaTargets.removeIf(server -> {
            ServerId sid = server.getServerId();
            boolean isPrimaryOrReplica = sid.equals(primaryServer) || currentReplicas.contains(sid);
            boolean isInactive = !clusterManager.isServerActive(sid);
            return isPrimaryOrReplica || isInactive;
        });

        if (nonReplicaTargets.isEmpty()) {
            logger.info("No secondary target available for hot region {}", regionId);
            return null;
        }

        ClusterManager.ServerInfo targetServer = nonReplicaTargets.stream()
            .min(Comparator.comparingDouble(this::calculateServerLoad))
            .orElse(null);
        if (targetServer == null) {
            return null;
        }

        // 只使用 ADD_READ_REPLICA 动作
        // MOVE_REGION 暂未完整实现（缺少通知 RegionServer 的逻辑），暂时禁用
        HotSpotAction addReplicaAction = new HotSpotAction(regionId, HotSpotActionType.ADD_READ_REPLICA,
            primaryServer, targetServer.getServerId(), hotSpotType);
        return addReplicaAction;
    }

    private void addReadReplica(String regionId, ServerId targetServerId) {
        logger.info("Adding read replica for {} on {}", regionId, targetServerId);
        if (recoveryCoordinator != null) {
            recoveryCoordinator.setDesiredReplicaCount(regionId, targetReadReplicaCount);
        }
        // ZK 写入由 RecoveryCoordinator.performRecovery() 在 bootstrap 成功后通过
        // ensureReplicaRegistered + markReplicaReady 完成，此处不预写
        recoveryCoordinator.bootstrapReplica(regionId, targetServerId);
    }

    private void splitHotRegion(String regionId) {
        logger.info("Triggering split for hot region: {}", regionId);
        boolean accepted = splitCoordinator.checkAndSplitRegion(regionId);
        if (!accepted) {
            logger.info("Split request skipped for hot region: {} (cooldown/already queued/no eligible primary)", regionId);
        }
    }

    private double calculateServerLoad(ClusterManager.ServerInfo server) {
        int regionCount = server.getRegionLoads().size();
        long totalRequests = 0;
        for (ClusterManager.RegionLoad load : server.getRegionLoads().values()) {
            totalRequests += load.getReadRequests() + load.getWriteRequests();
        }
        return regionCount * 10 + totalRequests / 1000.0;
    }

    public enum HotSpotType {
        READ,
        WRITE
    }

    public enum HotSpotActionType {
        ADD_READ_REPLICA,
        SPLIT_REGION
    }

    public static class HotSpotAction {
        private final String regionId;
        private final HotSpotActionType type;
        private final ServerId sourceServer;
        private final ServerId targetServer;
        private final HotSpotType hotSpotType;
        private final long createdAt;

        public HotSpotAction(String regionId,
                             HotSpotActionType type,
                             ServerId sourceServer,
                             ServerId targetServer,
                             HotSpotType hotSpotType) {
            this.regionId = regionId;
            this.type = type;
            this.sourceServer = sourceServer;
            this.targetServer = targetServer;
            this.hotSpotType = hotSpotType;
            this.createdAt = System.currentTimeMillis();
        }

        public String getRegionId() {
            return regionId;
        }

        public HotSpotActionType getType() {
            return type;
        }

        public ServerId getSourceServer() {
            return sourceServer;
        }

        public ServerId getTargetServer() {
            return targetServer;
        }

        public HotSpotType getHotSpotType() {
            return hotSpotType;
        }

        public long getCreatedAt() {
            return createdAt;
        }
    }

    public static class HotSpotInfo {
        private final String regionId;
        private final HotSpotType type;
        private final long detectedTime;
        private final long requestCount;

        public HotSpotInfo(String regionId, HotSpotType type, long requestCount) {
            this.regionId = regionId;
            this.type = type;
            this.requestCount = requestCount;
            this.detectedTime = System.currentTimeMillis();
        }

        public String getRegionId() {
            return regionId;
        }

        public HotSpotType getType() {
            return type;
        }

        public long getDetectedTime() {
            return detectedTime;
        }

        public long getRequestCount() {
            return requestCount;
        }

        public boolean isReadHotSpot() {
            return type == HotSpotType.READ;
        }
    }

    public static class HotSpotSettings {
        private final long readThresholdPerInterval;
        private final long writeThresholdPerInterval;
        private final int targetReadReplicaCount;
        private final long cooldownMs;

        public HotSpotSettings(long readThresholdPerInterval,
                               long writeThresholdPerInterval,
                               int targetReadReplicaCount,
                               long cooldownMs) {
            this.readThresholdPerInterval = readThresholdPerInterval;
            this.writeThresholdPerInterval = writeThresholdPerInterval;
            this.targetReadReplicaCount = targetReadReplicaCount;
            this.cooldownMs = cooldownMs;
        }

        // Backward-compatible 5-arg constructor (growthThreshold ignored)
        public HotSpotSettings(long readThresholdPerInterval,
                               long writeThresholdPerInterval,
                               double growthThreshold,
                               int targetReadReplicaCount,
                               long cooldownMs) {
            this(readThresholdPerInterval, writeThresholdPerInterval, targetReadReplicaCount, cooldownMs);
        }

        public long getReadThresholdPerInterval() {
            return readThresholdPerInterval;
        }

        public long getWriteThresholdPerInterval() {
            return writeThresholdPerInterval;
        }

        public int getTargetReadReplicaCount() {
            return targetReadReplicaCount;
        }

        public long getCooldownMs() {
            return cooldownMs;
        }
    }

    private static class RegionLoadSnapshot {
        private final long timestamp;
        private final ClusterManager.RegionLoad load;

        private RegionLoadSnapshot(long timestamp, ClusterManager.RegionLoad load) {
            this.timestamp = timestamp;
            this.load = load;
        }
    }

    /**
     * Per-interval delta, time-normalized to requests per second.
     */
    private static class IntervalDelta {
        final double readPerSec;
        final double writePerSec;
        final long intervalMs;

        IntervalDelta(double readPerSec, double writePerSec, long intervalMs) {
            this.readPerSec = readPerSec;
            this.writePerSec = writePerSec;
            this.intervalMs = intervalMs;
        }

        double combinedPerSec() {
            return readPerSec + writePerSec;
        }
    }
}
