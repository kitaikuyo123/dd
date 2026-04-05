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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Detects hot regions and emits standardized actions so hotspot mitigation can
 * be scheduled by the master together with regular balance actions.
 */
public class HotSpotCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(HotSpotCoordinator.class);

    public static final long DEFAULT_READ_THRESHOLD_PER_INTERVAL = 200;
    public static final long DEFAULT_WRITE_THRESHOLD_PER_INTERVAL = 100;
    public static final double DEFAULT_GROWTH_THRESHOLD = 1.2;
    public static final int DEFAULT_TARGET_READ_REPLICA_COUNT = 3;
    public static final long DEFAULT_COOLDOWN_MS = TimeUnit.MINUTES.toMillis(5);

    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final RegionSplitCoordinator splitCoordinator;
    private final RecoveryCoordinator recoveryCoordinator;

    private final Map<String, HotSpotInfo> hotSpots = new ConcurrentHashMap<>();
    private final Map<String, Queue<RegionLoadSnapshot>> loadHistory = new ConcurrentHashMap<>();
    private final Queue<HotSpotAction> pendingActions = new ConcurrentLinkedQueue<>();
    private final Map<String, Long> cooldownUntilMs = new ConcurrentHashMap<>();
    private volatile long readThresholdPerInterval = DEFAULT_READ_THRESHOLD_PER_INTERVAL;
    private volatile long writeThresholdPerInterval = DEFAULT_WRITE_THRESHOLD_PER_INTERVAL;
    private volatile double growthThreshold = DEFAULT_GROWTH_THRESHOLD;
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
        this.growthThreshold = Math.max(1.0, settings.getGrowthThreshold());
        this.targetReadReplicaCount = Math.max(1, settings.getTargetReadReplicaCount());
        this.cooldownMs = Math.max(0L, settings.getCooldownMs());
    }

    public long getReadThresholdPerInterval() {
        return readThresholdPerInterval;
    }

    public long getWriteThresholdPerInterval() {
        return writeThresholdPerInterval;
    }

    public double getGrowthThreshold() {
        return growthThreshold;
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

    public void recordRegionLoad(String regionId, ClusterManager.RegionLoad load) {
        Queue<RegionLoadSnapshot> history = loadHistory.computeIfAbsent(
            regionId, ignored -> new ConcurrentLinkedQueue<>());
        history.offer(new RegionLoadSnapshot(System.currentTimeMillis(), load));

        // 使用可配置的时间窗口
        long cutoff = System.currentTimeMillis() - historyWindowMs;
        while (!history.isEmpty() && history.peek().timestamp < cutoff) {
            history.poll();
        }
    }

    public void configureHistoryWindow(long windowMs, int minSnapshots) {
        this.historyWindowMs = Math.max(30000L, windowMs);
        this.minSnapshotCount = Math.max(2, minSnapshots);
    }

    public List<HotSpotAction> drainPendingActions() {
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
    }

    public Map<String, HotSpotInfo> getCurrentHotSpots() {
        return new HashMap<>(hotSpots);
    }

    private void detectAndPlanHotSpots() {
        clearExpiredCooldowns();

        for (String regionId : loadHistory.keySet()) {
            Queue<RegionLoadSnapshot> history = loadHistory.get(regionId);
            if (history == null || history.size() < minSnapshotCount) {
                continue;
            }

            HotSpotType hotSpotType = analyzeHotSpot(history);

            // 始终更新热点状态（用于监控显示）
            if (hotSpotType != null) {
                long requestCount = estimateRequestDelta(history);
                hotSpots.put(regionId, new HotSpotInfo(regionId, hotSpotType, requestCount));
            } else {
                // 不再是热点时移除
                hotSpots.remove(regionId);
            }

            // 冷却期只阻止动作规划
            long now = System.currentTimeMillis();
            if (hotSpotType == null || cooldownUntilMs.getOrDefault(regionId, 0L) > now) {
                continue;
            }

            HotSpotAction action = planHotSpotAction(regionId, hotSpotType);
            if (action != null) {
                logger.info("Hot spot action planned: region={} type={} action={} target={}",
                    regionId, hotSpotType, action.getType(), action.getTargetServer());
                pendingActions.offer(action);
            }
            cooldownUntilMs.put(regionId, now + cooldownMs);
        }
    }

    private void clearExpiredCooldowns() {
        long now = System.currentTimeMillis();
        cooldownUntilMs.entrySet().removeIf(entry -> entry.getValue() <= now);
    }

    private HotSpotType analyzeHotSpot(Queue<RegionLoadSnapshot> history) {
        List<RegionLoadSnapshot> snapshots = new ArrayList<>(history);
        if (snapshots.size() < 3) {
            return null;
        }

        RegionLoadSnapshot latest = snapshots.get(snapshots.size() - 1);
        RegionLoadSnapshot previous = snapshots.get(snapshots.size() - 2);

        long readRequests = latest.load.getReadRequests() - previous.load.getReadRequests();
        long writeRequests = latest.load.getWriteRequests() - previous.load.getWriteRequests();

        if (readRequests > readThresholdPerInterval) {
            return isSustainedGrowth(snapshots) ? HotSpotType.READ_GROWING : HotSpotType.READ;
        }

        if (writeRequests > writeThresholdPerInterval) {
            return isSustainedGrowth(snapshots) ? HotSpotType.WRITE_GROWING : HotSpotType.WRITE;
        }

        return null;
    }

    private boolean isSustainedGrowth(List<RegionLoadSnapshot> snapshots) {
        if (snapshots.size() < 3) {
            return false;
        }

        double growthSum = 0;
        for (int i = 1; i < snapshots.size(); i++) {
            long prevTotal = snapshots.get(i - 1).load.getReadRequests()
                + snapshots.get(i - 1).load.getWriteRequests();
            long currTotal = snapshots.get(i).load.getReadRequests()
                + snapshots.get(i).load.getWriteRequests();

            if (prevTotal > 0) {
                growthSum += (double) currTotal / prevTotal;
            }
        }

        double avgGrowth = growthSum / (snapshots.size() - 1);
        return avgGrowth > growthThreshold;
    }

    private long estimateRequestDelta(Queue<RegionLoadSnapshot> history) {
        List<RegionLoadSnapshot> snapshots = new ArrayList<>(history);
        if (snapshots.size() < 2) {
            return 0;
        }

        RegionLoadSnapshot latest = snapshots.get(snapshots.size() - 1);
        RegionLoadSnapshot previous = snapshots.get(snapshots.size() - 2);
        long latestTotal = latest.load.getReadRequests() + latest.load.getWriteRequests();
        long previousTotal = previous.load.getReadRequests() + previous.load.getWriteRequests();
        return Math.max(0, latestTotal - previousTotal);
    }

    public double calculateDisplayScore(String regionId, long replicationLag) {
        HotSpotInfo info = hotSpots.get(regionId);
        if (info == null) {
            return replicationLag > 0 ? Math.min(1.0, replicationLag / 1000.0) : 0.0;
        }

        long threshold = info.isReadHotSpot() ? readThresholdPerInterval : writeThresholdPerInterval;
        double pressure = threshold > 0 ? (double) info.getRequestCount() / threshold : 0.0;
        double lagPenalty = replicationLag > 0 ? Math.min(1.0, replicationLag / 1000.0) : 0.0;
        return pressure + lagPenalty;
    }

    private HotSpotAction planHotSpotAction(String regionId, HotSpotType type) {
        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            return null;
        }

        switch (type) {
            case READ:
            case READ_GROWING:
                return planReadReplica(regionId, type);
            case WRITE:
            case WRITE_GROWING:
                return new HotSpotAction(regionId, HotSpotActionType.SPLIT_REGION, null, null, type);
            default:
                return null;
        }
    }

    private HotSpotAction planReadReplica(String regionId, HotSpotType hotSpotType) {
        ServerId primaryServer = clusterManager.getPrimaryServerForRegion(regionId);
        if (primaryServer == null) {
            return null;
        }

        List<ClusterManager.ServerInfo> servers = new ArrayList<>(clusterManager.getActiveServers());
        List<ServerId> currentReplicas = clusterManager.getReplicaServers(regionId);

        // 过滤掉主服务器、现有副本、以及心跳过期的服务器
        long heartbeatTimeout = 60000L; // 1 分钟
        long now = System.currentTimeMillis();

        List<ClusterManager.ServerInfo> nonReplicaTargets = new ArrayList<>(servers);
        nonReplicaTargets.removeIf(server -> {
            ServerId sid = server.getServerId();
            boolean isPrimaryOrReplica = sid.equals(primaryServer) || currentReplicas.contains(sid);
            boolean isStale = (now - server.getLastHeartbeat()) > heartbeatTimeout;
            return isPrimaryOrReplica || isStale;
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

        long requestPressure = hotSpots.containsKey(regionId)
            ? hotSpots.get(regionId).getRequestCount()
            : 0L;

        // 只使用 ADD_READ_REPLICA 动作
        // MOVE_REGION 暂未完整实现（缺少通知 RegionServer 的逻辑），暂时禁用
        HotSpotAction addReplicaAction = new HotSpotAction(regionId, HotSpotActionType.ADD_READ_REPLICA,
            null, targetServer.getServerId(), hotSpotType);
        return addReplicaAction;
    }

    private void addReadReplica(String regionId, ServerId targetServerId) {
        logger.info("Adding read replica for {} on {}", regionId, targetServerId);
        clusterManager.addReplica(regionId, targetServerId);
        if (recoveryCoordinator != null) {
            recoveryCoordinator.bootstrapReplica(regionId, targetServerId);
        }
    }

    private void splitHotRegion(String regionId) {
        logger.info("Triggering split for hot region: {}", regionId);
        splitCoordinator.checkAndSplitRegion(regionId);
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
        READ_GROWING,
        WRITE,
        WRITE_GROWING
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
            return type == HotSpotType.READ || type == HotSpotType.READ_GROWING;
        }
    }

    public static class HotSpotSettings {
        private final long readThresholdPerInterval;
        private final long writeThresholdPerInterval;
        private final double growthThreshold;
        private final int targetReadReplicaCount;
        private final long cooldownMs;

        public HotSpotSettings(long readThresholdPerInterval,
                               long writeThresholdPerInterval,
                               double growthThreshold,
                               int targetReadReplicaCount,
                               long cooldownMs) {
            this.readThresholdPerInterval = readThresholdPerInterval;
            this.writeThresholdPerInterval = writeThresholdPerInterval;
            this.growthThreshold = growthThreshold;
            this.targetReadReplicaCount = targetReadReplicaCount;
            this.cooldownMs = cooldownMs;
        }

        public long getReadThresholdPerInterval() {
            return readThresholdPerInterval;
        }

        public long getWriteThresholdPerInterval() {
            return writeThresholdPerInterval;
        }

        public double getGrowthThreshold() {
            return growthThreshold;
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
}
