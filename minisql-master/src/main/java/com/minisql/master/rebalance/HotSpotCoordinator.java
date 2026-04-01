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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Detects hot regions and emits standardized actions so hotspot mitigation can
 * be scheduled by the master together with regular balance actions.
 */
public class HotSpotCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(HotSpotCoordinator.class);

    private static final long HOTSPOT_READ_THRESHOLD = 10000;
    private static final long HOTSPOT_WRITE_THRESHOLD = 5000;
    private static final double HOTSPOT_GROWTH_THRESHOLD = 2.0;
    private static final int TARGET_READ_REPLICA_COUNT = 3;

    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final RegionSplitCoordinator splitCoordinator;
    private final RecoveryCoordinator recoveryCoordinator;

    private final Map<String, HotSpotInfo> hotSpots = new ConcurrentHashMap<>();
    private final Map<String, Queue<RegionLoadSnapshot>> loadHistory = new ConcurrentHashMap<>();
    private final Queue<HotSpotAction> pendingActions = new ConcurrentLinkedQueue<>();
    private final Map<String, Long> cooldownUntilMs = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;

    public HotSpotCoordinator(ClusterManager clusterManager,
                              MetadataManager metadataManager,
                              RegionSplitCoordinator splitCoordinator,
                              RecoveryCoordinator recoveryCoordinator) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.splitCoordinator = splitCoordinator;
        this.recoveryCoordinator = recoveryCoordinator;
    }

    public void startHotSpotDetection() {
        if (scheduler != null && !scheduler.isShutdown()) {
            return;
        }
        scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::planPendingActions, 10, 10, TimeUnit.SECONDS);
        System.out.println("HotSpot detection started");
    }

    public void stopHotSpotDetection() {
        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = null;
        }
        System.out.println("HotSpot detection stopped");
    }

    public List<HotSpotAction> planPendingActions() {
        detectAndPlanHotSpots();
        return drainPendingActions();
    }

    public void recordRegionLoad(String regionId, ClusterManager.RegionLoad load) {
        Queue<RegionLoadSnapshot> history = loadHistory.computeIfAbsent(
            regionId, ignored -> new ConcurrentLinkedQueue<>());
        history.offer(new RegionLoadSnapshot(System.currentTimeMillis(), load));

        long oneMinuteAgo = System.currentTimeMillis() - 60000;
        while (!history.isEmpty() && history.peek().timestamp < oneMinuteAgo) {
            history.poll();
        }
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
            case MOVE_REGION:
                // Region move actions are executed by MasterServiceImpl so they can
                // reuse the validated migration orchestration path.
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
            long now = System.currentTimeMillis();
            if (cooldownUntilMs.getOrDefault(regionId, 0L) > now) {
                continue;
            }

            Queue<RegionLoadSnapshot> history = loadHistory.get(regionId);
            if (history == null || history.size() < 3) {
                continue;
            }

            HotSpotType hotSpotType = analyzeHotSpot(history);
            if (hotSpotType == null) {
                continue;
            }

            long requestCount = estimateRequestDelta(history);
            hotSpots.put(regionId, new HotSpotInfo(regionId, hotSpotType, requestCount));

            HotSpotAction action = planHotSpotAction(regionId, hotSpotType);
            if (action != null) {
                logger.info("Hot spot action planned: region={} type={} action={} target={}",
                    regionId, hotSpotType, action.getType(), action.getTargetServer());
                pendingActions.offer(action);
            }
            cooldownUntilMs.put(regionId, now + TimeUnit.MINUTES.toMillis(30));
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

        if (readRequests > HOTSPOT_READ_THRESHOLD) {
            return isSustainedGrowth(snapshots) ? HotSpotType.READ_GROWING : HotSpotType.READ;
        }

        if (writeRequests > HOTSPOT_WRITE_THRESHOLD) {
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
        return avgGrowth > HOTSPOT_GROWTH_THRESHOLD;
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
        List<ClusterManager.ServerInfo> nonReplicaTargets = new ArrayList<>(servers);
        nonReplicaTargets.removeIf(server -> server.getServerId().equals(primaryServer)
            || currentReplicas.contains(server.getServerId()));

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

        ScoredHotSpotAction bestAction = null;

        HotSpotAction addReplicaAction = new HotSpotAction(regionId, HotSpotActionType.ADD_READ_REPLICA,
            null, targetServer.getServerId(), hotSpotType);
        bestAction = pickBetter(bestAction,
            scoreAddReplicaAction(addReplicaAction, currentReplicas.size(), targetServer, requestPressure));

        HotSpotAction moveRegionAction = new HotSpotAction(regionId, HotSpotActionType.MOVE_REGION,
            primaryServer, targetServer.getServerId(), hotSpotType);
        bestAction = pickBetter(bestAction,
            scoreMoveRegionAction(moveRegionAction, currentReplicas.size(), targetServer, requestPressure));

        return bestAction != null ? bestAction.action : null;
    }

    private void addReadReplica(String regionId, ServerId targetServerId) {
        logger.info("Adding read replica for {} on {}", regionId, targetServerId);
        clusterManager.addReplica(regionId, targetServerId);
        if (recoveryCoordinator != null) {
            recoveryCoordinator.bootstrapReplica(regionId, targetServerId);
        }
    }

    private void splitHotRegion(String regionId) {
        System.out.println("Triggering split for hot region: " + regionId);
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

    private ScoredHotSpotAction scoreAddReplicaAction(HotSpotAction action,
                                                      int currentReplicaCount,
                                                      ClusterManager.ServerInfo targetServer,
                                                      long requestPressure) {
        double score = 0;
        score += Math.max(0, TARGET_READ_REPLICA_COUNT - currentReplicaCount) * 25.0;
        score += Math.max(0, 100.0 - calculateServerLoad(targetServer));
        score += Math.min(30.0, requestPressure / 1000.0);
        return new ScoredHotSpotAction(action, score);
    }

    private ScoredHotSpotAction scoreMoveRegionAction(HotSpotAction action,
                                                      int currentReplicaCount,
                                                      ClusterManager.ServerInfo targetServer,
                                                      long requestPressure) {
        double score = 0;
        if (currentReplicaCount >= TARGET_READ_REPLICA_COUNT) {
            score += 45.0;
        }
        score += Math.max(0, 100.0 - calculateServerLoad(targetServer)) * 0.8;
        score += Math.min(25.0, requestPressure / 1500.0);
        return new ScoredHotSpotAction(action, score);
    }

    private ScoredHotSpotAction pickBetter(ScoredHotSpotAction current, ScoredHotSpotAction candidate) {
        if (candidate == null) {
            return current;
        }
        if (current == null || candidate.score > current.score) {
            return candidate;
        }
        return current;
    }

    public enum HotSpotType {
        READ,
        READ_GROWING,
        WRITE,
        WRITE_GROWING
    }

    public enum HotSpotActionType {
        ADD_READ_REPLICA,
        SPLIT_REGION,
        MOVE_REGION
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
    }

    private static class ScoredHotSpotAction {
        private final HotSpotAction action;
        private final double score;

        private ScoredHotSpotAction(HotSpotAction action, double score) {
            this.action = action;
            this.score = score;
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
