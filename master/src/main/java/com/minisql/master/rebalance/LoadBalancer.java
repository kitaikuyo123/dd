package com.minisql.master.rebalance;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.state.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 负载均衡器
 * 基于 Region 数量进行负载均衡决策，策略简单可预测（类 HBase 默认策略）。
 */
public class LoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);
    private static final double DEFAULT_BALANCE_THRESHOLD = 20.0;
    private static final long DEFAULT_MIN_MIGRATION_INTERVAL_MS = 300000L;

    private final LoadCalculator loadCalculator = new LoadCalculator();
    private volatile Strategy strategy = Strategy.LOAD_BASED;
    private int roundRobinIndex = 0; // guarded by this
    private volatile double balanceThreshold = DEFAULT_BALANCE_THRESHOLD;
    private volatile long minMigrationIntervalMs = DEFAULT_MIN_MIGRATION_INTERVAL_MS;

    public enum Strategy {
        RANDOM,
        ROUND_ROBIN,
        LOAD_BASED;

        public static Strategy fromString(String value) {
            if (value == null || value.isEmpty()) {
                return LOAD_BASED;
            }
            try {
                return Strategy.valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return LOAD_BASED;
            }
        }
    }

    /**
     * 负载计算器
     * 基于 Region 数量计算服务器负载分数
     */
    public static class LoadCalculator {

        private static final double SCORE_PER_REGION = 10.0;

        public LoadCalculator() {
        }

        /**
         * 计算服务器负载分数（每个 Region 贡献 10 分）
         */
        public double calculateLoadScore(ClusterManager.ServerInfo server) {
            return server.getRegionLoads().size() * SCORE_PER_REGION;
        }

        /**
         * 获取服务器剩余容量（100 - 负载分数）
         */
        public double getRemainingCapacity(ClusterManager.ServerInfo server) {
            return Math.max(0, 100 - calculateLoadScore(server));
        }

        /**
         * 判断服务器是否过载（负载 > 70）
         */
        public boolean isOverloaded(ClusterManager.ServerInfo server) {
            return calculateLoadScore(server) > 70;
        }

        /**
         * 判断服务器是否空闲（负载 < 30）
         */
        public boolean isIdle(ClusterManager.ServerInfo server) {
            return calculateLoadScore(server) < 30;
        }
    }

    // 上次迁移时间
    private volatile long lastBalanceTime = 0;

    // 迁移预算控制
    private volatile int maxMigrationsPerRound = 3;
    private volatile RegionMigrationCoordinator migrationCoordinator;

    // 热点感知
    private final HotSpotRegistry hotSpotRegistry = new HotSpotRegistry();
    private volatile double hotSpotPenaltyWeight = 15.0;

    /**
     * 设置每轮最大并发迁移数
     */
    public void setMaxMigrationsPerRound(int max) {
        if (max < 1) {
            return;
        }
        this.maxMigrationsPerRound = max;
    }

    public int getMaxMigrationsPerRound() {
        return maxMigrationsPerRound;
    }

    /**
     * 注入迁移协调器，用于查询进行中的迁移数量
     */
    public void setMigrationCoordinator(RegionMigrationCoordinator coordinator) {
        this.migrationCoordinator = coordinator;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy != null ? strategy : Strategy.LOAD_BASED;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setBalanceThreshold(double balanceThreshold) {
        if (balanceThreshold <= 0.0) {
            return;
        }
        this.balanceThreshold = balanceThreshold;
    }

    public double getBalanceThreshold() {
        return balanceThreshold;
    }

    public void setMinMigrationIntervalMs(long minMigrationIntervalMs) {
        if (minMigrationIntervalMs < 0L) {
            return;
        }
        this.minMigrationIntervalMs = minMigrationIntervalMs;
    }

    public long getMinMigrationIntervalMs() {
        return minMigrationIntervalMs;
    }

    /**
     * 为 Region 选择最优的 RegionServer
     * 基于负载分数和热点感知选择最合适的服务器
     */
    public ServerId selectServerForRegion(Region region, List<ClusterManager.ServerInfo> servers) {
        if (servers.isEmpty()) {
            return null;
        }

        switch (strategy) {
            case RANDOM:
                return selectRandomServer(servers);
            case ROUND_ROBIN:
                return selectRoundRobinServer(servers);
            case LOAD_BASED:
            default:
                break;
        }

        // 过滤掉过载的服务器
        List<ClusterManager.ServerInfo> candidates = new ArrayList<>();
        for (ClusterManager.ServerInfo server : servers) {
            if (!loadCalculator.isOverloaded(server)) {
                candidates.add(server);
            }
        }

        if (candidates.isEmpty()) {
            return servers.stream()
                .min(Comparator.comparingDouble(loadCalculator::calculateLoadScore))
                .map(ClusterManager.ServerInfo::getServerId)
                .orElse(null);
        }

        // 获取 Region 所属表名用于热点惩罚计算
        String tableName = region != null ? region.getTableName() : null;

        // 选择剩余容量最大的服务器（扣除热点惩罚）
        ClusterManager.ServerInfo bestServer = candidates.stream()
            .max(Comparator.comparingDouble(server -> {
                double remainingCapacity = loadCalculator.getRemainingCapacity(server);
                if (tableName != null) {
                    int hotCount = hotSpotRegistry.countHotRegionsForTableOnServer(
                        tableName, server.getRegionLoads().keySet());
                    remainingCapacity -= hotCount * hotSpotPenaltyWeight;
                }
                return remainingCapacity;
            }))
            .orElse(null);

        return bestServer != null ? bestServer.getServerId() : null;
    }

    /**
     * 计算需要迁移的 Region
     */
    public List<BalanceAction> computeBalanceActions(List<ClusterManager.ServerInfo> servers) {
        List<BalanceAction> actions = new ArrayList<>();

        if (servers.size() <= 1) {
            return actions;
        }

        if (strategy != Strategy.LOAD_BASED) {
            BalanceAction simpleAction = computeSimpleStrategyAction(servers);
            if (simpleAction != null) {
                actions.add(simpleAction);
                synchronized (this) {
                    lastBalanceTime = System.currentTimeMillis();
                }
            }
            return actions;
        }

        // 检查迁移冷却时间
        long now;
        synchronized (this) {
            now = System.currentTimeMillis();
            if (now - lastBalanceTime < minMigrationIntervalMs) {
                return actions;
            }
        }

        // 检查迁移预算
        int ongoingCount = migrationCoordinator != null
            ? migrationCoordinator.getOngoingMigrationCount() : 0;
        int budget = Math.max(0, maxMigrationsPerRound - ongoingCount);
        if (budget <= 0) {
            return actions;
        }

        // 计算每个服务器的负载分数
        Map<ClusterManager.ServerInfo, Double> loadScores = new HashMap<>();
        for (ClusterManager.ServerInfo server : servers) {
            loadScores.put(server, loadCalculator.calculateLoadScore(server));
        }

        // 找出平均负载
        double avgLoad = loadScores.values().stream()
            .mapToDouble(Double::doubleValue)
            .average()
            .orElse(0);

        // 分类服务器
        List<ClusterManager.ServerInfo> overloadedServers = new ArrayList<>();
        List<ClusterManager.ServerInfo> underloadedServers = new ArrayList<>();

        for (Map.Entry<ClusterManager.ServerInfo, Double> entry : loadScores.entrySet()) {
            double diff = entry.getValue() - avgLoad;
            if (diff > balanceThreshold) {
                overloadedServers.add(entry.getKey());
            } else if (diff < -balanceThreshold) {
                underloadedServers.add(entry.getKey());
            }
        }

        // 按负载降序排列过载服务器
        overloadedServers.sort((a, b) -> Double.compare(loadScores.get(b), loadScores.get(a)));

        // 按剩余容量降序排列低载服务器
        underloadedServers.sort((a, b) -> Double.compare(
            loadCalculator.getRemainingCapacity(b),
            loadCalculator.getRemainingCapacity(a)));

        // 生成迁移动作
        Set<String> scheduledRegions = new HashSet<>();

        for (ClusterManager.ServerInfo overloaded : overloadedServers) {
            double currentLoad = loadScores.get(overloaded);
            double targetLoad = avgLoad;

            for (ClusterManager.ServerInfo underloaded : underloadedServers) {
                if (currentLoad <= targetLoad) {
                    break;
                }

                // 选择要迁移的 Region
                String regionId = selectBestRegionToMove(overloaded, underloaded, scheduledRegions);

                if (regionId != null) {
                    actions.add(new BalanceAction(
                        regionId,
                        overloaded.getServerId(),
                        underloaded.getServerId()
                    ));
                    scheduledRegions.add(regionId);
                    currentLoad -= 10.0; // 每个 Region 固定贡献 10 分
                }
            }
        }

        if (!actions.isEmpty()) {
            if (actions.size() > budget) {
                logger.info("Migration budget: {}/{} used, truncating {} actions to {}",
                    ongoingCount, maxMigrationsPerRound, actions.size(), budget);
                actions = new ArrayList<>(actions.subList(0, budget));
            }
            synchronized (this) {
                lastBalanceTime = now;
            }
        }

        return actions;
    }

    /**
     * 选择要迁移的 Region（排除已调度和目标已有的）
     */
    private String selectBestRegionToMove(ClusterManager.ServerInfo source,
                                          ClusterManager.ServerInfo target,
                                          Set<String> excludedRegions) {
        for (String regionId : source.getRegionLoads().keySet()) {
            if (!excludedRegions.contains(regionId)
                && !target.getRegionLoads().containsKey(regionId)) {
                return regionId;
            }
        }
        return null;
    }

    private ServerId selectRandomServer(List<ClusterManager.ServerInfo> servers) {
        List<ClusterManager.ServerInfo> candidates = new ArrayList<>(servers);
        candidates.removeIf(loadCalculator::isOverloaded);
        List<ClusterManager.ServerInfo> effective = candidates.isEmpty() ? servers : candidates;
        return effective.get(ThreadLocalRandom.current().nextInt(effective.size())).getServerId();
    }

    private synchronized ServerId selectRoundRobinServer(List<ClusterManager.ServerInfo> servers) {
        List<ClusterManager.ServerInfo> candidates = new ArrayList<>(servers);
        candidates.removeIf(loadCalculator::isOverloaded);
        List<ClusterManager.ServerInfo> effective = candidates.isEmpty() ? servers : candidates;
        int index = Math.floorMod(roundRobinIndex++, effective.size());
        return effective.get(index).getServerId();
    }

    private synchronized BalanceAction computeSimpleStrategyAction(List<ClusterManager.ServerInfo> servers) {
        long now = System.currentTimeMillis();
        if (now - lastBalanceTime < minMigrationIntervalMs) {
            return null;
        }

        ClusterManager.ServerInfo source = servers.stream()
            .max(Comparator.comparingInt(server -> server.getRegionLoads().size()))
            .orElse(null);
        if (source == null || source.getRegionLoads().isEmpty()) {
            return null;
        }

        List<ClusterManager.ServerInfo> targets = new ArrayList<>(servers);
        targets.removeIf(server -> server.getServerId().equals(source.getServerId()));
        if (targets.isEmpty()) {
            return null;
        }

        ClusterManager.ServerInfo target;
        if (strategy == Strategy.RANDOM) {
            target = targets.get(ThreadLocalRandom.current().nextInt(targets.size()));
        } else {
            int index = Math.floorMod(roundRobinIndex++, targets.size());
            target = targets.get(index);
        }

        String regionId = source.getRegionLoads().keySet().stream().findFirst().orElse(null);
        if (regionId == null) {
            return null;
        }

        return new BalanceAction(regionId, source.getServerId(), target.getServerId());
    }

    /**
     * 获取热点注册表实例
     */
    public HotSpotRegistry getHotSpotRegistry() {
        return hotSpotRegistry;
    }

    /**
     * 设置热点惩罚权重
     */
    public void setHotSpotPenaltyWeight(double weight) {
        if (weight >= 0) {
            this.hotSpotPenaltyWeight = weight;
        }
    }

    /**
     * 热点注册表
     * 维护热点 Region 与其所属表的映射关系，供放置决策参考
     */
    public static class HotSpotRegistry {
        private final Map<String, String> regionToTable = new ConcurrentHashMap<>();

        public void updateHotSpots(Map<String, ?> hotSpots, java.util.function.Function<String, String> tableResolver) {
            regionToTable.keySet().retainAll(hotSpots.keySet());
            for (String regionId : hotSpots.keySet()) {
                String tableName = tableResolver != null ? tableResolver.apply(regionId) : null;
                if (tableName != null) {
                    regionToTable.put(regionId, tableName);
                }
            }
        }

        public void clearHotSpot(String regionId) {
            regionToTable.remove(regionId);
        }

        public int countHotRegionsForTableOnServer(String tableName, Set<String> regionIdsOnServer) {
            if (tableName == null || regionIdsOnServer == null) {
                return 0;
            }
            int count = 0;
            for (Map.Entry<String, String> entry : regionToTable.entrySet()) {
                if (tableName.equals(entry.getValue()) && regionIdsOnServer.contains(entry.getKey())) {
                    count++;
                }
            }
            return count;
        }

        public boolean isHotSpot(String regionId) {
            return regionToTable.containsKey(regionId);
        }
    }

    /**
     * 负载均衡动作
     */
    public static class BalanceAction {
        private final String regionId;
        private final ServerId source;
        private final ServerId target;
        private final long createTime;

        public BalanceAction(String regionId, ServerId source, ServerId target) {
            this.regionId = regionId;
            this.source = source;
            this.target = target;
            this.createTime = System.currentTimeMillis();
        }

        public String getRegionId() { return regionId; }
        public ServerId getSource() { return source; }
        public ServerId getTarget() { return target; }
        public long getCreateTime() { return createTime; }
    }

}
