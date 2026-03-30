package com.minisql.master.rebalance;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.state.ClusterManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 负载均衡器（改进版）
 * 基于综合负载指标进行 Region 分配和迁移决策
 */
public class LoadBalancer {

    private final LoadCalculator loadCalculator = new LoadCalculator();
    private final Random random = new Random();
    private volatile Strategy strategy = Strategy.LOAD_BASED;
    private volatile int roundRobinIndex = 0;

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
     * 负载计算器（内部类）
     * 综合计算服务器的负载分数，用于负载均衡决策
     */
    public static class LoadCalculator {

        // 权重配置 (剔除 CPU，重分配权重)
        private static final double CPU_WEIGHT = 0.0;
        private static final double MEMORY_WEIGHT = 0.3;
        private static final double DISK_WEIGHT = 0.3;
        private static final double REGION_COUNT_WEIGHT = 0.2;
        private static final double REQUEST_WEIGHT = 0.2;

        // 请求历史记录（用于计算增长率）
        private final Map<String, RequestHistory> requestHistories = new ConcurrentHashMap<>();

        public LoadCalculator() {
        }

        /**
         * 计算服务器综合负载分数（0-100，越小越空闲）
         */
        public double calculateLoadScore(ClusterManager.ServerInfo server) {
            ClusterManager.ServerMetrics metrics = server.getMetrics();
            Map<String, ClusterManager.RegionLoad> regionLoads = server.getRegionLoads();

            if (metrics == null) {
                // 没有指标数据时，只根据 Region 数量估算
                return regionLoads.size() * 10.0;
            }

            // 1. CPU 使用率（0-100）
            double cpuScore = metrics.getCpuUsage();

            // 2. 内存使用率（0-100）
            double memoryScore = metrics.getMemoryUsage();

            // 3. 磁盘使用率（0-100）
            double diskUsage = 0;
            if (metrics.getTotalSpace() > 0) {
                diskUsage = 100.0 * (metrics.getTotalSpace() - metrics.getAvailableSpace()) / metrics.getTotalSpace();
            }
            double diskScore = diskUsage;

            // 4. Region 数量分数（假设最大承载 100 个 Region）
            double regionScore = Math.min(100, regionLoads.size() * 100.0 / 100);

            // 5. 请求负载分数
            double requestScore = calculateRequestScore(server);

            // 加权计算综合分数
            return cpuScore * CPU_WEIGHT +
                   memoryScore * MEMORY_WEIGHT +
                   diskScore * DISK_WEIGHT +
                   regionScore * REGION_COUNT_WEIGHT +
                   requestScore * REQUEST_WEIGHT;
        }

        /**
         * 计算请求负载分数
         */
        private double calculateRequestScore(ClusterManager.ServerInfo server) {
            long totalRequests = 0;
            for (ClusterManager.RegionLoad load : server.getRegionLoads().values()) {
                totalRequests += load.getReadRequests() + load.getWriteRequests();
            }

            // 更新历史记录
            String serverId = server.getServerId().toString();
            RequestHistory history = requestHistories.computeIfAbsent(serverId, k -> new RequestHistory());
            history.addRecord(totalRequests);

            // 计算当前的真实 QPS (原逻辑中 growthRate 即为 QPS)
            double currentQps = history.getGrowthRate();
            
            // 默认单节点设计的最高满峰 QPS 为 5000
            double MAX_TARGET_QPS = 5000.0;
            
            // 基于真实 QPS 计算负载分数，而非累积请求总数
            double baseScore = Math.min(100, (currentQps / MAX_TARGET_QPS) * 100.0);

            // 如果单节点当前 QPS 高于 500，稍微增加惩罚分数，避免瞬间倾斜
            if (currentQps > 500) {
                baseScore += 10;
            }

            return Math.min(100, baseScore);
        }

        /**
         * 获取服务器剩余容量估计（0-100，越大越能承载更多负载）
         */
        public double getRemainingCapacity(ClusterManager.ServerInfo server) {
            double loadScore = calculateLoadScore(server);
            return Math.max(0, 100 - loadScore);
        }

        /**
         * 判断服务器是否过载
         */
        public boolean isOverloaded(ClusterManager.ServerInfo server) {
            return calculateLoadScore(server) > 80;
        }

        /**
         * 判断服务器是否空闲
         */
        public boolean isIdle(ClusterManager.ServerInfo server) {
            return calculateLoadScore(server) < 30;
        }

        /**
         * 请求历史记录
         */
        private static class RequestHistory {
            private long lastTimestamp = System.currentTimeMillis();
            private long lastRequestCount = 0;
            private double growthRate = 0;

            void addRecord(long totalRequests) {
                long now = System.currentTimeMillis();
                long timeDelta = now - lastTimestamp;

                if (timeDelta > 0 && lastRequestCount > 0) {
                    long requestDelta = totalRequests - lastRequestCount;
                    growthRate = (requestDelta * 1000.0) / timeDelta; // 每秒请求增长
                }

                lastTimestamp = now;
                lastRequestCount = totalRequests;
            }

            double getGrowthRate() {
                return growthRate;
            }
        }
    }

    // 均衡阈值：负载差异超过此值触发迁移
    private static final double BALANCE_THRESHOLD = 20.0;

    // 最小迁移间隔（毫秒）
    private static final long MIN_MIGRATION_INTERVAL = 300000; // 5分钟

    // 上次迁移时间
    private volatile long lastBalanceTime = 0;

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy != null ? strategy : Strategy.LOAD_BASED;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    /**
     * 为 Region 选择最优的 RegionServer
     * 基于综合负载分数选择最合适的服务器
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
            // 所有服务器都过载，选择负载最低的
            return servers.stream()
                .min(Comparator.comparingDouble(loadCalculator::calculateLoadScore))
                .map(ClusterManager.ServerInfo::getServerId)
                .orElse(null);
        }

        // 选择剩余容量最大的服务器
        ClusterManager.ServerInfo bestServer = candidates.stream()
            .max(Comparator.comparingDouble(loadCalculator::getRemainingCapacity))
            .orElse(null);

        return bestServer != null ? bestServer.getServerId() : null;
    }

    /**
     * 计算需要迁移的 Region（改进版）
     * 基于综合负载分数而非简单的 Region 数量
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
                lastBalanceTime = System.currentTimeMillis();
            }
            return actions;
        }

        // 检查迁移冷却时间
        long now = System.currentTimeMillis();
        if (now - lastBalanceTime < MIN_MIGRATION_INTERVAL) {
            return actions; // 冷却期内不迁移
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
            if (diff > BALANCE_THRESHOLD) {
                overloadedServers.add(entry.getKey());
            } else if (diff < -BALANCE_THRESHOLD) {
                underloadedServers.add(entry.getKey());
            }
        }

        // 按负载降序排列过载服务器（先处理负载最高的）
        overloadedServers.sort((a, b) -> Double.compare(loadScores.get(b), loadScores.get(a)));

        // 按剩余容量降序排列低载服务器（优先选择容量最大的）
        underloadedServers.sort((a, b) -> Double.compare(
            loadCalculator.getRemainingCapacity(b),
            loadCalculator.getRemainingCapacity(a)
        ));

        // 生成迁移动作
        Set<String> scheduledRegions = new HashSet<>();

        for (ClusterManager.ServerInfo overloaded : overloadedServers) {
            double currentLoad = loadScores.get(overloaded);

            // 需要降低到的目标负载
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

                    // 估算迁移后的负载变化
                    ClusterManager.RegionLoad regionLoad = overloaded.getRegionLoads().get(regionId);
                    if (regionLoad != null) {
                        double regionWeight = estimateRegionWeight(regionLoad);
                        currentLoad -= regionWeight;
                    }
                }
            }
        }

        if (!actions.isEmpty()) {
            lastBalanceTime = now;
        }

        return actions;
    }

    /**
     * 选择最佳的 Region 进行迁移
     */
    private String selectBestRegionToMove(ClusterManager.ServerInfo source,
                                          ClusterManager.ServerInfo target,
                                          Set<String> excludedRegions) {
        Map<String, ClusterManager.RegionLoad> regionLoads = source.getRegionLoads();

        String bestRegion = null;
        double bestScore = -1;

        for (Map.Entry<String, ClusterManager.RegionLoad> entry : regionLoads.entrySet()) {
            String regionId = entry.getKey();

            if (excludedRegions.contains(regionId)) {
                continue;
            }

            ClusterManager.RegionLoad load = entry.getValue();

            // 计算迁移分数：优先迁移大但不是很热的 Region
            double size = load.getStoreFileSize();
            double requests = load.getReadRequests() + load.getWriteRequests();

            // 分数 = 大小分数 - 热度惩罚
            double score = size / (1 + requests / 10000.0);

            if (score > bestScore) {
                bestScore = score;
                bestRegion = regionId;
            }
        }

        return bestRegion;
    }

    /**
     * 估算 Region 的负载权重
     */
    private double estimateRegionWeight(ClusterManager.RegionLoad load) {
        // 基于大小和请求量估算
        double sizeWeight = load.getStoreFileSize() / (100 * 1024 * 1024.0); // 每 100MB = 1 分
        double requestWeight = (load.getReadRequests() + load.getWriteRequests()) / 10000.0; // 每 10000 请求 = 1 分
        return sizeWeight + requestWeight;
    }

    /**
     * 检查是否需要立即均衡
     */
    public boolean needsImmediateRebalance(List<ClusterManager.ServerInfo> servers) {
        if (servers.size() <= 1) {
            return false;
        }

        // 检查是否有服务器严重过载
        int overloadedCount = 0;
        for (ClusterManager.ServerInfo server : servers) {
            if (loadCalculator.isOverloaded(server)) {
                overloadedCount++;
            }
        }

        // 如果超过 30% 的服务器过载，需要立即均衡
        return overloadedCount > servers.size() * 0.3;
    }

    /**
     * 获取服务器负载排名（用于调试）
     */
    public List<ServerLoadRank> getServerLoadRanking(List<ClusterManager.ServerInfo> servers) {
        List<ServerLoadRank> ranks = new ArrayList<>();

        for (ClusterManager.ServerInfo server : servers) {
            double score = loadCalculator.calculateLoadScore(server);
            double capacity = loadCalculator.getRemainingCapacity(server);
            int regionCount = server.getRegionLoads().size();

            ranks.add(new ServerLoadRank(
                server.getServerId(),
                score,
                capacity,
                regionCount
            ));
        }

        ranks.sort(Comparator.comparingDouble(ServerLoadRank::getLoadScore));
        return ranks;
    }

    private ServerId selectRandomServer(List<ClusterManager.ServerInfo> servers) {
        List<ClusterManager.ServerInfo> candidates = new ArrayList<>(servers);
        candidates.removeIf(loadCalculator::isOverloaded);
        List<ClusterManager.ServerInfo> effective = candidates.isEmpty() ? servers : candidates;
        return effective.get(random.nextInt(effective.size())).getServerId();
    }

    private synchronized ServerId selectRoundRobinServer(List<ClusterManager.ServerInfo> servers) {
        List<ClusterManager.ServerInfo> candidates = new ArrayList<>(servers);
        candidates.removeIf(loadCalculator::isOverloaded);
        List<ClusterManager.ServerInfo> effective = candidates.isEmpty() ? servers : candidates;
        int index = Math.floorMod(roundRobinIndex++, effective.size());
        return effective.get(index).getServerId();
    }

    private BalanceAction computeSimpleStrategyAction(List<ClusterManager.ServerInfo> servers) {
        long now = System.currentTimeMillis();
        if (now - lastBalanceTime < MIN_MIGRATION_INTERVAL) {
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
            target = targets.get(random.nextInt(targets.size()));
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

    /**
     * 服务器负载排名
     */
    public static class ServerLoadRank {
        private final ServerId serverId;
        private final double loadScore;
        private final double remainingCapacity;
        private final int regionCount;

        public ServerLoadRank(ServerId serverId, double loadScore, double remainingCapacity, int regionCount) {
            this.serverId = serverId;
            this.loadScore = loadScore;
            this.remainingCapacity = remainingCapacity;
            this.regionCount = regionCount;
        }

        public ServerId getServerId() { return serverId; }
        public double getLoadScore() { return loadScore; }
        public double getRemainingCapacity() { return remainingCapacity; }
        public int getRegionCount() { return regionCount; }
    }
}
