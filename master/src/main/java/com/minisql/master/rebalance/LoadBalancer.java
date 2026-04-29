package com.minisql.master.rebalance;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.state.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import com.minisql.master.rebalance.RegionMigrationCoordinator;

/**
 * 负载均衡器（改进版）
 * 基于综合负载指标进行 Region 分配和迁移决策
 */
public class LoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);
    private static final double DEFAULT_BALANCE_THRESHOLD = 20.0;
    private static final long DEFAULT_MIN_MIGRATION_INTERVAL_MS = 300000L;
    private static final double DEFAULT_MAX_TARGET_QPS = 5000.0;

    private final LoadCalculator loadCalculator = new LoadCalculator();
    private final Random random = new Random();
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
     * 负载计算器（内部类）
     * 综合计算服务器的负载分数，用于负载均衡决策
     *
     * 五个指标权重可通过 configureWeights 调整，使用时自动归一化为百分比。
     * 默认权重: CPU=25, 内存=25, 磁盘=20, Region数量=15, 请求负载=15
     */
    public static class LoadCalculator {

        // 可配置权重（相对值，使用时自动归一化）
        private volatile int cpuWeight = 25;
        private volatile int memoryWeight = 25;
        private volatile int diskWeight = 20;
        private volatile int regionCountWeight = 15;
        private volatile int requestWeight = 15;

        // EWMA 跟踪器（用于负载预测）
        private final Map<String, EwmaTracker> requestTrackers = new ConcurrentHashMap<>();
        private volatile double maxTargetQps = DEFAULT_MAX_TARGET_QPS;

        // EWMA 参数
        private volatile double ewmaAlpha = 0.3;
        private volatile double ewmaTrendThreshold = 5.0;
        private volatile int ewmaPredictionSteps = 2;

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
            double diskScore = 0;
            if (metrics.getTotalSpace() > 0) {
                diskScore = 100.0 * (metrics.getTotalSpace() - metrics.getAvailableSpace()) / metrics.getTotalSpace();
            }

            // 4. Region 数量分数（假设最大承载 100 个 Region）
            double regionScore = Math.min(100, regionLoads.size() * 100.0 / 100);

            // 5. 请求负载分数
            double requestScore = calculateRequestScore(server);

            // 加权计算综合分数（自动归一化）
            double totalWeight = cpuWeight + memoryWeight + diskWeight + regionCountWeight + requestWeight;
            if (totalWeight <= 0) {
                totalWeight = 100;
            }
            return (cpuScore * cpuWeight +
                    memoryScore * memoryWeight +
                    diskScore * diskWeight +
                    regionScore * regionCountWeight +
                    requestScore * requestWeight) / totalWeight;
        }

        /**
         * 计算请求负载分数
         * 使用 EWMA 预测的 QPS 替代简单均值，提供趋势感知
         */
        private double calculateRequestScore(ClusterManager.ServerInfo server) {
            long totalRequests = 0;
            for (ClusterManager.RegionLoad load : server.getRegionLoads().values()) {
                totalRequests += load.getReadRequests() + load.getWriteRequests();
            }

            // 更新 EWMA 跟踪器
            String serverId = server.getServerId().toString();
            EwmaTracker tracker = requestTrackers.computeIfAbsent(serverId, k -> new EwmaTracker());
            tracker.addSample(totalRequests);

            // 使用预测 QPS（考虑趋势方向）
            double predictedQps = tracker.getPredictedQps(ewmaPredictionSteps);

            double effectiveMaxTargetQps = maxTargetQps > 0.0 ? maxTargetQps : DEFAULT_MAX_TARGET_QPS;
            double baseScore = Math.min(100, (predictedQps / effectiveMaxTargetQps) * 100.0);

            // QPS 超过阈值时加惩罚，防止突发倾斜
            double penaltyThreshold = effectiveMaxTargetQps * 0.1;
            if (predictedQps > penaltyThreshold) {
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
            return calculateLoadScore(server) > 70;
        }

        /**
         * 判断服务器是否空闲
         */
        public boolean isIdle(ClusterManager.ServerInfo server) {
            return calculateLoadScore(server) < 30;
        }

        /**
         * EWMA（指数加权移动平均）跟踪器
         * 通过指数平滑计算 QPS 趋势，并预测未来负载。
         *
         * 算法：
         *   ewma_new = alpha * current_qps + (1 - alpha) * ewma_old
         *   trend = ewma_new - ewma_old
         *   predicted = ewma + trend * steps
         */
        private class EwmaTracker {
            private double ewma = 0;
            private double previousEwma = 0;
            private long lastTimestamp = 0;
            private long lastRequestCount = -1;
            private boolean initialized = false;

            synchronized void addSample(long totalRequests) {
                long now = System.currentTimeMillis();

                if (lastRequestCount >= 0 && lastTimestamp > 0) {
                    long timeDelta = now - lastTimestamp;
                    if (timeDelta > 0) {
                        long requestDelta = totalRequests - lastRequestCount;
                        double currentQps = (requestDelta * 1000.0) / timeDelta;

                        previousEwma = ewma;
                        double alpha = ewmaAlpha;
                        ewma = alpha * currentQps + (1 - alpha) * ewma;
                        initialized = true;
                    }
                }

                lastTimestamp = now;
                lastRequestCount = totalRequests;
            }

            synchronized double getPredictedQps(int steps) {
                if (!initialized) {
                    return 0;
                }
                double trend = ewma - previousEwma;
                double predicted = ewma + trend * steps;
                return Math.max(0, predicted);
            }

            synchronized double getTrend() {
                if (!initialized) {
                    return 0;
                }
                return ewma - previousEwma;
            }

            synchronized double getEwma() {
                return ewma;
            }
        }

        public void setMaxTargetQps(double maxTargetQps) {
            if (maxTargetQps <= 0.0) {
                return;
            }
            this.maxTargetQps = maxTargetQps;
        }

        /**
         * 配置 EWMA 参数
         *
         * @param alpha          平滑系数（0-1），越大越重视最新数据
         * @param trendThreshold 趋势判定阈值
         * @param predictionSteps 预测步数
         */
        public void configureEwma(double alpha, double trendThreshold, int predictionSteps) {
            if (alpha > 0 && alpha < 1) {
                this.ewmaAlpha = alpha;
            }
            if (trendThreshold > 0) {
                this.ewmaTrendThreshold = trendThreshold;
            }
            if (predictionSteps > 0 && predictionSteps <= 10) {
                this.ewmaPredictionSteps = predictionSteps;
            }
        }

        /**
         * 配置五个指标的权重（相对值，使用时自动归一化为百分比）
         *
         * @param cpu        CPU 使用率权重
         * @param memory     内存使用率权重
         * @param disk       磁盘使用率权重
         * @param regionCount Region 数量权重
         * @param request    请求负载权重
         */
        public void configureWeights(int cpu, int memory, int disk, int regionCount, int request) {
            if (cpu < 0 || memory < 0 || disk < 0 || regionCount < 0 || request < 0) {
                return;
            }
            if (cpu + memory + disk + regionCount + request == 0) {
                return;
            }
            this.cpuWeight = cpu;
            this.memoryWeight = memory;
            this.diskWeight = disk;
            this.regionCountWeight = regionCount;
            this.requestWeight = request;
        }
    }

    /** 获取负载计算器实例 */
    public LoadCalculator getLoadCalculator() {
        return loadCalculator;
    }

    /**
     * 配置负载计算器权重
     */
    public void configureWeights(int cpu, int memory, int disk, int regionCount, int request) {
        loadCalculator.configureWeights(cpu, memory, disk, regionCount, request);
    }

    /**
     * 配置 EWMA 负载预测参数
     */
    public void configureEwma(double alpha, double trendThreshold, int predictionSteps) {
        loadCalculator.configureEwma(alpha, trendThreshold, predictionSteps);
    }

    // 上次迁移时间
    private volatile long lastBalanceTime = 0;

    // 迁移预算控制
    private volatile int maxMigrationsPerRound = 3;
    private volatile RegionMigrationCoordinator migrationCoordinator;

    // Region 选择成本模型权重
    private volatile double regionBenefitWeight = 1.0;
    private volatile double regionCostWeight = 0.5;
    private volatile double regionWritePenaltyWeight = 0.8;
    private volatile double regionFitWeight = 0.3;

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

    public void setMaxTargetQps(double maxTargetQps) {
        loadCalculator.setMaxTargetQps(maxTargetQps);
    }

    /**
     * 配置 Region 选择成本模型的四个权重
     *
     * @param benefit      负载降低收益权重（默认 1.0）
     * @param cost         迁移传输成本权重（默认 0.5）
     * @param writePenalty 写密集惩罚权重（默认 0.8）
     * @param fit          目标服务器适配度权重（默认 0.3）
     */
    public void configureRegionSelectionWeights(double benefit, double cost, double writePenalty, double fit) {
        if (benefit >= 0) this.regionBenefitWeight = benefit;
        if (cost >= 0) this.regionCostWeight = cost;
        if (writePenalty >= 0) this.regionWritePenaltyWeight = writePenalty;
        if (fit >= 0) this.regionFitWeight = fit;
    }

    /**
     * 为 Region 选择最优的 RegionServer
     * 基于综合负载分数和热点感知选择最合适的服务器
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

        // 选择调整后剩余容量最大的服务器（扣除热点惩罚）
        ClusterManager.ServerInfo bestServer = candidates.stream()
            .max(Comparator.comparingDouble(server -> {
                double remainingCapacity = loadCalculator.getRemainingCapacity(server);
                // 热点惩罚：如果目标服务器已有同表的热点 Region，降低其优先级
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
        if (now - lastBalanceTime < minMigrationIntervalMs) {
            return actions; // 冷却期内不迁移
        }

        // 检查迁移预算
        int ongoingCount = migrationCoordinator != null
            ? migrationCoordinator.getOngoingMigrationCount() : 0;
        int budget = Math.max(0, maxMigrationsPerRound - ongoingCount);
        if (budget <= 0) {
            return actions; // 已达到最大并发迁移数
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
                String regionId = selectBestRegionToMove(overloaded, underloaded, scheduledRegions,
                    loadScores.get(overloaded), avgLoad);

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
            // 按预算截断迁移动作
            if (actions.size() > budget) {
                logger.info("Migration budget: {}/{} used, truncating {} actions to {}",
                    ongoingCount, maxMigrationsPerRound, actions.size(), budget);
                actions = new ArrayList<>(actions.subList(0, budget));
            }
            lastBalanceTime = now;
        }

        return actions;
    }

    /**
     * 选择最佳的 Region 进行迁移（多因子成本模型）
     *
     * 评分公式：
     *   score = W_benefit * load_reduction
     *         - W_cost * migration_cost
     *         - W_write * write_penalty
     *         + W_fit * target_fit
     *
     * @param source           源服务器
     * @param target           目标服务器
     * @param excludedRegions  已调度或不可迁移的 Region 集合
     * @param sourceScore      源服务器当前负载分数
     * @param avgLoad          集群平均负载分数
     */
    private String selectBestRegionToMove(ClusterManager.ServerInfo source,
                                          ClusterManager.ServerInfo target,
                                          Set<String> excludedRegions,
                                          double sourceScore,
                                          double avgLoad) {
        Map<String, ClusterManager.RegionLoad> regionLoads = source.getRegionLoads();

        // 计算源服务器总 QPS（用于 load_reduction 计算）
        long sourceTotalRequests = 0;
        for (ClusterManager.RegionLoad load : regionLoads.values()) {
            sourceTotalRequests += load.getReadRequests() + load.getWriteRequests();
        }

        String bestRegion = null;
        double bestScore = Double.NEGATIVE_INFINITY;

        for (Map.Entry<String, ClusterManager.RegionLoad> entry : regionLoads.entrySet()) {
            String regionId = entry.getKey();

            if (excludedRegions.contains(regionId)) {
                continue;
            }
            if (target != null && target.getRegionLoads().containsKey(regionId)) {
                continue;
            }

            ClusterManager.RegionLoad load = entry.getValue();

            // 因子1：负载降低收益 — 该 Region 对源服务器过载的贡献度
            double loadReduction = computeLoadReduction(load, sourceTotalRequests, sourceScore, avgLoad);

            // 因子2：迁移传输成本
            double migrationCost = computeMigrationCost(load);

            // 因子3：写密集惩罚
            double writePenalty = computeWritePenalty(load);

            // 因子4：目标服务器适配度
            double targetFit = computeTargetFit(load, target);

            double score = regionBenefitWeight * loadReduction
                         - regionCostWeight * migrationCost
                         - regionWritePenaltyWeight * writePenalty
                         + regionFitWeight * targetFit;

            if (score > bestScore) {
                bestScore = score;
                bestRegion = regionId;
            }
        }

        return bestRegion;
    }

    /**
     * 计算迁移该 Region 能降低的负载
     * 贡献度 = (regionQPS / serverQPS) * (sourceScore - avgLoad)
     */
    private double computeLoadReduction(ClusterManager.RegionLoad regionLoad,
                                         long sourceTotalRequests,
                                         double sourceScore,
                                         double avgLoad) {
        long regionRequests = regionLoad.getReadRequests() + regionLoad.getWriteRequests();
        double contribution = sourceTotalRequests > 0
            ? (double) regionRequests / sourceTotalRequests : 0;
        return contribution * Math.max(0, sourceScore - avgLoad);
    }

    /**
     * 计算迁移传输成本（以 100MB 为单位）
     */
    private double computeMigrationCost(ClusterManager.RegionLoad load) {
        return load.getStoreFileSize() / (100.0 * 1024 * 1024);
    }

    /**
     * 计算写密集惩罚
     * 写比例越高惩罚越大（二次方增长）
     */
    private double computeWritePenalty(ClusterManager.RegionLoad load) {
        long reads = load.getReadRequests();
        long writes = load.getWriteRequests();
        long total = reads + writes;
        if (total == 0) {
            return 0;
        }
        double writeRatio = (double) writes / total;
        return writeRatio * writeRatio * 10.0; // 惩罚因子
    }

    /**
     * 计算目标服务器接收 Region 后的容量余量
     * 余量 = 1 - (targetScore + regionWeight) / 100
     */
    private double computeTargetFit(ClusterManager.RegionLoad load, ClusterManager.ServerInfo target) {
        if (target == null) {
            return 0;
        }
        double targetScore = loadCalculator.calculateLoadScore(target);
        double regionWeight = estimateRegionWeight(load);
        return Math.max(0, 1.0 - (targetScore + regionWeight) / 100.0);
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
     * 维护热点 Region 与其所属表的映射关系，供负载均衡放置决策时参考。
     */
    public static class HotSpotRegistry {
        private final Map<String, String> regionToTable = new ConcurrentHashMap<>();

        /**
         * 更新热点信息
         * @param hotSpots    当前热点 Region 映射（regionId -> HotSpotInfo）
         * @param tableResolver 通过 regionId 解析 tableName 的函数
         */
        public void updateHotSpots(Map<String, ?> hotSpots, java.util.function.Function<String, String> tableResolver) {
            // 清除已消失的热点
            regionToTable.keySet().retainAll(hotSpots.keySet());
            // 更新或新增热点映射
            for (String regionId : hotSpots.keySet()) {
                String tableName = tableResolver != null ? tableResolver.apply(regionId) : null;
                if (tableName != null) {
                    regionToTable.put(regionId, tableName);
                }
            }
        }

        /**
         * 清除指定 Region 的热点标记
         */
        public void clearHotSpot(String regionId) {
            regionToTable.remove(regionId);
        }

        /**
         * 统计指定服务器上同表热点 Region 的数量
         * @param tableName     表名
         * @param regionIdsOnServer 服务器上的所有 Region ID 集合
         * @return 同表热点 Region 数量
         */
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

        /**
         * 判断指定 Region 是否为热点
         */
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
