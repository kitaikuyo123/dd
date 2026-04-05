package com.minisql.master.recover;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.rpc.GrpcRegionServerCommandClient;
import com.minisql.master.rpc.RegionServerCommandClient;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.zookeeper.DistributedLock;

import java.util.*;
import java.util.concurrent.*;

/**
 * 自动故障转移管理器
 * 负责在副本故障时自动选举新的主副本并更新路由信息
 *
 * 改进特性：
 * - 指数退避冷却机制
 * - 紧急故障转移模式
 * - 故障转移计数器
 * - SLF4J 日志记录
 */
public class FailoverCoordinator {

    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final ReplicaMonitor replicaMonitor;
    private final ReplicaLifecycleManager lifecycleManager;
    private final RegionServerCommandClient commandClient;
    private final ExecutorService executor;

    // 配置参数
    private final int maxFailoverRetries;
    private final long baseFailoverCooldownMs;
    private final long maxFailoverCooldownMs;
    private final long failoverTimeoutMs;
    private final long emergencyFailoverThresholdMs;

    // 故障转移历史记录：regionId -> FailoverState
    private final Map<String, FailoverState> failoverStates = new ConcurrentHashMap<>();

    // 正在进行的故障转移：regionId -> Future
    private final Map<String, Future<?>> ongoingFailovers = new ConcurrentHashMap<>();

    // ZooKeeper 客户端
    private com.minisql.zookeeper.ZkClient zkClient;
    private MonitoringService monitoringService;

    // SLF4J 日志
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FailoverCoordinator.class);

    /**
     * 故障转移状态（包含计数器和冷却时间）
     */
    private static class FailoverState {
        long lastFailoverTime;
        int failoverCount;
        long currentCooldownMs;
        long lastSuccessfulFailoverTime;

        FailoverState() {
            this.lastFailoverTime = 0;
            this.failoverCount = 0;
            this.currentCooldownMs = 0;
            this.lastSuccessfulFailoverTime = 0;
        }

        /**
         * 应用指数退避
         */
        void recordFailover(long baseCooldown, long maxCooldown) {
            this.lastFailoverTime = System.currentTimeMillis();
            this.failoverCount++;

            // 指数退避：每次失败后冷却时间翻倍，但不超过最大值
            this.currentCooldownMs = Math.min(
                baseCooldown * (1L << Math.min(failoverCount, 10)),
                maxCooldown
            );
        }

        /**
         * 记录成功的故障转移，重置计数器
         */
        void recordSuccess() {
            this.lastSuccessfulFailoverTime = System.currentTimeMillis();
            // 成功完成后，减少故障计数（但不完全清零）
            this.failoverCount = Math.max(0, failoverCount - 1);
            this.currentCooldownMs = 0;
        }

        /**
         * 检查是否在冷却期内
         */
        boolean isInCooldown() {
            if (currentCooldownMs <= 0) return false;
            return System.currentTimeMillis() - lastFailoverTime < currentCooldownMs;
        }

        /**
         * 获取剩余的冷却时间（毫秒）
         */
        long getRemainingCooldownMs() {
            if (currentCooldownMs <= 0) return 0;
            long elapsed = System.currentTimeMillis() - lastFailoverTime;
            return Math.max(0, currentCooldownMs - elapsed);
        }
    }

    public FailoverCoordinator(ClusterManager clusterManager,
                               MetadataManager metadataManager,
                               ReplicaMonitor replicaMonitor,
                               ReplicaLifecycleManager lifecycleManager) {
        this(clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            new GrpcRegionServerCommandClient(clusterManager), 3, 30000, 300000, 10000, 60000);
    }

    public FailoverCoordinator(ClusterManager clusterManager,
                               MetadataManager metadataManager,
                               ReplicaMonitor replicaMonitor,
                               ReplicaLifecycleManager lifecycleManager,
                               RegionServerCommandClient commandClient,
                               int maxFailoverRetries,
                               long baseFailoverCooldownMs,
                               long maxFailoverCooldownMs,
                               long failoverTimeoutMs,
                               long emergencyFailoverThresholdMs) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.replicaMonitor = replicaMonitor;
        this.lifecycleManager = lifecycleManager;
        this.commandClient = commandClient;
        this.maxFailoverRetries = maxFailoverRetries;
        this.baseFailoverCooldownMs = baseFailoverCooldownMs;
        this.maxFailoverCooldownMs = maxFailoverCooldownMs;
        this.failoverTimeoutMs = failoverTimeoutMs;
        this.emergencyFailoverThresholdMs = emergencyFailoverThresholdMs;

        this.executor = Executors.newFixedThreadPool(3, r -> {
            Thread t = new Thread(r, "Failover-Worker");
            t.setDaemon(true);
            return t;
        });

        // 注册故障回调
        replicaMonitor.registerCallback(new ReplicaMonitor.FailoverCallback() {
            @Override
            public void onReplicaFailed(String regionId, ServerId failedReplica) {
                lifecycleManager.transition(regionId, failedReplica,
                    ReplicaLifecycleManager.ReplicaLifecycleState.OFFLINE,
                    "Replica failure detected");
                ReplicaInfo primary = replicaMonitor.getPrimary(regionId);
                if (primary != null && primary.getServerId().equals(failedReplica)) {
                    logger.info("Primary replica failed, triggering failover for region: {}", regionId);
                    triggerFailover(regionId, false);
                }
            }

            @Override
            public void onReplicaLagging(String regionId, ServerId laggingReplica, long lagMs) {
                lifecycleManager.transition(regionId, laggingReplica,
                    ReplicaLifecycleManager.ReplicaLifecycleState.LAGGING,
                    "Replica lagging: " + lagMs);
                logger.warn("Replica lagging detected: {} for region: {} with lag: {}ms",
                          laggingReplica, regionId, lagMs);
            }
        });
    }

    /**
     * 设置 ZooKeeper 客户端
     */
    public void setZkClient(com.minisql.zookeeper.ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void setMonitoringService(MonitoringService monitoringService) {
        this.monitoringService = monitoringService;
    }

    /**
     * 触发故障转移（普通模式）
     */
    public void triggerFailover(String regionId) {
        triggerFailover(regionId, false);
    }

    /**
     * 触发故障转移
     * @param emergency 是否紧急模式（跳过冷却时间）
     */
    public void triggerFailover(String regionId, boolean emergency) {
        FailoverState state = failoverStates.computeIfAbsent(regionId, k -> new FailoverState());

        // 检查冷却时间（紧急模式跳过）
        if (!emergency && state.isInCooldown()) {
            logger.warn("Failover cooldown in effect for region: {}, remaining: {}ms",
                       regionId, state.getRemainingCooldownMs());
            return;
        }

        // 检查是否已有正在进行的故障转移
        if (ongoingFailovers.containsKey(regionId)) {
            logger.warn("Failover already in progress for region: {}", regionId);
            return;
        }

        // 检查故障转移次数是否超过限制
        if (state.failoverCount >= maxFailoverRetries && !emergency) {
            logger.error("Max failover retries ({}) reached for region: {}. Manual intervention required.",
                        maxFailoverRetries, regionId);
            return;
        }

        // 提交故障转移任务
        Future<?> future = executor.submit(() -> {
            try {
                executeFailover(regionId);
            } finally {
                ongoingFailovers.remove(regionId);
            }
        });

        ongoingFailovers.put(regionId, future);
        logger.info("Failover task submitted for region: {}", regionId);
    }

    /**
     * 紧急故障转移（当数据完全不可用时立即触发）
     */
    public void triggerEmergencyFailover(String regionId) {
        logger.warn("EMERGENCY FAILOVER triggered for region: {}", regionId);
        recordEvent("FAILOVER_TRIGGERED", "WARN", regionId, null, null, null,
            "Emergency failover triggered", null);
        triggerFailover(regionId, true);
    }

    /**
     * 执行故障转移流程
     */
    private void executeFailover(String regionId) {
        DistributedLock lock = null;
        logger.info("Starting failover for region: {}", regionId);

        try {
            lock = acquireRegionLock(regionId);
            // 1. 选择新的主副本
            ReplicaInfo newPrimary = selectNewPrimary(regionId);
            if (newPrimary == null) {
                logger.error("No suitable replica found for failover in region: {}", regionId);
                recordEvent("FAILOVER_TRIGGERED", "ERROR", regionId, null, null, null,
                    "Failover aborted: no suitable replica", null);
                return;
            }

            // 2. 等待故障转移超时内完成
            long startTime = System.currentTimeMillis();

            // 3. 提升为新的主副本
            if (!isCandidateCaughtUp(regionId, newPrimary.getServerId())) {
                lifecycleManager.transition(regionId, newPrimary.getServerId(),
                    ReplicaLifecycleManager.ReplicaLifecycleState.FAILED,
                    "Failover candidate is not caught up");
                logger.error("Refusing to promote lagging replica {} for region: {}",
                        newPrimary.getServerId(), regionId);
                return;
            }
            lifecycleManager.transition(regionId, newPrimary.getServerId(),
                ReplicaLifecycleManager.ReplicaLifecycleState.PROMOTING,
                "Promoting failover candidate");
            if (!promoteReplica(regionId, newPrimary.getServerId())) {
                lifecycleManager.transition(regionId, newPrimary.getServerId(),
                    ReplicaLifecycleManager.ReplicaLifecycleState.FAILED,
                    "Promotion RPC failed");
                logger.error("Failed to promote replica {} for region: {}",
                        newPrimary.getServerId(), regionId);
                recordEvent("FAILOVER_TRIGGERED", "ERROR", regionId, null, null, toServerName(newPrimary.getServerId()),
                    "Failover promotion RPC failed", null);
                return;
            }
            replicaMonitor.promoteToPrimary(regionId, newPrimary.getServerId());
            recordEvent("PRIMARY_PROMOTED", "INFO", regionId, null, null, toServerName(newPrimary.getServerId()),
                "Primary promoted during failover", null);
            lifecycleManager.transition(regionId, newPrimary.getServerId(),
                ReplicaLifecycleManager.ReplicaLifecycleState.PRIMARY_READY,
                "Failover completed");

            // 4. 更新 ClusterManager 的路由信息
            clusterManager.updateRegionAssignment(regionId, newPrimary.getServerId());
            updateMetadataPrimary(regionId, newPrimary.getServerId());

            // 5. 更新 ZooKeeper
            updateZooKeeper(regionId, newPrimary.getServerId());

            // 6. 记录故障转移历史（应用指数退避）
            FailoverState state = failoverStates.computeIfAbsent(regionId, k -> new FailoverState());
            state.recordFailover(baseFailoverCooldownMs, maxFailoverCooldownMs);

            // 7. 通知相关组件
            notifyFailoverComplete(regionId, newPrimary);

            logger.info("Failover completed for region: {} new primary: {} (took {}ms)",
                       regionId, newPrimary.getServerId(), (System.currentTimeMillis() - startTime));
            recordEvent("FAILOVER_TRIGGERED", "INFO", regionId, null, null, toServerName(newPrimary.getServerId()),
                "Failover completed", "durationMs=" + (System.currentTimeMillis() - startTime));

        } catch (Exception e) {
            logger.error("Failover failed for region: {}: {}", regionId, e.getMessage(), e);
            recordEvent("FAILOVER_TRIGGERED", "ERROR", regionId, null, null, null,
                "Failover failed", e.getMessage());

            // 记录失败的故障转移
            FailoverState state = failoverStates.computeIfAbsent(regionId, k -> new FailoverState());
            state.recordFailover(baseFailoverCooldownMs, maxFailoverCooldownMs);
        } finally {
            releaseLock(lock);
        }
    }

    /**
     * 选择新的主副本
     */
    private ReplicaInfo selectNewPrimary(String regionId) {
        // 使用 ReplicaMonitor 选择最健康的从副本
        ReplicaInfo candidate = replicaMonitor.selectHealthiestSecondary(regionId);

        if (candidate == null) {
            // 没有健康的从副本，尝试从 ClusterManager 获取活跃服务器
            logger.info("No healthy secondary found, checking active servers...");

            // 获取该 Region 的所有副本
            List<ReplicaInfo> replicas = replicaMonitor.getReplicas(regionId);
            for (ReplicaInfo replica : replicas) {
                if (replica.isHealthy() && !replica.isPrimary()) {
                    return replica;
                }
            }

            return null;
        }

        // 验证候选副本的复制延迟
        if (candidate.getReplicationLag() > failoverTimeoutMs) {
            logger.warn("Candidate replica has too much lag: {}ms",
                             candidate.getReplicationLag());
            // 尝试寻找下一个最佳候选
            return findNextBestCandidate(regionId, candidate);
        }

        return candidate;
    }

    /**
     * 寻找下一个最佳候选
     */
    private ReplicaInfo findNextBestCandidate(String regionId, ReplicaInfo exclude) {
        List<ReplicaInfo> replicas = replicaMonitor.getReplicas(regionId);
        ReplicaInfo best = null;
        long minLag = Long.MAX_VALUE;

        for (ReplicaInfo replica : replicas) {
            if (replica == exclude) continue;
            if (!replica.isHealthy()) continue;

            if (replica.getReplicationLag() < minLag) {
                minLag = replica.getReplicationLag();
                best = replica;
            }
        }

        return best;
    }

    /**
     * 更新 ZooKeeper 中的主副本信息
     */
    private void updateZooKeeper(String regionId, ServerId newPrimary) {
        // The authoritative primary path now lives under /minisql/tables/... and is
        // updated through MetadataManager.registerRegionForTable().
        updateMetadataPrimary(regionId, newPrimary);
    }

    /**
     * 通知故障转移完成
     */
    private void updateMetadataPrimary(String regionId, ServerId newPrimary) {
        if (metadataManager == null) {
            return;
        }

        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            logger.warn("Region {} not found in metadata during failover", regionId);
            return;
        }

        region.setPrimary(newPrimary);
        if (!region.getReplicas().contains(newPrimary)) {
            region.addReplica(newPrimary);
        }
        metadataManager.registerRegionForTable(region, newPrimary);
    }

    private void notifyFailoverComplete(String regionId, ReplicaInfo newPrimary) {
        // 这里可以添加通知其他组件的逻辑
        // 例如：通过 gRPC 通知 RegionServer、更新负载均衡器等
        logger.info("Notifying components about failover completion for region: {}", regionId);
    }

    private void recordEvent(String type, String severity, String regionId, String tableName,
                             String sourceServer, String targetServer, String message, String details) {
        if (monitoringService != null) {
            monitoringService.recordEvent(type, severity, regionId, tableName, sourceServer, targetServer, message, details);
        }
    }

    private String toServerName(ServerId serverId) {
        return serverId == null ? null : serverId.getHost() + ":" + serverId.getPort();
    }

    /**
     * 手动触发故障转移（用于维护操作）
     */
    public void manualFailover(String regionId, ServerId targetPrimary) {
        DistributedLock lock = null;
        logger.info("Manual failover requested for region: {} to: {}", regionId, targetPrimary);

        try {
            lock = acquireRegionLock(regionId);
            ReplicaInfo targetReplica = null;
            List<ReplicaInfo> replicas = replicaMonitor.getReplicas(regionId);
            for (ReplicaInfo replica : replicas) {
                if (replica.getServerId().equals(targetPrimary)) {
                    targetReplica = replica;
                    break;
                }
            }

            if (targetReplica == null) {
                logger.error("Target replica not found: {}", targetPrimary);
                return;
            }

            if (!isCandidateCaughtUp(regionId, targetPrimary)) {
                logger.error("Target replica is not caught up enough for manual failover: {}", targetPrimary);
                return;
            }
            if (!promoteReplica(regionId, targetPrimary)) {
                logger.error("Failed to promote target replica: {}", targetPrimary);
                return;
            }
            replicaMonitor.promoteToPrimary(regionId, targetPrimary);
            clusterManager.updateRegionAssignment(regionId, targetPrimary);
            updateMetadataPrimary(regionId, targetPrimary);
            updateZooKeeper(regionId, targetPrimary);

            FailoverState state = failoverStates.computeIfAbsent(regionId, k -> new FailoverState());
            state.recordSuccess();

            logger.info("Manual failover completed: {} is now primary for region: {}",
                       targetPrimary, regionId);
        } catch (Exception e) {
            logger.error("Manual failover failed for region {}: {}", regionId, e.getMessage(), e);
        } finally {
            releaseLock(lock);
        }
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
            logger.warn("Failed to release failover lock for region", e);
        }
    }

    /**
     * 获取故障转移历史（regionId -> 故障转移次数）
     */
    public Map<String, Integer> getFailoverHistory() {
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<String, FailoverState> entry : failoverStates.entrySet()) {
            result.put(entry.getKey(), entry.getValue().failoverCount);
        }
        return result;
    }

    /**
     * 获取故障转移状态
     */
    public FailoverState getFailoverState(String regionId) {
        return failoverStates.get(regionId);
    }

    /**
     * 清除故障转移历史（用于测试）
     */
    public void clearFailoverHistory() {
        failoverStates.clear();
    }

    /**
     * 检查是否可以进行故障转移
     */
    public boolean canFailover(String regionId) {
        FailoverState state = failoverStates.get(regionId);

        // 检查冷却时间
        if (state != null && state.isInCooldown()) {
            logger.debug("Failover cooldown in effect for region: {}, remaining: {}ms",
                        regionId, state.getRemainingCooldownMs());
            return false;
        }

        // 检查是否有正在进行的故障转移
        if (ongoingFailovers.containsKey(regionId)) {
            return false;
        }

        // 检查是否有可用的候选副本
        ReplicaInfo candidate = selectNewPrimary(regionId);
        return candidate != null && isCandidateCaughtUp(regionId, candidate.getServerId());
    }

    /**
     * 获取正在进行的故障转移列表
     */
    private boolean isCandidateCaughtUp(String regionId, ServerId candidate) {
        List<ServerId> replicas = clusterManager.getReplicaServers(regionId);
        if (replicas == null || replicas.isEmpty()) {
            return true;
        }

        long candidateSeq = clusterManager.getReplicaSequenceId(regionId, candidate);
        long maxKnownSeq = candidateSeq;
        for (ServerId replica : replicas) {
            maxKnownSeq = Math.max(maxKnownSeq, clusterManager.getReplicaSequenceId(regionId, replica));
        }

        if (candidateSeq < maxKnownSeq) {
            logger.warn("Candidate {} for region {} is behind: candidateSeq={}, maxKnownSeq={}",
                    candidate, regionId, candidateSeq, maxKnownSeq);
            return false;
        }
        return true;
    }

    private boolean promoteReplica(String regionId, ServerId targetServer) {
        long fencingToken = clusterManager.getFencingToken(regionId) + 1;
        try {
            if (clusterManager.getFencingToken(regionId) >= fencingToken) {
                return true;
            }
            if (!commandClient.promoteToPrimary(targetServer, regionId, fencingToken).getStatus().getSuccess()) {
                logger.error("Promote RPC failed for region {} on {}", regionId, targetServer);
                return false;
            }
            clusterManager.updateFencingToken(regionId, fencingToken);
            return true;
        } catch (Exception e) {
            logger.error("Failed to promote replica {} for region {}: {}", targetServer, regionId, e.getMessage(), e);
            return false;
        }
    }

    public Set<String> getOngoingFailovers() {
        return ongoingFailovers.keySet();
    }

    /**
     * 停止故障转移管理器
     */
    public void shutdown() {
        // 取消正在进行的故障转移
        for (Future<?> future : ongoingFailovers.values()) {
            future.cancel(true);
        }
        ongoingFailovers.clear();

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("FailoverCoordinator shut down");
    }

    /**
     * 获取故障转移状态摘要
     */
    public String getStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Failover Manager Status ===\n");
        sb.append("Ongoing failovers: ").append(ongoingFailovers.size()).append("\n");
        sb.append("Failover history: ").append(failoverStates.size()).append(" regions\n");
        sb.append("Base cooldown period: ").append(baseFailoverCooldownMs / 1000).append("s\n");
        sb.append("Max cooldown period: ").append(maxFailoverCooldownMs / 1000).append("s\n");
        return sb.toString();
    }

    /**
     * 获取详细的故障转移状态
     */
    public String getDetailedStatus() {
        StringBuilder sb = new StringBuilder(getStatus());
        sb.append("\n=== Region Status ===\n");
        for (Map.Entry<String, FailoverState> entry : failoverStates.entrySet()) {
            FailoverState state = entry.getValue();
            sb.append("  ").append(entry.getKey())
              .append(": count=").append(state.failoverCount)
              .append(", cooldown=").append(state.currentCooldownMs)
              .append(", inCooldown=").append(state.isInCooldown())
              .append("\n");
        }
        return sb.toString();
    }
}
