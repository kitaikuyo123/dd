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
import com.minisql.replication.ReplicationCoordinator;
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
    private ReplicationCoordinator replicationCoordinator;

    // 配置参数
    private final int maxFailoverRetries;
    private final long baseFailoverCooldownMs;
    private final long maxFailoverCooldownMs;
    private final long failoverTimeoutMs;

    // 故障转移历史记录：regionId -> FailoverState
    private final Map<String, FailoverState> failoverStates = new ConcurrentHashMap<>();

    // 正在进行的故障转移：regionId -> Future
    private final Map<String, Future<?>> ongoingFailovers = new ConcurrentHashMap<>();

    // Sentinel value used during the atomic check-and-reserve window in triggerFailover
    private static final Future<?> FAILOVER_SENTINEL = new CompletableFuture<>();

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

        FailoverState() {
            this.lastFailoverTime = 0;
            this.failoverCount = 0;
            this.currentCooldownMs = 0;
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
            new GrpcRegionServerCommandClient(clusterManager), 3, 30000, 300000, 10000, 60000, 3);
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
        this(clusterManager, metadataManager, replicaMonitor, lifecycleManager,
            commandClient, maxFailoverRetries, baseFailoverCooldownMs,
            maxFailoverCooldownMs, failoverTimeoutMs, emergencyFailoverThresholdMs, 3);
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
                               long emergencyFailoverThresholdMs,
                               int threadPoolSize) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.replicaMonitor = replicaMonitor;
        this.lifecycleManager = lifecycleManager;
        this.commandClient = commandClient;
        this.maxFailoverRetries = maxFailoverRetries;
        this.baseFailoverCooldownMs = baseFailoverCooldownMs;
        this.maxFailoverCooldownMs = maxFailoverCooldownMs;
        this.failoverTimeoutMs = failoverTimeoutMs;

        int poolSize = Math.max(1, threadPoolSize);
        this.executor = Executors.newFixedThreadPool(poolSize, r -> {
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

    public void setReplicationCoordinator(ReplicationCoordinator replicationCoordinator) {
        this.replicationCoordinator = replicationCoordinator;
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

        // 检查故障转移次数是否超过限制
        if (state.failoverCount >= maxFailoverRetries && !emergency) {
            logger.error("Max failover retries ({}) reached for region: {}. Manual intervention required.",
                        maxFailoverRetries, regionId);
            return;
        }

        // Record failover NOW to set cooldown before submitting to executor.
        // This prevents a race where the executor task hasn't called recordFailover
        // yet by the time the next triggerFailover checks isInCooldown.
        state.recordFailover(baseFailoverCooldownMs, maxFailoverCooldownMs);

        // Atomically check-and-reserve to prevent concurrent submissions
        if (ongoingFailovers.putIfAbsent(regionId, FAILOVER_SENTINEL) != null) {
            logger.warn("Failover already in progress for region: {}", regionId);
            return;
        }

        Future<?> future = executor.submit(() -> {
            try {
                executeFailover(regionId);
            } finally {
                ongoingFailovers.remove(regionId);
            }
        });

        // Replace sentinel with real future
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

            // 4. 更新路由信息：先更新 MetadataManager 再更新 ClusterManager
            updateMetadataPrimary(regionId, newPrimary.getServerId());
            clusterManager.updateRegionAssignment(regionId, newPrimary.getServerId());

            // 5. 更新 ZooKeeper
            updateZooKeeper(regionId, newPrimary.getServerId());

            // 6. 通知相关组件
            notifyFailoverComplete(regionId, newPrimary);

            logger.info("Failover completed for region: {} new primary: {} (took {}ms)",
                       regionId, newPrimary.getServerId(), (System.currentTimeMillis() - startTime));
            recordEvent("FAILOVER_TRIGGERED", "INFO", regionId, null, null, toServerName(newPrimary.getServerId()),
                "Failover completed", "durationMs=" + (System.currentTimeMillis() - startTime));

        } catch (Exception e) {
            logger.error("Failover failed for region: {}: {}", regionId, e.getMessage(), e);
            recordEvent("FAILOVER_TRIGGERED", "ERROR", regionId, null, null, null,
                "Failover failed", e.getMessage());
        } finally {
            releaseLock(lock);
        }
    }

    /**
     * 选择新的主副本
     */
    private ReplicaInfo selectNewPrimary(String regionId) {
        List<ReplicaInfo> replicas = replicaMonitor.getReplicas(regionId);
        ReplicaInfo candidate = null;
        long minLag = Long.MAX_VALUE;

        for (ReplicaInfo replica : replicas) {
            if (replica == null || replica.isPrimary() || !replica.isHealthy()) {
                continue;
            }
            if (!clusterManager.isServerActive(replica.getServerId())) {
                logger.info("Skip inactive failover candidate {} for region {}",
                    replica.getServerId(), regionId);
                continue;
            }
            if (replica.getReplicationLag() < minLag) {
                minLag = replica.getReplicationLag();
                candidate = replica;
            }
        }

        if (candidate == null) {
            logger.info("No healthy active secondary found for failover in region {}", regionId);
            return null;
        }

        if (candidate.getReplicationLag() > failoverTimeoutMs) {
            logger.warn("Candidate replica has too much lag: {}ms",
                candidate.getReplicationLag());
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
            if (replica.isPrimary()) continue;
            if (!clusterManager.isServerActive(replica.getServerId())) continue;

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
            if (!clusterManager.isServerActive(targetPrimary)) {
                logger.error("Target replica is not active for manual failover: {}", targetPrimary);
                return;
            }
            if (!promoteReplica(regionId, targetPrimary)) {
                logger.error("Failed to promote target replica: {}", targetPrimary);
                return;
            }
            replicaMonitor.promoteToPrimary(regionId, targetPrimary);
            updateMetadataPrimary(regionId, targetPrimary);
            clusterManager.updateRegionAssignment(regionId, targetPrimary);
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
}
