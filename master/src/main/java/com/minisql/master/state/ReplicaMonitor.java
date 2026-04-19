package com.minisql.master.state;

import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * 副本监控器
 * 负责维护 Region 副本的运行态指标；成员存活与故障收敛以 ZooKeeper 为准。
 */
public class ReplicaMonitor {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaMonitor.class);

    private final Map<String, List<ReplicaInfo>> regionReplicas = new ConcurrentHashMap<>();

    // 配置参数
    private final long replicationLagThresholdMs;

    // 故障回调接口
    public interface FailoverCallback {
        void onReplicaFailed(String regionId, ServerId failedReplica);
        void onReplicaLagging(String regionId, ServerId laggingReplica, long lagMs);
        default void onReplicaRecovered(String regionId, ServerId recoveredReplica) {
        }
    }

    private final List<FailoverCallback> callbacks = new CopyOnWriteArrayList<>();

    public ReplicaMonitor(ClusterManager clusterManager) {
        this(clusterManager, 10000);
    }

    public ReplicaMonitor(ClusterManager clusterManager,
                          long replicationLagThresholdMs) {
        this.replicationLagThresholdMs = replicationLagThresholdMs;
    }

    /**
     * 注册副本
     */
    public void registerReplica(String regionId, ReplicaInfo replica) {
        List<ReplicaInfo> replicas = regionReplicas.computeIfAbsent(regionId, k -> new CopyOnWriteArrayList<>());
        replicas.removeIf(existing -> existing.getServerId().equals(replica.getServerId()));
        replicas.add(replica);
        logger.info("Replica registered: {}", replica);
    }

    /**
     * 移除副本
     */
    public void removeReplica(String regionId, ServerId serverId) {
        List<ReplicaInfo> replicas = regionReplicas.get(regionId);
        if (replicas != null) {
            replicas.removeIf(r -> r.getServerId().equals(serverId));
            logger.info("Replica removed: {} from region: {}", serverId, regionId);
        }
    }

    public void removeRegion(String regionId) {
        if (regionReplicas.remove(regionId) != null) {
            logger.info("Replica monitor removed region: {}", regionId);
        }
    }

    /**
     * 获取 Region 的所有副本
     */
    public List<ReplicaInfo> getReplicas(String regionId) {
        return regionReplicas.getOrDefault(regionId, Collections.emptyList());
    }

    /**
     * 获取主副本
     */
    public ReplicaInfo getPrimary(String regionId) {
        List<ReplicaInfo> replicas = regionReplicas.get(regionId);
        if (replicas != null) {
            for (ReplicaInfo replica : replicas) {
                if (replica.isPrimary()) {
                    return replica;
                }
            }
        }
        return null;
    }

    /**
     * 获取从副本列表
     */
    public List<ReplicaInfo> getSecondaries(String regionId) {
        List<ReplicaInfo> replicas = regionReplicas.get(regionId);
        if (replicas != null) {
            List<ReplicaInfo> secondaries = new ArrayList<>();
            for (ReplicaInfo replica : replicas) {
                if (!replica.isPrimary()) {
                    secondaries.add(replica);
                }
            }
            return secondaries;
        }
        return Collections.emptyList();
    }

    /**
     * 更新副本心跳
     */
    public void updateHeartbeat(String regionId, ServerId serverId, long replicationLag) {
        List<ReplicaInfo> replicas = regionReplicas.get(regionId);
        if (replicas != null) {
            for (ReplicaInfo replica : replicas) {
                if (replica.getServerId().equals(serverId)) {
                    replica.heartbeat();
                    replica.setReplicationLag(replicationLag);

                    // 更新复制延迟状态
                    if (replicationLag > replicationLagThresholdMs) {
                        replica.setState(ReplicaInfo.ReplicaState.LAGGING);
                        notifyReplicaLagging(regionId, serverId, replicationLag);
                    } else if (replica.getState() == ReplicaInfo.ReplicaState.LAGGING) {
                        replica.setState(ReplicaInfo.ReplicaState.SECONDARY);
                    }
                    if (replica.getState() == ReplicaInfo.ReplicaState.OFFLINE) {
                        replica.setState(replica.isPrimary()
                            ? ReplicaInfo.ReplicaState.PRIMARY
                            : ReplicaInfo.ReplicaState.SECONDARY);
                        notifyReplicaRecovered(regionId, serverId);
                    }
                    return;
                }
            }
        }
    }


    /**
     * 注册故障回调
     */
    public void registerCallback(FailoverCallback callback) {
        callbacks.add(callback);
    }

    /**
     * 移除故障回调
     */
    public void removeCallback(FailoverCallback callback) {
        callbacks.remove(callback);
    }

    /**
     * 通知副本延迟
     */
    private void notifyReplicaLagging(String regionId, ServerId laggingReplica, long lagMs) {
        for (FailoverCallback callback : callbacks) {
            try {
                callback.onReplicaLagging(regionId, laggingReplica, lagMs);
            } catch (Exception e) {
                logger.error("Error in lag callback: {}", e.getMessage(), e);
            }
        }
    }

    private void notifyReplicaRecovered(String regionId, ServerId recoveredReplica) {
        for (FailoverCallback callback : callbacks) {
            try {
                callback.onReplicaRecovered(regionId, recoveredReplica);
            } catch (Exception e) {
                logger.error("Error in recovery callback: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * 获取所有区域的副本信息
     */
    public Map<String, List<ReplicaInfo>> getAllReplicas() {
        return new HashMap<>(regionReplicas);
    }

    /**
     * 获取健康副本数量
     */
    public int getHealthyReplicaCount(String regionId) {
        List<ReplicaInfo> replicas = regionReplicas.get(regionId);
        if (replicas == null) return 0;

        int count = 0;
        for (ReplicaInfo replica : replicas) {
            if (replica.isHealthy()) {
                count++;
            }
        }
        return count;
    }

    /**
     * 检查区域是否有足够数量的健康副本
     */
    public boolean hasMinimumHealthyReplicas(String regionId, int minimum) {
        return getHealthyReplicaCount(regionId) >= minimum;
    }

    /**
     * 获取复制延迟最大的副本
     */
    public ReplicaInfo getMostLaggingReplica(String regionId) {
        List<ReplicaInfo> replicas = regionReplicas.get(regionId);
        if (replicas == null || replicas.isEmpty()) return null;

        ReplicaInfo mostLagging = null;
        long maxLag = -1;

        for (ReplicaInfo replica : replicas) {
            if (replica.getReplicationLag() > maxLag) {
                maxLag = replica.getReplicationLag();
                mostLagging = replica;
            }
        }

        return mostLagging;
    }

    /**
     * 选择最健康的从副本（用于故障转移）
     */
    public ReplicaInfo selectHealthiestSecondary(String regionId) {
        List<ReplicaInfo> secondaries = getSecondaries(regionId);
        if (secondaries.isEmpty()) return null;

        ReplicaInfo best = null;
        long minLag = Long.MAX_VALUE;

        for (ReplicaInfo replica : secondaries) {
            if (replica.isHealthy() && replica.getReplicationLag() < minLag) {
                minLag = replica.getReplicationLag();
                best = replica;
            }
        }

        return best;
    }

    /**
     * 提升副本为主副本
     */
    public void promoteToPrimary(String regionId, ServerId serverId) {
        List<ReplicaInfo> replicas = regionReplicas.get(regionId);
        if (replicas != null) {
            for (ReplicaInfo replica : replicas) {
                if (replica.getServerId().equals(serverId)) {
                    // 降级当前的主副本
                    for (ReplicaInfo r : replicas) {
                        if (r.isPrimary()) {
                            r.setState(ReplicaInfo.ReplicaState.SECONDARY);
                            r.setLastPromotionTime(System.currentTimeMillis());
                        }
                    }
                    // 提升新的主副本
                    replica.setState(ReplicaInfo.ReplicaState.PRIMARY);
                    replica.setLastPromotionTime(System.currentTimeMillis());
                    logger.info("Promoted {} to primary for region: {}", serverId, regionId);
                    return;
                }
            }
        }
    }

    /**
     * 获取集群副本状态摘要
     */
    public String getClusterStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Cluster Replica Status ===\n");

        for (Map.Entry<String, List<ReplicaInfo>> entry : regionReplicas.entrySet()) {
            String regionId = entry.getKey();
            sb.append("Region: ").append(regionId).append("\n");

            for (ReplicaInfo replica : entry.getValue()) {
                sb.append("  - ").append(replica.getServerId())
                  .append(" [").append(replica.getState()).append("]")
                  .append(" lag: ").append(replica.getReplicationLag()).append("ms")
                  .append("\n");
            }
        }

        return sb.toString();
    }

}
