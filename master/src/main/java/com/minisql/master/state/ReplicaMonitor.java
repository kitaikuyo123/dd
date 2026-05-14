package com.minisql.master.state;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * 副本健康监控器
 *
 * 只存储运行时健康数据（心跳、延迟、状态），不存储拓扑列表。
 * 拓扑（primary + replicas 列表）从 Region 元数据实时读取，
 * 消除了手动同步拓扑的负担。
 */
public class ReplicaMonitor {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaMonitor.class);

    private final MetadataManager metadataManager;
    private final long replicationLagThresholdMs;

    // 运行时健康状态：regionId → (serverId → ReplicaInfo)
    private final Map<String, Map<ServerId, ReplicaInfo>> healthStates = new ConcurrentHashMap<>();

    public interface FailoverCallback {
        void onReplicaFailed(String regionId, ServerId failedReplica);
        void onReplicaLagging(String regionId, ServerId laggingReplica, long lagMs);
        default void onReplicaRecovered(String regionId, ServerId recoveredReplica) {
        }
    }

    private final List<FailoverCallback> callbacks = new CopyOnWriteArrayList<>();

    public ReplicaMonitor(ClusterManager clusterManager, MetadataManager metadataManager) {
        this(clusterManager, metadataManager, 10000);
    }

    public ReplicaMonitor(ClusterManager clusterManager, MetadataManager metadataManager,
                          long replicationLagThresholdMs) {
        this.metadataManager = metadataManager;
        this.replicationLagThresholdMs = replicationLagThresholdMs;
    }

    /**
     * 注册/更新副本的运行时健康状态
     */
    public void registerReplica(String regionId, ReplicaInfo replica) {
        healthStates.computeIfAbsent(regionId, k -> new ConcurrentHashMap<>())
            .put(replica.getServerId(), replica);
        logger.info("Replica registered: {}", replica);
    }

    /**
     * 移除副本的运行时健康状态
     */
    public void removeReplica(String regionId, ServerId serverId) {
        Map<ServerId, ReplicaInfo> health = healthStates.get(regionId);
        if (health != null) {
            health.remove(serverId);
            logger.info("Replica removed: {} from region: {}", serverId, regionId);
        }
    }

    /**
     * 移除 Region 的所有运行时健康状态
     */
    public void removeRegion(String regionId) {
        healthStates.remove(regionId);
        logger.info("Replica monitor removed region: {}", regionId);
    }

    /**
     * 获取 Region 的所有副本（拓扑从 Region 元数据读取，叠加运行时健康数据）
     */
    public List<ReplicaInfo> getReplicas(String regionId) {
        Region region = metadataManager.getRegion(regionId);
        if (region == null || region.getReplicas() == null) {
            return Collections.emptyList();
        }

        Map<ServerId, ReplicaInfo> health = healthStates
            .computeIfAbsent(regionId, k -> new ConcurrentHashMap<>());
        ServerId primary = region.getPrimary();

        List<ReplicaInfo> result = new ArrayList<>();
        for (ServerId server : region.getReplicas()) {
            ReplicaInfo ri = health.computeIfAbsent(server, s -> {
                ReplicaInfo info = new ReplicaInfo(regionId, s, null, null, null);
                info.setState(s.equals(primary)
                    ? ReplicaInfo.ReplicaState.PRIMARY
                    : ReplicaInfo.ReplicaState.SECONDARY);
                return info;
            });
            result.add(ri);
        }
        return result;
    }

    /**
     * 获取主副本（从 Region 元数据读取，叠加运行时健康数据）
     */
    public ReplicaInfo getPrimary(String regionId) {
        Region region = metadataManager.getRegion(regionId);
        if (region == null) return null;
        ServerId primary = region.getPrimary();
        if (primary == null) return null;

        Map<ServerId, ReplicaInfo> health = healthStates.get(regionId);
        if (health != null) {
            ReplicaInfo ri = health.get(primary);
            if (ri != null) return ri;
        }
        return new ReplicaInfo(regionId, primary, null, null, null, ReplicaInfo.ReplicaState.PRIMARY);
    }

    /**
     * 提升副本为主副本（只更新运行时健康状态，拓扑由 Region 元数据管理）
     */
    public void promoteToPrimary(String regionId, ServerId serverId) {
        Map<ServerId, ReplicaInfo> health = healthStates.get(regionId);
        if (health == null) return;

        for (ReplicaInfo ri : health.values()) {
            if (ri.getState() == ReplicaInfo.ReplicaState.PRIMARY) {
                ri.setState(ReplicaInfo.ReplicaState.SECONDARY);
                ri.setLastPromotionTime(System.currentTimeMillis());
            }
        }

        ReplicaInfo target = health.get(serverId);
        if (target != null) {
            target.setState(ReplicaInfo.ReplicaState.PRIMARY);
            target.setLastPromotionTime(System.currentTimeMillis());
            logger.info("Promoted {} to primary for region: {}", serverId, regionId);
        }
    }

    /**
     * 更新副本心跳
     */
    public void updateHeartbeat(String regionId, ServerId serverId, long replicationLag) {
        Map<ServerId, ReplicaInfo> health = healthStates.get(regionId);
        if (health == null) return;

        ReplicaInfo replica = health.get(serverId);
        if (replica == null) return;

        replica.heartbeat();
        replica.setReplicationLag(replicationLag);

        if (replicationLag > replicationLagThresholdMs) {
            replica.setState(ReplicaInfo.ReplicaState.LAGGING);
            notifyReplicaLagging(regionId, serverId, replicationLag);
        } else if (replica.getState() == ReplicaInfo.ReplicaState.LAGGING) {
            replica.setState(ReplicaInfo.ReplicaState.SECONDARY);
        }
        if (replica.getState() == ReplicaInfo.ReplicaState.OFFLINE) {
            Region region = metadataManager.getRegion(regionId);
            boolean isPrimary = region != null && serverId.equals(region.getPrimary());
            replica.setState(isPrimary
                ? ReplicaInfo.ReplicaState.PRIMARY
                : ReplicaInfo.ReplicaState.SECONDARY);
            notifyReplicaRecovered(regionId, serverId);
        }
    }

    /**
     * 注册故障回调
     */
    public void registerCallback(FailoverCallback callback) {
        callbacks.add(callback);
    }

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
}
