package com.minisql.master.rebalance;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.recover.RecoveryCoordinator;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.zookeeper.DistributedLock;
import com.minisql.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Shared helper methods used by RegionSplitCoordinator, RegionMergeCoordinator,
 * and RegionMigrationCoordinator to avoid duplicating the same logic across all three.
 */
class RebalanceSupport {

    private static final Logger logger = LoggerFactory.getLogger(RebalanceSupport.class);

    final ClusterManager clusterManager;
    final MetadataManager metadataManager;
    final LoadBalancer loadBalancer;

    volatile RecoveryCoordinator recoveryCoordinator;
    volatile ReplicationCoordinator replicationCoordinator;
    volatile ReplicaMonitor replicaMonitor;
    volatile ReplicaLifecycleManager lifecycleManager;
    volatile MonitoringService monitoringService;
    volatile ZkClient zkClient;

    RebalanceSupport(ClusterManager clusterManager,
                     MetadataManager metadataManager,
                     LoadBalancer loadBalancer) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.loadBalancer = loadBalancer;
    }

    // --- Replica topology ---

    void ensureReplicaTopology(String regionId) {
        Region region = metadataManager.getRegion(regionId);
        if (region == null || region.getPrimary() == null) {
            return;
        }

        int targetReplicationFactor = resolveReplicationFactor(region);
        List<ServerId> selectedServers = selectServersForReplication(region, targetReplicationFactor);
        if (selectedServers.isEmpty()) {
            selectedServers.add(region.getPrimary());
        }

        region.setPrimary(selectedServers.get(0));
        region.setReplicas(new ArrayList<>(selectedServers));
        metadataManager.registerRegionForTable(region, region.getPrimary());
        clusterManager.assignRegionToServer(regionId, region.getPrimary());
        clusterManager.updateRegionState(regionId, Region.State.OPEN);
        for (ServerId server : selectedServers) {
            clusterManager.addReplica(regionId, server);
        }

        if (recoveryCoordinator != null) {
            for (int i = 1; i < selectedServers.size(); i++) {
                recoveryCoordinator.bootstrapReplica(regionId, selectedServers.get(i));
            }
        }
    }

    List<ServerId> selectServersForReplication(Region region, int replicationFactor) {
        int normalizedFactor = Math.max(1, replicationFactor);
        LinkedHashSet<ServerId> selected = new LinkedHashSet<>();
        if (region.getPrimary() != null) {
            selected.add(region.getPrimary());
        }
        if (region.getReplicas() != null) {
            selected.addAll(region.getReplicas());
        }

        List<ClusterManager.ServerInfo> candidates = new ArrayList<>(clusterManager.getActiveServersList());
        candidates.removeIf(info -> info == null || info.getServerId() == null || selected.contains(info.getServerId()));
        while (selected.size() < normalizedFactor && !candidates.isEmpty()) {
            ServerId serverId = loadBalancer.selectServerForRegion(region, candidates);
            if (serverId == null) {
                break;
            }
            selected.add(serverId);
            candidates.removeIf(info -> serverId.equals(info.getServerId()));
        }
        return new ArrayList<>(selected);
    }

    int resolveReplicationFactor(Region region) {
        com.minisql.common.model.Table table = metadataManager.getTable(region.getTableName());
        if (table != null && table.getProperties() != null) {
            return Math.max(1, table.getProperties().getReplicationFactor());
        }
        return 3;
    }

    // --- ReplicaMonitor sync ---

    /**
     * Remove stale servers from ReplicaMonitor and register any servers
     * present in Region metadata but missing from ReplicaMonitor.
     * Must be called after Region metadata has been updated.
     */
    void syncReplicaMonitor(String regionId, ServerId... removeServers) {
        if (replicaMonitor == null) return;

        for (ServerId server : removeServers) {
            replicaMonitor.removeReplica(regionId, server);
        }

        Region region = metadataManager.getRegion(regionId);
        if (region == null || region.getReplicas() == null) return;

        LinkedHashSet<ServerId> tracked = new LinkedHashSet<>();
        for (ReplicaInfo ri : replicaMonitor.getReplicas(regionId)) {
            tracked.add(ri.getServerId());
        }

        for (ServerId server : region.getReplicas()) {
            if (!tracked.contains(server)) {
                boolean isPrimary = server.equals(region.getPrimary());
                replicaMonitor.registerReplica(regionId, new ReplicaInfo(
                    regionId, server, null, null, null,
                    isPrimary ? ReplicaInfo.ReplicaState.PRIMARY : ReplicaInfo.ReplicaState.SECONDARY));
            }
        }
    }

    // --- Runtime cleanup ---

    void cleanupRegionRuntime(String regionId, String tableName) {
        clusterManager.removeRegionMetadata(tableName, regionId);
        if (replicaMonitor != null) {
            replicaMonitor.removeRegion(regionId);
        }
        if (lifecycleManager != null) {
            lifecycleManager.removeRegion(regionId);
        }
        if (recoveryCoordinator != null) {
            recoveryCoordinator.clearDesiredReplicaCount(regionId);
        }
        if (replicationCoordinator != null) {
            replicationCoordinator.removeReplicaGroup(regionId);
        }
    }

    // --- Distributed lock ---

    DistributedLock acquireRegionLock(String regionId) throws Exception {
        if (zkClient == null) {
            return null;
        }
        DistributedLock lock = new DistributedLock(zkClient.getClient(),
            "/minisql/locks/regions/" + regionId);
        lock.acquire();
        return lock;
    }

    void releaseLock(DistributedLock lock) {
        if (lock == null) {
            return;
        }
        try {
            if (lock.isAcquiredInThisProcess()) {
                lock.release();
            }
        } catch (Exception e) {
            logger.warn("Failed to release rebalance lock: {}", e.getMessage(), e);
        }
    }

    // --- Event recording ---

    void recordEvent(String type, String severity, String regionId, ServerId serverId,
                     String message, String details) {
        if (monitoringService != null) {
            monitoringService.recordEvent(type, severity, regionId, null,
                serverId == null ? null : serverId.getHost() + ":" + serverId.getPort(),
                null, message, details);
        }
    }

    void recordEvent(String type, String severity, String regionId, ServerId sourceServer,
                     ServerId targetServer, String message, String details) {
        if (monitoringService != null) {
            monitoringService.recordEvent(type, severity, regionId, null,
                sourceServer == null ? null : sourceServer.getHost() + ":" + sourceServer.getPort(),
                targetServer == null ? null : targetServer.getHost() + ":" + targetServer.getPort(),
                message, details);
        }
    }

    // --- Proto conversion ---

    static Region convertProtoToRegion(com.minisql.common.proto.CommonProto.RegionInfo proto) {
        Region region = new Region();
        region.setRegionId(proto.getRegionId());
        region.setTableName(proto.getTableName());
        region.setStartKey(proto.getStartKey().toByteArray());
        region.setEndKey(proto.getEndKey().toByteArray());
        return region;
    }
}
