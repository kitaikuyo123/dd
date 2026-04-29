package com.minisql.replication;

import com.minisql.common.model.ServerId;

/** 主副本故障转移协调器，管理主副本自动切换流程 */
public class PrimaryFailoverCoordinator {

    private final ReplicaGroupRegistry registry;
    private final ReplicationConfig config;
    private final PrimaryChangeNotifier notifier;

    public PrimaryFailoverCoordinator(ReplicaGroupRegistry registry,
                                      ReplicationConfig config,
                                      PrimaryChangeNotifier notifier) {
        this.registry = registry;
        this.config = config;
        this.notifier = notifier;
    }

    public void promoteToPrimary(String regionId, ServerId newPrimary) {
        ReplicaGroup group = registry.getReplicaGroup(regionId);
        if (group == null) {
            throw new IllegalArgumentException("Replica group not found: " + regionId);
        }
        if (!group.getReplicas().contains(newPrimary)) {
            throw new IllegalArgumentException("Server is not a replica: " + newPrimary);
        }

        ServerId oldPrimary = group.getPrimary();
        registry.promoteToPrimary(regionId, newPrimary);
        notifier.notifyPrimaryChange(regionId, oldPrimary, newPrimary);
    }

    public void failover(String regionId) {
        ReplicaGroup group = registry.getReplicaGroup(regionId);
        if (group == null) {
            throw new IllegalArgumentException("Replica group not found: " + regionId);
        }

        ServerId bestReplica = null;
        long bestSequence = Long.MIN_VALUE;
        long bestUpdate = Long.MIN_VALUE;
        ServerId currentPrimary = group.getPrimary();

        for (ServerId replica : group.getReplicas()) {
            if (replica.equals(currentPrimary)) {
                continue;
            }

            ReplicaRole role = group.getReplicaRole(replica);
            if (!role.canBePromoted()) {
                continue;
            }

            ReplicaGroup.ReplicaState state = group.getReplicaState(replica);
            if (!isHealthy(state)) {
                continue;
            }

            long appliedSequence = state != null ? state.getLastAppliedSequenceId() : 0L;
            long lastUpdate = state != null ? state.getLastUpdateTime() : 0L;
            if (appliedSequence > bestSequence || (appliedSequence == bestSequence && lastUpdate > bestUpdate)) {
                bestReplica = replica;
                bestSequence = appliedSequence;
                bestUpdate = lastUpdate;
            }
        }

        if (bestReplica == null) {
            throw new IllegalStateException("No suitable replica found for failover in region: " + regionId);
        }

        promoteToPrimary(regionId, bestReplica);
    }

    private boolean isHealthy(ReplicaGroup.ReplicaState state) {
        if (state == null) {
            return true;
        }
        if (state.getLastUpdateTime() == 0L) {
            return true;
        }
        // An idle replica (no replication activity) is considered healthy.
        // Staleness only matters when replication was previously active but stalled.
        if (!state.isReplicationActive()) {
            return true;
        }
        long unhealthyThreshold = config.getHealthCheckIntervalMs() * 3;
        return System.currentTimeMillis() - state.getLastUpdateTime() <= unhealthyThreshold;
    }
}
