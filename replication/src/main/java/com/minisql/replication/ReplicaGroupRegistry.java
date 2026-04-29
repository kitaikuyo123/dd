package com.minisql.replication;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 副本组注册表，维护 Region 到副本组的映射关系 */
public class ReplicaGroupRegistry {

    private final Map<String, ReplicaGroup> replicaGroups = new ConcurrentHashMap<>();

    public ReplicaGroup createReplicaGroup(Region region, List<ServerId> replicaServers, int replicationFactor) {
        if (replicaServers == null || replicaServers.isEmpty()) {
            throw new IllegalArgumentException("At least one server is required for replication group creation");
        }

        String regionId = region.getRegionId();
        ReplicaGroup group = new ReplicaGroup(regionId);
        group.setPrimary(replicaServers.get(0));
        for (int i = 0; i < replicationFactor && i < replicaServers.size(); i++) {
            ServerId serverId = replicaServers.get(i);
            group.addReplica(serverId, i == 0 ? ReplicaRole.PRIMARY : ReplicaRole.SECONDARY);
            group.updateReplicaState(serverId, 0L, 0L);
        }
        replicaGroups.put(regionId, group);
        return group;
    }

    public ReplicaGroup getReplicaGroup(String regionId) {
        return replicaGroups.get(regionId);
    }

    public Map<String, ReplicaGroup> getAllReplicaGroups() {
        return Collections.unmodifiableMap(replicaGroups);
    }

    public void removeReplicaGroup(String regionId) {
        replicaGroups.remove(regionId);
    }

    public void addReplica(String regionId, ServerId replica, ReplicaRole role) {
        ReplicaGroup group = requireGroup(regionId);
        group.addReplica(replica, role);
    }

    public void removeReplica(String regionId, ServerId replica) {
        ReplicaGroup group = replicaGroups.get(regionId);
        if (group != null) {
            group.removeReplica(replica);
        }
    }

    public void promoteToPrimary(String regionId, ServerId newPrimary) {
        ReplicaGroup group = requireGroup(regionId);
        if (!group.getReplicas().contains(newPrimary)) {
            throw new IllegalArgumentException("Server is not a replica: " + newPrimary);
        }
        group.setPrimary(newPrimary);
        for (ServerId replica : group.getReplicas()) {
            group.setReplicaRole(replica, replica.equals(newPrimary) ? ReplicaRole.PRIMARY : ReplicaRole.SECONDARY);
        }
    }

    public void updateReplicaProgress(String regionId, ServerId replica, long appliedSequenceId, long lagInBytes) {
        requireGroup(regionId).updateReplicaState(replica, appliedSequenceId, lagInBytes);
    }

    public void updateReplicaRole(String regionId, ServerId replica, ReplicaRole role) {
        requireGroup(regionId).setReplicaRole(replica, role);
    }

    public void recordPrimaryProgress(String regionId, long sequenceId) {
        ReplicaGroup group = requireGroup(regionId);
        ServerId primary = group.getPrimary();
        if (primary != null) {
            group.updateReplicaState(primary, sequenceId, 0L);
            group.setReplicaRole(primary, ReplicaRole.PRIMARY);
        }
    }

    private ReplicaGroup requireGroup(String regionId) {
        ReplicaGroup group = replicaGroups.get(regionId);
        if (group == null) {
            throw new IllegalArgumentException("Replica group not found: " + regionId);
        }
        return group;
    }
}
