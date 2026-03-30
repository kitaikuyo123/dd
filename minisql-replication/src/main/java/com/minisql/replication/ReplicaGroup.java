package com.minisql.replication;

import com.minisql.common.model.ServerId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 副本组
 * 管理一个Region的所有副本
 * 负责模块: 开发者C
 */
public class ReplicaGroup {

    private final String regionId;
    private volatile ServerId primary;
    private final List<ServerId> replicas;
    private final Map<ServerId, ReplicaState> replicaStates;
    private final Map<ServerId, ReplicaRole> replicaRoles;

    public ReplicaGroup(String regionId) {
        this.regionId = regionId;
        this.replicas = new CopyOnWriteArrayList<>();
        this.replicaStates = new ConcurrentHashMap<>();
        this.replicaRoles = new ConcurrentHashMap<>();
    }

    public void setPrimary(ServerId primary) {
        ServerId previousPrimary = this.primary;
        this.primary = primary;
        if (previousPrimary != null && !previousPrimary.equals(primary)) {
            replicaRoles.put(previousPrimary, ReplicaRole.SECONDARY);
        }
        if (primary != null) {
            addReplica(primary, ReplicaRole.PRIMARY);
            replicaRoles.put(primary, ReplicaRole.PRIMARY);
        }
    }

    public ServerId getPrimary() {
        return primary;
    }

    public void addReplica(ServerId replica) {
        addReplica(replica, ReplicaRole.SECONDARY);
    }

    public void addReplica(ServerId replica, ReplicaRole role) {
        if (!replicas.contains(replica)) {
            replicas.add(replica);
            replicaStates.put(replica, new ReplicaState());
        }
        replicaRoles.put(replica, role);
        ReplicaState state = replicaStates.computeIfAbsent(replica, ignored -> new ReplicaState());
        state.setRole(role);
        if (state.getLastUpdateTime() == 0) {
            state.setLastUpdateTime(System.currentTimeMillis());
        }
    }

    public void removeReplica(ServerId replica) {
        replicas.remove(replica);
        replicaStates.remove(replica);
        replicaRoles.remove(replica);
        if (replica.equals(primary)) {
            primary = null;
        }
    }

    public List<ServerId> getReplicas() {
        return Collections.unmodifiableList(replicas);
    }

    public String getRegionId() {
        return regionId;
    }

    public void updateReplicaState(ServerId replica, long appliedSequenceId, long lagInBytes) {
        ReplicaState state = replicaStates.computeIfAbsent(replica, ignored -> new ReplicaState());
        state.setLastAppliedSequenceId(appliedSequenceId);
        state.setReplicationLag(lagInBytes);
        state.setLastUpdateTime(System.currentTimeMillis());
        ReplicaRole role = replicaRoles.get(replica);
        if (role != null) {
            state.setRole(role);
        }
    }

    public ReplicaState getReplicaState(ServerId replica) {
        return replicaStates.get(replica);
    }

    public ReplicaRole getReplicaRole(ServerId replica) {
        return replicaRoles.getOrDefault(replica, ReplicaRole.SECONDARY);
    }

    public void setReplicaRole(ServerId replica, ReplicaRole role) {
        if (!replicas.contains(replica)) {
            addReplica(replica, role);
            return;
        }
        replicaRoles.put(replica, role);
        ReplicaState state = replicaStates.computeIfAbsent(replica, ignored -> new ReplicaState());
        state.setRole(role);
        state.setLastUpdateTime(System.currentTimeMillis());
        if (role == ReplicaRole.PRIMARY) {
            primary = replica;
        }
    }

    public Map<ServerId, ReplicaRole> getReplicaRoles() {
        return Collections.unmodifiableMap(replicaRoles);
    }

    /**
     * 副本状态
     */
    public static class ReplicaState {
        private volatile long lastAppliedSequenceId;
        private volatile long replicationLag;
        private volatile long lastUpdateTime;
        private volatile ReplicaRole role = ReplicaRole.SECONDARY;

        public long getLastAppliedSequenceId() {
            return lastAppliedSequenceId;
        }

        public void setLastAppliedSequenceId(long lastAppliedSequenceId) {
            this.lastAppliedSequenceId = lastAppliedSequenceId;
        }

        public long getReplicationLag() {
            return replicationLag;
        }

        public void setReplicationLag(long replicationLag) {
            this.replicationLag = replicationLag;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void setLastUpdateTime(long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public ReplicaRole getRole() {
            return role;
        }

        public void setRole(ReplicaRole role) {
            this.role = role;
        }
    }
}
