package com.minisql.replication;

import com.minisql.common.model.ServerId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 副本组
 *
 * 管理一个 Region 的副本运行时状态（同步位点、角色、lag 等）。
 * 拓扑信息（primary、replica 列表）委托给 TopologyProvider，
 * 不再独立存储，避免与 Region 元数据不一致。
 */
public class ReplicaGroup {

    private final String regionId;
    private final TopologyProvider topology;

    private final Map<ServerId, ReplicaState> replicaStates;
    private final Map<ServerId, ReplicaRole> replicaRoles;

    public ReplicaGroup(String regionId, TopologyProvider topology) {
        this.regionId = regionId;
        this.topology = topology;
        this.replicaStates = new ConcurrentHashMap<>();
        this.replicaRoles = new ConcurrentHashMap<>();
    }

    /** 向后兼容：创建使用 LocalTopologyProvider 的实例 */
    public ReplicaGroup(String regionId) {
        this(regionId, new LocalTopologyProvider());
    }

    public void setPrimary(ServerId primary) {
        ServerId previousPrimary = this.topology.getPrimary();
        this.topology.setPrimary(primary);
        if (previousPrimary != null && !previousPrimary.equals(primary)) {
            replicaRoles.put(previousPrimary, ReplicaRole.SECONDARY);
        }
        if (primary != null) {
            ensureRuntimeState(primary);
            replicaRoles.put(primary, ReplicaRole.PRIMARY);
        }
    }

    public ServerId getPrimary() {
        return topology.getPrimary();
    }

    public void addReplica(ServerId replica) {
        addReplica(replica, ReplicaRole.SECONDARY);
    }

    public void addReplica(ServerId replica, ReplicaRole role) {
        this.topology.addReplica(replica);
        ensureRuntimeState(replica);
        replicaRoles.put(replica, role);
        ReplicaState state = replicaStates.get(replica);
        state.setRole(role);
        if (state.getLastUpdateTime() == 0) {
            state.setLastUpdateTime(System.currentTimeMillis());
        }
    }

    public void removeReplica(ServerId replica) {
        this.topology.removeReplica(replica);
        replicaStates.remove(replica);
        replicaRoles.remove(replica);
    }

    public List<ServerId> getReplicas() {
        return topology.getReplicas();
    }

    public String getRegionId() {
        return regionId;
    }

    public void updateReplicaState(ServerId replica, long appliedSequenceId, long lagInBytes) {
        ReplicaState state = replicaStates.computeIfAbsent(replica, ignored -> new ReplicaState());
        state.setLastAppliedSequenceId(appliedSequenceId);
        state.setReplicationLag(lagInBytes);
        state.setLastUpdateTime(System.currentTimeMillis());
        state.setReplicationActive(true);
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
        topology.addReplica(replica);
        ensureRuntimeState(replica);
        replicaRoles.put(replica, role);
        ReplicaState state = replicaStates.computeIfAbsent(replica, ignored -> new ReplicaState());
        state.setRole(role);
        state.setLastUpdateTime(System.currentTimeMillis());
        if (role == ReplicaRole.PRIMARY) {
            topology.setPrimary(replica);
        }
    }

    public Map<ServerId, ReplicaRole> getReplicaRoles() {
        return Collections.unmodifiableMap(replicaRoles);
    }

    private void ensureRuntimeState(ServerId replica) {
        replicaStates.computeIfAbsent(replica, ignored -> new ReplicaState());
    }

    /**
     * 副本状态
     */
    public static class ReplicaState {
        private volatile long lastAppliedSequenceId;
        private volatile long replicationLag;
        private volatile long lastUpdateTime;
        private volatile boolean replicationActive;
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

        public boolean isReplicationActive() {
            return replicationActive;
        }

        public void setReplicationActive(boolean replicationActive) {
            this.replicationActive = replicationActive;
        }
    }
}
