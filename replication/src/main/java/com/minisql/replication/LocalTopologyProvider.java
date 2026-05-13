package com.minisql.replication;

import com.minisql.common.model.ServerId;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 本地可变拓扑提供者
 *
 * 用于 RegionServer 端和测试，拓扑在内存中独立维护。
 */
public class LocalTopologyProvider implements TopologyProvider {

    private volatile ServerId primary;
    private final CopyOnWriteArrayList<ServerId> replicas = new CopyOnWriteArrayList<>();

    public LocalTopologyProvider() {
    }

    public LocalTopologyProvider(ServerId primary, List<ServerId> replicas) {
        this.primary = primary;
        if (replicas != null) {
            this.replicas.addAll(replicas);
        }
    }

    @Override
    public ServerId getPrimary() {
        return primary;
    }

    @Override
    public List<ServerId> getReplicas() {
        return Collections.unmodifiableList(replicas);
    }

    @Override
    public void setPrimary(ServerId primary) {
        this.primary = primary;
        if (primary != null && !replicas.contains(primary)) {
            replicas.addIfAbsent(primary);
        }
    }

    @Override
    public void addReplica(ServerId replica) {
        replicas.addIfAbsent(replica);
    }

    @Override
    public void removeReplica(ServerId replica) {
        replicas.remove(replica);
        if (replica != null && replica.equals(primary)) {
            primary = null;
        }
    }
}
