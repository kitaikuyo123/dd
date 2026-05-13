package com.minisql.replication;

import com.minisql.common.model.ServerId;

import java.util.List;

/**
 * 拓扑信息提供者接口
 *
 * 解耦 ReplicaGroup 与拓扑存储，使拓扑可以来自 Region 元数据（Master 端）
 * 或本地状态（RegionServer 端）。
 * 默认提供空实现的可选变更方法，只读提供者无需覆写。
 */
public interface TopologyProvider {

    ServerId getPrimary();

    List<ServerId> getReplicas();

    default void setPrimary(ServerId primary) {}

    default void addReplica(ServerId replica) {}

    default void removeReplica(ServerId replica) {}
}
