package com.minisql.replication;

/**
 * 副本角色枚举
 * 定义 Region 副本在复制组中的角色
 */
public enum ReplicaRole {
    /**
     * 主副本
     * - 处理所有写请求
     * - 负责将数据复制到从副本
     * - 只有一个主副本
     */
    PRIMARY,

    /**
     * 从副本
     * - 接收来自主副本的复制数据
     * - 可以处理读请求（读写分离）
     * - 多个从副本
     */
    SECONDARY,

    /**
     * 候选副本
     * - 正在加入复制组，进行全量同步
     * - 不参与读写请求处理
     * - 同步完成后自动转为 SECONDARY
     */
    CANDIDATE,

    /**
     * 观察者副本
     * - 接收复制数据但不参与多数派确认
     * - 用于备份或分析场景
     */
    OBSERVER;

    /**
     * 检查角色是否可以处理写请求
     */
    public boolean canWrite() {
        return this == PRIMARY;
    }

    /**
     * 检查角色是否可以处理读请求
     */
    public boolean canRead() {
        return this == PRIMARY || this == SECONDARY;
    }

    /**
     * 检查角色是否可以被选为主副本
     */
    public boolean canBePromoted() {
        return this == SECONDARY || this == CANDIDATE;
    }

    /**
     * 检查角色是否参与复制确认
     */
    public boolean participatesInQuorum() {
        return this == PRIMARY || this == SECONDARY;
    }
}
