package com.minisql.common.model;

/**
 * 副本信息
 * 用于跟踪 Region 副本的状态和信息
 */
public class ReplicaInfo {

    /**
     * 副本状态枚举
     */
    public enum ReplicaState {
        PRIMARY,      // 主副本
        SECONDARY,    // 从副本（正常）
        LAGGING,      // 从副本（复制延迟）
        OFFLINE,      // 副本离线
        INITIALIZING  // 初始化中
    }

    private String regionId;
    private ServerId serverId;
    private ReplicaState state;
    private volatile long lastHeartbeat;
    private volatile long replicationLag;  // 复制延迟（毫秒）

    // MySQL 配置信息
    private String mysqlUrl;
    private String mysqlUser;
    private String mysqlPassword;

    private long createdTime;
    private long lastPromotionTime;  // 上次提升为主副本的时间

    public ReplicaInfo() {
        this.createdTime = System.currentTimeMillis();
        this.lastHeartbeat = System.currentTimeMillis();
        this.replicationLag = 0;
        this.state = ReplicaState.INITIALIZING;
    }

    public ReplicaInfo(String regionId, ServerId serverId, String mysqlUrl, String mysqlUser, String mysqlPassword) {
        this();
        this.regionId = regionId;
        this.serverId = serverId;
        this.mysqlUrl = mysqlUrl;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;
    }

    public ReplicaInfo(String regionId, ServerId serverId, String mysqlUrl, String mysqlUser, String mysqlPassword, ReplicaState state) {
        this(regionId, serverId, mysqlUrl, mysqlUser, mysqlPassword);
        this.state = state;
    }

    // Getters and Setters

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public ServerId getServerId() {
        return serverId;
    }

    public void setServerId(ServerId serverId) {
        this.serverId = serverId;
    }

    public ReplicaState getState() {
        return state;
    }

    public void setState(ReplicaState state) {
        this.state = state;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public long getReplicationLag() {
        return replicationLag;
    }

    public void setReplicationLag(long replicationLag) {
        this.replicationLag = replicationLag;
    }

    public String getMysqlUrl() {
        return mysqlUrl;
    }

    public void setMysqlUrl(String mysqlUrl) {
        this.mysqlUrl = mysqlUrl;
    }

    public String getMysqlUser() {
        return mysqlUser;
    }

    public void setMysqlUser(String mysqlUser) {
        this.mysqlUser = mysqlUser;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getLastPromotionTime() {
        return lastPromotionTime;
    }

    public void setLastPromotionTime(long lastPromotionTime) {
        this.lastPromotionTime = lastPromotionTime;
    }

    /**
     * 检查副本是否健康
     */
    public boolean isHealthy() {
        return state == ReplicaState.PRIMARY || state == ReplicaState.SECONDARY;
    }

    /**
     * 检查副本是否是主副本
     */
    public boolean isPrimary() {
        return state == ReplicaState.PRIMARY;
    }

    /**
     * 更新心跳时间
     */
    public void heartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
    }

    /**
     * 检查是否超时
     *
     * @param timeoutMs 超时时间（毫秒）
     * @return 如果超时返回 true
     */
    public boolean isTimeout(long timeoutMs) {
        return System.currentTimeMillis() - lastHeartbeat > timeoutMs;
    }

    @Override
    public String toString() {
        return "ReplicaInfo{" +
                "regionId='" + regionId + '\'' +
                ", serverId=" + serverId +
                ", state=" + state +
                ", replicationLag=" + replicationLag + "ms" +
                ", mysqlUrl='" + mysqlUrl + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicaInfo that = (ReplicaInfo) o;

        if (!regionId.equals(that.regionId)) return false;
        return serverId.equals(that.serverId);
    }

    @Override
    public int hashCode() {
        int result = regionId.hashCode();
        result = 31 * result + serverId.hashCode();
        return result;
    }
}
