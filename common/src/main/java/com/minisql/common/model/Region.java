package com.minisql.common.model;

import com.minisql.common.utils.BytesUtil;

import java.io.Serializable;
import java.util.*;

/**
 * 数据分片Region
 */
public class Region implements Serializable, Comparable<Region> {
    private static final long serialVersionUID = 1L;

    // Region状态
    public enum State {
        INIT,           // 初始化
        OPENING,        // 正在打开
        OPEN,           // 正常运行
        CLOSING,        // 正在关闭
        CLOSED,         // 已关闭
        OFFLINE,        // 离线（故障）
        SPLITTING,      // 正在分裂
        SPLIT,          // 已分裂
        MERGING,        // 正在合并
        FAILED          // 失败
    }

    private String regionId;                    // Region唯一标识
    private String tableName;                   // 所属表名
    private byte[] startKey;                    // 起始Key（包含）
    private byte[] endKey;                      // 结束Key（不包含）
    private List<ServerId> replicas;            // 副本所在服务器列表
    private ServerId primary;                   // 主副本
    private State state;                        // Region状态
    private long createTime;                    // 创建时间
    private long lastUpdateTime;                // 最后更新时间
    private long estimatedSize;                 // 估计大小（字节）
    private long readRequestCount;              // 读请求计数
    private long writeRequestCount;             // 写请求计数

    public Region() {
        this.replicas = new ArrayList<>();
        this.state = State.INIT;
        this.createTime = System.currentTimeMillis();
        this.lastUpdateTime = this.createTime;
    }

    public Region(String regionId, String tableName, byte[] startKey, byte[] endKey) {
        this();
        this.regionId = regionId;
        this.tableName = tableName;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    /**
     * 检查key是否属于该Region
     */
    public boolean contains(byte[] key) {
        return BytesUtil.isKeyInRange(key, startKey, endKey);
    }

    /**
     * 与另一个Region是否相邻
     */
    public boolean isAdjacent(Region other) {
        return Arrays.equals(this.endKey, other.startKey) ||
                Arrays.equals(other.endKey, this.startKey);
    }

    /**
     * 判断Region是否为空（无数据范围）
     */
    public boolean isEmpty() {
        return Arrays.equals(startKey, endKey);
    }

    /**
     * 获取Region大小估计值（MB）
     */
    public double getSizeInMB() {
        return estimatedSize / (1024.0 * 1024.0);
    }

    /**
     * 记录读请求
     */
    public void recordRead() {
        this.readRequestCount++;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 记录写请求
     */
    public void recordWrite() {
        this.writeRequestCount++;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    // Getters and Setters
    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    public List<ServerId> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<ServerId> replicas) {
        this.replicas = replicas;
    }

    public void addReplica(ServerId serverId) {
        if (!this.replicas.contains(serverId)) {
            this.replicas.add(serverId);
        }
    }

    public void removeReplica(ServerId serverId) {
        this.replicas.remove(serverId);
    }

    public ServerId getPrimary() {
        return primary;
    }

    public void setPrimary(ServerId primary) {
        this.primary = primary;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }

    public void setEstimatedSize(long estimatedSize) {
        this.estimatedSize = estimatedSize;
    }

    public long getReadRequestCount() {
        return readRequestCount;
    }

    public long getWriteRequestCount() {
        return writeRequestCount;
    }

    public void setWriteRequestCount(long writeRequestCount) {
        this.writeRequestCount = writeRequestCount;
    }

    @Override
    public int compareTo(Region other) {
        return BytesUtil.compareTo(this.startKey, other.startKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Region region = (Region) o;
        return Objects.equals(regionId, region.regionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(regionId);
    }

    @Override
    public String toString() {
        return "Region{" +
                "regionId='" + regionId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", startKey=" + Arrays.toString(startKey) +
                ", endKey=" + Arrays.toString(endKey) +
                ", primary=" + primary +
                ", state=" + state +
                ", estimatedSize=" + estimatedSize +
                '}';
    }
}
