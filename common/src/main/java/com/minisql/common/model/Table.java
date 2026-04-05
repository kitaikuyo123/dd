package com.minisql.common.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 表元数据
 * 支持复合主键（分区键 + 聚类键）
 */
public class Table implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tableName;
    private List<Column> columns;

    // 复合主键支持
    // partitionKeys: 分区键列表，用于数据分布（rowKey 的组成）
    // clusteringKeys: 聚类键列表，用于 Region 内排序（支持区间查询）
    private List<String> partitionKeys;
    private List<String> clusteringKeys;

    // 向后兼容：单列主键
    private String primaryKey;

    private List<String> regionIds;
    private long createTime;
    private TableProperties properties;

    public Table() {
        this.columns = new ArrayList<>();
        this.regionIds = new ArrayList<>();
        this.createTime = System.currentTimeMillis();
        this.properties = new TableProperties();
    }

    public Table(String tableName) {
        this();
        this.tableName = tableName;
    }

    public void addColumn(Column column) {
        this.columns.add(column);
    }

    public void addRegion(String regionId) {
        this.regionIds.add(regionId);
    }

    // Getters and Setters
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public List<String> getClusteringKeys() {
        return clusteringKeys;
    }

    public void setClusteringKeys(List<String> clusteringKeys) {
        this.clusteringKeys = clusteringKeys;
    }

    /**
     * 获取所有主键列（分区键 + 聚类键）
     */
    public List<String> getAllPrimaryKeys() {
        List<String> allKeys = new ArrayList<>();
        if (partitionKeys != null) {
            allKeys.addAll(partitionKeys);
        }
        if (clusteringKeys != null) {
            allKeys.addAll(clusteringKeys);
        }
        // 向后兼容：如果只有 primaryKey，返回它
        if (allKeys.isEmpty() && primaryKey != null) {
            allKeys.add(primaryKey);
        }
        return allKeys;
    }

    /**
     * 获取分区键列定义
     */
    public List<Column> getPartitionKeyColumns() {
        List<Column> result = new ArrayList<>();
        if (partitionKeys == null) {
            return result;
        }
        for (Column column : columns) {
            if (partitionKeys.contains(column.getName())) {
                result.add(column);
            }
        }
        return result;
    }

    /**
     * 获取聚类键列定义
     */
    public List<Column> getClusteringKeyColumns() {
        List<Column> result = new ArrayList<>();
        if (clusteringKeys == null) {
            return result;
        }
        for (Column column : columns) {
            if (clusteringKeys.contains(column.getName())) {
                result.add(column);
            }
        }
        return result;
    }

    public List<String> getRegionIds() {
        return regionIds;
    }

    public void setRegionIds(List<String> regionIds) {
        this.regionIds = regionIds;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public TableProperties getProperties() {
        return properties;
    }

    public void setProperties(TableProperties properties) {
        this.properties = properties;
    }

    /**
     * 表属性配置
     */
    public static class TableProperties implements Serializable {
        private static final long serialVersionUID = 1L;

        private int maxRegionSize = 256 * 1024 * 1024;  // 256MB
        private int replicationFactor = 3;

        public int getMaxRegionSize() {
            return maxRegionSize;
        }

        public void setMaxRegionSize(int maxRegionSize) {
            this.maxRegionSize = maxRegionSize;
        }

        public int getReplicationFactor() {
            return replicationFactor;
        }

        public void setReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
        }
    }
}
