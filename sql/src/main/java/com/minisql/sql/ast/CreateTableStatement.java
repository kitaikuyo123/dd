package com.minisql.sql.ast;

import java.util.ArrayList;
import java.util.List;

/**
 * CREATE TABLE 语句
 * 支持复合主键（分区键 + 聚类键）
 */
public class CreateTableStatement extends Statement {
    private String table;
    private List<ColumnDef> columns;

    // 复合主键支持
    // partitionKeys: 分区键列表，用于数据分布（rowKey 的组成）
    // clusteringKeys: 聚类键列表，用于 Region 内排序（支持区间查询）
    private List<String> partitionKeys;
    private List<String> clusteringKeys;

    // 向后兼容：单列主键
    private String primaryKey;

    @Override
    public StatementType getType() {
        return StatementType.CREATE_TABLE;
    }

    // Getters and Setters
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public List<ColumnDef> getColumns() { return columns; }
    public void setColumns(List<ColumnDef> columns) { this.columns = columns; }

    public String getPrimaryKey() { return primaryKey; }
    public void setPrimaryKey(String primaryKey) { this.primaryKey = primaryKey; }

    public List<String> getPartitionKeys() { return partitionKeys; }
    public void setPartitionKeys(List<String> partitionKeys) { this.partitionKeys = partitionKeys; }

    public List<String> getClusteringKeys() { return clusteringKeys; }
    public void setClusteringKeys(List<String> clusteringKeys) { this.clusteringKeys = clusteringKeys; }

    /**
     * 辅助方法：添加分区键
     */
    public void addPartitionKey(String key) {
        if (partitionKeys == null) {
            partitionKeys = new ArrayList<>();
        }
        partitionKeys.add(key);
    }

    /**
     * 辅助方法：添加聚类键
     */
    public void addClusteringKey(String key) {
        if (clusteringKeys == null) {
            clusteringKeys = new ArrayList<>();
        }
        clusteringKeys.add(key);
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
}
