package com.minisql.common.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Table 复合主键支持单元测试
 */
@DisplayName("Table 复合主键支持单元测试")
class TableCompositeKeyTest {

    @Test
    @DisplayName("测试设置和获取分区键")
    void testPartitionKeys() {
        Table table = new Table("sensor_data");

        List<String> partitionKeys = Arrays.asList("sensor_id", "bucket");
        table.setPartitionKeys(partitionKeys);

        assertNotNull(table.getPartitionKeys());
        assertEquals(2, table.getPartitionKeys().size());
        assertEquals("sensor_id", table.getPartitionKeys().get(0));
        assertEquals("bucket", table.getPartitionKeys().get(1));
    }

    @Test
    @DisplayName("测试设置和获取聚类键")
    void testClusteringKeys() {
        Table table = new Table("logs");

        List<String> clusteringKeys = Arrays.asList("timestamp", "id");
        table.setClusteringKeys(clusteringKeys);

        assertNotNull(table.getClusteringKeys());
        assertEquals(2, table.getClusteringKeys().size());
        assertEquals("timestamp", table.getClusteringKeys().get(0));
        assertEquals("id", table.getClusteringKeys().get(1));
    }

    @Test
    @DisplayName("测试 getAllPrimaryKeys - 复合主键")
    void testGetAllPrimaryKeysComposite() {
        Table table = new Table("sensor_data");
        table.setPartitionKeys(Arrays.asList("sensor_id", "bucket"));
        table.setClusteringKeys(Arrays.asList("timestamp"));

        List<String> allKeys = table.getAllPrimaryKeys();

        assertEquals(3, allKeys.size());
        assertEquals("sensor_id", allKeys.get(0));
        assertEquals("bucket", allKeys.get(1));
        assertEquals("timestamp", allKeys.get(2));
    }

    @Test
    @DisplayName("测试 getAllPrimaryKeys - 向后兼容单列主键")
    void testGetAllPrimaryKeysLegacy() {
        Table table = new Table("users");
        table.setPrimaryKey("id");

        List<String> allKeys = table.getAllPrimaryKeys();

        assertEquals(1, allKeys.size());
        assertEquals("id", allKeys.get(0));
    }

    @Test
    @DisplayName("测试 getAllPrimaryKeys - 分区键优先")
    void testGetAllPrimaryKeysPartitionKeysPriority() {
        Table table = new Table("logs");
        table.setPrimaryKey("id"); // 向后兼容字段
        table.setPartitionKeys(Arrays.asList("log_type", "date"));
        table.setClusteringKeys(Arrays.asList("timestamp"));

        List<String> allKeys = table.getAllPrimaryKeys();

        // 应该返回分区键 + 聚类键，而不是 primaryKey
        assertEquals(3, allKeys.size());
        assertEquals("log_type", allKeys.get(0));
        assertEquals("date", allKeys.get(1));
        assertEquals("timestamp", allKeys.get(2));
    }

    @Test
    @DisplayName("测试 getPartitionKeyColumns")
    void testGetPartitionKeyColumns() {
        Table table = new Table("sensor_data");

        Column sensorIdCol = new Column("sensor_id", Column.ColumnType.VARCHAR, 32);
        Column bucketCol = new Column("bucket", Column.ColumnType.INT);
        Column timestampCol = new Column("timestamp", Column.ColumnType.BIGINT);
        Column valueCol = new Column("value", Column.ColumnType.DOUBLE);

        table.setColumns(Arrays.asList(sensorIdCol, bucketCol, timestampCol, valueCol));
        table.setPartitionKeys(Arrays.asList("sensor_id", "bucket"));

        List<Column> partitionKeyColumns = table.getPartitionKeyColumns();

        assertEquals(2, partitionKeyColumns.size());
        assertEquals("sensor_id", partitionKeyColumns.get(0).getName());
        assertEquals("bucket", partitionKeyColumns.get(1).getName());
    }

    @Test
    @DisplayName("测试 getClusteringKeyColumns")
    void testGetClusteringKeyColumns() {
        Table table = new Table("sensor_data");

        Column sensorIdCol = new Column("sensor_id", Column.ColumnType.VARCHAR, 32);
        Column bucketCol = new Column("bucket", Column.ColumnType.INT);
        Column timestampCol = new Column("timestamp", Column.ColumnType.BIGINT);
        Column valueCol = new Column("value", Column.ColumnType.DOUBLE);

        table.setColumns(Arrays.asList(sensorIdCol, bucketCol, timestampCol, valueCol));
        table.setClusteringKeys(Arrays.asList("timestamp"));

        List<Column> clusteringKeyColumns = table.getClusteringKeyColumns();

        assertEquals(1, clusteringKeyColumns.size());
        assertEquals("timestamp", clusteringKeyColumns.get(0).getName());
    }

    @Test
    @DisplayName("测试空分区键和聚类键")
    void testEmptyPartitionAndClusteringKeys() {
        Table table = new Table("users");
        table.setPrimaryKey("id");

        // 没有设置分区键和聚类键
        assertNull(table.getPartitionKeys());
        assertNull(table.getClusteringKeys());

        // getAllPrimaryKeys 应该返回单列主键
        List<String> allKeys = table.getAllPrimaryKeys();
        assertEquals(1, allKeys.size());
        assertEquals("id", allKeys.get(0));
    }

    @Test
    @DisplayName("测试只有分区键无聚类键")
    void testPartitionKeysOnly() {
        Table table = new Table("users");
        table.setPartitionKeys(Arrays.asList("user_id", "region"));

        assertNotNull(table.getPartitionKeys());
        assertEquals(2, table.getPartitionKeys().size());

        assertNull(table.getClusteringKeys());

        List<String> allKeys = table.getAllPrimaryKeys();
        assertEquals(2, allKeys.size());
        assertEquals("user_id", allKeys.get(0));
        assertEquals("region", allKeys.get(1));
    }
}
