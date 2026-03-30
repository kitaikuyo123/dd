package com.minisql.master.recover;

import com.minisql.storage.MySQLConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据导出器
 * 用于从源 MySQL 实例导出 Region 数据
 */
public class DataExporter {

    private final MySQLConfig mysqlConfig;
    private static final int BATCH_SIZE = 1000;

    public DataExporter(MySQLConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    /**
     * 导出 Region 数据
     *
     * @param regionId Region ID
     * @return 导出结果
     */
    public ExportResult exportRegion(String regionId) throws SQLException {
        List<byte[]> rowKeys = new ArrayList<>();
        Map<byte[], Object> data = new HashMap<>();
        long totalCount = 0;

        String countSql = "SELECT COUNT(*) FROM kv_store WHERE region_id = ?";
        String selectSql = "SELECT * FROM kv_store WHERE region_id = ?";

        try (Connection conn = getConnection()) {
            // 获取总行数
            try (PreparedStatement countStmt = conn.prepareStatement(countSql)) {
                countStmt.setString(1, regionId);
                try (ResultSet rs = countStmt.executeQuery()) {
                    if (rs.next()) {
                        totalCount = rs.getLong(1);
                    }
                }
            }

            // 分批读取数据
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setString(1, regionId);
                stmt.setFetchSize(BATCH_SIZE);

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        byte[] rowKey = rs.getBytes("row_key");
                        rowKeys.add(rowKey);

                        // 读取整行数据
                        Map<String, Object> rowData = new HashMap<>();
                        rowData.put("row_key", rowKey);
                        rowData.put("column_family", rs.getString("column_family"));
                        rowData.put("qualifier", rs.getBytes("qualifier"));
                        rowData.put("timestamp", rs.getLong("timestamp"));
                        rowData.put("value", rs.getBytes("value"));
                        rowData.put("type", rs.getInt("type"));

                        data.put(rowKey, rowData);
                    }
                }
            }
        }

        System.out.println("Exported " + rowKeys.size() + " rows from region: " + regionId);

        return new ExportResult(totalCount, rowKeys, data);
    }

    /**
     * 获取数据库连接
     */
    private Connection getConnection() throws SQLException {
        return mysqlConfig.createDataSource().getConnection();
    }

    /**
     * 分批导出 Region 数据（用于大表）
     *
     * @param regionId Region ID
     * @param batchSize 批次大小
     * @param callback 每批数据导出后的回调
     */
    public void exportRegionBatched(String regionId, int batchSize, BatchExportCallback callback) throws SQLException {
        String sql = "SELECT * FROM kv_store WHERE region_id = ? LIMIT ? OFFSET ?";

        try (Connection conn = getConnection()) {
            int offset = 0;
            boolean hasMore = true;

            while (hasMore) {
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, regionId);
                    stmt.setInt(2, batchSize);
                    stmt.setInt(3, offset);

                    try (ResultSet rs = stmt.executeQuery()) {
                        List<Map<String, Object>> batch = new ArrayList<>();
                        hasMore = false;

                        while (rs.next()) {
                            hasMore = true;
                            Map<String, Object> rowData = new HashMap<>();
                            rowData.put("row_key", rs.getBytes("row_key"));
                            rowData.put("column_family", rs.getString("column_family"));
                            rowData.put("qualifier", rs.getBytes("qualifier"));
                            rowData.put("timestamp", rs.getLong("timestamp"));
                            rowData.put("value", rs.getBytes("value"));
                            rowData.put("type", rs.getInt("type"));

                            batch.add(rowData);
                        }

                        if (!batch.isEmpty()) {
                            callback.onBatchExported(batch, offset, batchSize);
                        }

                        offset += batchSize;
                    }
                }
            }
        }
    }

    /**
     * 批量导出回调接口
     */
    public interface BatchExportCallback {
        void onBatchExported(List<Map<String, Object>> batch, int offset, int batchSize);
    }

    /**
     * 数据导出结果
     */
    public static class ExportResult {
        private final long rowCount;
        private final List<byte[]> rowKeys;
        private final Map<byte[], Object> data;

        public ExportResult(long rowCount, List<byte[]> rowKeys, Map<byte[], Object> data) {
            this.rowCount = rowCount;
            this.rowKeys = rowKeys;
            this.data = data;
        }

        public long getRowCount() { return rowCount; }
        public List<byte[]> getRowKeys() { return rowKeys; }
        public Map<byte[], Object> getData() { return data; }
    }
}
