package com.minisql.master.recover;

import com.minisql.storage.MySQLConfig;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * 数据导入器
 * 用于将数据导入到目标 MySQL 实例
 */
public class DataImporter {

    private final MySQLConfig mysqlConfig;
    private static final int BATCH_SIZE = 500;

    public DataImporter(MySQLConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    /**
     * 导入数据到目标 MySQL
     *
     * @param regionId Region ID
     * @param exportResult 导出结果
     * @param listener 进度监听器
     */
    public void importData(String regionId, DataExporter.ExportResult exportResult,
                           ImportProgressListener listener) throws Exception {
        String sql = "INSERT INTO kv_store (row_key, column_family, qualifier, timestamp, value, type) " +
                     "VALUES (?, ?, ?, ?, ?, ?) " +
                     "ON DUPLICATE KEY UPDATE " +
                     "column_family = VALUES(column_family), " +
                     "qualifier = VALUES(qualifier), " +
                     "timestamp = VALUES(timestamp), " +
                     "value = VALUES(value), " +
                     "type = VALUES(type)";

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                List<byte[]> rowKeys = exportResult.getRowKeys();
                Map<byte[], Object> data = exportResult.getData();

                int batchCount = 0;
                int importedCount = 0;

                for (byte[] rowKey : rowKeys) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> rowData = (Map<String, Object>) data.get(rowKey);
                    if (rowData == null) {
                        continue;
                    }

                    // 设置参数
                    stmt.setBytes(1, rowKey);
                    stmt.setString(2, (String) rowData.get("column_family"));
                    stmt.setBytes(3, (byte[]) rowData.get("qualifier"));
                    stmt.setLong(4, (Long) rowData.get("timestamp"));
                    stmt.setBytes(5, (byte[]) rowData.get("value"));
                    stmt.setInt(6, (Integer) rowData.get("type"));

                    stmt.addBatch();
                    batchCount++;

                    // 批量执行
                    if (batchCount >= BATCH_SIZE) {
                        stmt.executeBatch();
                        conn.commit();
                        batchCount = 0;
                        importedCount += BATCH_SIZE;

                        // 通知进度
                        if (listener != null) {
                            listener.onProgress(importedCount);
                        }
                    }
                }

                // 执行剩余的
                if (batchCount > 0) {
                    stmt.executeBatch();
                    conn.commit();
                    importedCount += batchCount;

                    if (listener != null) {
                        listener.onProgress(importedCount);
                    }
                }
            }

            conn.setAutoCommit(true);
        }

        System.out.println("Imported data to region: " + regionId);
    }

    /**
     * 获取数据库连接
     */
    private Connection getConnection() throws SQLException {
        return mysqlConfig.createDataSource().getConnection();
    }

    /**
     * 批量导入数据（用于流式导入）
     *
     * @param regionId Region ID
     * @param dataBatch 数据批次
     * @param isLastBatch 是否是最后一批
     */
    public void importBatch(String regionId, List<Map<String, Object>> dataBatch, boolean isLastBatch) throws SQLException {
        if (dataBatch.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO kv_store (row_key, column_family, qualifier, timestamp, value, type) " +
                     "VALUES (?, ?, ?, ?, ?, ?) " +
                     "ON DUPLICATE KEY UPDATE " +
                     "column_family = VALUES(column_family), " +
                     "qualifier = VALUES(qualifier), " +
                     "timestamp = VALUES(timestamp), " +
                     "value = VALUES(value), " +
                     "type = VALUES(type)";

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (Map<String, Object> rowData : dataBatch) {
                    stmt.setBytes(1, (byte[]) rowData.get("row_key"));
                    stmt.setString(2, (String) rowData.get("column_family"));
                    stmt.setBytes(3, (byte[]) rowData.get("qualifier"));
                    stmt.setLong(4, (Long) rowData.get("timestamp"));
                    stmt.setBytes(5, (byte[]) rowData.get("value"));
                    stmt.setInt(6, (Integer) rowData.get("type"));

                    stmt.addBatch();
                }

                stmt.executeBatch();
                conn.commit();

                if (isLastBatch) {
                    System.out.println("Final batch imported to region: " + regionId);
                }
            }

            conn.setAutoCommit(true);
        }
    }

    /**
     * 导入进度监听器
     */
    public interface ImportProgressListener {
        void onProgress(long migratedRows);
    }
}
