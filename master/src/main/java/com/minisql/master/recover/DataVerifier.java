package com.minisql.master.recover;

import com.minisql.common.utils.BytesUtil;
import com.minisql.storage.MySQLConfig;

import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;

/**
 * 数据一致性验证器
 * 用于验证迁移后源和目标数据的一致性
 *
 * 支持三种验证方式：
 * 1. 行数对比 - 快速但不精确
 * 2. Checksum 对比 - 精确且高效
 * 3. 逐行对比 - 最精确但最慢
 */
public class DataVerifier {

    private final DataSource sourceDataSource;
    private final DataSource targetDataSource;

    public DataVerifier(MySQLConfig sourceConfig, MySQLConfig targetConfig) {
        this.sourceDataSource = sourceConfig.createDataSource();
        this.targetDataSource = targetConfig.createDataSource();
    }

    public void close() {
        if (sourceDataSource instanceof HikariDataSource) {
            ((HikariDataSource) sourceDataSource).close();
        }
        if (targetDataSource instanceof HikariDataSource) {
            ((HikariDataSource) targetDataSource).close();
        }
    }

    /**
     * 验证结果
     */
    public static class VerificationResult {
        private final boolean consistent;
        private final String message;
        private final long sourceCount;
        private final long targetCount;
        private final String sourceChecksum;
        private final String targetChecksum;
        private final List<String> inconsistentKeys;

        private VerificationResult(Builder builder) {
            this.consistent = builder.consistent;
            this.message = builder.message;
            this.sourceCount = builder.sourceCount;
            this.targetCount = builder.targetCount;
            this.sourceChecksum = builder.sourceChecksum;
            this.targetChecksum = builder.targetChecksum;
            this.inconsistentKeys = builder.inconsistentKeys;
        }

        public boolean isConsistent() { return consistent; }
        public String getMessage() { return message; }
        public long getSourceCount() { return sourceCount; }
        public long getTargetCount() { return targetCount; }
        public String getSourceChecksum() { return sourceChecksum; }
        public String getTargetChecksum() { return targetChecksum; }
        public List<String> getInconsistentKeys() { return inconsistentKeys; }

        @Override
        public String toString() {
            return "VerificationResult{" +
                   "consistent=" + consistent +
                   ", message='" + message + '\'' +
                   ", sourceCount=" + sourceCount +
                   ", targetCount=" + targetCount +
                   '}';
        }

        private static class Builder {
            private boolean consistent = true;
            private String message = "OK";
            private long sourceCount;
            private long targetCount;
            private String sourceChecksum;
            private String targetChecksum;
            private List<String> inconsistentKeys = new ArrayList<>();

            Builder consistent(boolean consistent) {
                this.consistent = consistent;
                return this;
            }

            Builder message(String message) {
                this.message = message;
                return this;
            }

            Builder sourceCount(long count) {
                this.sourceCount = count;
                return this;
            }

            Builder targetCount(long count) {
                this.targetCount = count;
                return this;
            }

            Builder sourceChecksum(String checksum) {
                this.sourceChecksum = checksum;
                return this;
            }

            Builder targetChecksum(String checksum) {
                this.targetChecksum = checksum;
                return this;
            }

            VerificationResult build() {
                return new VerificationResult(this);
            }
        }
    }

    /**
     * 快速验证：只对比行数
     */
    public VerificationResult verifyRowCount(String regionId) throws SQLException {
        long sourceCount = getRowCount(sourceDataSource, regionId);
        long targetCount = getRowCount(targetDataSource, regionId);

        boolean consistent = sourceCount == targetCount;

        return new VerificationResult.Builder()
                .consistent(consistent)
                .message(consistent ? "Row count matches" :
                        "Row count mismatch: source=" + sourceCount + ", target=" + targetCount)
                .sourceCount(sourceCount)
                .targetCount(targetCount)
                .build();
    }

    /**
     * Checksum 验证：对比数据的 MD5 校验和
     * 推荐使用的方式，平衡了速度和准确性
     */
    public VerificationResult verifyChecksum(String regionId) throws SQLException {
        // 先验证行数
        VerificationResult rowResult = verifyRowCount(regionId);
        if (!rowResult.isConsistent()) {
            return rowResult;
        }

        // 计算 checksum
        String sourceChecksum = calculateChecksum(sourceDataSource, regionId);
        String targetChecksum = calculateChecksum(targetDataSource, regionId);

        boolean consistent = sourceChecksum.equals(targetChecksum);

        return new VerificationResult.Builder()
                .consistent(consistent)
                .message(consistent ? "Checksum matches" : "Checksum mismatch")
                .sourceChecksum(sourceChecksum)
                .targetChecksum(targetChecksum)
                .sourceCount(rowResult.getSourceCount())
                .targetCount(rowResult.getTargetCount())
                .build();
    }

    /**
     * 获取行数
     */
    private long getRowCount(DataSource ds, String regionId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM kv_store";

        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }

    /**
     * 计算 checksum
     * 对所有数据按 row_key+qualifier+timestamp+value 计算 MD5
     */
    private String calculateChecksum(DataSource ds, String regionId) throws SQLException {
        String sql = "SELECT row_key, qualifier, timestamp, value FROM kv_store ORDER BY row_key, qualifier, timestamp";

        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            MessageDigest md = MessageDigest.getInstance("MD5");

            while (rs.next()) {
                byte[] rowKey = rs.getBytes("row_key");
                String qualifier = rs.getString("qualifier");
                long timestamp = rs.getLong("timestamp");
                byte[] value = rs.getBytes("value");

                // 合并所有字段
                md.update(rowKey != null ? rowKey : new byte[0]);
                md.update(qualifier != null ? qualifier.getBytes() : new byte[0]);
                md.update(java.nio.ByteBuffer.allocate(8).putLong(timestamp).array());
                md.update(value != null ? value : new byte[0]);
            }

            return BytesUtil.bytesToHex(md.digest()).replace(" ", "");
        } catch (NoSuchAlgorithmException e) {
            throw new SQLException("MD5 algorithm not available", e);
        }
    }

}
