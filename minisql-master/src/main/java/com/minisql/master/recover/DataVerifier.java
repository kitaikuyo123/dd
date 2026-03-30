package com.minisql.master.recover;

import com.minisql.storage.MySQLConfig;

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

    private final MySQLConfig sourceConfig;
    private final MySQLConfig targetConfig;

    private static final int BATCH_SIZE = 1000;

    public DataVerifier(MySQLConfig sourceConfig, MySQLConfig targetConfig) {
        this.sourceConfig = sourceConfig;
        this.targetConfig = targetConfig;
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

            Builder inconsistentKeys(List<String> keys) {
                this.inconsistentKeys = keys;
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
        long sourceCount = getRowCount(sourceConfig, regionId);
        long targetCount = getRowCount(targetConfig, regionId);

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
        String sourceChecksum = calculateChecksum(sourceConfig, regionId);
        String targetChecksum = calculateChecksum(targetConfig, regionId);

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
     * 逐行验证：最精确但最慢
     * 可以发现具体的不一致数据
     */
    public VerificationResult verifyRowByRow(String regionId) throws SQLException {
        // 先验证行数
        VerificationResult rowResult = verifyRowCount(regionId);
        if (!rowResult.isConsistent()) {
            return rowResult;
        }

        List<String> inconsistentKeys = new ArrayList<>();

        // 获取源数据
        Map<String, RowData> sourceData = loadAllData(sourceConfig, regionId);
        Map<String, RowData> targetData = loadAllData(targetConfig, regionId);

        // 对比
        for (Map.Entry<String, RowData> entry : sourceData.entrySet()) {
            String key = entry.getKey();
            RowData sourceRow = entry.getValue();
            RowData targetRow = targetData.get(key);

            if (targetRow == null) {
                inconsistentKeys.add(key + " (missing in target)");
            } else if (!sourceRow.equals(targetRow)) {
                inconsistentKeys.add(key + " (data mismatch)");
            }
        }

        // 检查目标中多出的数据
        for (String key : targetData.keySet()) {
            if (!sourceData.containsKey(key)) {
                inconsistentKeys.add(key + " (extra in target)");
            }
        }

        boolean consistent = inconsistentKeys.isEmpty();

        return new VerificationResult.Builder()
                .consistent(consistent)
                .message(consistent ? "All rows match" :
                        "Found " + inconsistentKeys.size() + " inconsistent rows")
                .inconsistentKeys(inconsistentKeys)
                .sourceCount(sourceData.size())
                .targetCount(targetData.size())
                .build();
    }

    /**
     * 增量验证：验证从某个时间点以来的数据
     * 适用于迁移过程中的持续验证
     */
    public VerificationResult verifyIncremental(String regionId, long fromTimestamp) throws SQLException {
        long sourceCount = getRowCountSince(sourceConfig, regionId, fromTimestamp);
        long targetCount = getRowCountSince(targetConfig, regionId, fromTimestamp);

        boolean consistent = sourceCount == targetCount;

        if (!consistent) {
            return new VerificationResult.Builder()
                    .consistent(false)
                    .message("Incremental row count mismatch since " + fromTimestamp)
                    .sourceCount(sourceCount)
                    .targetCount(targetCount)
                    .build();
        }

        // 计算增量 checksum
        String sourceChecksum = calculateChecksumSince(sourceConfig, regionId, fromTimestamp);
        String targetChecksum = calculateChecksumSince(targetConfig, regionId, fromTimestamp);

        consistent = sourceChecksum.equals(targetChecksum);

        return new VerificationResult.Builder()
                .consistent(consistent)
                .message(consistent ? "Incremental checksum matches" : "Incremental checksum mismatch")
                .sourceChecksum(sourceChecksum)
                .targetChecksum(targetChecksum)
                .sourceCount(sourceCount)
                .targetCount(targetCount)
                .build();
    }

    /**
     * 获取行数
     */
    private long getRowCount(MySQLConfig config, String regionId) throws SQLException {
        // 注意：当前 kv_store 表没有 region_id 列
        // 这里假设使用 row_key 的范围来标识 region
        // 实际使用时需要根据具体设计调整
        String sql = "SELECT COUNT(*) FROM kv_store";

        try (Connection conn = config.createDataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }

    /**
     * 获取指定时间点之后的行数
     */
    private long getRowCountSince(MySQLConfig config, String regionId, long timestamp) throws SQLException {
        String sql = "SELECT COUNT(*) FROM kv_store WHERE timestamp >= ?";

        try (Connection conn = config.createDataSource().getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, timestamp);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return 0;
            }
        }
    }

    /**
     * 计算 checksum
     * 对所有数据按 row_key+qualifier+timestamp+value 计算 MD5
     */
    private String calculateChecksum(MySQLConfig config, String regionId) throws SQLException {
        String sql = "SELECT row_key, qualifier, timestamp, value FROM kv_store ORDER BY row_key, qualifier, timestamp";

        try (Connection conn = config.createDataSource().getConnection();
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

            return bytesToHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new SQLException("MD5 algorithm not available", e);
        }
    }

    /**
     * 计算指定时间点之后的 checksum
     */
    private String calculateChecksumSince(MySQLConfig config, String regionId, long timestamp) throws SQLException {
        String sql = "SELECT row_key, qualifier, timestamp, value FROM kv_store " +
                     "WHERE timestamp >= ? ORDER BY row_key, qualifier, timestamp";

        try (Connection conn = config.createDataSource().getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, timestamp);

            MessageDigest md = MessageDigest.getInstance("MD5");

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    byte[] rowKey = rs.getBytes("row_key");
                    String qualifier = rs.getString("qualifier");
                    long ts = rs.getLong("timestamp");
                    byte[] value = rs.getBytes("value");

                    md.update(rowKey != null ? rowKey : new byte[0]);
                    md.update(qualifier != null ? qualifier.getBytes() : new byte[0]);
                    md.update(java.nio.ByteBuffer.allocate(8).putLong(ts).array());
                    md.update(value != null ? value : new byte[0]);
                }
            }

            return bytesToHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new SQLException("MD5 algorithm not available", e);
        }
    }

    /**
     * 加载所有数据用于逐行对比
     */
    private Map<String, RowData> loadAllData(MySQLConfig config, String regionId) throws SQLException {
        Map<String, RowData> data = new HashMap<>();

        String sql = "SELECT row_key, column_family, qualifier, timestamp, value, type FROM kv_store ORDER BY row_key";

        try (Connection conn = config.createDataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                byte[] rowKey = rs.getBytes("row_key");
                String key = rowKey != null ? java.util.Base64.getEncoder().encodeToString(rowKey) : "";

                RowData rowData = new RowData(
                    rs.getString("column_family"),
                    rs.getString("qualifier"),
                    rs.getLong("timestamp"),
                    rs.getBytes("value"),
                    rs.getInt("type")
                );

                data.put(key, rowData);
            }
        }

        return data;
    }

    /**
     * 行数据
     */
    private static class RowData {
        final String columnFamily;
        final String qualifier;
        final long timestamp;
        final byte[] value;
        final int type;

        RowData(String columnFamily, String qualifier, long timestamp, byte[] value, int type) {
            this.columnFamily = columnFamily;
            this.qualifier = qualifier;
            this.timestamp = timestamp;
            this.value = value;
            this.type = type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RowData)) return false;
            RowData rowData = (RowData) o;
            return timestamp == rowData.timestamp &&
                   type == rowData.type &&
                   Objects.equals(columnFamily, rowData.columnFamily) &&
                   Objects.equals(qualifier, rowData.qualifier) &&
                   Arrays.equals(value, rowData.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnFamily, qualifier, timestamp, type, Arrays.hashCode(value));
        }
    }

    /**
     * byte 数组转十六进制字符串
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
