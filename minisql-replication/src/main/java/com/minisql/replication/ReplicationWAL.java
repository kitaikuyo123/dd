package com.minisql.replication;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.minisql.common.model.KeyValue;
import com.minisql.storage.MySQLConfig;

import javax.sql.DataSource;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persistent write-ahead log for replication.
 */
public class ReplicationWAL implements AutoCloseable {

    private static final Gson GSON = new Gson();
    private static final Type MUTATION_LIST_TYPE = new TypeToken<List<MutationRecord>>() { }.getType();

    private final MySQLConfig mysqlConfig;
    private volatile DataSource dataSource;
    private final Map<String, Long> sequenceIdCache = new ConcurrentHashMap<>();

    public ReplicationWAL(MySQLConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    public void initialize() throws SQLException {
        String createTableSql =
            "CREATE TABLE IF NOT EXISTS replication_wal (" +
            "    region_id VARCHAR(64) NOT NULL, " +
            "    sequence_id BIGINT NOT NULL, " +
            "    mutations JSON NOT NULL, " +
            "    timestamp BIGINT NOT NULL, " +
            "    applied_by JSON, " +
            "    PRIMARY KEY (region_id, sequence_id), " +
            "    INDEX idx_region_timestamp (region_id, timestamp)" +
            ")";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSql);
        }
    }

    public long getCurrentSequenceId(String regionId) {
        Long cached = sequenceIdCache.get(regionId);
        if (cached != null) {
            return cached;
        }

        String sql = "SELECT MAX(sequence_id) FROM replication_wal WHERE region_id = ?";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, regionId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long maxSeqId = rs.getLong(1);
                    sequenceIdCache.put(regionId, maxSeqId);
                    return maxSeqId;
                }
            }
        } catch (SQLException e) {
            System.err.println("Failed to load sequenceId for region " + regionId + ": " + e.getMessage());
        }

        sequenceIdCache.put(regionId, 0L);
        return 0L;
    }

    public ReplicationLogEntry append(String regionId, List<KeyValue> mutations) throws SQLException {
        long sequenceId = getNextSequenceId(regionId);
        long timestamp = System.currentTimeMillis();

        String sql = "INSERT INTO replication_wal (region_id, sequence_id, mutations, timestamp, applied_by) " +
            "VALUES (?, ?, ?, ?, NULL)";

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, regionId);
            stmt.setLong(2, sequenceId);
            stmt.setString(3, toJson(mutations));
            stmt.setLong(4, timestamp);
            stmt.executeUpdate();
        }

        return new ReplicationLogEntry(sequenceId, timestamp, mutations);
    }

    public List<ReplicationLogEntry> appendBatch(String regionId, List<List<KeyValue>> mutationBatches) throws SQLException {
        List<ReplicationLogEntry> entries = new ArrayList<>();
        long currentSeqId = getCurrentSequenceId(regionId);
        long timestamp = System.currentTimeMillis();

        String sql = "INSERT INTO replication_wal (region_id, sequence_id, mutations, timestamp, applied_by) " +
            "VALUES (?, ?, ?, ?, NULL)";

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (List<KeyValue> mutations : mutationBatches) {
                currentSeqId++;
                stmt.setString(1, regionId);
                stmt.setLong(2, currentSeqId);
                stmt.setString(3, toJson(mutations));
                stmt.setLong(4, timestamp);
                stmt.addBatch();
                entries.add(new ReplicationLogEntry(currentSeqId, timestamp, mutations));
            }
            stmt.executeBatch();
        }

        sequenceIdCache.put(regionId, currentSeqId);
        return entries;
    }

    public List<ReplicationLogEntry> getEntries(String regionId, long fromSequenceId) throws SQLException {
        List<ReplicationLogEntry> entries = new ArrayList<>();
        String sql = "SELECT sequence_id, mutations, timestamp FROM replication_wal " +
            "WHERE region_id = ? AND sequence_id > ? ORDER BY sequence_id ASC";

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, regionId);
            stmt.setLong(2, fromSequenceId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long sequenceId = rs.getLong("sequence_id");
                    long timestamp = rs.getLong("timestamp");
                    List<KeyValue> mutations = fromJson(rs.getString("mutations"));
                    entries.add(new ReplicationLogEntry(sequenceId, timestamp, mutations));
                }
            }
        }
        return entries;
    }

    public void markAsApplied(String regionId, long sequenceId, String replicaAddress) throws SQLException {
        String sql = "UPDATE replication_wal SET applied_by = " +
            "CASE " +
            "  WHEN applied_by IS NULL OR JSON_VALID(applied_by) = 0 THEN JSON_ARRAY(?) " +
            "  WHEN JSON_CONTAINS(applied_by, JSON_QUOTE(?)) = 0 THEN JSON_ARRAY_APPEND(applied_by, '$', ?) " +
            "  ELSE applied_by " +
            "END " +
            "WHERE region_id = ? AND sequence_id = ?";

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, replicaAddress);
            stmt.setString(2, replicaAddress);
            stmt.setString(3, replicaAddress);
            stmt.setString(4, regionId);
            stmt.setLong(5, sequenceId);
            stmt.executeUpdate();
        }
    }

    public void cleanup(String regionId, int maxRetention) throws SQLException {
        String sql = "DELETE FROM replication_wal WHERE region_id = ? AND sequence_id <= " +
            "(SELECT MAX(sequence_id) - ? FROM replication_wal WHERE region_id = ?)";

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, regionId);
            stmt.setInt(2, maxRetention);
            stmt.setString(3, regionId);
            stmt.executeUpdate();
        }
    }

    public void deleteRegion(String regionId) throws SQLException {
        String sql = "DELETE FROM replication_wal WHERE region_id = ?";

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, regionId);
            stmt.executeUpdate();
        }

        sequenceIdCache.remove(regionId);
    }

    @Override
    public void close() {
        if (dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
            } catch (Exception e) {
                System.err.println("Failed to close WAL data source: " + e.getMessage());
            }
        }
    }

    private DataSource getDataSource() {
        if (dataSource == null) {
            synchronized (this) {
                if (dataSource == null) {
                    dataSource = mysqlConfig.createDataSource();
                }
            }
        }
        return dataSource;
    }

    private Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private synchronized long getNextSequenceId(String regionId) {
        Long current = sequenceIdCache.get(regionId);
        if (current == null) {
            current = getCurrentSequenceId(regionId);
        }
        long next = current + 1;
        sequenceIdCache.put(regionId, next);
        return next;
    }

    private String toJson(List<KeyValue> mutations) {
        List<MutationRecord> records = new ArrayList<>();
        for (KeyValue kv : mutations) {
            records.add(new MutationRecord(
                encodeBytes(kv.getRowKey()),
                kv.getFamily(),
                kv.getQualifier(),
                kv.getTimestamp(),
                encodeBytes(kv.getValue()),
                kv.getType() == null ? KeyValue.Type.PUT.name() : kv.getType().name()
            ));
        }
        return GSON.toJson(records, MUTATION_LIST_TYPE);
    }

    private List<KeyValue> fromJson(String json) {
        List<MutationRecord> records = GSON.fromJson(json, MUTATION_LIST_TYPE);
        List<KeyValue> result = new ArrayList<>();
        if (records == null) {
            return result;
        }

        for (MutationRecord record : records) {
            KeyValue kv = new KeyValue();
            kv.setRowKey(decodeBytes(record.rowKey));
            kv.setFamily(record.family);
            kv.setQualifier(record.qualifier);
            kv.setTimestamp(record.timestamp);
            kv.setValue(decodeBytes(record.value));
            kv.setType(KeyValue.Type.valueOf(record.type));
            result.add(kv);
        }
        return result;
    }

    private String encodeBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return Base64.getEncoder().encodeToString(bytes);
    }

    private byte[] decodeBytes(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return new byte[0];
        }
        return Base64.getDecoder().decode(encoded);
    }

    private static final class MutationRecord {
        private final String rowKey;
        private final String family;
        private final String qualifier;
        private final long timestamp;
        private final String value;
        private final String type;

        private MutationRecord(String rowKey, String family, String qualifier, long timestamp, String value, String type) {
            this.rowKey = rowKey;
            this.family = family;
            this.qualifier = qualifier;
            this.timestamp = timestamp;
            this.value = value;
            this.type = type;
        }
    }
}
