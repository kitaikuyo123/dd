package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Persistent write-ahead log for replication.
 *
 * <p>NOTE: The MySQL-backed implementation has been removed during the RocksDB migration.
 * This class is a placeholder; WAL functionality is disabled until a RocksDB-based
 * implementation is provided.
 */
public class ReplicationWAL implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationWAL.class);

    public ReplicationWAL() {
        logger.info("ReplicationWAL initialized (no-op placeholder)");
    }

    public void initialize() {
        // No-op placeholder
    }

    public long getCurrentSequenceId(String regionId) {
        return 0L;
    }

    public ReplicationLogEntry append(String regionId, List<KeyValue> mutations) throws SQLException {
        throw new SQLException("ReplicationWAL is not implemented (placeholder)");
    }

    public List<ReplicationLogEntry> appendBatch(String regionId, List<List<KeyValue>> mutationBatches) throws SQLException {
        throw new SQLException("ReplicationWAL is not implemented (placeholder)");
    }

    public List<ReplicationLogEntry> getEntries(String regionId, long fromSequenceId) throws SQLException {
        return new ArrayList<>();
    }

    public void markAsApplied(String regionId, long sequenceId, String replicaAddress) throws SQLException {
        // No-op placeholder
    }

    public void cleanup(String regionId, int maxRetention) throws SQLException {
        // No-op placeholder
    }

    public void deleteRegion(String regionId) throws SQLException {
        // No-op placeholder
    }

    @Override
    public void close() {
        // No-op placeholder
    }
}
