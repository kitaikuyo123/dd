package com.minisql.regionserver;

import com.minisql.common.model.KeyValue;
import com.minisql.storage.MySQLStorageEngine;
import com.minisql.storage.StorageEngine;
import com.minisql.storage.StorageScanFilter;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MySQL-backed region storage.
 */
public class MySQLRegionStorage {

    private static final Logger logger = LoggerFactory.getLogger(MySQLRegionStorage.class);

    private final String regionId;
    private final StorageEngine storageEngine;
    private final AtomicLong readRequestCount = new AtomicLong(0);
    private final AtomicLong writeRequestCount = new AtomicLong(0);
    private volatile long actualSize = 0;
    private volatile long lastSizeSyncTime = 0;

    /**
     * Shared-pool constructor: all regions on a RegionServer share one HikariDataSource.
     */
    public MySQLRegionStorage(String regionId, HikariDataSource sharedDataSource) {
        this.regionId = regionId;
        this.storageEngine = new MySQLStorageEngine(sharedDataSource, regionId);
        logger.info("MySQLRegionStorage initialized for region: {} (shared pool)", regionId);
    }

    public void start() throws IOException {
        logger.info("MySQLRegionStorage started for region: {}", regionId);
    }

    public void put(KeyValue kv) throws IOException {
        writeRequestCount.incrementAndGet();
        storageEngine.put(kv.getRowKey(), kv);
    }

    public void put(List<KeyValue> kvs) throws IOException {
        writeRequestCount.addAndGet(kvs.size());
        storageEngine.batchPut(kvs);
    }

    public KeyValue get(byte[] rowKey) {
        readRequestCount.incrementAndGet();
        List<KeyValue> results = storageEngine.get(rowKey);
        return results != null && !results.isEmpty() ? results.get(0) : null;
    }

    public Iterator<KeyValue> scan(byte[] startKey, byte[] endKey) {
        return storageEngine.scan(startKey, endKey);
    }

    public Iterator<KeyValue> scan(StorageScanFilter filter) {
        return storageEngine.scan(filter);
    }

    public void delete(byte[] rowKey) throws IOException {
        storageEngine.delete(rowKey);
    }

    public synchronized void flush() throws IOException {
        storageEngine.flush();
    }

    public synchronized void compact(boolean major) throws IOException {
        if (major) {
            storageEngine.compact(true);
        }
    }

    public void close() throws IOException {
        logger.info("MySQLRegionStorage closing for region: {}", regionId);
        storageEngine.close();
    }

    public void dropTable() throws IOException {
        logger.info("Dropping table for region: {}", regionId);
        if (storageEngine instanceof MySQLStorageEngine) {
            ((MySQLStorageEngine) storageEngine).dropTable();
        }
    }

    public long getReadRequestCount() {
        return readRequestCount.get();
    }

    public long getWriteRequestCount() {
        return writeRequestCount.get();
    }

    public String getRegionId() {
        return regionId;
    }

    public StorageEngine getStorageEngine() {
        return storageEngine;
    }

    public long getStoreFileSize() {
        long now = System.currentTimeMillis();
        if (now - lastSizeSyncTime > 10000) {
            synchronized (this) {
                if (now - lastSizeSyncTime > 10000) {
                    return getActualTableSize();
                }
            }
        }
        return actualSize;
    }

    public synchronized long getActualTableSize() {
        if (storageEngine instanceof MySQLStorageEngine) {
            MySQLStorageEngine engine = (MySQLStorageEngine) storageEngine;
            try {
                try (Connection conn = engine.getConnection()) {
                    Long tablespaceSize = queryTablespaceFileSize(conn, engine.getTableName());
                    if (tablespaceSize != null) {
                        this.actualSize = tablespaceSize;
                        this.lastSizeSyncTime = System.currentTimeMillis();
                        return tablespaceSize;
                    }

                    Long reportedTableSize = queryReportedTableSize(conn, engine.getTableName());
                    if (reportedTableSize != null) {
                        this.actualSize = reportedTableSize;
                        this.lastSizeSyncTime = System.currentTimeMillis();
                        return reportedTableSize;
                    }

                    this.actualSize = 0;
                    this.lastSizeSyncTime = System.currentTimeMillis();
                    logger.warn("No size metadata found for table: {}, region: {}. Returning 0.",
                        engine.getTableName(), regionId);
                    return 0;
                }
            } catch (SQLException e) {
                logger.error("Failed to get actual table size from MySQL metadata for region: {}, table: {}", 
                    regionId, engine.getTableName(), e);
            }
        }
        logger.warn("Returning last known actual size {} for region: {}", actualSize, regionId);
        return actualSize;
    }

    private Long queryTablespaceFileSize(Connection conn, String tableName) {
        try {
            String qualifiedTableName = currentDatabaseName(conn) + "/" + tableName;
            Long fileSize = querySingleSize(conn,
                "SELECT FILE_SIZE AS size FROM information_schema.INNODB_TABLESPACES WHERE NAME = ?",
                qualifiedTableName);
            if (fileSize != null) {
                return fileSize;
            }
            return querySingleSize(conn,
                "SELECT FILE_SIZE AS size FROM information_schema.INNODB_SYS_TABLESPACES WHERE NAME = ?",
                qualifiedTableName);
        } catch (SQLException e) {
            logger.debug("Unable to resolve current database name for tablespace size lookup: {}", e.getMessage());
            return null;
        }
    }

    private Long queryReportedTableSize(Connection conn, String tableName) {
        return querySingleSize(conn,
            "SELECT data_length + index_length AS size " +
                "FROM information_schema.TABLES " +
                "WHERE table_schema = DATABASE() AND table_name = ?",
            tableName);
    }

    private Long querySingleSize(Connection conn, String sql, String parameter) {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, parameter);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("size");
                }
                return null;
            }
        } catch (SQLException e) {
            logger.debug("Size metadata query failed: sql={}, parameter={}, message={}", sql, parameter, e.getMessage());
            return null;
        }
    }

    private String currentDatabaseName(Connection conn) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement("SELECT DATABASE()");
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                String database = rs.getString(1);
                if (database != null && !database.isBlank()) {
                    return database;
                }
            }
        }
        throw new SQLException("Unable to determine current database name");
    }
    public long getMemStoreSize() {
        return 0;
    }

    public void logPrimaryPromotion(String serverId) throws IOException {
        logger.info("Primary promotion logged for region: {} on server: {}", regionId, serverId);
    }
}
