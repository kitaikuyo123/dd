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
    private volatile long estimatedSize = 0;
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

        int keySize = kv.getRowKey() != null ? kv.getRowKey().length : 0;
        int valueSize = kv.getValue() != null ? kv.getValue().length : 0;
        estimatedSize += keySize + valueSize;
    }

    public void put(List<KeyValue> kvs) throws IOException {
        writeRequestCount.addAndGet(kvs.size());
        storageEngine.batchPut(kvs);

        for (KeyValue kv : kvs) {
            int keySize = kv.getRowKey() != null ? kv.getRowKey().length : 0;
            int valueSize = kv.getValue() != null ? kv.getValue().length : 0;
            estimatedSize += keySize + valueSize;
        }
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
                    getActualTableSize();
                    lastSizeSyncTime = System.currentTimeMillis();
                }
            }
        }
        return estimatedSize;
    }

    public synchronized long getActualTableSize() {
        if (storageEngine instanceof MySQLStorageEngine) {
            try {
                MySQLStorageEngine engine = (MySQLStorageEngine) storageEngine;
                String sql = "SELECT data_length + index_length AS size " +
                             "FROM information_schema.TABLES " +
                             "WHERE table_schema = DATABASE() AND table_name = ?";
                try (Connection conn = engine.getConnection();
                     PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, engine.getTableName());
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        long actualSize = rs.getLong("size");
                        long oldSize = this.estimatedSize;
                        this.estimatedSize = actualSize;

                        String sizeInfo = formatSize(actualSize);
                        String diffInfo = "";
                        if (oldSize != actualSize) {
                            long diff = actualSize - oldSize;
                            String diffSign = diff >= 0 ? "+" : "";
                            diffInfo = " (change: " + diffSign + formatSize(diff) + ")";
                        }
                        System.out.printf("[RegionSize Calibrated] Region: %s | Table: %s | Size: %s%s | Estimated: %s%n",
                            regionId, engine.getTableName(), sizeInfo, diffInfo, formatSize(oldSize));

                        return actualSize;
                    }
                }
            } catch (SQLException e) {
                logger.warn("Failed to get actual table size from MySQL metadata", e);
            }
        }
        return estimatedSize;
    }

    private String formatSize(long size) {
        if (size >= 1024L * 1024 * 1024) {
            return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
        } else if (size >= 1024L * 1024) {
            return String.format("%.2f MB", size / (1024.0 * 1024));
        } else if (size >= 1024L) {
            return String.format("%.2f KB", size / 1024.0);
        }
        return size + " B";
    }

    public long getMemStoreSize() {
        return 0;
    }

    public void logPrimaryPromotion(String serverId) throws IOException {
        logger.info("Primary promotion logged for region: {} on server: {}", regionId, serverId);
    }
}