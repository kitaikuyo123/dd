package com.minisql.regionserver;

import com.minisql.common.model.KeyValue;
import com.minisql.storage.MySQLConfig;
import com.minisql.storage.MySQLStorageEngine;
import com.minisql.storage.StorageScanFilter;
import com.minisql.storage.StorageEngine;
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
 * 基于 MySQL 的 Region 存储实现
 */
public class MySQLRegionStorage {

    private static final Logger logger = LoggerFactory.getLogger(MySQLRegionStorage.class);

    private final String regionId;
    private final StorageEngine storageEngine;
    private final AtomicLong readRequestCount = new AtomicLong(0);
    private final AtomicLong writeRequestCount = new AtomicLong(0);
    private volatile long estimatedSize = 0;
    private volatile long lastSizeSyncTime = 0;

    public MySQLRegionStorage(String regionId, MySQLConfig config) {
        this.regionId = regionId;
        this.storageEngine = new MySQLStorageEngine(config, regionId);
        logger.info("MySQLRegionStorage initialized for region: {}", regionId);
    }

    /**
     * 共享连接池构造函数：同一 RegionServer 上所有 Region 共享同一个 HikariDataSource
     * 避免每个 Region 独占一个连接池而耗尽 MySQL max_connections
     */
    public MySQLRegionStorage(String regionId, HikariDataSource sharedDataSource) {
        this.regionId = regionId;
        this.storageEngine = new MySQLStorageEngine(sharedDataSource, regionId);
        logger.info("MySQLRegionStorage initialized for region: {} (shared pool)", regionId);
    }

    /**
     * 启动存储引擎
     */
    public void start() throws IOException {
        logger.info("MySQLRegionStorage started for region: {}", regionId);
    }

    /**
     * 写入数据
     */
    public void put(KeyValue kv) throws IOException {
        writeRequestCount.incrementAndGet();
        storageEngine.put(kv.getRowKey(), kv);

        // 更新大小估算
        int keySize = kv.getRowKey() != null ? kv.getRowKey().length : 0;
        int valueSize = kv.getValue() != null ? kv.getValue().length : 0;
        estimatedSize += keySize + valueSize;
    }

    /**
     * 批量写入
     */
    public void put(List<KeyValue> kvs) throws IOException {
        writeRequestCount.addAndGet(kvs.size());
        storageEngine.batchPut(kvs);

        // 更新大小估算
        for (KeyValue kv : kvs) {
            int keySize = kv.getRowKey() != null ? kv.getRowKey().length : 0;
            int valueSize = kv.getValue() != null ? kv.getValue().length : 0;
            estimatedSize += keySize + valueSize;
        }
    }

    /**
     * 读取数据
     */
    public KeyValue get(byte[] rowKey) {
        readRequestCount.incrementAndGet();
        List<KeyValue> results = storageEngine.get(rowKey);
        return results != null && !results.isEmpty() ? results.get(0) : null;
    }

    /**
     * 范围扫描
     */
    public Iterator<KeyValue> scan(byte[] startKey, byte[] endKey) {
        return storageEngine.scan(startKey, endKey);
    }

    public Iterator<KeyValue> scan(StorageScanFilter filter) {
        return storageEngine.scan(filter);
    }

    /**
     * 删除数据
     */
    public void delete(byte[] rowKey) throws IOException {
        storageEngine.delete(rowKey);
    }

    /**
     * Flush 数据（MySQL 不需要手动 flush）
     */
    public synchronized void flush() throws IOException {
        storageEngine.flush();
    }

    /**
     * 执行 Compaction（MySQL 可选清理旧版本）
     */
    public synchronized void compact(boolean major) throws IOException {
        if (major) {
            storageEngine.compact(true);
        }
    }

    /**
     * 关闭存储引擎
     */
    public void close() throws IOException {
        logger.info("MySQLRegionStorage closing for region: {}", regionId);
        storageEngine.close();
    }

    /**
     * 删除 MySQL 表
     */
    public void dropTable() throws IOException {
        logger.info("Dropping table for region: {}", regionId);
        if (storageEngine instanceof MySQLStorageEngine) {
            ((MySQLStorageEngine) storageEngine).dropTable();
        }
    }

    /**
     * 获取读取请求数
     */
    public long getReadRequestCount() {
        return readRequestCount.get();
    }

    /**
     * 获取写入请求数
     */
    public long getWriteRequestCount() {
        return writeRequestCount.get();
    }

    /**
     * 获取 Region ID
     */
    public String getRegionId() {
        return regionId;
    }

    /**
     * 获取存储引擎（用于直接访问）
     */
    public StorageEngine getStorageEngine() {
        return storageEngine;
    }

    /**
     * 获取存储大小（自动定期同步 MySQL 表物理大小合并估算值）
     */
    public long getStoreFileSize() {
        long now = System.currentTimeMillis();
        if (now - lastSizeSyncTime > 60000) { // 每 60 秒同步一次真实大小
            synchronized (this) {
                if (now - lastSizeSyncTime > 60000) {
                    getActualTableSize();
                    lastSizeSyncTime = System.currentTimeMillis();
                }
            }
        }
        return estimatedSize;
    }

    /**
     * 从 MySQL 元数据获取实际表大小（定期调用以校准估算值）
     */
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
                        // 校准估算值
                        this.estimatedSize = actualSize;
                        return actualSize;
                    }
                }
            } catch (SQLException e) {
                logger.warn("Failed to get actual table size from MySQL metadata", e);
            }
        }
        return estimatedSize;
    }

    /**
     * 获取 MemStore 大小（MySQL 返回 0）
     */
    public long getMemStoreSize() {
        return 0;
    }

    /**
     * 记录主副本晋升事件（MySQL 不需要）
     */
    public void logPrimaryPromotion(String serverId) throws IOException {
        logger.info("Primary promotion logged for region: {} on server: {}", regionId, serverId);
    }
}
