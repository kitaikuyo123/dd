package com.minisql.regionserver;

import com.minisql.common.model.KeyValue;
import com.minisql.storage.StorageEngine;
import com.minisql.storage.StorageScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Region storage backed by a pluggable StorageEngine (MySQL, RocksDB, etc.).
 */
public class RegionStorage {

    private static final Logger logger = LoggerFactory.getLogger(RegionStorage.class);

    private final String regionId;
    private final StorageEngine storageEngine;
    private final AtomicLong readRequestCount = new AtomicLong(0);
    private final AtomicLong writeRequestCount = new AtomicLong(0);
    private volatile long actualSize = 0;
    private volatile long lastSizeSyncTime = 0;

    public RegionStorage(String regionId, StorageEngine storageEngine) {
        this.regionId = regionId;
        this.storageEngine = storageEngine;
        logger.info("RegionStorage initialized for region: {} with {}", regionId,
            storageEngine.getClass().getSimpleName());
    }

    public void start() throws IOException {
        logger.info("RegionStorage started for region: {}", regionId);
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
        readRequestCount.incrementAndGet();
        return storageEngine.scan(startKey, endKey);
    }

    public Iterator<KeyValue> scan(StorageScanFilter filter) {
        readRequestCount.incrementAndGet();
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
        logger.info("RegionStorage closing for region: {}", regionId);
        storageEngine.close();
    }

    public void dropData() throws IOException {
        logger.info("Dropping data for region: {}", regionId);
        storageEngine.dropData();
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
        this.actualSize = storageEngine.estimateSizeBytes();
        this.lastSizeSyncTime = System.currentTimeMillis();
        return actualSize;
    }

    public long getMemStoreSize() {
        return 0;
    }
}
