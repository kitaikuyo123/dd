package com.minisql.storage;

import com.minisql.common.model.KeyValue;

import java.util.Iterator;
import java.util.List;

/**
 * Storage engine abstraction.
 */
public interface StorageEngine {

    void put(byte[] key, KeyValue value);

    void batchPut(List<KeyValue> values);

    List<KeyValue> get(byte[] key);

    default Iterator<KeyValue> scan(byte[] startKey, byte[] endKey) {
        return scan(StorageScanFilter.newBuilder()
            .startKey(startKey)
            .endKey(endKey)
            .build());
    }

    default Iterator<KeyValue> scan(StorageScanFilter filter) {
        if (filter == null) {
            return scan(new StorageScanFilter(null, null, null, null));
        }
        return scan(filter);
    }

    void delete(byte[] key);

    void flush();

    void compact(boolean major);

    void close();

    /**
     * Drop all data for this storage instance.
     * Default no-op; implementations should override.
     */
    default void dropData() {}

    /**
     * Estimate the on-disk size in bytes.
     * Returns 0 by default.
     */
    default long estimateSizeBytes() { return 0L; }

    /**
     * Estimate the in-memory write buffer (MemTable) size in bytes.
     * Returns 0 by default.
     */
    default long estimateMemTableSize() { return 0L; }
}
