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
}
