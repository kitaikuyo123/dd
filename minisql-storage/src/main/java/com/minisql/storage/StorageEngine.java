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
        return scan(StorageScanRequest.newBuilder()
            .startKey(startKey)
            .endKey(endKey)
            .build());
    }

    default Iterator<KeyValue> scan(StorageScanRequest request) {
        return scan((StorageScanFilter) request);
    }

    default Iterator<KeyValue> scan(StorageScanFilter filter) {
        if (filter == null) {
            return scan(new StorageScanRequest(null, null, null, null));
        }
        return scan(new StorageScanRequest(
            filter.getStartKey(),
            filter.getEndKey(),
            filter.getColumnPredicates(),
            filter.getProjectedQualifiers()
        ));
    }

    void delete(byte[] key);

    void flush();

    void compact(boolean major);

    void close();
}
