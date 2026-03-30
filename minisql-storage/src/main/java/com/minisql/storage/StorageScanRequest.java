package com.minisql.storage;

import java.util.List;

/**
 * Preferred scan request type for the storage engine API.
 */
public class StorageScanRequest extends StorageScanFilter {

    public StorageScanRequest(byte[] startKey,
                              byte[] endKey,
                              List<StorageColumnPredicate> columnPredicates,
                              List<String> projectedQualifiers) {
        super(startKey, endKey, columnPredicates, projectedQualifiers);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private byte[] startKey;
        private byte[] endKey;
        private List<StorageColumnPredicate> columnPredicates;
        private List<String> projectedQualifiers;

        public Builder startKey(byte[] startKey) {
            this.startKey = StorageScanFilter.copy(startKey);
            return this;
        }

        public Builder endKey(byte[] endKey) {
            this.endKey = StorageScanFilter.copy(endKey);
            return this;
        }

        public Builder columnPredicates(List<StorageColumnPredicate> columnPredicates) {
            this.columnPredicates = columnPredicates;
            return this;
        }

        public Builder projectedQualifiers(List<String> projectedQualifiers) {
            this.projectedQualifiers = projectedQualifiers;
            return this;
        }

        public StorageScanRequest build() {
            return new StorageScanRequest(startKey, endKey, columnPredicates, projectedQualifiers);
        }
    }
}
