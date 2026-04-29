package com.minisql.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** 不可变的存储扫描请求和过滤条件 */
public class StorageScanFilter {

    private final byte[] startKey;
    private final byte[] endKey;
    private final List<StorageColumnPredicate> columnPredicates;
    private final List<String> projectedQualifiers;

    public StorageScanFilter(byte[] startKey,
                             byte[] endKey,
                             List<StorageColumnPredicate> columnPredicates,
                             List<String> projectedQualifiers) {
        this.startKey = copy(startKey);
        this.endKey = copy(endKey);
        this.columnPredicates = columnPredicates == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(columnPredicates));
        this.projectedQualifiers = projectedQualifiers == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(projectedQualifiers));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Alias for builder() for compatibility
     */
    public static Builder newBuilder() {
        return builder();
    }

    public byte[] getStartKey() {
        return copy(startKey);
    }

    public byte[] getEndKey() {
        return copy(endKey);
    }

    public List<StorageColumnPredicate> getColumnPredicates() {
        return columnPredicates;
    }

    public boolean hasColumnPredicates() {
        return !columnPredicates.isEmpty();
    }

    public List<String> getProjectedQualifiers() {
        return projectedQualifiers;
    }

    public boolean hasProjectedQualifiers() {
        return !projectedQualifiers.isEmpty();
    }

    protected static byte[] copy(byte[] value) {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    public static class Builder {
        private byte[] startKey;
        private byte[] endKey;
        private List<StorageColumnPredicate> columnPredicates = Collections.emptyList();
        private List<String> projectedQualifiers = Collections.emptyList();

        public Builder startKey(byte[] startKey) {
            this.startKey = copy(startKey);
            return this;
        }

        public Builder endKey(byte[] endKey) {
            this.endKey = copy(endKey);
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

        public StorageScanFilter build() {
            return new StorageScanFilter(startKey, endKey, columnPredicates, projectedQualifiers);
        }
    }
}
