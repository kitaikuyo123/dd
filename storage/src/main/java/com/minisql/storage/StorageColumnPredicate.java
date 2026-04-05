package com.minisql.storage;

import java.util.Arrays;

/**
 * A storage-layer predicate on a single logical column qualifier.
 */
public class StorageColumnPredicate {

    private final String qualifier;
    private final String operator;
    private final byte[] value;

    public StorageColumnPredicate(String qualifier, String operator, byte[] value) {
        this.qualifier = qualifier;
        this.operator = operator;
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    public String getQualifier() {
        return qualifier;
    }

    public String getOperator() {
        return operator;
    }

    public byte[] getValue() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }
}
