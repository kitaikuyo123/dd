package com.minisql.storage;

import java.util.Arrays;

/** 存储层列过滤谓词 */
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

    /**
     * Test whether a KeyValue's qualifier matches this predicate's column.
     */
    public boolean matchesQualifier(String kvQualifier) {
        return qualifier != null && qualifier.equals(kvQualifier);
    }
}
