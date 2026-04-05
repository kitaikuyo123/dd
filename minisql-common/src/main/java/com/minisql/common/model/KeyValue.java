package com.minisql.common.model;

import com.minisql.common.utils.BytesUtil;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * 键值对，存储引擎的基本单元
 */
public class KeyValue implements Serializable, Comparable<KeyValue> {
    private static final long serialVersionUID = 1L;

    // 操作类型
    public enum Type {
        PUT((byte) 4),
        DELETE((byte) 8);

        private final byte code;

        Type(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static Type fromCode(byte code) {
            for (Type type : values()) {
                if (type.code == code) return type;
            }
            throw new IllegalArgumentException("Unknown code: " + code);
        }
    }

    private byte[] rowKey;      // 行键
    private String family;      // 列族
    private String qualifier;   // 列限定符
    private long timestamp;     // 时间戳
    private byte[] value;       // 值
    private Type type;          // 操作类型

    public KeyValue() {
        this.timestamp = System.currentTimeMillis();
        this.type = Type.PUT;
    }

    public KeyValue(byte[] rowKey, String family, String qualifier, byte[] value) {
        this();
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
    }

    public KeyValue(byte[] rowKey, String family, String qualifier, long timestamp, byte[] value) {
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
        this.timestamp = timestamp;
        this.value = value;
        this.type = Type.PUT;
    }

    // Builder模式
    public static Builder builder(byte[] rowKey) {
        return new Builder(rowKey);
    }

    public static class Builder {
        private final KeyValue kv;

        public Builder(byte[] rowKey) {
            kv = new KeyValue();
            kv.rowKey = rowKey;
        }

        public Builder family(String family) {
            kv.family = family;
            return this;
        }

        public Builder qualifier(String qualifier) {
            kv.qualifier = qualifier;
            return this;
        }

        public Builder timestamp(long timestamp) {
            kv.timestamp = timestamp;
            return this;
        }

        public Builder value(byte[] value) {
            kv.value = value;
            return this;
        }

        public Builder type(Type type) {
            kv.type = type;
            return this;
        }

        public KeyValue build() {
            return kv;
        }
    }

    // Getters and Setters
    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * 获取完整列名
     */
    public String getColumn() {
        return family + ":" + qualifier;
    }

    /**
     * 是否为删除标记
     */
    public boolean isDelete() {
        return type == Type.DELETE;
    }

    @Override
    public int compareTo(KeyValue other) {
        // 先比较rowKey
        int cmp = BytesUtil.compareTo(this.rowKey, other.rowKey);
        if (cmp != 0) return cmp;

        // 再比较family
        cmp = this.family.compareTo(other.family);
        if (cmp != 0) return cmp;

        // 再比较qualifier
        cmp = this.qualifier.compareTo(other.qualifier);
        if (cmp != 0) return cmp;

        // 最后比较timestamp（倒序，新的在前）
        return Long.compare(other.timestamp, this.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValue keyValue = (KeyValue) o;
        return timestamp == keyValue.timestamp &&
                Arrays.equals(rowKey, keyValue.rowKey) &&
                Objects.equals(family, keyValue.family) &&
                Objects.equals(qualifier, keyValue.qualifier) &&
                Arrays.equals(value, keyValue.value) &&
                type == keyValue.type;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(family, qualifier, timestamp, type);
        result = 31 * result + Arrays.hashCode(rowKey);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "rowKey=" + Arrays.toString(rowKey) +
                ", family='" + family + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + Arrays.toString(value) +
                ", type=" + type +
                '}';
    }
}
