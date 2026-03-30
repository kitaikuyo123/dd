package com.minisql.storage;

import com.minisql.common.model.KeyValue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves MVCC visibility and delete markers from MySQL result sets.
 */
public class KeyValueVisibilityResolver {

    public interface KeyValueMapper {
        KeyValue map(ResultSet rs) throws SQLException;
    }

    public List<KeyValue> materializeLatestValues(ResultSet rs, KeyValueMapper mapper) throws SQLException {
        Map<BytesKey, RowAccumulator> rows = new LinkedHashMap<>();
        while (rs.next()) {
            KeyValue kv = mapper.map(rs);
            rows.computeIfAbsent(new BytesKey(kv.getRowKey()), ignored -> new RowAccumulator()).accept(kv);
        }
        return flatten(rows);
    }

    public List<KeyValue> resolveLatestValues(Iterable<KeyValue> values) {
        Map<BytesKey, RowAccumulator> rows = new LinkedHashMap<>();
        for (KeyValue kv : values) {
            rows.computeIfAbsent(new BytesKey(kv.getRowKey()), ignored -> new RowAccumulator()).accept(kv);
        }
        return flatten(rows);
    }

    public List<KeyValue> materializeVisibleCells(ResultSet rs, KeyValueMapper mapper) throws SQLException {
        List<KeyValue> results = new ArrayList<>();
        while (rs.next()) {
            results.add(mapper.map(rs));
        }
        return results;
    }

    private List<KeyValue> flatten(Map<BytesKey, RowAccumulator> rows) {
        List<KeyValue> results = new ArrayList<>();
        for (RowAccumulator accumulator : rows.values()) {
            results.addAll(accumulator.toKeyValues());
        }
        return results;
    }

    private static class RowAccumulator {
        private long rowDeleteTimestamp = Long.MIN_VALUE;
        private final Map<String, Long> columnDeleteTimestamps = new LinkedHashMap<>();
        private final Map<String, KeyValue> latestColumns = new LinkedHashMap<>();

        void accept(KeyValue kv) {
            String columnKey = buildColumnKey(kv.getFamily(), kv.getQualifier());
            if (kv.isDelete()) {
                if (isRowDeleteMarker(kv)) {
                    rowDeleteTimestamp = Math.max(rowDeleteTimestamp, kv.getTimestamp());
                } else {
                    columnDeleteTimestamps.merge(columnKey, kv.getTimestamp(), Math::max);
                }
                return;
            }

            long deleteTimestamp = Math.max(rowDeleteTimestamp, columnDeleteTimestamps.getOrDefault(columnKey, Long.MIN_VALUE));
            if (kv.getTimestamp() <= deleteTimestamp) {
                return;
            }
            latestColumns.putIfAbsent(columnKey, kv);
        }

        List<KeyValue> toKeyValues() {
            return new ArrayList<>(latestColumns.values());
        }

        private boolean isRowDeleteMarker(KeyValue kv) {
            return (kv.getFamily() == null || kv.getFamily().isEmpty())
                && (kv.getQualifier() == null || kv.getQualifier().isEmpty());
        }

        private String buildColumnKey(String family, String qualifier) {
            return (family == null ? "" : family) + '\u0000' + (qualifier == null ? "" : qualifier);
        }
    }

    private static class BytesKey {
        private final byte[] value;

        BytesKey(byte[] value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof BytesKey && Arrays.equals(value, ((BytesKey) obj).value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }
}
