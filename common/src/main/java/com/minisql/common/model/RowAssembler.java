package com.minisql.common.model;

import com.minisql.common.utils.BytesUtil;
import com.minisql.common.utils.RowKeySerializer;

import java.util.*;

/**
 * 行组装器
 *
 * 功能：将同一 rowKey 的多个 KeyValue 聚合成一个完整的 Row
 *
 * 使用场景：
 * 1. Scan 操作返回多个 KeyValue，需要组装成 Row
 * 2. Get 操作可能返回多个版本，需要取最新版本
 */
public class RowAssembler {

    /**
     * 将 KeyValue 列表聚合成 Row 列表
     * 同一 rowKey 的多个 KeyValue 会聚合成一个 Row
     *
     * @param keyValues KeyValue 列表（应按 rowKey 排序）
     * @param schema 表结构（用于类型转换）
     * @return Row 列表
     */
    public static List<Row> assemble(List<KeyValue> keyValues, Table schema) {
        if (keyValues == null || keyValues.isEmpty()) {
            return Collections.emptyList();
        }

        Map<BytesKey, Row> rowMap = new LinkedHashMap<>();

        for (KeyValue kv : keyValues) {
            // 跳过删除标记
            if (kv.isDelete()) {
                continue;
            }

            BytesKey rowKey = new BytesKey(kv.getRowKey());
            Row row = rowMap.computeIfAbsent(rowKey, k -> new Row(kv.getRowKey()));

            // 从 KeyValue 中提取列名和值
            String columnName = extractColumnName(kv, schema);
            Object value = deserializeValue(kv.getValue(), schema, columnName);

            // 设置列值（后写入的会覆盖先写入的，假设输入已按 timestamp 排序）
            row.setColumn(columnName, value);
        }

        // 从 rowKey 中恢复主键值（因为写入时主键列没有被写入 KeyValue）
        for (Row row : rowMap.values()) {
            restorePrimaryKeyFromRowKey(row, schema);
        }

        return new ArrayList<>(rowMap.values());
    }

    /**
     * 从 rowKey 中恢复主键值到 Row
     */
    private static void restorePrimaryKeyFromRowKey(Row row, Table schema) {
        if (schema == null || row == null) {
            return;
        }

        String pkName = schema.getPrimaryKey();
        if (pkName == null || pkName.isEmpty()) {
            return;
        }

        // 如果 Row 中已经有主键值，跳过
        if (row.getColumn(pkName) != null) {
            return;
        }

        // 查找主键列类型
        Column pkColumn = null;
        for (Column col : schema.getColumns()) {
            if (col.getName().equals(pkName)) {
                pkColumn = col;
                break;
            }
        }

        if (pkColumn == null) {
            return;
        }

        // 从 rowKey 反序列化主键值
        try {
            Object pkValue = RowKeySerializer.deserialize(row.getRowKey(), pkColumn.getType());
            row.setColumn(pkName, pkValue);
        } catch (Exception e) {
            // 反序列化失败，打印日志但不中断
        }
    }

    /**
     * 将 KeyValue 迭代器聚合成 Row 迭代器
     * 适用于流式处理场景
     */
    public static class RowIterator implements Iterator<Row> {
        private final Iterator<KeyValue> kvIterator;
        private final Table schema;
        private Row nextRow;
        private BytesKey currentRowKey;

        public RowIterator(Iterator<KeyValue> kvIterator, Table schema) {
            this.kvIterator = kvIterator;
            this.schema = schema;
            this.currentRowKey = null;
            this.nextRow = null;
        }

        @Override
        public boolean hasNext() {
            if (nextRow != null) {
                return true;
            }

            if (!kvIterator.hasNext()) {
                return false;
            }

            // 组装下一行
            assembleNextRow();
            return nextRow != null;
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Row result = nextRow;
            nextRow = null;
            return result;
        }

        private void assembleNextRow() {
            Map<String, Object> columnValues = new LinkedHashMap<>();
            byte[] rowKeyBytes = null;
            long maxTimestamp = 0;

            while (kvIterator.hasNext()) {
                KeyValue kv = kvIterator.next();

                // 跳过删除标记
                if (kv.isDelete()) {
                    continue;
                }

                // 检查是否进入新的 rowKey
                if (currentRowKey != null) {
                    BytesKey thisRowKey = new BytesKey(kv.getRowKey());
                    if (!currentRowKey.equals(thisRowKey)) {
                        // 进入新的 rowKey，保存当前行并返回
                        nextRow = createRow(rowKeyBytes, columnValues, maxTimestamp);
                        currentRowKey = thisRowKey;
                        return;
                    }
                } else {
                    currentRowKey = new BytesKey(kv.getRowKey());
                }

                rowKeyBytes = kv.getRowKey();
                maxTimestamp = Math.max(maxTimestamp, kv.getTimestamp());

                String columnName = extractColumnName(kv, schema);
                Object value = deserializeValue(kv.getValue(), schema, columnName);
                columnValues.put(columnName, value);
            }

            // 处理最后一行
            if (rowKeyBytes != null && !columnValues.isEmpty()) {
                nextRow = createRow(rowKeyBytes, columnValues, maxTimestamp);
            }
        }

        private Row createRow(byte[] rowKey, Map<String, Object> values, long timestamp) {
            Row row = new Row(rowKey);
            row.setValues(new HashMap<>(values));
            row.setTimestamp(timestamp);

            // 从 rowKey 中恢复主键值
            restorePrimaryKeyFromRowKey(row, schema);

            return row;
        }
    }

    /**
     * 从 KeyValue 中提取列名
     * 策略：
     * 1. 如果 family 为空或空字符串，直接使用 qualifier
     * 2. 否则使用 family:qualifier 格式
     */
    private static String extractColumnName(KeyValue kv, Table schema) {
        String family = kv.getFamily();
        String qualifier = kv.getQualifier();

        // 简单模式：没有列族，直接使用 qualifier
        if (family == null || family.isEmpty() || "".equals(family)) {
            return qualifier;
        }

        // 列族模式：family:qualifier
        // 尝试匹配 schema 中的列名
        String fullColumn = family + ":" + qualifier;

        // 检查是否直接匹配
        if (schema != null) {
            for (Column col : schema.getColumns()) {
                if (col.getName().equals(fullColumn)) {
                    return fullColumn;
                }
                // 也检查是否 qualifier 直接匹配列名
                if (col.getName().equals(qualifier)) {
                    return qualifier;
                }
            }
        }

        // 默认返回 qualifier（假设 family 为空或使用默认列族）
        return qualifier;
    }

    /**
     * 反序列化值
     * 根据 schema 中的列类型进行类型转换
     */
    private static Object deserializeValue(byte[] valueBytes, Table schema, String columnName) {
        if (valueBytes == null || valueBytes.length == 0) {
            return null;
        }

        // 查找列类型
        Column.ColumnType type = findColumnType(schema, columnName);

        if (type == null) {
            // 未知类型，返回原始字节
            return valueBytes;
        }

        return RowKeySerializer.deserialize(valueBytes, type);
    }

    /**
     * 查找列类型
     */
    private static Column.ColumnType findColumnType(Table schema, String columnName) {
        if (schema == null || schema.getColumns() == null) {
            return null;
        }

        for (Column col : schema.getColumns()) {
            if (col.getName().equals(columnName)) {
                return col.getType();
            }
        }

        return null;
    }

    /**
     * 字节数组包装类，用于 HashMap 的 key
     */
    private static class BytesKey {
        private final byte[] bytes;

        public BytesKey(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BytesKey bytesKey = (BytesKey) o;
            return BytesUtil.equals(this.bytes, bytesKey.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    /**
     * 清空行（用于重置）
     */
    public static void clearRow(Row row) {
        if (row != null) {
            row.getValues().clear();
        }
    }

    /**
     * 合并多个 KeyValue 为单个 Row（适用于 Get 操作）
     */
    public static Row mergeToRow(List<KeyValue> keyValues, Table schema) {
        if (keyValues == null || keyValues.isEmpty()) {
            return null;
        }

        Row row = null;
        for (KeyValue kv : keyValues) {
            if (kv.isDelete()) {
                continue;
            }

            if (row == null) {
                row = new Row(kv.getRowKey());
            }

            String columnName = extractColumnName(kv, schema);
            Object value = deserializeValue(kv.getValue(), schema, columnName);
            row.setColumn(columnName, value);
        }

        // 从 rowKey 中恢复主键值
        restorePrimaryKeyFromRowKey(row, schema);

        return row;
    }
}
