package com.minisql.common.model;

import com.minisql.common.utils.RowKeySerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * KeyValue 转换器
 *
 * 功能：Row 与 KeyValue[] 之间的双向转换
 *
 * 转换规则：
 * - Row → KeyValue[]：每个列转换为一个 KeyValue
 * - KeyValue[] → Row：将同一 rowKey 的多个 KeyValue 聚合成 Row
 */
public class KeyValueConverter {

    /**
     * 默认列族名
     */
    public static final String DEFAULT_FAMILY = "";

    /**
     * 将 Row 转换为 KeyValue 数组
     * 每个非主键列转换为一个 KeyValue
     *
     * @param row 行数据
     * @param schema 表结构
     * @return KeyValue 数组
     */
    public static KeyValue[] rowToKeyValues(Row row, Table schema) {
        if (row == null || schema == null) {
            return new KeyValue[0];
        }

        List<KeyValue> keyValues = new ArrayList<>();
        byte[] rowKey = row.getRowKey();
        long timestamp = row.getTimestamp();

        // 遍历所有列
        for (Column col : schema.getColumns()) {
            String colName = col.getName();

            // 跳过主键列（主键已经编码在 rowKey 中）
            if (colName.equals(schema.getPrimaryKey())) {
                continue;
            }

            Object value = row.getColumn(colName);
            // 处理 null 值
            if (value == null) {
                if (!col.isNullable()) {
                    // 非空列不能为 null，跳过或使用默认值
                    if (col.getDefaultValue() != null) {
                        value = col.getDefaultValue();
                    } else {
                        continue;
                    }
                } else {
                    // 可空列，使用删除标记
                    KeyValue kv = KeyValue.builder(rowKey)
                        .family(DEFAULT_FAMILY)
                        .qualifier(colName)
                        .timestamp(timestamp)
                        .type(KeyValue.Type.DELETE)
                        .build();
                    keyValues.add(kv);
                    continue;
                }
            }

            // 序列化值
            byte[] valueBytes = serializeValue(value, col.getType());
            KeyValue kv = KeyValue.builder(rowKey)
                .family(DEFAULT_FAMILY)
                .qualifier(colName)
                .timestamp(timestamp)
                .value(valueBytes)
                .type(KeyValue.Type.PUT)
                .build();

            keyValues.add(kv);
        }

        return keyValues.toArray(new KeyValue[0]);
    }

    /**
     * 将 Row 转换为 KeyValue 列表
     */
    public static List<KeyValue> rowToKeyValuesList(Row row, Table schema) {
        KeyValue[] kvs = rowToKeyValues(row, schema);
        List<KeyValue> result = new ArrayList<>(kvs.length);
        for (KeyValue kv : kvs) {
            result.add(kv);
        }
        return result;
    }

    /**
     * 将 KeyValue 数组转换为 Row
     */
    public static Row keyValuesToRow(KeyValue[] keyValues, Table schema) {
        if (keyValues == null || keyValues.length == 0) {
            return null;
        }
        return RowAssembler.mergeToRow(java.util.Arrays.asList(keyValues), schema);
    }

    /**
     * 将 KeyValue 列表转换为 Row 列表
     */
    public static List<Row> keyValuesToRows(List<KeyValue> keyValues, Table schema) {
        return RowAssembler.assemble(keyValues, schema);
    }

    /**
     * 根据主键值创建 RowKey
     * 支持复合主键（分区键）
     * @param pkValue 主键值（单列主键）或主键值列表（复合主键）
     * @param schema 表结构
     * @return rowKey 字节数组
     */
    public static byte[] createRowKey(Object pkValue, Table schema) {
        // 向后兼容：单列主键
        String pkName = schema.getPrimaryKey();
        if (pkName == null || pkName.isEmpty()) {
            throw new IllegalArgumentException("Table must have a primary key");
        }

        // 查找主键列
        Column pkColumn = null;
        for (Column col : schema.getColumns()) {
            if (col.getName().equals(pkName)) {
                pkColumn = col;
                break;
            }
        }

        if (pkColumn == null) {
            throw new IllegalArgumentException("Primary key column not found: " + pkName);
        }

        return RowKeySerializer.serialize(pkValue, pkColumn.getType());
    }

    /**
     * 从 Row 中提取分区键值（复合主键）
     * @param row Row 对象
     * @param schema 表结构
     * @return rowKey 字节数组
     */
    public static byte[] createRowKeyFromRow(Row row, Table schema) {
        List<String> partitionKeys = schema.getPartitionKeys();

        // 如果有分区键，使用复合主键方式
        if (partitionKeys != null && !partitionKeys.isEmpty()) {
            List<Object> values = new ArrayList<>();
            List<Column.ColumnType> types = new ArrayList<>();

            for (String pkName : partitionKeys) {
                Object value = row.getColumn(pkName);
                if (value == null) {
                    throw new IllegalArgumentException("Partition key value is null: " + pkName);
                }
                values.add(value);

                // 查找列类型
                for (Column col : schema.getColumns()) {
                    if (col.getName().equals(pkName)) {
                        types.add(col.getType());
                        break;
                    }
                }
            }

            return RowKeySerializer.serializeComposite(values, types);
        }

        // 向后兼容：单列主键
        String pkName = schema.getPrimaryKey();
        if (pkName == null) {
            throw new IllegalArgumentException("Table must have a primary key");
        }

        Object pkValue = row.getColumn(pkName);
        if (pkValue == null) {
            throw new IllegalArgumentException("Primary key value is null");
        }

        Column pkColumn = null;
        for (Column col : schema.getColumns()) {
            if (col.getName().equals(pkName)) {
                pkColumn = col;
                break;
            }
        }

        if (pkColumn == null) {
            throw new IllegalArgumentException("Primary key column not found: " + pkName);
        }

        return RowKeySerializer.serialize(pkValue, pkColumn.getType());
    }

    /**
     * 从 Row 中提取主键值
     */
    public static Object extractPrimaryKeyValue(Row row, Table schema) {
        String pkName = schema.getPrimaryKey();
        if (pkName == null) {
            return null;
        }
        return row.getColumn(pkName);
    }

    /**
     * 从 rowKey 反序列化主键值
     */
    public static Object deserializePrimaryKey(byte[] rowKey, Table schema) {
        String pkName = schema.getPrimaryKey();
        if (pkName == null) {
            return null;
        }

        // 查找主键列
        Column pkColumn = null;
        for (Column col : schema.getColumns()) {
            if (col.getName().equals(pkName)) {
                pkColumn = col;
                break;
            }
        }

        if (pkColumn == null) {
            throw new IllegalArgumentException("Primary key column not found: " + pkName);
        }

        return RowKeySerializer.deserialize(rowKey, pkColumn.getType());
    }

    /**
     * 序列化单个值
     */
    private static byte[] serializeValue(Object value, Column.ColumnType type) {
        return RowKeySerializer.serialize(value, type);
    }

    /**
     * 创建带前缀的 RowKey（用于范围查询）
     * @param prefix 前缀
     * @return 加前缀后的 rowKey
     */
    public static byte[] prefixRowKey(byte[] rowKey, String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return rowKey;
        }

        byte[] prefixBytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] result = new byte[prefixBytes.length + rowKey.length];
        System.arraycopy(prefixBytes, 0, result, 0, prefixBytes.length);
        System.arraycopy(rowKey, 0, result, prefixBytes.length, rowKey.length);
        return result;
    }

    /**
     * 判断 KeyValue 是否匹配给定的列过滤
     * @param kv KeyValue
     * @param columns 列名列表（null 表示不过滤）
     * @return 是否匹配
     */
    public static boolean matchesColumns(KeyValue kv, List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return true;
        }

        String qualifier = kv.getQualifier();
        return columns.contains(qualifier);
    }

    /**
     * 过滤 KeyValue 列表，只保留指定的列
     */
    public static List<KeyValue> filterColumns(List<KeyValue> keyValues, List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return keyValues;
        }

        List<KeyValue> result = new ArrayList<>();
        for (KeyValue kv : keyValues) {
            if (matchesColumns(kv, columns)) {
                result.add(kv);
            }
        }
        return result;
    }

    /**
     * 将 Map 转换为 Row
     */
    public static Row mapToRow(Map<String, Object> values, Table schema) {
        Row row = new Row();

        // 从 values 中提取主键值并设置 rowKey
        String pkName = schema.getPrimaryKey();
        if (pkName != null && values.containsKey(pkName)) {
            Object pkValue = values.get(pkName);
            row.setRowKey(createRowKey(pkValue, schema));
        }

        // 设置所有列值
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            row.setColumn(entry.getKey(), entry.getValue());
        }

        return row;
    }

    /**
     * 将 Row 转换为 Map
     */
    public static Map<String, Object> rowToMap(Row row) {
        return row.getValues();
    }
}
