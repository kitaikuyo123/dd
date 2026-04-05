package com.minisql.common.utils;

import com.minisql.common.model.Column;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RowKey 序列化工具类
 *
 * 功能：将各种类型的主键值序列化为保序的 byte[]
 *
 * 序列化规则：
 * - INT/BIGINT：大端序，保持数值顺序
 * - VARCHAR/STRING：UTF-8 编码，对于保序需要在前面加长度
 * - UUID：16 字节定长
 * - TIMESTAMP：8 字节毫秒时间戳
 * - 复合主键：按顺序拼接每个部分
 */
public class RowKeySerializer {

    // Bit masks for serialization
    private static final int INT_SIGN_BIT_MASK = 0x80000000;
    private static final long LONG_SIGN_BIT_MASK = 0x8000000000000000L;
    private static final int FLOAT_NEGATIVE_MASK = 0xFFFFFFFF;
    private static final int FLOAT_SIGN_BIT_MASK = 0x80000000;
    private static final long DOUBLE_NEGATIVE_MASK = 0xFFFFFFFFFFFFFFFFL;
    private static final long DOUBLE_SIGN_BIT_MASK = 0x8000000000000000L;

    /**
     * 将单个值序列化为字节数组
     * @param value 值对象
     * @param type 值类型
     * @return 序列化后的字节数组
     */
    public static byte[] serialize(Object value, Column.ColumnType type) {
        if (value == null) {
            return new byte[]{0}; // null 值标记
        }

        switch (type) {
            case INT:
                return serializeInt((Integer) value);
            case BIGINT:
                return serializeLong((Long) value);
            case FLOAT:
                return serializeFloat((Float) value);
            case DOUBLE:
                return serializeDouble((Double) value);
            case VARCHAR:
            case CHAR:
            case STRING:
                return serializeString((String) value);
            case BOOLEAN:
                return serializeBoolean((Boolean) value);
            case TIMESTAMP:
                return serializeTimestamp((Long) value);
            case BLOB:
                return (byte[]) value;
            default:
                // 未知类型，尝试作为字符串处理
                return serializeString(value.toString());
        }
    }

    /**
     * 从字节数组反序列化单个值
     * @param bytes 字节数组
     * @param type 值类型
     * @return 反序列化后的值
     */
    public static Object deserialize(byte[] bytes, Column.ColumnType type) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        switch (type) {
            case INT:
                return deserializeInt(bytes);
            case BIGINT:
                return deserializeLong(bytes);
            case FLOAT:
                return deserializeFloat(bytes);
            case DOUBLE:
                return deserializeDouble(bytes);
            case VARCHAR:
            case CHAR:
            case STRING:
                return deserializeString(bytes);
            case BOOLEAN:
                return deserializeBoolean(bytes);
            case TIMESTAMP:
                return deserializeTimestamp(bytes);
            case BLOB:
                return bytes;
            default:
                return deserializeString(bytes);
        }
    }

    /**
     * 序列化复合主键
     * @param values 主键值列表
     * @param types 对应的类型列表
     * @return 序列化后的字节数组
     */
    public static byte[] serializeComposite(List<Object> values, List<Column.ColumnType> types) {
        if (values == null || values.isEmpty()) {
            return new byte[0];
        }

        List<byte[]> parts = new ArrayList<>();
        int totalLength = 0;

        for (int i = 0; i < values.size(); i++) {
            byte[] part = serialize(values.get(i), types.get(i));
            // 为了能够正确分割复合主键，每个部分前面加上长度前缀（2 字节）
            byte[] withLength = ByteBuffer.allocate(2 + part.length)
                .putShort((short) part.length)
                .put(part)
                .array();
            parts.add(withLength);
            totalLength += withLength.length;
        }

        // 拼接所有部分
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        for (byte[] part : parts) {
            buffer.put(part);
        }
        return buffer.array();
    }

    /**
     * 反序列化复合主键
     * @param bytes 字节数组
     * @param types 类型列表
     * @return 反序列化后的值列表
     */
    public static List<Object> deserializeComposite(byte[] bytes, List<Column.ColumnType> types) {
        List<Object> values = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        for (Column.ColumnType type : types) {
            // 读取长度前缀
            short length = buffer.getShort();
            byte[] part = new byte[length];
            buffer.get(part);
            values.add(deserialize(part, type));
        }

        return values;
    }

    // ==================== 具体类型的序列化方法 ====================

    /**
     * 序列化 INT 类型
     * 使用大端序，保证数值顺序与字节顺序一致
     */
    public static byte[] serializeInt(int value) {
        // 将 int 转换为无符号序保序的格式
        // XOR 符号位，使得负数也能正确排序
        return BytesUtil.toBytes(value ^ INT_SIGN_BIT_MASK);
    }

    public static int deserializeInt(byte[] bytes) {
        int value = BytesUtil.toInt(bytes);
        return value ^ INT_SIGN_BIT_MASK;
    }

    /**
     * 序列化 BIGINT 类型
     */
    public static byte[] serializeLong(long value) {
        // XOR 符号位，保证保序
        return BytesUtil.toBytes(value ^ LONG_SIGN_BIT_MASK);
    }

    public static long deserializeLong(byte[] bytes) {
        long value = BytesUtil.toLong(bytes);
        return value ^ LONG_SIGN_BIT_MASK;
    }

    /**
     * 序列化 FLOAT 类型
     * IEEE 754 格式，需要处理符号位和指数
     */
    public static byte[] serializeFloat(float value) {
        int bits = Float.floatToIntBits(value);
        // 调整位模式以保证保序
        int adjusted = bits;
        if (bits < 0) {
            adjusted = bits ^ FLOAT_NEGATIVE_MASK;
        } else {
            adjusted = bits ^ FLOAT_SIGN_BIT_MASK;
        }
        return BytesUtil.toBytes(adjusted);
    }

    public static float deserializeFloat(byte[] bytes) {
        int bits = BytesUtil.toInt(bytes);
        int adjusted = bits;
        if ((bits & FLOAT_SIGN_BIT_MASK) == 0) {
            adjusted = bits ^ FLOAT_SIGN_BIT_MASK;
        } else {
            adjusted = bits ^ FLOAT_NEGATIVE_MASK;
        }
        return Float.intBitsToFloat(adjusted);
    }

    /**
     * 序列化 DOUBLE 类型
     */
    public static byte[] serializeDouble(double value) {
        long bits = Double.doubleToLongBits(value);
        // 调整位模式以保证保序
        long adjusted = bits;
        if (bits < 0) {
            adjusted = bits ^ DOUBLE_NEGATIVE_MASK;
        } else {
            adjusted = bits ^ DOUBLE_SIGN_BIT_MASK;
        }
        return BytesUtil.toBytes(adjusted);
    }

    public static double deserializeDouble(byte[] bytes) {
        long bits = BytesUtil.toLong(bytes);
        long adjusted = bits;
        if ((bits & DOUBLE_SIGN_BIT_MASK) == 0) {
            adjusted = bits ^ DOUBLE_SIGN_BIT_MASK;
        } else {
            adjusted = bits ^ DOUBLE_NEGATIVE_MASK;
        }
        return Double.longBitsToDouble(adjusted);
    }

    /**
     * 序列化字符串类型
     * 格式：UTF-8 字节 + 0x00 终止符
     * 使用零填充保证字典序：按字节逐个比较，相同前缀时短字符串在前
     */
    public static byte[] serializeString(String value) {
        if (value == null) {
            return new byte[]{0x00}; // null 标记（最小的字节）
        }
        byte[] utf8Bytes = value.getBytes(StandardCharsets.UTF_8);
        // 添加终止符，保证字典序
        byte[] result = new byte[utf8Bytes.length + 1];
        System.arraycopy(utf8Bytes, 0, result, 0, utf8Bytes.length);
        result[utf8Bytes.length] = 0x00;
        return result;
    }

    public static String deserializeString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        if (bytes.length == 1 && bytes[0] == 0x00) {
            return null;
        }
        // 找到终止符
        int nullIndex = -1;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == 0x00) {
                nullIndex = i;
                break;
            }
        }
        if (nullIndex == -1) {
            // 没有终止符，使用全部字节
            return new String(bytes, StandardCharsets.UTF_8);
        }
        byte[] utf8Bytes = new byte[nullIndex];
        System.arraycopy(bytes, 0, utf8Bytes, 0, nullIndex);
        return new String(utf8Bytes, StandardCharsets.UTF_8);
    }

    /**
     * 序列化布尔值
     */
    public static byte[] serializeBoolean(Boolean value) {
        return new byte[]{value ? (byte) 1 : (byte) 0};
    }

    public static Boolean deserializeBoolean(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return bytes[0] == 1;
    }

    /**
     * 序列化时间戳（毫秒）
     */
    public static byte[] serializeTimestamp(Long timestamp) {
        return serializeLong(timestamp);
    }

    public static Long deserializeTimestamp(byte[] bytes) {
        return deserializeLong(bytes);
    }
}
