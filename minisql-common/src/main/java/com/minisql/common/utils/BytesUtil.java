package com.minisql.common.utils;

/**
 * 字节数组工具类
 */
public class BytesUtil {

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * 将字符串转换为UTF-8字节数组
     */
    public static byte[] toBytes(String s) {
        if (s == null) {
            return null;
        }
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * 将字节数组转换为UTF-8字符串
     */
    public static String toString(byte[] b) {
        if (b == null) {
            return null;
        }
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * 将long转换为字节数组（大端序）
     */
    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    /**
     * 将字节数组转换为long（大端序）
     */
    public static long toLong(byte[] bytes) {
        return toLong(bytes, 0);
    }

    public static long toLong(byte[] bytes, int offset) {
        long l = 0;
        for (int i = offset; i < offset + 8; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    /**
     * 将int转换为字节数组（大端序）
     */
    public static byte[] toBytes(int val) {
        byte[] b = new byte[4];
        for (int i = 3; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    /**
     * 将字节数组转换为int（大端序）
     */
    public static int toInt(byte[] bytes) {
        return toInt(bytes, 0);
    }

    public static int toInt(byte[] bytes, int offset) {
        int n = 0;
        for (int i = offset; i < offset + 4; i++) {
            n <<= 8;
            n ^= bytes[i] & 0xFF;
        }
        return n;
    }

    /**
     * 比较两个字节数组
     */
    public static int compareTo(byte[] left, byte[] right) {
        return compareTo(left, 0, left.length, right, 0, right.length);
    }

    public static int compareTo(byte[] buffer1, int offset1, int length1,
                                 byte[] buffer2, int offset2, int length2) {
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
            int a = buffer1[i] & 0xFF;
            int b = buffer2[j] & 0xFF;
            if (a != b) {
                return a - b;
            }
        }
        return length1 - length2;
    }

    /**
     * 获取下一个字典序的字节数组
     */
    public static byte[] nextKey(byte[] key) {
        if (key == null || key.length == 0) {
            return new byte[]{0};
        }
        byte[] next = new byte[key.length + 1];
        System.arraycopy(key, 0, next, 0, key.length);
        next[key.length] = 0;
        return next;
    }

    /**
     * 计算字节数组的哈希值
     */
    public static int hashCode(byte[] bytes) {
        int hash = 1;
        for (byte b : bytes) {
            hash = (31 * hash) + b;
        }
        return hash;
    }

    /**
     * 字节数组是否相等
     */
    public static boolean equals(byte[] a, byte[] b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) return false;
        }
        return true;
    }
}
