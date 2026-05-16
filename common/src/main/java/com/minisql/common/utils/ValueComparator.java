package com.minisql.common.utils;

/** 类型安全的值比较工具 */
public final class ValueComparator {

    private ValueComparator() {}

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static int compare(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;   // nulls last
        }
        if (right == null) {
            return -1;  // nulls last
        }
        if (left instanceof Comparable && right instanceof Comparable
                && left.getClass().isInstance(right)) {
            return ((Comparable) left).compareTo(right);
        }
        // Fallback: String comparison
        return left.toString().compareTo(right.toString());
    }

    /**
     * 带数值强制转换的比较：先尝试 Double 解析，失败则回退到字符串比较
     */
    public static int compareWithNumericCoercion(Object left, Object right) {
        if (left == null && right == null) return 0;
        if (left == null) return 1;    // nulls last
        if (right == null) return -1;  // nulls last
        try {
            return Double.compare(
                Double.parseDouble(String.valueOf(left)),
                Double.parseDouble(String.valueOf(right)));
        } catch (NumberFormatException ignored) {
            return String.valueOf(left).compareTo(String.valueOf(right));
        }
    }
}
