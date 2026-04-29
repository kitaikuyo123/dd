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
}
