package com.minisql.common.utils;

/**
 * Type-safe value comparison utility.
 *
 * <p>Handles nulls (nulls-last), Comparable types of any kind,
 * and falls back to String comparison for heterogeneous types.
 */
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
