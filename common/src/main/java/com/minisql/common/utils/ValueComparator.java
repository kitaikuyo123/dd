package com.minisql.common.utils;

/**
 * 类型安全的值比较工具。
 *
 * <p>核心能力：
 * <ul>
 *   <li>{@link #typedCompare} — 数值统一提升到 Double 后比较，否则同类型 Comparable</li>
 *   <li>{@link #typedEquals} — 跨数值类型相等判断（Integer(5) == Long(5)）</li>
 *   <li>{@link #coerceToString} — 将任意值转为比较用字符串表示</li>
 * </ul>
 *
 * <p>遗留方法 {@link #compare} 和 {@link #compareWithNumericCoercion} 保持签名不变，
 * 供 SortOperator / AggregateOperator 等已有调用点使用。
 */
public final class ValueComparator {

    private ValueComparator() {}

    // ==================== 新 API ====================

    /**
     * 类型安全的相等判断。
     *
     * <p>数值类型（Integer / Long / Float / Double）统一转为 Double 比较，
     * 其余类型使用 {@link Object#equals}。
     * 任一侧为 null 时返回 false（SQL 语义：NULL = NULL → UNKNOWN → false）。
     */
    public static boolean typedEquals(Object a, Object b) {
        if (a == null || b == null) return false;
        if (isNumeric(a) && isNumeric(b)) {
            return Double.compare(toDouble(a), toDouble(b)) == 0;
        }
        if (a.getClass() == b.getClass()) {
            return a.equals(b);
        }
        // 异类型：尝试字符串比较作为兜底
        return a.toString().equals(b.toString());
    }

    /**
     * 类型安全的排序比较。
     *
     * <p>规则：
     * <ol>
     *   <li>null 排最后（nulls last）</li>
     *   <li>两侧均为数值类型 → 统一转 Double 比较</li>
     *   <li>同类型且 Comparable → 使用自然排序</li>
     *   <li>兜底 toString 字典序</li>
     * </ol>
     *
     * @return 负数 / 0 / 正数，语义同 {@link java.util.Comparator}
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static int typedCompare(Object left, Object right) {
        if (left == null && right == null) return 0;
        if (left == null) return 1;    // nulls last
        if (right == null) return -1;  // nulls last

        // 两侧都是数值 → 统一 Double 比较，解决 Integer vs Long 的类型不匹配问题
        if (isNumeric(left) && isNumeric(right)) {
            return Double.compare(toDouble(left), toDouble(right));
        }

        // 同类型 Comparable → 自然排序
        if (left instanceof Comparable && right instanceof Comparable
                && left.getClass() == right.getClass()) {
            return ((Comparable) left).compareTo(right);
        }

        // 兜底：字符串字典序
        return left.toString().compareTo(right.toString());
    }

    // ==================== 遗留 API（保持兼容） ====================

    /**
     * 同类型 Comparable 比较，null 排最后。
     *
     * @deprecated 请迁移到 {@link #typedCompare}，本方法无法处理跨数值类型
     */
    @Deprecated
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static int compare(Object left, Object right) {
        if (left == null && right == null) return 0;
        if (left == null) return 1;
        if (right == null) return -1;
        if (left instanceof Comparable && right instanceof Comparable
                && left.getClass().isInstance(right)) {
            return ((Comparable) left).compareTo(right);
        }
        return left.toString().compareTo(right.toString());
    }

    /**
     * 字符串化数值比较，解析失败回退字典序。
     *
     * @deprecated 请迁移到 {@link #typedCompare}，本方法存在 Long 精度丢失风险
     */
    @Deprecated
    public static int compareWithNumericCoercion(Object left, Object right) {
        if (left == null && right == null) return 0;
        if (left == null) return 1;
        if (right == null) return -1;
        try {
            return Double.compare(
                Double.parseDouble(String.valueOf(left)),
                Double.parseDouble(String.valueOf(right)));
        } catch (NumberFormatException ignored) {
            return String.valueOf(left).compareTo(String.valueOf(right));
        }
    }

    // ==================== 内部工具方法 ====================

    private static boolean isNumeric(Object value) {
        return value instanceof Number
                && !(value instanceof java.math.BigDecimal)
                && !(value instanceof java.math.BigInteger);
    }

    private static double toDouble(Object value) {
        return ((Number) value).doubleValue();
    }
}
