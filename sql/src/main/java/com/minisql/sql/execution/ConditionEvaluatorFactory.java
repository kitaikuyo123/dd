package com.minisql.sql.execution;

import com.minisql.common.utils.ValueComparator;
import com.minisql.sql.ast.*;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 条件求值器工厂。
 *
 * <p>根据 AST 条件节点类型创建对应的 {@link ConditionEvaluator}。
 * 使用类型安全的 {@link ValueComparator#typedEquals} 和
 * {@link ValueComparator#typedCompare} 替代原来的字符串比较。
 */
public final class ConditionEvaluatorFactory {

    private ConditionEvaluatorFactory() {}

    /**
     * 为任意条件节点创建求值器。
     *
     * @throws IllegalArgumentException 未知条件类型
     */
    public static ConditionEvaluator create(Condition condition) {
        if (condition == null) return row -> true;

        if (condition instanceof SimpleCondition) {
            return createSimple((SimpleCondition) condition);
        }
        if (condition instanceof CompoundCondition) {
            return createCompound((CompoundCondition) condition);
        }
        if (condition instanceof NotCondition) {
            return createNot((NotCondition) condition);
        }
        if (condition instanceof BetweenCondition) {
            return createBetween((BetweenCondition) condition);
        }
        if (condition instanceof InCondition) {
            return createIn((InCondition) condition);
        }
        if (condition instanceof IsNullCondition) {
            return createIsNull((IsNullCondition) condition);
        }
        if (condition instanceof InSubqueryCondition) {
            return createInSubquery((InSubqueryCondition) condition);
        }
        if (condition instanceof ExistsCondition) {
            return createExists((ExistsCondition) condition);
        }
        throw new IllegalArgumentException("Unknown condition type: " + condition.getClass().getName());
    }

    /**
     * SimpleCondition 求值器。
     *
     * <p>使用类型安全比较：列值为 Integer/Long/Double 等时直接按数值比较，
     * 不再经过 String.valueOf() 转换。
     * 列值为 null 时返回 false（SQL 三值逻辑：NULL → UNKNOWN → false）。
     */
    private static ConditionEvaluator createSimple(SimpleCondition sc) {
        String op = sc.getOperator().toUpperCase();
        return row -> {
            Object leftVal = resolveValue(row, sc.getColumn());
            if (leftVal == null) return false;

            Object rightVal = sc.isValueColumnReference()
                    ? resolveValue(row, sc.getValue())
                    : sc.getValue();
            if (rightVal == null) return false;

            // 右侧是字面量字符串时，尝试将其强转为与左侧匹配的类型
            if (!sc.isValueColumnReference() && leftVal instanceof Number) {
                rightVal = tryParseNumeric(rightVal.toString());
            }

            switch (op) {
                case "=":
                case "==":
                    return ValueComparator.typedEquals(leftVal, rightVal);
                case "!=":
                case "<>":
                    return !ValueComparator.typedEquals(leftVal, rightVal);
                case ">":
                    return ValueComparator.typedCompare(leftVal, rightVal) > 0;
                case ">=":
                    return ValueComparator.typedCompare(leftVal, rightVal) >= 0;
                case "<":
                    return ValueComparator.typedCompare(leftVal, rightVal) < 0;
                case "<=":
                    return ValueComparator.typedCompare(leftVal, rightVal) <= 0;
                case "LIKE":
                    return matchLike(String.valueOf(leftVal), String.valueOf(rightVal));
                default:
                    throw new UnsupportedOperationException("Unsupported operator: " + op);
            }
        };
    }

    /**
     * CompoundCondition 求值器（AND / OR）。
     *
     * <p>AND 支持短路求值：左侧为 false 时直接返回，不计算右侧。
     */
    private static ConditionEvaluator createCompound(CompoundCondition cc) {
        ConditionEvaluator leftEval = create(cc.getLeft());
        ConditionEvaluator rightEval = create(cc.getRight());
        boolean isAnd = "AND".equalsIgnoreCase(cc.getOperator());

        if (isAnd) {
            return row -> leftEval.evaluate(row) && rightEval.evaluate(row);
        } else {
            return row -> leftEval.evaluate(row) || rightEval.evaluate(row);
        }
    }

    // ==================== 新条件类型求值器 ====================

    private static ConditionEvaluator createNot(NotCondition nc) {
        ConditionEvaluator innerEval = create(nc.getInner());
        return row -> !innerEval.evaluate(row);
    }

    private static ConditionEvaluator createBetween(BetweenCondition bc) {
        return row -> {
            Object val = resolveValue(row, bc.getColumn());
            if (val == null) return false;
            Object low = bc.getLow();
            Object high = bc.getHigh();
            if (val instanceof Number) {
                low = tryParseNumeric(low.toString());
                high = tryParseNumeric(high.toString());
            }
            boolean inRange = ValueComparator.typedCompare(val, low) >= 0
                && ValueComparator.typedCompare(val, high) <= 0;
            return bc.isNegated() != inRange;
        };
    }

    private static ConditionEvaluator createIn(InCondition ic) {
        Set<Object> valueSet = new HashSet<>();
        for (String v : ic.getValues()) {
            valueSet.add(v);
        }
        return row -> {
            Object val = resolveValue(row, ic.getColumn());
            if (val == null) return false;
            boolean found;
            if (val instanceof Number) {
                found = false;
                for (Object v : valueSet) {
                    if (ValueComparator.typedEquals(val, tryParseNumeric(v.toString()))) {
                        found = true;
                        break;
                    }
                }
            } else {
                found = valueSet.contains(val.toString());
            }
            return ic.isNegated() != found;
        };
    }

    private static ConditionEvaluator createIsNull(IsNullCondition inc) {
        return row -> {
            Object val = resolveValue(row, inc.getColumn());
            boolean isNull = (val == null);
            return inc.isNegated() != isNull;
        };
    }

    /**
     * IN 子查询求值器。resolvedValues 由外部通过 setter 注入子查询结果。
     */
    private static ConditionEvaluator createInSubquery(InSubqueryCondition isc) {
        return row -> {
            if (isc.getResolvedValues() == null) return false;
            Object val = resolveValue(row, isc.getColumn());
            if (val == null) return false;
            boolean found = isc.getResolvedValues().contains(val);
            return isc.isNegated() != found;
        };
    }

    /**
     * EXISTS 子查询求值器。hasResults 由外部通过 setter 注入子查询结果。
     */
    private static ConditionEvaluator createExists(ExistsCondition ec) {
        return row -> {
            boolean hasResults = ec.getHasResults() != null && ec.getHasResults();
            return ec.isNegated() != hasResults;
        };
    }

    // ==================== 工具方法 ====================

    /**
     * 从行中解析列值，支持 table.column 点号限定。
     * 先精确匹配，再尝试去掉限定符后的短名。
     */
    static Object resolveValue(Row row, String reference) {
        if (reference == null) return null;
        Object direct = row.getValue(reference);
        if (direct != null) return direct;
        int dot = reference.lastIndexOf('.');
        if (dot >= 0) {
            return row.getValue(reference.substring(dot + 1));
        }
        return null;
    }

    /** LIKE 模式匹配：% → .* , _ → . */
    static boolean matchLike(String text, String pattern) {
        return text.matches(pattern.replace("%", ".*").replace("_", "."));
    }

    /** 将字符串字面量尝试解析为数值 */
    private static Object tryParseNumeric(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ignored) {
            // 不是整数
        }
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException ignored) {
            // 也不是浮点数
        }
        return text;
    }
}
