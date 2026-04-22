package com.minisql.regionserver;

import com.minisql.common.model.Column;
import com.minisql.common.model.Table;
import com.minisql.common.utils.RowKeySerializer;
import com.minisql.storage.StorageColumnPredicate;
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SimpleCondition;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builds storage-level row_key bounds for predicates that can be translated
 * safely to indexed storage range scans.
 */
final class PredicatePushdownPlanner {

    private PredicatePushdownPlanner() {
    }

    static PushdownPlan plan(Table table, Condition condition) {
        if (table == null || condition == null) {
            return PushdownPlan.none();
        }

        List<String> keys = table.getAllPrimaryKeys();
        if (keys == null || keys.size() != 1) {
            return PushdownPlan.none();
        }

        String primaryKey = keys.get(0);
        Column primaryColumn = findColumn(table, primaryKey);
        if (primaryColumn == null) {
            return PushdownPlan.none();
        }

        return planCondition(table, primaryKey, primaryColumn, condition);
    }

    private static PushdownPlan planCondition(Table table, String primaryKey, Column primaryColumn, Condition condition) {
        if (condition instanceof SimpleCondition) {
            return planSimple(table, primaryKey, primaryColumn, (SimpleCondition) condition);
        }

        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            if (!"AND".equalsIgnoreCase(compound.getOperator())) {
                return PushdownPlan.none();
            }

            PushdownPlan left = planCondition(table, primaryKey, primaryColumn, compound.getLeft());
            PushdownPlan right = planCondition(table, primaryKey, primaryColumn, compound.getRight());
            return left.intersect(right);
        }

        return PushdownPlan.none();
    }

    private static PushdownPlan planSimple(Table table, String primaryKey, Column primaryColumn, SimpleCondition condition) {
        if (!primaryKey.equalsIgnoreCase(condition.getColumn())) {
            return planNonPrimary(table, condition);
        }

        byte[] rowKey;
        try {
            Object typedValue = parseLiteral(condition.getValue(), primaryColumn.getType());
            rowKey = RowKeySerializer.serialize(typedValue, primaryColumn.getType());
        } catch (RuntimeException ex) {
            return PushdownPlan.none();
        }

        switch (condition.getOperator()) {
            case "=":
            case "==":
                return exact(rowKey);
            case ">":
                return greaterThan(rowKey);
            case ">=":
                return atLeast(rowKey);
            case "<":
                return lessThan(rowKey);
            case "<=":
                return atMost(rowKey);
            default:
                return PushdownPlan.none();
        }
    }

    private static PushdownPlan planNonPrimary(Table table, SimpleCondition condition) {
        String operator = condition.getOperator();
        if (!isSupportedStorageOperator(operator)) {
            return PushdownPlan.none();
        }

        Column column = findColumn(table, condition.getColumn());
        if (column == null || !isSupportedStorageType(column.getType())) {
            return PushdownPlan.none();
        }

        byte[] valueBytes;
        try {
            Object typedValue = parseLiteral(condition.getValue(), column.getType());
            valueBytes = RowKeySerializer.serialize(typedValue, column.getType());
        } catch (RuntimeException ex) {
            return PushdownPlan.none();
        }

        return PushdownPlan.forColumnPredicate(
            new StorageColumnPredicate(column.getName(), operator, valueBytes)
        );
    }

    private static boolean isSupportedStorageOperator(String operator) {
        return "=".equals(operator)
            || "==".equals(operator)
            || ">".equals(operator)
            || ">=".equals(operator)
            || "<".equals(operator)
            || "<=".equals(operator);
    }

    private static boolean isSupportedStorageType(Column.ColumnType type) {
        switch (type) {
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case VARCHAR:
            case CHAR:
            case STRING:
            case BOOLEAN:
            case TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    private static PushdownPlan exact(byte[] rowKey) {
        byte[] next = nextLexicographicKey(rowKey);
        if (next == null) {
            return PushdownPlan.none();
        }
        return new PushdownPlan(rowKey, next, Collections.emptyList(), true, true);
    }

    private static PushdownPlan greaterThan(byte[] rowKey) {
        byte[] next = nextLexicographicKey(rowKey);
        if (next == null) {
            return PushdownPlan.none();
        }
        return new PushdownPlan(next, null, Collections.emptyList(), true, true);
    }

    private static PushdownPlan atLeast(byte[] rowKey) {
        return new PushdownPlan(rowKey, null, Collections.emptyList(), true, true);
    }

    private static PushdownPlan lessThan(byte[] rowKey) {
        return new PushdownPlan(null, rowKey, Collections.emptyList(), true, true);
    }

    private static PushdownPlan atMost(byte[] rowKey) {
        byte[] next = nextLexicographicKey(rowKey);
        if (next == null) {
            return PushdownPlan.none();
        }
        return new PushdownPlan(null, next, Collections.emptyList(), true, true);
    }

    static byte[] nextLexicographicKey(byte[] value) {
        if (value == null || value.length == 0) {
            return null;
        }

        byte[] next = Arrays.copyOf(value, value.length);
        for (int i = next.length - 1; i >= 0; i--) {
            int unsigned = next[i] & 0xFF;
            if (unsigned != 0xFF) {
                next[i] = (byte) (unsigned + 1);
                return Arrays.copyOf(next, i + 1);
            }
        }
        return null;
    }

    private static Column findColumn(Table table, String columnName) {
        for (Column column : table.getColumns()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }

    private static Object parseLiteral(String value, Column.ColumnType type) {
        switch (type) {
            case INT:
                return Integer.parseInt(value);
            case BIGINT:
            case TIMESTAMP:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case VARCHAR:
            case CHAR:
            case STRING:
                return value;
            default:
                throw new IllegalArgumentException("Unsupported pushdown type: " + type);
        }
    }

    static final class PushdownPlan {
        private final byte[] startKey;
        private final byte[] endKey;
        private final List<StorageColumnPredicate> columnPredicates;
        private final boolean pushdown;
        private final boolean fullyPushedDown;

        private PushdownPlan(byte[] startKey, byte[] endKey, List<StorageColumnPredicate> columnPredicates, boolean pushdown, boolean fullyPushedDown) {
            this.startKey = startKey;
            this.endKey = endKey;
            this.columnPredicates = columnPredicates == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(columnPredicates));
            this.pushdown = pushdown;
            this.fullyPushedDown = fullyPushedDown;
        }

        static PushdownPlan none() {
            return new PushdownPlan(null, null, Collections.emptyList(), false, false);
        }

        static PushdownPlan forColumnPredicate(StorageColumnPredicate predicate) {
            List<StorageColumnPredicate> predicates = new ArrayList<>();
            predicates.add(predicate);
            return new PushdownPlan(null, null, predicates, true, true);
        }

        boolean canPushDown() {
            return pushdown;
        }

        boolean isFullyPushedDown() {
            return fullyPushedDown;
        }

        byte[] getStartKey() {
            return startKey;
        }

        byte[] getEndKey() {
            return endKey;
        }

        List<StorageColumnPredicate> getColumnPredicates() {
            return columnPredicates;
        }

        PushdownPlan intersect(PushdownPlan other) {
            if (other == null) {
                return this;
            }
            if (!this.pushdown) {
                return other;
            }
            if (!other.pushdown) {
                return this;
            }

            byte[] mergedStart = maxStart(this.startKey, other.startKey);
            byte[] mergedEnd = minEnd(this.endKey, other.endKey);
            if (mergedStart != null && mergedEnd != null && compareBytes(mergedStart, mergedEnd) >= 0) {
                return none();
            }
            List<StorageColumnPredicate> mergedPredicates = new ArrayList<>(this.columnPredicates);
            mergedPredicates.addAll(other.columnPredicates);
            boolean mergedFully = this.fullyPushedDown && other.fullyPushedDown;
            return new PushdownPlan(mergedStart, mergedEnd, mergedPredicates, true, mergedFully);
        }

        private static byte[] maxStart(byte[] left, byte[] right) {
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            return compareBytes(left, right) >= 0 ? left : right;
        }

        private static byte[] minEnd(byte[] left, byte[] right) {
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            return compareBytes(left, right) <= 0 ? left : right;
        }
    }

    static int compareBytes(byte[] left, byte[] right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }

        int minLength = Math.min(left.length, right.length);
        for (int i = 0; i < minLength; i++) {
            int a = left[i] & 0xFF;
            int b = right[i] & 0xFF;
            if (a != b) {
                return Integer.compare(a, b);
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
