package com.minisql.client.executor;

import com.minisql.common.model.Column;
import com.minisql.common.model.Table;
import com.minisql.common.utils.RowKeySerializer;
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SimpleCondition;

/**
 * 扫描范围分析器：从 WHERE 条件中提取主键扫描范围，用于 Client 端 Region 裁剪。
 *
 * <p>只处理单主键列上的等值和范围条件（=, >, >=, <, <=）及 AND 组合。
 * 无法提取范围时返回 fullScan，由 RS 做全量过滤。
 */
final class ScanRangeAnalyzer {

    private ScanRangeAnalyzer() {}

    static class ScanRange {
        final byte[] lowerBound; // inclusive, null = -∞
        final byte[] upperBound; // exclusive, null = +∞
        final boolean fullScan;

        private ScanRange(byte[] lowerBound, byte[] upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.fullScan = false;
        }

        static ScanRange fullScan() {
            return new ScanRange(null, null) {
                @Override boolean isFullScan() { return true; }
            };
        }

        boolean isFullScan() { return fullScan; }
    }

    static ScanRange computeScanRange(Table table, Condition where) {
        if (table == null || where == null) {
            return ScanRange.fullScan();
        }

        java.util.List<String> keys = table.getAllPrimaryKeys();
        if (keys == null || keys.size() != 1) {
            return ScanRange.fullScan();
        }

        String primaryKey = keys.get(0);
        Column primaryColumn = findColumn(table, primaryKey);
        if (primaryColumn == null) {
            return ScanRange.fullScan();
        }

        return analyze(primaryKey, primaryColumn, where);
    }

    private static ScanRange analyze(String primaryKey, Column primaryColumn, Condition condition) {
        if (condition instanceof SimpleCondition) {
            return analyzeSimple(primaryKey, primaryColumn, (SimpleCondition) condition);
        }
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            if (!"AND".equalsIgnoreCase(compound.getOperator())) {
                return ScanRange.fullScan();
            }
            ScanRange left = analyze(primaryKey, primaryColumn, compound.getLeft());
            ScanRange right = analyze(primaryKey, primaryColumn, compound.getRight());
            return intersect(left, right);
        }
        return ScanRange.fullScan();
    }

    private static ScanRange analyzeSimple(String primaryKey, Column primaryColumn, SimpleCondition cond) {
        if (!primaryKey.equalsIgnoreCase(cond.getColumn())) {
            return ScanRange.fullScan();
        }

        byte[] rowKey;
        try {
            Object typedValue = parseLiteral(cond.getValue(), primaryColumn.getType());
            rowKey = RowKeySerializer.serialize(typedValue, primaryColumn.getType());
        } catch (Exception e) {
            return ScanRange.fullScan();
        }

        switch (cond.getOperator()) {
            case "=":
            case "==":
                byte[] next = nextKey(rowKey);
                return next != null ? new ScanRange(rowKey, next) : ScanRange.fullScan();
            case ">":
                byte[] gt = nextKey(rowKey);
                return gt != null ? new ScanRange(gt, null) : ScanRange.fullScan();
            case ">=":
                return new ScanRange(rowKey, null);
            case "<":
                return new ScanRange(null, rowKey);
            case "<=":
                byte[] lte = nextKey(rowKey);
                return lte != null ? new ScanRange(null, lte) : ScanRange.fullScan();
            default:
                return ScanRange.fullScan();
        }
    }

    private static ScanRange intersect(ScanRange a, ScanRange b) {
        if (a.isFullScan()) return b;
        if (b.isFullScan()) return a;

        byte[] lower = max(a.lowerBound, b.lowerBound);
        byte[] upper = min(a.upperBound, b.upperBound);

        if (lower != null && upper != null && compare(lower, upper) >= 0) {
            return ScanRange.fullScan();
        }
        return new ScanRange(lower, upper);
    }

    private static byte[] max(byte[] a, byte[] b) {
        if (a == null) return b;
        if (b == null) return a;
        return compare(a, b) >= 0 ? a : b;
    }

    private static byte[] min(byte[] a, byte[] b) {
        if (a == null) return b;
        if (b == null) return a;
        return compare(a, b) <= 0 ? a : b;
    }

    private static int compare(byte[] a, byte[] b) {
        return com.minisql.common.utils.BytesUtil.compareTo(a, b);
    }

    static byte[] nextKey(byte[] key) {
        if (key == null || key.length == 0) return null;
        byte[] next = java.util.Arrays.copyOf(key, key.length);
        for (int i = next.length - 1; i >= 0; i--) {
            int unsigned = next[i] & 0xFF;
            if (unsigned != 0xFF) {
                next[i] = (byte) (unsigned + 1);
                return java.util.Arrays.copyOf(next, i + 1);
            }
        }
        return null;
    }

    private static Column findColumn(Table table, String columnName) {
        for (Column col : table.getColumns()) {
            if (col.getName().equalsIgnoreCase(columnName)) {
                return col;
            }
        }
        return null;
    }

    private static Object parseLiteral(String value, Column.ColumnType type) {
        switch (type) {
            case INT: return Integer.parseInt(value);
            case BIGINT:
            case TIMESTAMP: return Long.parseLong(value);
            case FLOAT: return Float.parseFloat(value);
            case DOUBLE: return Double.parseDouble(value);
            case BOOLEAN: return Boolean.parseBoolean(value);
            case VARCHAR:
            case CHAR:
            case STRING: return value;
            default: throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }
}
