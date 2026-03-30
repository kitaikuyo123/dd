package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.QueryPlan;
import com.minisql.sql.execution.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Join operator with explicit prefetch semantics.
 */
public class JoinOperator extends Operator {

    private final Operator left;
    private final Operator right;
    private final QueryPlan.JoinType joinType;
    private final JoinCondition condition;
    private final JoinAlgorithm algorithm;

    private boolean opened;
    private JoinStrategy strategy;
    private Row nextRow;

    public JoinOperator(Operator left, Operator right, QueryPlan.JoinType joinType,
                        JoinCondition condition) {
        this(left, right, joinType, condition, JoinAlgorithm.HASH);
    }

    public JoinOperator(Operator left, Operator right, QueryPlan.JoinType joinType,
                        JoinCondition condition, JoinAlgorithm algorithm) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.condition = condition;
        this.algorithm = algorithm;
    }

    @Override
    public void open() throws IOException {
        left.open();
        right.open();
        opened = true;
        strategy = algorithm == JoinAlgorithm.HASH ? new HashJoinStrategy() : new NestedLoopJoinStrategy();
        strategy.open();
        prefetch();
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            open();
        }
        if (nextRow == null) {
            return null;
        }
        Row result = nextRow;
        prefetch();
        return result;
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) {
            open();
        }
        return nextRow != null;
    }

    @Override
    public void close() throws IOException {
        opened = false;
        nextRow = null;
        strategy = null;
        left.close();
        right.close();
    }

    @Override
    public void reset() throws IOException {
        close();
        open();
    }

    @Override
    public String[] getOutputColumns() {
        String[] leftCols = left.getOutputColumns();
        String[] rightCols = right.getOutputColumns();
        String[] result = new String[leftCols.length + rightCols.length];
        System.arraycopy(leftCols, 0, result, 0, leftCols.length);
        System.arraycopy(rightCols, 0, result, leftCols.length, rightCols.length);
        return result;
    }

    private void prefetch() throws IOException {
        nextRow = strategy == null ? null : strategy.next();
    }

    private Row combineRows(Row leftRow, Row rightRow) {
        Object[] leftValues = leftRow.getValues();
        Object[] rightValues = rightRow.getValues();
        Object[] combined = new Object[leftValues.length + rightValues.length];
        System.arraycopy(leftValues, 0, combined, 0, leftValues.length);
        System.arraycopy(rightValues, 0, combined, leftValues.length, rightValues.length);
        return new Row(getOutputColumns(), combined);
    }

    private int findColumnIndex(String[] columns, String columnName) {
        String fallback = columnName != null && columnName.contains(".")
            ? columnName.substring(columnName.lastIndexOf('.') + 1)
            : columnName;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equalsIgnoreCase(columnName) || columns[i].equalsIgnoreCase(fallback)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found in join output: " + columnName);
    }

    public enum JoinAlgorithm {
        NESTED_LOOP,
        HASH
    }

    public static class JoinCondition {
        private final String leftColumn;
        private final String rightColumn;
        private final JoinOperatorType operator;

        public JoinCondition(String leftColumn, String rightColumn, JoinOperatorType operator) {
            this.leftColumn = leftColumn;
            this.rightColumn = rightColumn;
            this.operator = operator;
        }

        public String getLeftColumn() { return leftColumn; }
        public String getRightColumn() { return rightColumn; }
        public JoinOperatorType getOperator() { return operator; }
    }

    public enum JoinOperatorType {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        GREATER_THAN,
        LESS_EQUAL,
        GREATER_EQUAL
    }

    private interface JoinStrategy {
        void open() throws IOException;
        Row next() throws IOException;
    }

    private final class HashJoinStrategy implements JoinStrategy {
        private final Map<JoinKey, List<Row>> hashTable = new HashMap<>();
        private Iterator<Row> matchingRows;
        private Row currentRightRow;
        private int rightKeyIndex;

        @Override
        public void open() throws IOException {
            if (condition == null) {
                throw new IllegalArgumentException("Join condition is required");
            }
            if (condition.getOperator() != JoinOperatorType.EQUALS) {
                throw new IllegalArgumentException("Hash join only supports equality joins");
            }

            int leftKeyIndex = findColumnIndex(left.getOutputColumns(), condition.getLeftColumn());
            rightKeyIndex = findColumnIndex(right.getOutputColumns(), condition.getRightColumn());
            while (left.hasMore()) {
                Row row = left.nextRow();
                hashTable.computeIfAbsent(new JoinKey(row.getValue(leftKeyIndex)), ignored -> new ArrayList<>()).add(row);
            }
        }

        @Override
        public Row next() throws IOException {
            while (true) {
                if (matchingRows != null && matchingRows.hasNext()) {
                    return combineRows(matchingRows.next(), currentRightRow);
                }
                if (!right.hasMore()) {
                    return null;
                }
                currentRightRow = right.nextRow();
                List<Row> matches = hashTable.get(new JoinKey(currentRightRow.getValue(rightKeyIndex)));
                matchingRows = matches == null ? null : matches.iterator();
            }
        }
    }

    private final class NestedLoopJoinStrategy implements JoinStrategy {
        private Row currentLeftRow;
        private Row currentRightRow;
        private int leftKeyIndex;
        private int rightKeyIndex;

        @Override
        public void open() throws IOException {
            if (condition == null) {
                throw new IllegalArgumentException("Join condition is required");
            }
            leftKeyIndex = findColumnIndex(left.getOutputColumns(), condition.getLeftColumn());
            rightKeyIndex = findColumnIndex(right.getOutputColumns(), condition.getRightColumn());
            currentLeftRow = left.hasMore() ? left.nextRow() : null;
        }

        @Override
        public Row next() throws IOException {
            while (currentLeftRow != null) {
                if (currentRightRow == null) {
                    if (!right.hasMore()) {
                        currentLeftRow = left.hasMore() ? left.nextRow() : null;
                        if (currentLeftRow == null) {
                            return null;
                        }
                        right.reset();
                        if (!right.hasMore()) {
                            continue;
                        }
                    }
                    currentRightRow = right.nextRow();
                }

                Row result = compare(currentLeftRow, currentRightRow) ? combineRows(currentLeftRow, currentRightRow) : null;
                currentRightRow = right.hasMore() ? right.nextRow() : null;
                if (result != null) {
                    return result;
                }
            }
            return null;
        }

        private boolean compare(Row leftRow, Row rightRow) {
            Object leftValue = leftRow.getValue(leftKeyIndex);
            Object rightValue = rightRow.getValue(rightKeyIndex);
            switch (condition.getOperator()) {
                case EQUALS:
                    return Objects.equals(leftValue, rightValue);
                case NOT_EQUALS:
                    return !Objects.equals(leftValue, rightValue);
                case LESS_THAN:
                    return compareValues(leftValue, rightValue) < 0;
                case GREATER_THAN:
                    return compareValues(leftValue, rightValue) > 0;
                case LESS_EQUAL:
                    return compareValues(leftValue, rightValue) <= 0;
                case GREATER_EQUAL:
                    return compareValues(leftValue, rightValue) >= 0;
                default:
                    return false;
            }
        }
    }

    private int compareValues(Object leftValue, Object rightValue) {
        if (leftValue == null && rightValue == null) {
            return 0;
        }
        if (leftValue == null) {
            return -1;
        }
        if (rightValue == null) {
            return 1;
        }
        try {
            return Double.compare(Double.parseDouble(String.valueOf(leftValue)), Double.parseDouble(String.valueOf(rightValue)));
        } catch (NumberFormatException ignored) {
            return String.valueOf(leftValue).compareTo(String.valueOf(rightValue));
        }
    }

    private static class JoinKey {
        private final Object value;

        JoinKey(Object value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JoinKey joinKey = (JoinKey) o;
            return Objects.equals(value, joinKey.value);
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }
    }
}
