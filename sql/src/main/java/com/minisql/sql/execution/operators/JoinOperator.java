package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.QueryPlan;
import com.minisql.sql.execution.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 连接算子，支持 Hash Join 和嵌套循环两种执行策略
 *
 * 策略选择规则:
 *   - 等值连接条件（=）且算法指定为 HASH 时，使用 Hash Join
 *   - 其余情况（非等值条件或显式指定 NESTED_LOOP）使用嵌套循环连接
 *
 * Hash Join 以左表为构建侧建立哈希表，右表为探测侧逐行匹配。
 * 当构建侧行数超过 hashJoinMaxRows 阈值时，自动降级为嵌套循环连接以避免内存溢出。
 */
public class JoinOperator extends Operator {

    private static final Logger log = LoggerFactory.getLogger(JoinOperator.class);

    /** 左子算子（构建侧） */
    private final Operator left;
    /** 右子算子（探测侧） */
    private final Operator right;
    /** 连接条件 */
    private final JoinCondition condition;
    /** 指定的连接算法 */
    private final JoinAlgorithm algorithm;

    private boolean opened;
    /** 实际执行策略，在 open 时确定 */
    private JoinStrategy strategy;
    /** 预取的下一行结果 */
    private Row nextRow;

    /** Hash Join 构建侧行数上限，超过此阈值自动降级为嵌套循环 */
    private final int hashJoinMaxRows;

    public JoinOperator(Operator left, Operator right, QueryPlan.JoinType joinType,
                        JoinCondition condition) {
        this(left, right, joinType, condition, JoinAlgorithm.HASH, 100_000);
    }

    public JoinOperator(Operator left, Operator right, QueryPlan.JoinType joinType,
                        JoinCondition condition, JoinAlgorithm algorithm) {
        this(left, right, joinType, condition, algorithm, 100_000);
    }

    /**
     * @param hashJoinMaxRows Hash Join 构建侧行数上限，最小值为 1000
     */
    public JoinOperator(Operator left, Operator right, QueryPlan.JoinType joinType,
                        JoinCondition condition, JoinAlgorithm algorithm,
                        int hashJoinMaxRows) {
        this.left = left;
        this.right = right;
        this.condition = condition;
        this.algorithm = algorithm;
        this.hashJoinMaxRows = Math.max(1000, hashJoinMaxRows);
    }

    @Override
    public void open() throws IOException {
        left.open();
        right.open();
        opened = true;
        if (algorithm == JoinAlgorithm.HASH && condition != null
            && condition.getOperator() == JoinOperatorType.EQUALS) {
            strategy = new HashJoinStrategy();
        } else {
            strategy = new NestedLoopJoinStrategy();
        }
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

    /** 预取下一行结果，用于实现 pull 模型的迭代器接口 */
    private void prefetch() throws IOException {
        nextRow = strategy == null ? null : strategy.next();
    }

    /** 将左右两行合并为一行，输出列 = 左表列 + 右表列 */
    private Row combineRows(Row leftRow, Row rightRow) {
        Object[] leftValues = leftRow.getValues();
        Object[] rightValues = rightRow.getValues();
        Object[] combined = new Object[leftValues.length + rightValues.length];
        System.arraycopy(leftValues, 0, combined, 0, leftValues.length);
        System.arraycopy(rightValues, 0, combined, leftValues.length, rightValues.length);
        return new Row(getOutputColumns(), combined);
    }

    /** 在列名数组中查找指定列的位置，支持 table.column 格式的列名 */
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

    /** 连接算法枚举 */
    public enum JoinAlgorithm {
        NESTED_LOOP,
        HASH
    }

    /** 连接条件，包含左右列名和比较运算符 */
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

    /** 连接执行策略接口 */
    private interface JoinStrategy {
        void open() throws IOException;
        Row next() throws IOException;
    }

    /**
     * Hash Join 策略
     *
     * 执行流程:
     *   1. open 阶段: 遍历左表所有行，按连接键建立哈希表
     *   2. 若构建侧行数超过阈值，清空哈希表并降级为嵌套循环
     *   3. next 阶段: 逐行扫描右表，在哈希表中查找匹配行
     */
    private final class HashJoinStrategy implements JoinStrategy {
        private final Map<JoinKey, List<Row>> hashTable = new HashMap<>();
        private Iterator<Row> matchingRows;
        private Row currentRightRow;
        private int rightKeyIndex;
        private boolean fellBackToNestedLoop;
        private NestedLoopJoinStrategy fallback;

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

            int rowCount = 0;
            while (left.hasMore()) {
                Row row = left.nextRow();
                if (rowCount >= hashJoinMaxRows) {
                    // Hash table too large — fall back to nested loop join
                    log.warn("Hash join build side exceeded {} rows (got {}), falling back to nested loop join. " +
                        "Consider increasing hashJoinMaxRows or adding a more selective filter.",
                        hashJoinMaxRows, rowCount);
                    hashTable.clear();
                    fellBackToNestedLoop = true;
                    fallback = new NestedLoopJoinStrategy();
                    // The left rows we already read are gone, but the full scan
                    // means NestedLoopJoinStrategy will re-read from left.reset()
                    // which should re-open the child operator chain
                    left.reset();
                    fallback.open();
                    return;
                }
                hashTable.computeIfAbsent(new JoinKey(row.getValue(leftKeyIndex)), ignored -> new ArrayList<>()).add(row);
                rowCount++;
            }
        }

        @Override
        public Row next() throws IOException {
            if (fellBackToNestedLoop) {
                return fallback.next();
            }
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

    /**
     * 嵌套循环连接策略
     *
     * 对左表每一行，遍历右表所有行进行条件比较。
     * 时间复杂度 O(M*N)，适用于非等值连接或构建侧数据量过大的场景。
     */
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

    /** 比较两个值，优先尝试数值比较，失败则回退到字符串比较 */
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

    /** Hash Join 中的哈希键包装类 */
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
