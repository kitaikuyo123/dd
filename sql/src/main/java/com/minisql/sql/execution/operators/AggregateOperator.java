package com.minisql.sql.execution.operators;

import com.minisql.common.utils.ValueComparator;
import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;
import com.minisql.sql.execution.QueryPlan;

import java.io.IOException;
import java.util.*;

/**
 * 聚合算子
 * 支持 COUNT, SUM, AVG, MAX, MIN 等聚合函数
 */
public class AggregateOperator extends Operator {

    private final Operator child;
    private final List<QueryPlan.AggregateExpr> aggregates;
    private final List<String> groupByColumns;

    // 聚合状态
    private Map<GroupKey, AggregateState[]> groupStates;
    private Iterator<Map.Entry<GroupKey, AggregateState[]>> resultIterator;
    private boolean computed;
    private boolean opened;

    // 列索引缓存
    private int[] columnIndices;
    private int[] groupByIndices;

    public AggregateOperator(Operator child, List<QueryPlan.AggregateExpr> aggregates,
                             List<String> groupByColumns) {
        this.child = child;
        this.aggregates = aggregates;
        this.groupByColumns = groupByColumns != null ? groupByColumns : new ArrayList<>();
    }

    @Override
    public void open() throws IOException {
        child.open();
        opened = true;
        computed = false;
        groupStates = new HashMap<>();

        // 初始化列索引
        String[] inputColumns = child.getOutputColumns();
        columnIndices = new int[aggregates.size()];
        for (int i = 0; i < aggregates.size(); i++) {
            String col = aggregates.get(i).getColumn();
            if (col.equals("*")) {
                columnIndices[i] = -1; // COUNT(*)
            } else {
                columnIndices[i] = findColumnIndex(inputColumns, col);
            }
        }

        groupByIndices = new int[groupByColumns.size()];
        for (int i = 0; i < groupByColumns.size(); i++) {
            groupByIndices[i] = findColumnIndex(inputColumns, groupByColumns.get(i));
        }
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            throw new IOException("Operator is closed");
        }

        if (!computed) {
            computeAggregates();
        }

        if (resultIterator == null || !resultIterator.hasNext()) {
            return null;
        }

        Map.Entry<GroupKey, AggregateState[]> entry = resultIterator.next();
        GroupKey groupKey = entry.getKey();
        AggregateState[] states = entry.getValue();

        // 构建结果行：group by 列 + 聚合结果
        Object[] values = new Object[groupByColumns.size() + aggregates.size()];

        // Group by 列
        for (int i = 0; i < groupKey.values.length; i++) {
            values[i] = groupKey.values[i];
        }

        // 聚合结果
        for (int i = 0; i < states.length; i++) {
            values[groupByColumns.size() + i] = states[i].getResult();
        }

        return new Row(getOutputColumns(), values);
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) {
            return false;
        }
        if (!computed) {
            computeAggregates();
        }
        return resultIterator != null && resultIterator.hasNext();
    }

    /**
     * 计算所有聚合值
     */
    private void computeAggregates() throws IOException {
        // 遍历所有输入数据
        while (child.hasMore()) {
            Row row = child.nextRow();

            // 构建分组键
            GroupKey key = new GroupKey(groupByIndices.length);
            for (int i = 0; i < groupByIndices.length; i++) {
                key.values[i] = row.getValue(groupByIndices[i]);
            }

            // 获取或创建聚合状态
            AggregateState[] states = groupStates.computeIfAbsent(key, k -> {
                AggregateState[] newStates = new AggregateState[aggregates.size()];
                for (int i = 0; i < aggregates.size(); i++) {
                    newStates[i] = createState(aggregates.get(i).getType());
                }
                return newStates;
            });

            // 更新每个聚合状态
            for (int i = 0; i < aggregates.size(); i++) {
                Object value = columnIndices[i] >= 0 ? row.getValue(columnIndices[i]) : null;
                states[i].accumulate(value);
            }
        }

        // 如果没有数据且没有 GROUP BY，返回默认值
        if (groupStates.isEmpty() && groupByColumns.isEmpty()) {
            GroupKey emptyKey = new GroupKey(0);
            AggregateState[] states = new AggregateState[aggregates.size()];
            for (int i = 0; i < aggregates.size(); i++) {
                states[i] = createState(aggregates.get(i).getType());
            }
            groupStates.put(emptyKey, states);
        }

        resultIterator = groupStates.entrySet().iterator();
        computed = true;
    }

    private AggregateState createState(QueryPlan.AggregateType type) {
        switch (type) {
            case COUNT:
                return new CountState();
            case SUM:
                return new SumState();
            case AVG:
                return new AvgState();
            case MAX:
                return new MaxState();
            case MIN:
                return new MinState();
            default:
                throw new IllegalArgumentException("Unknown aggregate type: " + type);
        }
    }

    @Override
    public void close() throws IOException {
        opened = false;
        computed = false;
        groupStates = null;
        resultIterator = null;
        child.close();
    }

    @Override
    public void reset() throws IOException {
        close();
        open();
    }

    @Override
    public String[] getOutputColumns() {
        String[] columns = new String[groupByColumns.size() + aggregates.size()];

        // Group by 列
        for (int i = 0; i < groupByColumns.size(); i++) {
            columns[i] = groupByColumns.get(i);
        }

        // 聚合列
        for (int i = 0; i < aggregates.size(); i++) {
            QueryPlan.AggregateExpr expr = aggregates.get(i);
            String name = expr.getType().name() + "(" + expr.getColumn() + ")";
            if (expr.getAlias() != null) {
                name = expr.getAlias();
            }
            columns[groupByColumns.size() + i] = name;
        }

        return columns;
    }

    private int findColumnIndex(String[] columns, String columnName) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    /**
     * 聚合状态基类
     */
    private abstract static class AggregateState {
        abstract void accumulate(Object value);
        abstract Object getResult();
    }

    /**
     * COUNT 聚合
     */
    private static class CountState extends AggregateState {
        private long count = 0;

        @Override
        void accumulate(Object value) {
            count++;
        }

        @Override
        Object getResult() {
            return count;
        }
    }

    /**
     * SUM 聚合
     */
    private static class SumState extends AggregateState {
        private double sum = 0;

        @Override
        void accumulate(Object value) {
            if (value instanceof Number) {
                sum += ((Number) value).doubleValue();
            }
        }

        @Override
        Object getResult() {
            return sum;
        }
    }

    /**
     * AVG 聚合
     */
    private static class AvgState extends AggregateState {
        private double sum = 0;
        private long count = 0;

        @Override
        void accumulate(Object value) {
            if (value instanceof Number) {
                sum += ((Number) value).doubleValue();
                count++;
            }
        }

        @Override
        Object getResult() {
            return count > 0 ? sum / count : 0;
        }
    }

    /**
     * MAX 聚合
     */
    private static class MaxState extends AggregateState {
        private Object max;

        @Override
        void accumulate(Object value) {
            if (value instanceof Comparable) {
                if (max == null || ValueComparator.compare(value, max) > 0) {
                    max = value;
                }
            }
        }

        @Override
        Object getResult() {
            return max;
        }
    }

    /**
     * MIN 聚合
     */
    private static class MinState extends AggregateState {
        private Object min;

        @Override
        void accumulate(Object value) {
            if (value instanceof Comparable) {
                if (min == null || ValueComparator.compare(value, min) < 0) {
                    min = value;
                }
            }
        }

        @Override
        Object getResult() {
            return min;
        }
    }

    /**
     * 分组键
     */
    private static class GroupKey {
        final Object[] values;

        GroupKey(int size) {
            this.values = new Object[size];
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupKey groupKey = (GroupKey) o;
            return Arrays.equals(values, groupKey.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
