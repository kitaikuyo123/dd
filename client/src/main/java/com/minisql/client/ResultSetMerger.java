package com.minisql.client;

import com.minisql.sql.execution.Row;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * 结果集合并器
 * 用于合并来自多个 MySQL 实例的查询结果
 *
 * 支持功能：
 * 1. 简单的结果合并（无排序）
 * 2. 支持 ORDER BY 的全局排序
 * 3. 支持 LIMIT/OFFSET 的全局截取
 * 4. 支持聚合函数的合并计算
 */
public class ResultSetMerger {

    /**
     * 合并多个 ResultSet（无排序）
     */
    public static List<Row> mergeSimple(List<ResultSet> results) throws SQLException {
        List<Row> allRows = new ArrayList<>();

        for (ResultSet rs : results) {
            while (rs.next()) {
                allRows.add(readRow(rs));
            }
            rs.close();
        }

        return allRows;
    }

    /**
     * 合并多个 ResultSet，支持排序和限制
     *
     * @param results 多个结果集
     * @param orderByColumns 排序列名列表
     * @param ascending 是否升序
     * @param limit 限制返回行数（-1 表示不限制）
     * @param offset 跳过行数
     * @return 合并后的结果
     */
    public static List<Row> mergeSorted(List<ResultSet> results,
                                         List<String> orderByColumns,
                                         List<Boolean> ascending,
                                         int limit,
                                         int offset) throws SQLException {
        // 1. 合并所有结果
        List<Row> allRows = mergeSimple(results);

        // 2. 排序
        if (orderByColumns != null && !orderByColumns.isEmpty()) {
            allRows.sort(createComparator(orderByColumns, ascending));
        }

        // 3. 应用 offset 和 limit
        int fromIndex = Math.max(0, offset);
        int toIndex = (limit < 0) ? allRows.size() : Math.min(allRows.size(), fromIndex + limit);

        if (fromIndex >= allRows.size()) {
            return new ArrayList<>();
        }

        return allRows.subList(fromIndex, toIndex);
    }

    /**
     * 使用优先队列合并已排序的结果集（更高效）
     * 假设每个输入 ResultSet 已经是有序的
     */
    public static List<Row> mergeSortedEfficient(List<ResultSet> results,
                                                  List<String> orderByColumns,
                                                  List<Boolean> ascending,
                                                  int limit,
                                                  int offset) throws SQLException {
        if (results.isEmpty()) {
            return new ArrayList<>();
        }

        // 创建优先队列
        PriorityQueue<RowWithSource> pq = new PriorityQueue<>(createRowComparator(orderByColumns, ascending));

        // 初始化：从每个 ResultSet 读取第一行
        for (int i = 0; i < results.size(); i++) {
            ResultSet rs = results.get(i);
            if (rs.next()) {
                pq.offer(new RowWithSource(readRow(rs), i, rs));
            }
        }

        List<Row> result = new ArrayList<>();
        int count = 0;
        int skipped = 0;

        while (!pq.isEmpty()) {
            RowWithSource top = pq.poll();

            // 处理 offset
            if (skipped < offset) {
                skipped++;
            } else {
                result.add(top.row);
                count++;

                // 检查是否达到 limit
                if (limit >= 0 && count >= limit) {
                    break;
                }
            }

            // 从同一个 ResultSet 读取下一行
            if (top.sourceResultSet.next()) {
                pq.offer(new RowWithSource(readRow(top.sourceResultSet), top.sourceIndex, top.sourceResultSet));
            }
        }

        // 关闭所有 ResultSet
        for (ResultSet rs : results) {
            rs.close();
        }

        return result;
    }

    /**
     * 合并聚合结果
     *
     * @param results 多个部分聚合结果集
     * @param groupByColumns 分组列
     * @param aggregateFunctions 聚合函数列表
     * @return 合并后的聚合结果
     */
    public static List<Row> mergeAggregations(List<ResultSet> results,
                                               List<String> groupByColumns,
                                               List<AggregateFunction> aggregateFunctions) throws SQLException {
        // 使用 Map 存储分组结果
        Map<List<Object>, AggregationState> groups = new java.util.HashMap<>();

        for (ResultSet rs : results) {
            while (rs.next()) {
                // 读取分组键
                List<Object> groupKey = new ArrayList<>();
                for (String col : groupByColumns) {
                    groupKey.add(rs.getObject(col));
                }

                // 获取或创建聚合状态
                AggregationState state = groups.computeIfAbsent(groupKey, k -> new AggregationState(aggregateFunctions));

                // 更新聚合状态
                state.updateFromRow(rs);
            }
            rs.close();
        }

        // 构建最终结果
        List<Row> finalResult = new ArrayList<>();
        for (Map.Entry<List<Object>, AggregationState> entry : groups.entrySet()) {
            Row row = new Row();

            // 添加分组列
            for (int i = 0; i < groupByColumns.size(); i++) {
                row.addColumn(groupByColumns.get(i), entry.getKey().get(i));
            }

            // 添加聚合结果
            entry.getValue().finalizeResult(row);

            finalResult.add(row);
        }

        return finalResult;
    }

    /**
     * 创建排序比较器（public 可见）
     */
    public static Comparator<Row> createComparator(List<String> orderByColumns, List<Boolean> ascending) {
        return (row1, row2) -> {
            for (int i = 0; i < orderByColumns.size(); i++) {
                String col = orderByColumns.get(i);
                boolean asc = ascending != null && ascending.get(i);

                Object val1 = row1.getColumnValue(col);
                Object val2 = row2.getColumnValue(col);

                int cmp = compareValues(val1, val2);
                if (cmp != 0) {
                    return asc ? cmp : -cmp;
                }
            }
            return 0;
        };
    }

    /**
     * 创建带优先队列的比较器
     */
    private static Comparator<RowWithSource> createRowComparator(List<String> orderByColumns, List<Boolean> ascending) {
        return (rws1, rws2) -> {
            for (int i = 0; i < orderByColumns.size(); i++) {
                String col = orderByColumns.get(i);
                boolean asc = ascending != null && ascending.get(i);

                Object val1 = rws1.row.getColumnValue(col);
                Object val2 = rws2.row.getColumnValue(col);

                int cmp = compareValues(val1, val2);
                if (cmp != 0) {
                    return asc ? cmp : -cmp;
                }
            }
            return 0;
        };
    }

    /**
     * 比较两个值
     */
    @SuppressWarnings("unchecked")
    private static int compareValues(Object val1, Object val2) {
        if (val1 == null && val2 == null) {
            return 0;
        }
        if (val1 == null) {
            return -1;
        }
        if (val2 == null) {
            return 1;
        }

        if (val1 instanceof Comparable) {
            return ((Comparable<Object>) val1).compareTo(val2);
        }

        //  fallback 到字符串比较
        return val1.toString().compareTo(val2.toString());
    }

    /**
     * 从 ResultSet 读取一行
     */
    private static Row readRow(ResultSet rs) throws SQLException {
        Row row = new Row();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            Object value = rs.getObject(i);
            row.addColumn(columnName, value);
        }

        return row;
    }

    /**
     * 内部类：带来源信息的行
     */
    private static class RowWithSource {
        final Row row;
        final int sourceIndex;
        final ResultSet sourceResultSet;

        RowWithSource(Row row, int sourceIndex, ResultSet sourceResultSet) {
            this.row = row;
            this.sourceIndex = sourceIndex;
            this.sourceResultSet = sourceResultSet;
        }
    }

    /**
     * 聚合函数类型
     */
    public enum AggregateType {
        COUNT, SUM, AVG, MAX, MIN, COUNT_DISTINCT
    }

    /**
     * 聚合函数定义
     */
    public static class AggregateFunction {
        private final String name;
        private final AggregateType type;
        private final String columnName;
        private final boolean distinct;

        public AggregateFunction(String name, AggregateType type, String columnName, boolean distinct) {
            this.name = name;
            this.type = type;
            this.columnName = columnName;
            this.distinct = distinct;
        }

        public String getName() { return name; }
        public AggregateType getType() { return type; }
        public String getColumnName() { return columnName; }
        public boolean isDistinct() { return distinct; }
    }

    /**
     * 聚合状态
     * 用于合并来自多个 Region 的部分聚合结果
     */
    public static class AggregationState {
        private long count = 0;
        private double sum = 0;
        private Object max, min;
        private final List<AggregateFunction> functions;

        AggregationState(List<AggregateFunction> functions) {
            this.functions = functions;
        }

        /**
         * 从结果集读取部分聚合值并合并
         * 假设每个 Region 返回的是部分聚合结果
         */
        public void updateFromRow(ResultSet rs) throws SQLException {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                Object value = rs.getObject(i);
                updateFromValue(columnName, value);
            }
        }

        /**
         * 从 Row 读取部分聚合值并合并
         * 用于内存中的 Row 对象
         */
        public void updateFromRow(Row row) {
            for (String columnName : row.getColumnNames()) {
                Object value = row.getColumnValue(columnName);
                updateFromValue(columnName, value);
            }
        }

        /**
         * 更新单个聚合值
         */
        @SuppressWarnings("unchecked")
        private void updateFromValue(String columnName, Object value) {
            // 查找对应的聚合函数
            for (AggregateFunction func : functions) {
                String funcName = func.getName();

                // 匹配聚合函数列名（如 COUNT(*), SUM(col) 等）
                if (columnName.equalsIgnoreCase(funcName) ||
                    columnName.equalsIgnoreCase(func.getType().name() + "(" + func.getColumnName() + ")")) {

                    switch (func.getType()) {
                        case COUNT:
                        case COUNT_DISTINCT:
                            if (value instanceof Number) {
                                count += ((Number) value).longValue();
                            }
                            break;
                        case SUM:
                            if (value instanceof Number) {
                                sum += ((Number) value).doubleValue();
                            }
                            break;
                        case AVG:
                            // AVG 需要特殊处理，存储 sum 和 count
                            // 假设 AVG 结果是一个对象数组 {sum, count}
                            if (value instanceof Map) {
                                Map<String, Object> avgData = (Map<String, Object>) value;
                                if (avgData.containsKey("sum")) {
                                    sum += ((Number) avgData.get("sum")).doubleValue();
                                }
                                if (avgData.containsKey("count")) {
                                    count += ((Number) avgData.get("count")).longValue();
                                }
                            }
                            break;
                        case MAX:
                            if (max == null || compareValues(value, max) > 0) {
                                max = value;
                            }
                            break;
                        case MIN:
                            if (min == null || compareValues(value, min) < 0) {
                                min = value;
                            }
                            break;
                    }
                    break;
                }
            }
        }

        /**
         * 将最终聚合值写入结果行
         */
        void finalizeResult(Row row) {
            for (AggregateFunction func : functions) {
                String funcName = func.getName();
                Object result = null;

                switch (func.getType()) {
                    case COUNT:
                    case COUNT_DISTINCT:
                        result = count;
                        break;
                    case SUM:
                        result = sum;
                        break;
                    case AVG:
                        result = (count > 0) ? sum / count : null;
                        break;
                    case MAX:
                        result = max;
                        break;
                    case MIN:
                        result = min;
                        break;
                }

                row.addColumn(funcName, result);
            }
        }
    }

}
