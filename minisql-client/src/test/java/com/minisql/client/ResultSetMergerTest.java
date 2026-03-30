package com.minisql.client;

import com.minisql.sql.execution.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ResultSetMerger tests")
class ResultSetMergerTest {

    @Test
    @DisplayName("mergeSimple keeps all rows in source order")
    void mergeSimpleKeepsAllRowsInSourceOrder() throws SQLException {
        List<ResultSet> results = Arrays.asList(
            mockResultSet(Arrays.asList(
                row("id", 1, "name", "alice"),
                row("id", 2, "name", "bob")
            )),
            mockResultSet(Collections.singletonList(
                row("id", 3, "name", "charlie")
            ))
        );

        List<Row> merged = ResultSetMerger.mergeSimple(results);

        assertEquals(3, merged.size());
        assertEquals("alice", merged.get(0).getColumnValue("name"));
        assertEquals("bob", merged.get(1).getColumnValue("name"));
        assertEquals("charlie", merged.get(2).getColumnValue("name"));
    }

    @Test
    @DisplayName("mergeSorted applies ordering limit and offset")
    void mergeSortedAppliesOrderingLimitAndOffset() throws SQLException {
        List<ResultSet> results = Arrays.asList(
            mockResultSet(Arrays.asList(
                row("name", "charlie", "score", 70),
                row("name", "alice", "score", 95)
            )),
            mockResultSet(Arrays.asList(
                row("name", "david", "score", 88),
                row("name", "bob", "score", 91)
            ))
        );

        List<Row> merged = ResultSetMerger.mergeSorted(
            results,
            Collections.singletonList("name"),
            Collections.singletonList(true),
            2,
            1
        );

        assertEquals(2, merged.size());
        assertEquals("bob", merged.get(0).getColumnValue("name"));
        assertEquals("charlie", merged.get(1).getColumnValue("name"));
    }

    @Test
    @DisplayName("mergeAggregations combines partial results by group")
    void mergeAggregationsCombinesPartialResultsByGroup() throws SQLException {
        List<ResultSet> results = Arrays.asList(
            mockResultSet(Collections.singletonList(
                row("dept", "A", "COUNT(*)", 2L, "SUM(score)", 170.0)
            )),
            mockResultSet(Collections.singletonList(
                row("dept", "A", "COUNT(*)", 3L, "SUM(score)", 240.0)
            ))
        );

        List<ResultSetMerger.AggregateFunction> functions = Arrays.asList(
            new ResultSetMerger.AggregateFunction("COUNT(*)", ResultSetMerger.AggregateType.COUNT, "*", false),
            new ResultSetMerger.AggregateFunction("SUM(score)", ResultSetMerger.AggregateType.SUM, "score", false)
        );

        List<Row> merged = ResultSetMerger.mergeAggregations(
            results,
            Collections.singletonList("dept"),
            functions
        );

        assertEquals(1, merged.size());
        assertEquals("A", merged.get(0).getColumnValue("dept"));
        assertEquals(5L, merged.get(0).getColumnValue("COUNT(*)"));
        assertEquals(410.0, merged.get(0).getColumnValue("SUM(score)"));
    }

    @Test
    @DisplayName("row helper methods support client side result shaping")
    void rowHelperMethodsSupportClientSideResultShaping() {
        Row row = new Row();
        row.addColumn("id", 1);
        row.addColumn("name", "test");

        assertEquals(Arrays.asList("id", "name"), row.getColumnNames());
        assertEquals(Arrays.asList(1, "test"), Arrays.asList(row.getValues()));
        assertEquals("test", row.getColumnValue("name"));
        assertTrue(row.toString().contains("name"));
    }

    @Test
    @DisplayName("comparator supports mixed direction ordering")
    void comparatorSupportsMixedDirectionOrdering() {
        Row lowerName = row("name", "alice", "age", 30);
        Row higherName = row("name", "bob", "age", 20);

        assertTrue(
            ResultSetMerger.createComparator(Arrays.asList("name", "age"), Arrays.asList(true, false))
                .compare(lowerName, higherName) < 0
        );
    }

    private static Row row(Object... values) {
        Row row = new Row();
        for (int i = 0; i < values.length; i += 2) {
            row.addColumn(String.valueOf(values[i]), values[i + 1]);
        }
        return row;
    }

    private static ResultSet mockResultSet(List<Row> rows) {
        List<String> columnNames = rows.isEmpty() ? Collections.emptyList() : new ArrayList<>(rows.get(0).getColumnNames());
        final int[] index = {-1};

        ResultSetMetaData metaData = (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class<?>[]{ResultSetMetaData.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                    case "getColumnCount":
                        return columnNames.size();
                    case "getColumnLabel":
                    case "getColumnName":
                        return columnNames.get(((Integer) args[0]) - 1);
                    case "getColumnType":
                        return java.sql.Types.VARCHAR;
                    case "getColumnTypeName":
                        return "VARCHAR";
                    case "isNullable":
                        return ResultSetMetaData.columnNullable;
                    case "isWrapperFor":
                        return false;
                    case "unwrap":
                        throw new UnsupportedOperationException();
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            }
        );

        return (ResultSet) Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                    case "next":
                        index[0]++;
                        return index[0] < rows.size();
                    case "getMetaData":
                        return metaData;
                    case "getObject":
                        if (index[0] < 0 || index[0] >= rows.size()) {
                            return null;
                        }
                        if (args[0] instanceof Integer) {
                            String columnName = columnNames.get(((Integer) args[0]) - 1);
                            return rows.get(index[0]).getColumnValue(columnName);
                        }
                        return rows.get(index[0]).getColumnValue(String.valueOf(args[0]));
                    case "close":
                        return null;
                    case "isClosed":
                        return false;
                    case "isWrapperFor":
                        return false;
                    case "unwrap":
                        throw new UnsupportedOperationException();
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            }
        );
    }
}
