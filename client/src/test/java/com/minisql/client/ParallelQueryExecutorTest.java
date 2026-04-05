package com.minisql.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ParallelQueryExecutor tests")
class ParallelQueryExecutorTest {

    private Object parseQuerySpec(ParallelQueryExecutor executor, String sql) throws Exception {
        Method parseQuerySpec = ParallelQueryExecutor.class.getDeclaredMethod("parseQuerySpec", String.class);
        parseQuerySpec.setAccessible(true);
        return parseQuerySpec.invoke(executor, sql);
    }

    @SuppressWarnings("unchecked")
    private List<String> determineJoinProjectedQualifiers(ParallelQueryExecutor executor,
                                                          Object spec,
                                                          String tableName,
                                                          String tableAlias,
                                                          String joinSideField) throws Exception {
        Method method = ParallelQueryExecutor.class.getDeclaredMethod(
            "determineJoinProjectedQualifiers",
            spec.getClass(),
            String.class,
            String.class,
            List.class
        );
        method.setAccessible(true);

        Class<?> joinSpecClass = Class.forName("com.minisql.client.ParallelQueryExecutor$JoinQuerySpec");
        var joinSpecField = spec.getClass().getDeclaredField("joinSpec");
        joinSpecField.setAccessible(true);
        Object joinSpec = joinSpecField.get(spec);
        var joinColumnsField = joinSpecClass.getDeclaredField(joinSideField);
        joinColumnsField.setAccessible(true);

        return (List<String>) method.invoke(
            executor,
            spec,
            tableName,
            tableAlias,
            joinColumnsField.get(joinSpec)
        );
    }

    @Test
    @DisplayName("join projection pushdown keeps only columns needed by each side")
    void testDetermineJoinProjectedQualifiers() throws Exception {
        ParallelQueryExecutor executor = new ParallelQueryExecutor(null, Collections.emptyMap(), 5);
        Object spec = parseQuerySpec(
            executor,
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100 ORDER BY u.name"
        );

        List<String> leftProjected = determineJoinProjectedQualifiers(
            executor,
            spec,
            "users",
            "u",
            "leftConditions"
        );
        List<String> rightProjected = determineJoinProjectedQualifiers(
            executor,
            spec,
            "orders",
            "o",
            "rightConditions"
        );

        assertTrue(leftProjected.contains("id"));
        assertTrue(leftProjected.contains("name"));
        assertTrue(!leftProjected.contains("user_id"));

        assertTrue(rightProjected.contains("user_id"));
        assertTrue(rightProjected.contains("amount"));
        assertTrue(!rightProjected.contains("name"));
    }

    @Test
    @DisplayName("join projection pushdown keeps group by having and aggregate source columns")
    void testDetermineJoinProjectedQualifiersForGroupByAndHaving() throws Exception {
        ParallelQueryExecutor executor = new ParallelQueryExecutor(null, Collections.emptyMap(), 5);
        Object spec = parseQuerySpec(
            executor,
            "SELECT u.name, SUM(o.amount) AS total " +
                "FROM users u JOIN orders o ON u.id = o.user_id " +
                "WHERE o.amount >= 10 " +
                "GROUP BY u.name " +
                "HAVING SUM(o.amount) > 100 " +
                "ORDER BY u.name"
        );

        List<String> leftProjected = determineJoinProjectedQualifiers(
            executor,
            spec,
            "users",
            "u",
            "leftConditions"
        );
        List<String> rightProjected = determineJoinProjectedQualifiers(
            executor,
            spec,
            "orders",
            "o",
            "rightConditions"
        );

        assertTrue(leftProjected.contains("id"));
        assertTrue(leftProjected.contains("name"));

        assertTrue(rightProjected.contains("user_id"));
        assertTrue(rightProjected.contains("amount"));
        assertTrue(!rightProjected.contains("name"));
    }

    @Test
    @DisplayName("join projection pushdown keeps columns referenced by where clauses")
    void testDetermineJoinProjectedQualifiersForWhereClauses() throws Exception {
        ParallelQueryExecutor executor = new ParallelQueryExecutor(null, Collections.emptyMap(), 5);
        Object spec = parseQuerySpec(
            executor,
            "SELECT u.name " +
                "FROM users u JOIN orders o ON u.id = o.user_id " +
                "WHERE u.age >= 18 AND o.amount <= 200"
        );

        List<String> leftProjected = determineJoinProjectedQualifiers(
            executor,
            spec,
            "users",
            "u",
            "leftConditions"
        );
        List<String> rightProjected = determineJoinProjectedQualifiers(
            executor,
            spec,
            "orders",
            "o",
            "rightConditions"
        );

        assertTrue(leftProjected.contains("id"));
        assertTrue(leftProjected.contains("name"));
        assertTrue(leftProjected.contains("age"));

        assertTrue(rightProjected.contains("user_id"));
        assertTrue(rightProjected.contains("amount"));
    }
}
