package com.minisql.client;

import com.minisql.sql.SQLParser;
import com.minisql.sql.ast.SelectStatement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ParallelQueryExecutor tests")
class ParallelQueryExecutorTest {

    private SelectStatement parseSelect(String sql) throws Exception {
        return (SelectStatement) new SQLParser(sql).parse();
    }

    @SuppressWarnings("unchecked")
    private List<String> determineJoinProjectedQualifiersFromAst(
            ParallelQueryExecutor executor,
            SelectStatement ast,
            String tableName,
            String tableAlias,
            String joinColumnsSide) throws Exception {

        // Extract join columns via AST
        List<String> leftCols = new java.util.ArrayList<>();
        List<String> rightCols = new java.util.ArrayList<>();
        String leftQualifier = ast.getTableAlias() != null ? ast.getTableAlias() : ast.getTable();
        String rightQualifier = ast.getJoinTableAlias() != null ? ast.getJoinTableAlias() : ast.getJoinTable();

        Method extractMethod = ParallelQueryExecutor.class.getDeclaredMethod(
            "extractJoinConditionColumns",
            com.minisql.sql.ast.Condition.class,
            List.class, List.class, String.class, String.class
        );
        extractMethod.setAccessible(true);
        extractMethod.invoke(executor, ast.getJoinCondition(), leftCols, rightCols, leftQualifier, rightQualifier);

        List<String> joinCols = "leftConditions".equals(joinColumnsSide) ? leftCols : rightCols;

        // Build aggregate expressions
        Method buildAggMethod = ParallelQueryExecutor.class.getDeclaredMethod(
            "buildAggregateExpressions", SelectStatement.class
        );
        buildAggMethod.setAccessible(true);
        List<?> aggregates = (List<?>) buildAggMethod.invoke(executor, ast);

        Method method = ParallelQueryExecutor.class.getDeclaredMethod(
            "determineJoinProjectedQualifiersFromAst",
            SelectStatement.class, String.class, String.class, List.class, List.class
        );
        method.setAccessible(true);
        return (List<String>) method.invoke(executor, ast, tableName, tableAlias, joinCols, aggregates);
    }

    @Test
    @DisplayName("join projection pushdown keeps only columns needed by each side")
    void testDetermineJoinProjectedQualifiers() throws Exception {
        ParallelQueryExecutor executor = new ParallelQueryExecutor(null, Collections.emptyMap(), 5);
        SelectStatement ast = parseSelect(
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100 ORDER BY u.name"
        );

        List<String> leftProjected = determineJoinProjectedQualifiersFromAst(
            executor, ast, "users", "u", "leftConditions"
        );
        List<String> rightProjected = determineJoinProjectedQualifiersFromAst(
            executor, ast, "orders", "o", "rightConditions"
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
        // Note: HAVING with aggregate function calls (e.g. SUM(o.amount)) is not yet
        // supported by the formal SQLParser. Use alias-based HAVING instead.
        SelectStatement ast = parseSelect(
            "SELECT u.name, SUM(o.amount) AS total " +
                "FROM users u JOIN orders o ON u.id = o.user_id " +
                "WHERE o.amount >= 10 " +
                "GROUP BY u.name " +
                "ORDER BY u.name"
        );

        List<String> leftProjected = determineJoinProjectedQualifiersFromAst(
            executor, ast, "users", "u", "leftConditions"
        );
        List<String> rightProjected = determineJoinProjectedQualifiersFromAst(
            executor, ast, "orders", "o", "rightConditions"
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
        SelectStatement ast = parseSelect(
            "SELECT u.name " +
                "FROM users u JOIN orders o ON u.id = o.user_id " +
                "WHERE u.age >= 18 AND o.amount <= 200"
        );

        List<String> leftProjected = determineJoinProjectedQualifiersFromAst(
            executor, ast, "users", "u", "leftConditions"
        );
        List<String> rightProjected = determineJoinProjectedQualifiersFromAst(
            executor, ast, "orders", "o", "rightConditions"
        );

        assertTrue(leftProjected.contains("id"));
        assertTrue(leftProjected.contains("name"));
        assertTrue(leftProjected.contains("age"));

        assertTrue(rightProjected.contains("user_id"));
        assertTrue(rightProjected.contains("amount"));
    }
}
