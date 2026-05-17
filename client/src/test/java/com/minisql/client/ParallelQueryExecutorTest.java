package com.minisql.client;

import com.minisql.client.executor.QueryPlanner;
import com.minisql.sql.SQLParser;
import com.minisql.sql.ast.SelectStatement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 QueryPlanner 的 JOIN 列投影计算逻辑。
 *
 * <p>确保每个表只拉取 JOIN 条件、SELECT 列表、WHERE、GROUP BY、
 * HAVING 和聚合函数实际引用的列，避免传输冗余数据。
 */
@DisplayName("QueryPlanner JOIN 投影测试")
class ParallelQueryExecutorTest {

    private SelectStatement parseSelect(String sql) throws Exception {
        return (SelectStatement) new SQLParser(sql).parse();
    }

    private List<String> determineJoinProjectedQualifiers(
            SelectStatement ast, String tableName, String tableAlias,
            String joinColumnsSide) {

        List<String> leftCols = new ArrayList<>();
        List<String> rightCols = new ArrayList<>();
        String leftQualifier = ast.getTableAlias() != null ? ast.getTableAlias() : ast.getTable();
        String rightQualifier = ast.getJoinTableAlias() != null ? ast.getJoinTableAlias() : ast.getJoinTable();

        QueryPlanner.extractJoinConditionColumns(ast.getJoinCondition(),
            leftCols, rightCols, leftQualifier, rightQualifier);

        List<String> joinCols = "leftConditions".equals(joinColumnsSide) ? leftCols : rightCols;
        List<QueryPlanner.AggregateExpression> aggregates =
            QueryPlanner.buildAggregateExpressions(ast);

        return QueryPlanner.determineJoinProjectedQualifiers(
            ast, tableName, tableAlias, joinCols, aggregates);
    }

    @Test
    @DisplayName("JOIN 投影下推：每侧只保留所需列")
    void testDetermineJoinProjectedQualifiers() throws Exception {
        SelectStatement ast = parseSelect(
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100 ORDER BY u.name"
        );

        List<String> leftProjected = determineJoinProjectedQualifiers(ast, "users", "u", "leftConditions");
        List<String> rightProjected = determineJoinProjectedQualifiers(ast, "orders", "o", "rightConditions");

        assertTrue(leftProjected.contains("id"));
        assertTrue(leftProjected.contains("name"));
        assertTrue(!leftProjected.contains("user_id"));

        assertTrue(rightProjected.contains("user_id"));
        assertTrue(rightProjected.contains("amount"));
        assertTrue(!rightProjected.contains("name"));
    }

    @Test
    @DisplayName("JOIN 投影下推：保留 GROUP BY 列和聚合源列")
    void testDetermineJoinProjectedQualifiersForGroupByAndHaving() throws Exception {
        SelectStatement ast = parseSelect(
            "SELECT u.name, SUM(o.amount) AS total " +
                "FROM users u JOIN orders o ON u.id = o.user_id " +
                "WHERE o.amount >= 10 " +
                "GROUP BY u.name " +
                "ORDER BY u.name"
        );

        List<String> leftProjected = determineJoinProjectedQualifiers(ast, "users", "u", "leftConditions");
        List<String> rightProjected = determineJoinProjectedQualifiers(ast, "orders", "o", "rightConditions");

        assertTrue(leftProjected.contains("id"));
        assertTrue(leftProjected.contains("name"));

        assertTrue(rightProjected.contains("user_id"));
        assertTrue(rightProjected.contains("amount"));
        assertTrue(!rightProjected.contains("name"));
    }

    @Test
    @DisplayName("JOIN 投影下推：保留 WHERE 引用的列")
    void testDetermineJoinProjectedQualifiersForWhereClauses() throws Exception {
        SelectStatement ast = parseSelect(
            "SELECT u.name " +
                "FROM users u JOIN orders o ON u.id = o.user_id " +
                "WHERE u.age >= 18 AND o.amount <= 200"
        );

        List<String> leftProjected = determineJoinProjectedQualifiers(ast, "users", "u", "leftConditions");
        List<String> rightProjected = determineJoinProjectedQualifiers(ast, "orders", "o", "rightConditions");

        assertTrue(leftProjected.contains("id"));
        assertTrue(leftProjected.contains("name"));
        assertTrue(leftProjected.contains("age"));

        assertTrue(rightProjected.contains("user_id"));
        assertTrue(rightProjected.contains("amount"));
    }
}
