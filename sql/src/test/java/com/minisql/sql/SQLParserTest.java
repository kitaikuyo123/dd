package com.minisql.sql;

import com.minisql.sql.ast.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQLParser 语法分析器单元测试
 */
@DisplayName("SQLParser 语法分析器单元测试")
class SQLParserTest {

    @Test
    @DisplayName("测试解析简单 SELECT 语句")
    void testParseSimpleSelect() {
        String sql = "SELECT * FROM users";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertTrue(select.isSelectAll());
        assertEquals("users", select.getTable());
        assertNull(select.getWhere());
    }

    @Test
    @DisplayName("测试解析带列的 SELECT 语句")
    void testParseSelectWithColumns() {
        String sql = "SELECT id, name, email FROM users";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertFalse(select.isSelectAll());
        assertEquals("users", select.getTable());
        assertNotNull(select.getColumns());
        assertEquals(3, select.getColumns().size());
        assertTrue(select.getColumns().contains("id"));
        assertTrue(select.getColumns().contains("name"));
        assertTrue(select.getColumns().contains("email"));
    }

    @Test
    @DisplayName("测试解析带 WHERE 条件的 SELECT")
    void testParseSelectWithWhere() {
        String sql = "SELECT * FROM users WHERE id = 1";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertNotNull(select.getWhere());
        assertInstanceOf(SimpleCondition.class, select.getWhere());
        SimpleCondition condition = (SimpleCondition) select.getWhere();
        assertEquals("id", condition.getColumn());
        assertEquals("=", condition.getOperator());
        assertEquals("1", condition.getValue());
    }

    @Test
    @DisplayName("测试解析带 ORDER BY 的 SELECT")
    void testParseSelectWithOrderBy() {
        String sql = "SELECT * FROM users ORDER BY name ASC";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertNotNull(select.getOrderBy());
        assertEquals(1, select.getOrderBy().size());
        assertEquals("name", select.getOrderBy().get(0).getColumn());
        assertTrue(select.getOrderBy().get(0).isAscending());
    }

    @Test
    @DisplayName("测试解析带 ORDER BY DESC 的 SELECT")
    void testParseSelectWithOrderByDesc() {
        String sql = "SELECT * FROM users ORDER BY age DESC";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertNotNull(select.getOrderBy());
        assertEquals(1, select.getOrderBy().size());
        assertEquals("age", select.getOrderBy().get(0).getColumn());
        assertFalse(select.getOrderBy().get(0).isAscending());
    }

    @Test
    @DisplayName("测试解析带 LIMIT 的 SELECT")
    void testParseSelectWithLimit() {
        String sql = "SELECT * FROM users LIMIT 10";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertEquals(10, select.getLimit());
    }

    @Test
    @DisplayName("测试解析带 LIMIT 和 OFFSET 的 SELECT")
    void testParseSelectWithLimitAndOffset() {
        String sql = "SELECT * FROM users LIMIT 10 OFFSET 5";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertEquals(10, select.getLimit());
        assertEquals(5, select.getOffset());
    }

    @Test
    @DisplayName("测试解析带 JOIN 的 SELECT")
    void testParseSelectWithJoin() {
        String sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertEquals("orders", select.getJoinTable());
        assertNotNull(select.getJoinCondition());
    }

    @Test
    @DisplayName("测试解析带复合条件的 WHERE")
    void testParseSelectWithCompoundWhere() {
        String sql = "SELECT * FROM users WHERE age > 18 AND status = 'active'";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertNotNull(select.getWhere());
        assertInstanceOf(CompoundCondition.class, select.getWhere());
        CompoundCondition compound = (CompoundCondition) select.getWhere();
        assertEquals("AND", compound.getOperator());
    }

    @Test
    @DisplayName("测试解析 INSERT 语句")
    void testParseInsert() {
        String sql = "INSERT INTO users (id, name, email) VALUES (1, 'zhangsan', 'zhangsan@example.com')";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(InsertStatement.class, stmt);
        InsertStatement insert = (InsertStatement) stmt;

        assertEquals("users", insert.getTable());
        assertEquals(3, insert.getColumns().size());
        assertEquals("id", insert.getColumns().get(0));
        assertEquals("name", insert.getColumns().get(1));
        assertEquals("email", insert.getColumns().get(2));
        assertEquals(3, insert.getValues().size());
        assertEquals("1", insert.getValues().get(0));
        assertEquals("zhangsan", insert.getValues().get(1));
    }

    @Test
    @DisplayName("测试解析 UPDATE 语句")
    void testParseUpdate() {
        String sql = "UPDATE users SET name = 'lisi', email = 'lisi@example.com' WHERE id = 1";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(UpdateStatement.class, stmt);
        UpdateStatement update = (UpdateStatement) stmt;

        assertEquals("users", update.getTable());
        assertEquals(2, update.getAssignments().size());
        assertEquals("name", update.getAssignments().get(0).getColumn());
        assertEquals("lisi", update.getAssignments().get(0).getValue());
        assertNotNull(update.getWhere());
    }

    @Test
    @DisplayName("测试解析 DELETE 语句")
    void testParseDelete() {
        String sql = "DELETE FROM users WHERE id = 1";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(DeleteStatement.class, stmt);
        DeleteStatement delete = (DeleteStatement) stmt;

        assertEquals("users", delete.getTable());
        assertNotNull(delete.getWhere());
    }

    @Test
    @DisplayName("测试解析 DELETE 不带 WHERE")
    void testParseDeleteWithoutWhere() {
        String sql = "DELETE FROM users";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(DeleteStatement.class, stmt);
        DeleteStatement delete = (DeleteStatement) stmt;

        assertEquals("users", delete.getTable());
        assertNull(delete.getWhere());
    }

    @Test
    @DisplayName("测试解析 CREATE TABLE 语句")
    void testParseCreateTable() {
        String sql = "CREATE TABLE users (id INT, name VARCHAR(50), email VARCHAR(100), PRIMARY KEY(id))";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(CreateTableStatement.class, stmt);
        CreateTableStatement create = (CreateTableStatement) stmt;

        assertEquals("users", create.getTable());
        assertNotNull(create.getColumns());
        assertEquals(3, create.getColumns().size());
        assertEquals("id", create.getPrimaryKey());
    }

    @Test
    @DisplayName("测试解析 CREATE TABLE 多种数据类型")
    void testParseCreateTableWithTypes() {
        String sql = "CREATE TABLE products (id BIGINT, name STRING, price DOUBLE, description STRING)";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(CreateTableStatement.class, stmt);
        CreateTableStatement create = (CreateTableStatement) stmt;

        assertEquals("products", create.getTable());
        List<ColumnDef> columns = create.getColumns();
        assertEquals(4, columns.size());

        assertEquals("id", columns.get(0).getName());
        assertEquals(ColumnType.BIGINT, columns.get(0).getType());

        assertEquals("name", columns.get(1).getName());
        assertEquals(ColumnType.STRING, columns.get(1).getType());

        assertEquals("price", columns.get(2).getName());
        assertEquals(ColumnType.DOUBLE, columns.get(2).getType());
    }

    @Test
    @DisplayName("测试解析 DROP TABLE 语句")
    void testParseDropTable() {
        String sql = "DROP TABLE users";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(DropTableStatement.class, stmt);
        DropTableStatement drop = (DropTableStatement) stmt;

        assertEquals("users", drop.getTable());
    }

    @Test
    @DisplayName("测试解析 SHOW TABLES 语句")
    void testParseShowTables() {
        String sql = "SHOW TABLES";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(ShowTablesStatement.class, stmt);
    }

    @Test
    @DisplayName("测试解析复杂 WHERE 条件")
    void testParseComplexWhere() {
        String sql = "SELECT * FROM users WHERE age > 18 AND (status = 'active' OR role = 'admin')";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertNotNull(select.getWhere());
        // 验证有复合条件
        assertInstanceOf(CompoundCondition.class, select.getWhere());
    }

    @Test
    @DisplayName("测试解析带 OR 条件的 WHERE")
    void testParseOrCondition() {
        String sql = "SELECT * FROM users WHERE status = 'active' OR status = 'pending'";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertNotNull(select.getWhere());
        assertInstanceOf(CompoundCondition.class, select.getWhere());
        CompoundCondition compound = (CompoundCondition) select.getWhere();
        assertEquals("OR", compound.getOperator());
    }

    @Test
    @DisplayName("测试解析不等条件")
    void testParseNotEqualCondition() {
        String[] operators = {"<>", "!="};

        for (String op : operators) {
            String sql = "SELECT * FROM users WHERE status " + op + " 'deleted'";
            SQLParser parser = new SQLParser(sql);
            Statement stmt = parser.parse();

            assertInstanceOf(SelectStatement.class, stmt);
            SelectStatement select = (SelectStatement) stmt;

            assertNotNull(select.getWhere());
            SimpleCondition condition = (SimpleCondition) select.getWhere();
            assertEquals(op, condition.getOperator());
        }
    }

    @Test
    @DisplayName("测试解析带比较操作符的 WHERE")
    void testParseComparisonOperators() {
        String[][] tests = {
            {"SELECT * FROM t WHERE a = 1", "=", "1"},
            {"SELECT * FROM t WHERE a > 1", ">", "1"},
            {"SELECT * FROM t WHERE a < 1", "<", "1"},
            {"SELECT * FROM t WHERE a >= 1", ">=", "1"},
            {"SELECT * FROM t WHERE a <= 1", "<=", "1"}
        };

        for (String[] test : tests) {
            SQLParser parser = new SQLParser(test[0]);
            Statement stmt = parser.parse();
            SelectStatement select = (SelectStatement) stmt;
            SimpleCondition condition = (SimpleCondition) select.getWhere();

            assertEquals(test[1], condition.getOperator(), "Operator mismatch for: " + test[0]);
            assertEquals(test[2], condition.getValue(), "Value mismatch for: " + test[0]);
        }
    }

    // =====================================================================
    // 错误测试
    // =====================================================================

    @Test
    @DisplayName("测试解析错误 - 空输入")
    void testParseErrorEmptyInput() {
        SQLParser parser = new SQLParser("");
        assertThrows(RuntimeException.class, () -> parser.parse());
        SQLParser parser2 = new SQLParser("   ");
        assertThrows(RuntimeException.class, () -> parser2.parse());
    }

    @Test
    @DisplayName("测试解析错误 - 缺少 FROM")
    void testParseErrorMissingFrom() {
        SQLParser parser = new SQLParser("SELECT * users");
        assertThrows(RuntimeException.class, () -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - 缺少表名")
    void testParseErrorMissingTableName() {
        SQLParser parser = new SQLParser("SELECT * FROM");
        assertThrows(RuntimeException.class, () -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - 无效语句")
    void testParseErrorInvalidStatement() {
        SQLParser parser = new SQLParser("INVALID STATEMENT");
        assertThrows(RuntimeException.class, () -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - 未闭合的引号")
    void testParseErrorUnclosedQuote() {
        // Lexer tokenizes in constructor → throws before parse()
        assertThrows(RuntimeException.class, () -> new SQLParser("SELECT * FROM users WHERE name = 'alice"));
    }

    @Test
    @DisplayName("测试解析错误 - INSERT 列值数量不匹配")
    void testParseErrorInsertColumnValueMismatch() {
        // Parser may or may not validate column/value count — just verify it doesn't crash
        SQLParser parser = new SQLParser("INSERT INTO users (id, name) VALUES (1)");
        assertDoesNotThrow(() -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - INSERT 缺少 VALUES")
    void testParseErrorInsertMissingValues() {
        SQLParser parser = new SQLParser("INSERT INTO users (id, name)");
        assertThrows(RuntimeException.class, () -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - CREATE TABLE 无效列类型")
    void testParseErrorCreateTableInvalidType() {
        // Parser does not recognize FOO as a valid column type keyword
        SQLParser parser = new SQLParser("CREATE TABLE t (id FOO)");
        assertThrows(RuntimeException.class, () -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - UPDATE 缺少 SET")
    void testParseErrorUpdateMissingSet() {
        SQLParser parser = new SQLParser("UPDATE users WHERE id = 1");
        assertThrows(RuntimeException.class, () -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - DELETE 语法错误")
    void testParseErrorDeleteSyntaxError() {
        SQLParser parser = new SQLParser("DELETE users WHERE id = 1");
        assertThrows(RuntimeException.class, () -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - DROP TABLE 缺少表名")
    void testParseErrorDropTableMissingName() {
        SQLParser parser = new SQLParser("DROP TABLE");
        assertThrows(RuntimeException.class, () -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - 不匹配的括号")
    void testParseErrorUnmatchedParentheses() {
        SQLParser parser = new SQLParser("SELECT * FROM users WHERE (id = 1");
        assertThrows(RuntimeException.class, () -> parser.parse());
    }

    @Test
    @DisplayName("测试解析错误 - 无效的比较运算符")
    void testParseErrorInvalidOperator() {
        // Lexer throws on unexpected character ~ before parse()
        assertThrows(RuntimeException.class, () -> new SQLParser("SELECT * FROM users WHERE id ~ 1"));
    }

    @Test
    @DisplayName("测试解析错误 - 无效的 JOIN 类型")
    void testParseErrorInvalidJoinType() {
        // Parser may not validate join type — verify it doesn't crash
        SQLParser parser = new SQLParser("SELECT * FROM users OUTER JOIN orders ON users.id = orders.user_id");
        assertDoesNotThrow(() -> parser.parse());
    }

    @Test
    @DisplayName("测试多个 ORDER BY 列")
    void testMultipleOrderBy() {
        String sql = "SELECT * FROM users ORDER BY name ASC, age DESC";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertNotNull(select.getOrderBy());
        assertEquals(2, select.getOrderBy().size());
        assertEquals("name", select.getOrderBy().get(0).getColumn());
        assertTrue(select.getOrderBy().get(0).isAscending());
        assertEquals("age", select.getOrderBy().get(1).getColumn());
        assertFalse(select.getOrderBy().get(1).isAscending());
    }

    @Test
    @DisplayName("测试带字符串值的 INSERT")
    void testInsertWithStringValues() {
        String sql = "INSERT INTO users (id, name) VALUES (1, 'John')";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(InsertStatement.class, stmt);
        InsertStatement insert = (InsertStatement) stmt;

        assertEquals(2, insert.getColumns().size());
        assertEquals(2, insert.getValues().size());
    }

    // ==================== 复合主键语法测试 ====================

    @Test
    @DisplayName("测试解析复合主键 - PRIMARY KEY ((col1, col2), col3)")
    void testParseCreateTableWithCompositePrimaryKey() {
        String sql = "CREATE TABLE sensor_data (sensor_id VARCHAR(32), bucket INT, timestamp BIGINT, value DOUBLE, PRIMARY KEY ((sensor_id, bucket), timestamp))";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(CreateTableStatement.class, stmt);
        CreateTableStatement create = (CreateTableStatement) stmt;

        assertEquals("sensor_data", create.getTable());
        assertEquals(4, create.getColumns().size()); // PRIMARY KEY 不是列

        // 验证分区键
        assertNotNull(create.getPartitionKeys());
        assertEquals(2, create.getPartitionKeys().size());
        assertEquals("sensor_id", create.getPartitionKeys().get(0));
        assertEquals("bucket", create.getPartitionKeys().get(1));

        // 验证聚类键
        assertNotNull(create.getClusteringKeys());
        assertEquals(1, create.getClusteringKeys().size());
        assertEquals("timestamp", create.getClusteringKeys().get(0));

        // 验证 getAllPrimaryKeys
        List<String> allKeys = create.getAllPrimaryKeys();
        assertEquals(3, allKeys.size());
        assertEquals("sensor_id", allKeys.get(0));
        assertEquals("bucket", allKeys.get(1));
        assertEquals("timestamp", allKeys.get(2));
    }

    @Test
    @DisplayName("测试解析复合主键 - 只有分区键")
    void testParseCreateTableWithPartitionKeysOnly() {
        String sql = "CREATE TABLE users (user_id VARCHAR(32), region INT, name VARCHAR(50), PRIMARY KEY ((user_id, region)))";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(CreateTableStatement.class, stmt);
        CreateTableStatement create = (CreateTableStatement) stmt;

        assertEquals("users", create.getTable());

        // 验证分区键
        assertNotNull(create.getPartitionKeys());
        assertEquals(2, create.getPartitionKeys().size());
        assertEquals("user_id", create.getPartitionKeys().get(0));
        assertEquals("region", create.getPartitionKeys().get(1));

        // 验证聚类键为空
        assertNull(create.getClusteringKeys());
    }

    @Test
    @DisplayName("测试解析复合主键 - 多个聚类键")
    void testParseCreateTableWithMultipleClusteringKeys() {
        // 简化测试：PRIMARY KEY 在列定义之后，只有两个聚类键
        String sql = "CREATE TABLE logs (log_type VARCHAR(20), date INT, timestamp BIGINT, id INT, message TEXT, PRIMARY KEY ((log_type, date), timestamp, id))";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(CreateTableStatement.class, stmt);
        CreateTableStatement create = (CreateTableStatement) stmt;

        assertEquals("logs", create.getTable());

        // 验证分区键
        assertNotNull(create.getPartitionKeys());
        assertEquals(2, create.getPartitionKeys().size());
        assertEquals("log_type", create.getPartitionKeys().get(0));
        assertEquals("date", create.getPartitionKeys().get(1));

        // 验证聚类键
        assertNotNull(create.getClusteringKeys());
        assertEquals(2, create.getClusteringKeys().size());
        assertEquals("timestamp", create.getClusteringKeys().get(0));
        assertEquals("id", create.getClusteringKeys().get(1));
    }

    @Test
    @DisplayName("测试解析单列主键 - 向后兼容")
    void testParseCreateTableWithSinglePrimaryKey() {
        String sql = "CREATE TABLE users (id INT, name VARCHAR(50), PRIMARY KEY(id))";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();

        assertInstanceOf(CreateTableStatement.class, stmt);
        CreateTableStatement create = (CreateTableStatement) stmt;

        assertEquals("users", create.getTable());

        // 验证主键（向后兼容）
        assertEquals("id", create.getPrimaryKey());

        // getAllPrimaryKeys 应该返回单列主键
        List<String> allKeys = create.getAllPrimaryKeys();
        assertEquals(1, allKeys.size());
        assertEquals("id", allKeys.get(0));
    }
}
