package com.minisql.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("MiniSQLStatement")
class MiniSQLStatementTest {

    @Test
    @DisplayName("execute routes SHOW TABLES through executeQuery")
    void executeRoutesShowTablesAsQuery() throws SQLException {
        FakeConnection connection = new FakeConnection();
        MiniSQLStatement statement = new MiniSQLStatement(connection);

        boolean hasResultSet = statement.execute("SHOW TABLES;");

        assertTrue(hasResultSet);
        assertEquals("SHOW TABLES;", connection.lastQuerySql);
        assertSame(connection.resultSet, statement.getResultSet());
    }

    @Test
    @DisplayName("execute routes CREATE TABLE through executeUpdate")
    void executeRoutesCreateTableAsUpdate() throws SQLException {
        FakeConnection connection = new FakeConnection();
        MiniSQLStatement statement = new MiniSQLStatement(connection);

        boolean hasResultSet = statement.execute("CREATE TABLE products (id INT);");

        assertFalse(hasResultSet);
        assertEquals("CREATE TABLE products (id INT);", connection.lastUpdateSql);
    }

    private static final class FakeConnection extends MiniSQLConnection {
        private String lastQuerySql;
        private String lastUpdateSql;
        private final ResultSet resultSet = new MiniSQLResultSet();

        private FakeConnection() throws SQLException {
            super("jdbc:minisql://localhost:2181", new java.util.Properties());
        }

        @Override
        public ResultSet executeQuery(String sql) {
            lastQuerySql = sql;
            return resultSet;
        }

        @Override
        public int executeUpdate(String sql) {
            lastUpdateSql = sql;
            return 1;
        }

        @Override
        void reportSqlExecution(String sql, boolean success, long latencyMs, String errorMessage) {
            // no-op for unit test
        }
    }
}
