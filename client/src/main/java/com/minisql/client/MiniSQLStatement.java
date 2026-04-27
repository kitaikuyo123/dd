package com.minisql.client;

import java.sql.*;

/**
 * MiniSQL Statement implementation.
 */
public class MiniSQLStatement implements Statement {

    private final MiniSQLConnection connection;
    private boolean closed = false;
    private ResultSet currentResultSet;
    private int updateCount;
    private java.util.List<String> batchCommands;

    public MiniSQLStatement(MiniSQLConnection connection) {
        this.connection = connection;
        this.batchCommands = new java.util.ArrayList<>();
        this.updateCount = 0;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        closeCurrentResultSet();
        long start = System.currentTimeMillis();
        try {
            currentResultSet = connection.executeQuery(sql);
            connection.reportSqlExecution(sql, true, System.currentTimeMillis() - start, null);
            return currentResultSet;
        } catch (SQLException e) {
            connection.reportSqlExecution(sql, false, System.currentTimeMillis() - start, e.getMessage());
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        long start = System.currentTimeMillis();
        try {
            updateCount = connection.executeUpdate(sql);
            connection.reportSqlExecution(sql, true, System.currentTimeMillis() - start, null);
            return updateCount;
        } catch (SQLException e) {
            connection.reportSqlExecution(sql, false, System.currentTimeMillis() - start, e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        long start = System.currentTimeMillis();
        try {
            if (isQueryStatement(sql)) {
                currentResultSet = connection.executeQuery(sql);
                connection.reportSqlExecution(sql, true, System.currentTimeMillis() - start, null);
                return true;
            }
            updateCount = connection.executeUpdate(sql);
            connection.reportSqlExecution(sql, true, System.currentTimeMillis() - start, null);
            return false;
        } catch (SQLException e) {
            connection.reportSqlExecution(sql, false, System.currentTimeMillis() - start, e.getMessage());
            throw e;
        }
    }

    private boolean isQueryStatement(String sql) throws SQLException {
        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("SELECT ") || trimmed.startsWith("SHOW ");
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        closeCurrentResultSet();
    }

    private void closeCurrentResultSet() throws SQLException {
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batchCommands.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batchCommands.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        if (batchCommands.isEmpty()) {
            return new int[0];
        }

        int[] results = new int[batchCommands.size()];
        for (int i = 0; i < batchCommands.size(); i++) {
            results[i] = executeUpdate(batchCommands.get(i));
        }
        batchCommands.clear();
        return results;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {}

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {}

    @Override
    public void setCursorName(String name) throws SQLException {}

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {}

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {}

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {}

    @Override
    public void cancel() throws SQLException {}

    @Override
    public void setFetchDirection(int direction) throws SQLException {}

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {}

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {}

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {}

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
