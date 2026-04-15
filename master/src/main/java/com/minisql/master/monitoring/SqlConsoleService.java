package com.minisql.master.monitoring;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes ad-hoc SQL for the monitor web console by reusing the JDBC client.
 * The MiniSQL JDBC driver is registered explicitly because {@code mvn exec:java}
 * uses a custom ClassLoader where Java SPI auto-discovery may not work.
 */
public class SqlConsoleService {

    static {
        try {
            Class<?> driverClass = Class.forName("com.minisql.client.MiniSQLDriver");
            DriverManager.registerDriver((Driver) driverClass.getDeclaredConstructor().newInstance());
        } catch (Exception e) {
            // client JAR not on classpath — SQL console will fail at getConnection time
        }
    }

    private final String jdbcUrl;

    public SqlConsoleService(String zkConnectString) {
        this.jdbcUrl = "jdbc:minisql://" + zkConnectString;
    }

    public Map<String, Object> execute(String sql) {
        String trimmedSql = sql == null ? "" : sql.trim();
        if (trimmedSql.isEmpty()) {
            throw new IllegalArgumentException("SQL must not be empty");
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute(trimmedSql);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("sql", trimmedSql);
            payload.put("hasResultSet", hasResultSet);

            if (hasResultSet) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    payload.put("columns", extractColumns(resultSet));
                    payload.put("rows", extractRows(resultSet));
                }
            } else {
                payload.put("updateCount", statement.getUpdateCount());
            }
            return payload;
        } catch (SQLException e) {
            throw new IllegalStateException("SQL execution failed: " + e.getMessage(), e);
        }
    }

    private List<String> extractColumns(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<String> columns = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columns.add(metaData.getColumnLabel(i));
        }
        return columns;
    }

    private List<List<Object>> extractRows(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<List<Object>> rows = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                Object value = resultSet.getObject(i);
                if (value instanceof byte[]) {
                    row.add(new String((byte[]) value));
                } else {
                    row.add(value);
                }
            }
            rows.add(row);
        }
        return rows;
    }
}
