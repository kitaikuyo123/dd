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

    public List<Map<String, Object>> execute(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL must not be empty");
        }

        // Split by semicolons, respecting that ';' inside single-quoted strings is not a delimiter
        List<String> statements = splitStatements(sql);
        if (statements.isEmpty()) {
            throw new IllegalArgumentException("SQL must not be empty");
        }

        List<Map<String, Object>> results = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement()) {
            for (String stmt : statements) {
                String trimmed = stmt.trim();
                if (trimmed.isEmpty()) continue;

                try {
                    boolean hasResultSet = statement.execute(trimmed);
                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.put("sql", trimmed);
                    payload.put("success", true);

                    if (hasResultSet) {
                        try (ResultSet resultSet = statement.getResultSet()) {
                            payload.put("columns", extractColumns(resultSet));
                            payload.put("rows", extractRows(resultSet));
                        }
                    } else {
                        payload.put("updateCount", statement.getUpdateCount());
                    }
                    results.add(payload);
                } catch (SQLException e) {
                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.put("sql", trimmed);
                    payload.put("success", false);
                    payload.put("error", e.getMessage());
                    results.add(payload);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Connection failed: " + e.getMessage(), e);
        }
        return results;
    }

    /**
     * Split SQL text into individual statements by semicolons,
     * ignoring semicolons inside single-quoted string literals.
     */
    private List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inSingleQuote) {
                inSingleQuote = true;
                current.append(c);
            } else if (c == '\'' && inSingleQuote) {
                // Check for escaped quote ''
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    current.append("''");
                    i++;
                } else {
                    inSingleQuote = false;
                    current.append(c);
                }
            } else if (c == ';' && !inSingleQuote) {
                String trimmed = current.toString().trim();
                if (!trimmed.isEmpty()) {
                    statements.add(trimmed);
                }
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        String last = current.toString().trim();
        if (!last.isEmpty()) {
            statements.add(last);
        }
        return statements;
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
                    row.add(bytesToHexString((byte[]) value));
                } else {
                    row.add(value);
                }
            }
            rows.add(row);
        }
        return rows;
    }

    private static String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 3);
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b & 0xFF));
        }
        return sb.toString().trim();
    }
}
