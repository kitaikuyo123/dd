package com.minisql.client;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * MiniSQL PreparedStatement实现
 * 负责模块: 开发者C
 */
public class MiniSQLPreparedStatement extends MiniSQLStatement implements PreparedStatement {

    private final String originalSql;
    private final MiniSQLConnection connection;
    private final Map<Integer, Object> parameters;

    public MiniSQLPreparedStatement(MiniSQLConnection connection, String sql) {
        super(connection);
        this.connection = connection;
        this.originalSql = sql;
        this.parameters = new HashMap<>();
    }

    /**
     * 构建带参数的 SQL
     * 使用 StringBuilder 高效替换所有 ? 占位符
     */
    private String buildSql() {
        if (parameters.isEmpty()) {
            return originalSql;
        }

        StringBuilder result = new StringBuilder();
        int paramIndex = 1;
        int lastPos = 0;

        for (int i = 0; i < originalSql.length(); i++) {
            if (originalSql.charAt(i) == '?') {
                // 添加 ? 之前的部分
                result.append(originalSql, lastPos, i);
                // 替换为参数值
                Object value = parameters.get(paramIndex);
                String replacement = value == null ? "NULL" : formatValue(value);
                result.append(replacement);
                paramIndex++;
                lastPos = i + 1;
            }
        }
        // 添加剩余部分
        result.append(originalSql.substring(lastPos));

        return result.toString();
    }

    /**
     * 格式化值为 SQL 字符串
     */
    private String formatValue(Object value) {
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? "1" : "0";
        }
        return "'" + value.toString() + "'";
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(buildSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(buildSql());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        parameters.put(parameterIndex, null);
    }

    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        parameters.put(parameterIndex, x);
    }
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {}
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {}
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {}
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {}
    @Override
    public boolean execute() throws SQLException {
        return super.execute(buildSql());
    }
    @Override
    public void addBatch() throws SQLException {}
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {}
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {}
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {}
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {}
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {}
    @Override
    public ResultSetMetaData getMetaData() throws SQLException { return null; }
    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {}
    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {}
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {}
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {}
    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {}
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException { return null; }
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {}
    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {}
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {}
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {}
    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {}
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {}
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {}
    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {}
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {}
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {}
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {}
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {}
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {}
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {}
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {}
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {}
    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {}
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {}
    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {}
}
