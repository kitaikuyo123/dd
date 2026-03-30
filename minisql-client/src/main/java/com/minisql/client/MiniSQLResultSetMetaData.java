package com.minisql.client;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * MiniSQL ResultSetMetaData 实现
 */
public class MiniSQLResultSetMetaData implements ResultSetMetaData {

    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final String tableName;
    private final Map<String, Integer> sqlTypeMap;

    public MiniSQLResultSetMetaData(List<String> columnNames, List<String> columnTypes, String tableName) {
        this.columnNames = columnNames != null ? columnNames : new ArrayList<>();
        this.columnTypes = columnTypes != null ? columnTypes : new ArrayList<>();
        this.tableName = tableName;

        // 初始化 SQL 类型映射
        this.sqlTypeMap = new HashMap<>();
        sqlTypeMap.put("INT", Types.INTEGER);
        sqlTypeMap.put("INTEGER", Types.INTEGER);
        sqlTypeMap.put("BIGINT", Types.BIGINT);
        sqlTypeMap.put("LONG", Types.BIGINT);
        sqlTypeMap.put("FLOAT", Types.FLOAT);
        sqlTypeMap.put("DOUBLE", Types.DOUBLE);
        sqlTypeMap.put("BOOLEAN", Types.BOOLEAN);
        sqlTypeMap.put("VARCHAR", Types.VARCHAR);
        sqlTypeMap.put("CHAR", Types.CHAR);
        sqlTypeMap.put("STRING", Types.VARCHAR);
        sqlTypeMap.put("TEXT", Types.VARCHAR);
        sqlTypeMap.put("TIMESTAMP", Types.TIMESTAMP);
        sqlTypeMap.put("DATE", Types.DATE);
        sqlTypeMap.put("TIME", Types.TIME);
        sqlTypeMap.put("BLOB", Types.BLOB);
        sqlTypeMap.put("BINARY", Types.BINARY);
        sqlTypeMap.put("BYTE", Types.TINYINT);
        sqlTypeMap.put("SHORT", Types.SMALLINT);
        sqlTypeMap.put("DECIMAL", Types.DECIMAL);
        sqlTypeMap.put("NUMERIC", Types.NUMERIC);
        sqlTypeMap.put("REAL", Types.REAL);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Invalid column index: " + column);
        }
        return columnNames.get(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumnLabel(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Invalid column index: " + column);
        }

        if (columnTypes.isEmpty() || column > columnTypes.size()) {
            return Types.VARCHAR; // 默认类型
        }

        String typeName = columnTypes.get(column - 1);
        if (typeName == null) {
            return Types.VARCHAR;
        }

        Integer sqlType = sqlTypeMap.get(typeName.toUpperCase());
        return sqlType != null ? sqlType : Types.VARCHAR;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Invalid column index: " + column);
        }

        if (columnTypes.isEmpty() || column > columnTypes.size()) {
            return "VARCHAR";
        }

        return columnTypes.get(column - 1);
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return tableName != null ? tableName : "";
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    private static final int columnNullable = ResultSetMetaData.columnNullable;
    private static final int columnNotNull = ResultSetMetaData.columnNoNulls;

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        int type = getColumnType(column);
        return type == Types.INTEGER || type == Types.BIGINT ||
               type == Types.FLOAT || type == Types.DOUBLE ||
               type == Types.DECIMAL || type == Types.NUMERIC;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 10; // 默认显示宽度
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.INTEGER:
                return 10;
            case Types.BIGINT:
                return 19;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
                return 15;
            default:
                return 255;
        }
    }

    @Override
    public int getScale(int column) throws SQLException {
        int type = getColumnType(column);
        if (type == Types.FLOAT || type == Types.DOUBLE || type == Types.DECIMAL) {
            return 5;
        }
        return 0;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.INTEGER:
                return Integer.class.getName();
            case Types.BIGINT:
                return Long.class.getName();
            case Types.FLOAT:
                return Float.class.getName();
            case Types.DOUBLE:
                return Double.class.getName();
            case Types.BOOLEAN:
                return Boolean.class.getName();
            case Types.TIMESTAMP:
            case Types.DATE:
            case Types.TIME:
                return java.sql.Date.class.getName();
            case Types.BLOB:
                return byte[].class.getName();
            case Types.VARCHAR:
            case Types.CHAR:
            default:
                return String.class.getName();
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Unable to unwrap to " + iface.getName());
    }
}
