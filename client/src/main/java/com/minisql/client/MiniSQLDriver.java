package com.minisql.client;

import java.sql.*;
import java.util.Properties;

/**
 * MiniSQL JDBC 驱动实现
 * 支持 URL 格式：jdbc:minisql://zkhost:2181
 *
 * 通过 Java SPI 机制自动加载（META-INF/services/java.sql.Driver）
 */
public class MiniSQLDriver implements Driver {

    /**
     * 默认构造函数
     */
    public MiniSQLDriver() throws SQLException {
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        return new MiniSQLConnection(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith("jdbc:minisql://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // MiniSQL 不需要额外的连接属性
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Feature not supported");
    }
}
