package com.minisql.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * MySQL 存储引擎配置
 */
public class MySQLConfig {

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int maxPoolSize;
    private final long connectionTimeout;
    private final boolean autoCreateSchema;

    private MySQLConfig(Builder builder) {
        this.jdbcUrl = builder.jdbcUrl;
        this.username = builder.username;
        this.password = builder.password;
        this.maxPoolSize = builder.maxPoolSize;
        this.connectionTimeout = builder.connectionTimeout;
        this.autoCreateSchema = builder.autoCreateSchema;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public boolean isAutoCreateSchema() {
        return autoCreateSchema;
    }

    /**
     * 创建 HikariCP 数据源
     */
    public HikariDataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maxPoolSize);
        config.setConnectionTimeout(connectionTimeout);

        // MySQL 特定配置
        config.addDataSourceProperty("useSSL", "false");
        config.addDataSourceProperty("allowPublicKeyRetrieval", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return new HikariDataSource(config);
    }

    public static Builder builder(String jdbcUrl, String username, String password) {
        return new Builder(jdbcUrl, username, password);
    }

    public static class Builder {
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private int maxPoolSize = 20;
        private long connectionTimeout = 30000;
        private boolean autoCreateSchema = true;

        public Builder(String jdbcUrl, String username, String password) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
        }

        public Builder maxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }

        public Builder connectionTimeout(long connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder autoCreateSchema(boolean autoCreateSchema) {
            this.autoCreateSchema = autoCreateSchema;
            return this;
        }

        public MySQLConfig build() {
            return new MySQLConfig(this);
        }
    }
}
