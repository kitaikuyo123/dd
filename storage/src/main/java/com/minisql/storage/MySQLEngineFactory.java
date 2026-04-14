package com.minisql.storage;

import com.zaxxer.hikari.HikariDataSource;

/**
 * Factory that creates MySQL-backed storage engines sharing a single connection pool.
 */
public class MySQLEngineFactory implements StorageEngineFactory {

    private final HikariDataSource sharedDataSource;

    public MySQLEngineFactory(HikariDataSource sharedDataSource) {
        this.sharedDataSource = sharedDataSource;
    }

    @Override
    public StorageEngine create(String regionId) {
        return new MySQLStorageEngine(sharedDataSource, regionId);
    }

    @Override
    public void close() {
        if (sharedDataSource != null && !sharedDataSource.isClosed()) {
            sharedDataSource.close();
        }
    }
}
