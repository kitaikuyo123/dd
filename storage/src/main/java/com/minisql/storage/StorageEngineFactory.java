package com.minisql.storage;

/**
 * Factory for creating StorageEngine instances per region.
 * Implementations correspond to different storage backends (MySQL, RocksDB, etc.).
 */
public interface StorageEngineFactory {

    /**
     * Create a storage engine for the given region.
     */
    StorageEngine create(String regionId);

    /**
     * Release any shared resources held by this factory.
     */
    void close();
}
