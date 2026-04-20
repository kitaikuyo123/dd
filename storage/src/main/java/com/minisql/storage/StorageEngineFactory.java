package com.minisql.storage;

/**
 * Factory for creating StorageEngine instances per region.
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
