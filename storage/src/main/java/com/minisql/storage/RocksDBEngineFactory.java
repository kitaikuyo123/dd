package com.minisql.storage;

/**
 * Factory that creates RocksDB-backed storage engines.
 * Each region gets its own RocksDB instance under {dataDir}/{regionId}/.
 */
public class RocksDBEngineFactory implements StorageEngineFactory {

    private final RocksDBConfig config;

    public RocksDBEngineFactory(RocksDBConfig config) {
        this.config = config;
    }

    @Override
    public StorageEngine create(String regionId) {
        return new RocksDBStorageEngine(config, regionId);
    }

    @Override
    public void close() {
        // No shared resources; each engine manages its own RocksDB instance.
    }
}
