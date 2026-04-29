package com.minisql.storage;

/** 存储引擎工厂接口 */
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
