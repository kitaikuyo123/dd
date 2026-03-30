package com.minisql.sql.execution;

import com.minisql.common.model.Table;
import com.minisql.storage.StorageEngine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SQL 执行上下文
 * 存储查询执行所需的共享资源，如存储引擎、表元数据等
 */
public class ExecutionContext {

    // 表名到 StorageEngine 的映射（用于本地扫描）
    private final Map<String, StorageEngine> storageEngines;

    // 表名到 Table 元数据的映射
    private final Map<String, Table> tableMetadata;

    public ExecutionContext() {
        this.storageEngines = new ConcurrentHashMap<>();
        this.tableMetadata = new HashMap<>();
    }

    /**
     * 注册表的存储引擎
     */
    public void registerStorageEngine(String tableName, StorageEngine engine) {
        storageEngines.put(tableName, engine);
    }

    /**
     * 获取表的存储引擎
     */
    public StorageEngine getStorageEngine(String tableName) {
        return storageEngines.get(tableName);
    }

    /**
     * 注册表元数据
     */
    public void registerTableMetadata(Table table) {
        if (table != null && table.getTableName() != null) {
            tableMetadata.put(table.getTableName(), table);
        }
    }

    /**
     * 获取表元数据
     */
    public Table getTableMetadata(String tableName) {
        return tableMetadata.get(tableName);
    }

    /**
     * 关闭执行上下文，释放资源
     */
    public void close() {
        // 可以选择关闭所有存储引擎
        // 通常由上层管理，这里不做处理
    }
}
