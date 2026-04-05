package com.minisql.storage;

import com.minisql.common.model.KeyValue;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

/**
 * MySQL-backed KV storage engine facade.
 */
public class MySQLStorageEngine implements StorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(MySQLStorageEngine.class);

    private final HikariDataSource dataSource;
    private final boolean ownDataSource;  // 是否拥有数据源，共享模式下为 false
    private final boolean autoCreateSchema;
    private final String tableName;
    private final MySqlSchemaManager schemaManager;
    private final MySqlScanQueryBuilder queryBuilder;
    private final KeyValueVisibilityResolver visibilityResolver;

    private final String sqlInsert;
    private final String sqlGet;
    private final String sqlScan;
    private final String sqlDelete;

    public MySQLStorageEngine(MySQLConfig config, String regionId) {
        this.dataSource = config.createDataSource();
        this.ownDataSource = true;
        this.autoCreateSchema = config.isAutoCreateSchema();
        this.tableName = "kv_store_" + toSafeTableName(regionId);
        this.schemaManager = new MySqlSchemaManager();
        this.queryBuilder = new MySqlScanQueryBuilder();
        this.visibilityResolver = new KeyValueVisibilityResolver();
        this.sqlInsert = queryBuilder.insertSql(tableName);
        this.sqlGet = queryBuilder.getSql(tableName);
        this.sqlScan = queryBuilder.rangeScanSql(tableName);
        this.sqlDelete = queryBuilder.deleteSql(tableName);

        if (autoCreateSchema) {
            initializeSchema();
        }
        logger.info("MySQLStorageEngine initialized with table {}", tableName);
    }

    /**
     * 共享连接池构造函数 - 所有使用同一数据库的 Region 共享同一个 HikariDataSource
     * 这样整个 RegionServer 只会占用一个连接池的连接数，而不是 N 个 Region 十倍的连接数
     */
    public MySQLStorageEngine(HikariDataSource sharedDataSource, String regionId) {
        this.dataSource = sharedDataSource;
        this.ownDataSource = false;  // 不拥有，close() 时不关闭池
        this.autoCreateSchema = true;
        this.tableName = "kv_store_" + toSafeTableName(regionId);
        this.schemaManager = new MySqlSchemaManager();
        this.queryBuilder = new MySqlScanQueryBuilder();
        this.visibilityResolver = new KeyValueVisibilityResolver();
        this.sqlInsert = queryBuilder.insertSql(tableName);
        this.sqlGet = queryBuilder.getSql(tableName);
        this.sqlScan = queryBuilder.rangeScanSql(tableName);
        this.sqlDelete = queryBuilder.deleteSql(tableName);

        initializeSchema();
        logger.info("MySQLStorageEngine initialized with SHARED pool, table {}", tableName);
    }

    public MySQLStorageEngine(MySQLConfig config) {
        this(config, "default");
    }

    @Override
    public void put(byte[] key, KeyValue value) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlInsert)) {
            bindMutation(stmt, key, value);
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Failed to put key-value", e);
            throw new RuntimeException("Failed to put key-value", e);
        }
    }

    @Override
    public void batchPut(List<KeyValue> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(sqlInsert)) {
                for (KeyValue kv : values) {
                    bindMutation(stmt, kv.getRowKey(), kv);
                    stmt.addBatch();
                }
                stmt.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            logger.error("Failed to batch put", e);
            throw new RuntimeException("Failed to batch put", e);
        }
    }

    @Override
    public List<KeyValue> get(byte[] key) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlGet)) {
            stmt.setBytes(1, key);
            try (ResultSet rs = stmt.executeQuery()) {
                return visibilityResolver.materializeLatestValues(rs, this::resultSetToKeyValue);
            }
        } catch (SQLException e) {
            logger.error("Failed to get key-value", e);
            throw new RuntimeException("Failed to get key-value", e);
        }
    }

    @Override
    public Iterator<KeyValue> scan(StorageScanFilter request) {
        if (request == null || (!request.hasColumnPredicates() && !request.hasProjectedQualifiers())) {
            return scanRange(request == null ? null : request.getStartKey(), request == null ? null : request.getEndKey());
        }

        byte[] startKey = request.getStartKey() != null ? request.getStartKey() : new byte[0];
        byte[] endKey = request.getEndKey() != null ? request.getEndKey() : new byte[] {(byte) 0xFF};
        String sql = queryBuilder.buildPredicateScanSql(tableName, request);

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            int index = 1;
            stmt.setBytes(index++, startKey);
            stmt.setBytes(index++, endKey);
            stmt.setBytes(index++, startKey);
            stmt.setBytes(index++, endKey);

            for (String qualifier : request.getProjectedQualifiers()) {
                stmt.setString(index++, qualifier);
            }
            for (StorageColumnPredicate predicate : request.getColumnPredicates()) {
                if (!request.getColumnPredicates().subList(0, request.getColumnPredicates().indexOf(predicate)).stream()
                    .anyMatch(existing -> existing.getQualifier().equals(predicate.getQualifier()))) {
                    stmt.setString(index++, predicate.getQualifier());
                }
            }
            for (StorageColumnPredicate predicate : request.getColumnPredicates()) {
                stmt.setString(index++, predicate.getQualifier());
                stmt.setBytes(index++, predicate.getValue());
            }
            if (!request.hasColumnPredicates()) {
                stmt.setBytes(index++, startKey);
                stmt.setBytes(index++, endKey);
            }
            stmt.setBytes(index++, startKey);
            stmt.setBytes(index++, endKey);

            try (ResultSet rs = stmt.executeQuery()) {
                return visibilityResolver.materializeVisibleCells(rs, this::resultSetToKeyValue).iterator();
            }
        } catch (SQLException e) {
            logger.error("Failed to scan with predicate pushdown", e);
            throw new RuntimeException("Failed to scan with predicate pushdown", e);
        }
    }

    @Override
    public void delete(byte[] key) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlDelete)) {
            stmt.setBytes(1, key);
            stmt.setString(2, "");
            stmt.setString(3, "");
            stmt.setLong(4, System.currentTimeMillis());
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Failed to delete", e);
            throw new RuntimeException("Failed to delete", e);
        }
    }

    @Override
    public void flush() {
        logger.debug("Flush called - no-op for MySQL storage");
    }

    @Override
    public void compact(boolean major) {
        if (!major) {
            return;
        }
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(queryBuilder.compactSql(tableName))) {
            int affected = stmt.executeUpdate();
            logger.info("Major compaction completed for {}, removed {}", tableName, affected);
        } catch (SQLException e) {
            logger.error("Failed to compact", e);
        }
    }

    @Override
    public void close() {
        if (ownDataSource && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("MySQLStorageEngine closed (owned datasource)");
        } else if (!ownDataSource) {
            logger.debug("MySQLStorageEngine closed (shared datasource, pool kept alive)");
        }
    }

    public void dropTable() {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(queryBuilder.dropTableSql(tableName))) {
            stmt.executeUpdate();
            logger.info("Table dropped: {}", tableName);
        } catch (SQLException e) {
            logger.error("Failed to drop table", e);
            throw new RuntimeException("Failed to drop table", e);
        }
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public String getTableName() {
        return tableName;
    }

    private Iterator<KeyValue> scanRange(byte[] startKey, byte[] endKey) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlScan)) {
            stmt.setBytes(1, startKey != null ? startKey : new byte[0]);
            stmt.setBytes(2, endKey != null ? endKey : new byte[] {(byte) 0xFF});
            try (ResultSet rs = stmt.executeQuery()) {
                return visibilityResolver.materializeLatestValues(rs, this::resultSetToKeyValue).iterator();
            }
        } catch (SQLException e) {
            logger.error("Failed to scan", e);
            throw new RuntimeException("Failed to scan", e);
        }
    }

    private void initializeSchema() {
        try (Connection conn = dataSource.getConnection()) {
            schemaManager.initializeSchema(conn, tableName);
        } catch (SQLException e) {
            logger.error("Failed to initialize schema", e);
            throw new RuntimeException("Failed to initialize schema", e);
        }
    }

    private String toSafeTableName(String regionId) {
        return regionId.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private void bindMutation(PreparedStatement stmt, byte[] key, KeyValue value) throws SQLException {
        stmt.setBytes(1, key);
        stmt.setString(2, value.getFamily());
        stmt.setString(3, value.getQualifier());
        stmt.setLong(4, value.getTimestamp());
        if (value.getValue() != null) {
            stmt.setBytes(5, value.getValue());
        } else {
            stmt.setNull(5, java.sql.Types.BLOB);
        }
        stmt.setInt(6, value.isDelete() ? 1 : 0);
    }

    private KeyValue resultSetToKeyValue(ResultSet rs) throws SQLException {
        KeyValue kv = new KeyValue();
        kv.setRowKey(rs.getBytes("row_key"));
        kv.setFamily(rs.getString("family"));
        kv.setQualifier(rs.getString("qualifier"));
        kv.setTimestamp(rs.getLong("timestamp"));
        kv.setValue(rs.getBytes("value"));
        kv.setType(rs.getInt("is_deleted") == 1 ? KeyValue.Type.DELETE : KeyValue.Type.PUT);
        return kv;
    }
}
