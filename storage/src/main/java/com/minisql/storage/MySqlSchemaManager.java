package com.minisql.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Handles schema/bootstrap DDL for the MySQL storage engine.
 */
public class MySqlSchemaManager {

    public void initializeSchema(Connection conn, String tableName) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(createTableSql(tableName))) {
            stmt.executeUpdate();
        }
        try (PreparedStatement stmt = conn.prepareStatement(createMetadataTableSql())) {
            stmt.executeUpdate();
        }
    }

    public String createTableSql(String tableName) {
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
            + "row_key VARBINARY(1024) NOT NULL,"
            + "family VARCHAR(64) NOT NULL,"
            + "qualifier VARCHAR(256) NOT NULL,"
            + "timestamp BIGINT NOT NULL,"
            + "value BLOB,"
            + "is_deleted TINYINT(1) DEFAULT 0,"
            + "PRIMARY KEY (row_key, family, qualifier, timestamp),"
            + "INDEX idx_scan (row_key, timestamp),"
            + "INDEX idx_family_qualifier (family, qualifier)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
    }

    public String createMetadataTableSql() {
        return "CREATE TABLE IF NOT EXISTS table_metadata ("
            + "table_name VARCHAR(255) PRIMARY KEY,"
            + "schema_json TEXT,"
            + "created_at BIGINT"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
    }
}
