package com.minisql.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MySqlSchemaManager 单元测试")
class MySqlSchemaManagerTest {

    private MySqlSchemaManager manager;

    @BeforeEach
    void setUp() {
        manager = new MySqlSchemaManager();
    }

    // ---- createTableSql ----

    @Nested
    @DisplayName("createTableSql")
    class CreateTableSql {

        @Test
        @DisplayName("生成 CREATE TABLE IF NOT EXISTS 语句")
        void generatesCreateTableSql() {
            String sql = manager.createTableSql("test_table");

            assertTrue(sql.startsWith("CREATE TABLE IF NOT EXISTS test_table"));
        }

        @Test
        @DisplayName("包含所有必要列定义")
        void containsColumnDefinitions() {
            String sql = manager.createTableSql("t");

            assertTrue(sql.contains("row_key VARBINARY(1024) NOT NULL"));
            assertTrue(sql.contains("family VARCHAR(64) NOT NULL"));
            assertTrue(sql.contains("qualifier VARCHAR(256) NOT NULL"));
            assertTrue(sql.contains("timestamp BIGINT NOT NULL"));
            assertTrue(sql.contains("value BLOB"));
            assertTrue(sql.contains("is_deleted TINYINT(1) DEFAULT 0"));
        }

        @Test
        @DisplayName("包含正确的主键定义")
        void containsPrimaryKey() {
            String sql = manager.createTableSql("t");

            assertTrue(sql.contains("PRIMARY KEY (row_key, family, qualifier, timestamp)"));
        }

        @Test
        @DisplayName("包含扫描索引和列族索引")
        void containsIndexes() {
            String sql = manager.createTableSql("t");

            assertTrue(sql.contains("INDEX idx_scan (row_key, timestamp)"));
            assertTrue(sql.contains("INDEX idx_family_qualifier (family, qualifier)"));
        }

        @Test
        @DisplayName("使用 InnoDB 引擎和 utf8mb4_bin 排序规则")
        void usesInnoDBAndUtf8mb4() {
            String sql = manager.createTableSql("t");

            assertTrue(sql.contains("ENGINE=InnoDB"));
            assertTrue(sql.contains("DEFAULT CHARSET=utf8mb4"));
            assertTrue(sql.contains("COLLATE=utf8mb4_bin"));
        }

        @Test
        @DisplayName("不同表名生成不同 SQL")
        void differentTableNames() {
            String sql1 = manager.createTableSql("orders");
            String sql2 = manager.createTableSql("users");

            assertTrue(sql1.contains("orders"));
            assertTrue(sql2.contains("users"));
            assertNotEquals(sql1, sql2);
        }
    }

    // ---- createMetadataTableSql ----

    @Nested
    @DisplayName("createMetadataTableSql")
    class CreateMetadataTableSql {

        @Test
        @DisplayName("生成 table_metadata 建表语句")
        void generatesMetadataTableSql() {
            String sql = manager.createMetadataTableSql();

            assertTrue(sql.startsWith("CREATE TABLE IF NOT EXISTS table_metadata"));
        }

        @Test
        @DisplayName("包含 table_name, schema_json, created_at 三列")
        void containsColumns() {
            String sql = manager.createMetadataTableSql();

            assertTrue(sql.contains("table_name VARCHAR(255) PRIMARY KEY"));
            assertTrue(sql.contains("schema_json TEXT"));
            assertTrue(sql.contains("created_at BIGINT"));
        }

        @Test
        @DisplayName("使用 InnoDB 引擎和 utf8mb4_bin 排序规则")
        void usesInnoDBAndUtf8mb4() {
            String sql = manager.createMetadataTableSql();

            assertTrue(sql.contains("ENGINE=InnoDB"));
            assertTrue(sql.contains("COLLATE=utf8mb4_bin"));
        }

        @Test
        @DisplayName("多次调用返回相同 SQL")
        void isDeterministic() {
            String sql1 = manager.createMetadataTableSql();
            String sql2 = manager.createMetadataTableSql();

            assertEquals(sql1, sql2);
        }
    }
}
