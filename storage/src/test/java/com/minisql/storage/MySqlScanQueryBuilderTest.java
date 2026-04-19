package com.minisql.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MySqlScanQueryBuilder 单元测试")
class MySqlScanQueryBuilderTest {

    private MySqlScanQueryBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new MySqlScanQueryBuilder();
    }

    // ---- insertSql ----

    @Nested
    @DisplayName("insertSql")
    class InsertSql {

        @Test
        @DisplayName("生成包含 ON DUPLICATE KEY UPDATE 的 INSERT 语句")
        void generatesInsertWithUpsert() {
            String sql = builder.insertSql("test_table");

            assertTrue(sql.startsWith("INSERT INTO test_table"));
            assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
            assertTrue(sql.contains("value = VALUES(value)"));
            assertTrue(sql.contains("is_deleted = VALUES(is_deleted)"));
        }

        @Test
        @DisplayName("包含全部六个列占位符")
        void containsAllColumns() {
            String sql = builder.insertSql("t");

            assertTrue(sql.contains("row_key, family, qualifier, timestamp, value, is_deleted"));
            assertTrue(sql.contains("?, ?, ?, ?, ?, ?"));
        }

        @Test
        @DisplayName("不同表名生成不同 SQL")
        void differentTableNames() {
            String sql1 = builder.insertSql("orders");
            String sql2 = builder.insertSql("users");

            assertTrue(sql1.contains("orders"));
            assertTrue(sql2.contains("users"));
            assertNotEquals(sql1, sql2);
        }
    }

    // ---- getSql ----

    @Nested
    @DisplayName("getSql")
    class GetSql {

        @Test
        @DisplayName("生成带 WHERE row_key = ? 的 SELECT 语句")
        void generatesGetByRowKey() {
            String sql = builder.getSql("test_table");

            assertTrue(sql.startsWith("SELECT"));
            assertTrue(sql.contains("FROM test_table"));
            assertTrue(sql.contains("WHERE row_key = ?"));
        }

        @Test
        @DisplayName("按 timestamp DESC, family, qualifier 排序")
        void orderByTimestampDesc() {
            String sql = builder.getSql("t");

            assertTrue(sql.contains("ORDER BY timestamp DESC, family, qualifier"));
        }

        @Test
        @DisplayName("选取所有必要列")
        void selectsAllColumns() {
            String sql = builder.getSql("t");

            assertTrue(sql.contains("row_key"));
            assertTrue(sql.contains("family"));
            assertTrue(sql.contains("qualifier"));
            assertTrue(sql.contains("timestamp"));
            assertTrue(sql.contains("value"));
            assertTrue(sql.contains("is_deleted"));
        }
    }

    // ---- rangeScanSql ----

    @Nested
    @DisplayName("rangeScanSql")
    class RangeScanSql {

        @Test
        @DisplayName("生成带范围条件的 SELECT 语句")
        void generatesRangeScan() {
            String sql = builder.rangeScanSql("test_table");

            assertTrue(sql.contains("WHERE row_key >= ? AND row_key < ?"));
            assertTrue(sql.contains("FROM test_table"));
        }

        @Test
        @DisplayName("按 row_key, timestamp DESC, family, qualifier 排序")
        void orderByRowKeyThenTimestampDesc() {
            String sql = builder.rangeScanSql("t");

            assertTrue(sql.contains("ORDER BY row_key, timestamp DESC, family, qualifier"));
        }
    }

    // ---- deleteSql ----

    @Nested
    @DisplayName("deleteSql")
    class DeleteSql {

        @Test
        @DisplayName("生成带 is_deleted=1 的 INSERT 语句（墓碑标记）")
        void generatesTombstoneInsert() {
            String sql = builder.deleteSql("test_table");

            assertTrue(sql.startsWith("INSERT INTO test_table"));
            assertTrue(sql.contains("NULL, 1"));
        }

        @Test
        @DisplayName("包含六个值占位符，value 为 NULL，is_deleted 为 1")
        void correctPlaceholderPattern() {
            String sql = builder.deleteSql("t");

            assertTrue(sql.contains("?, ?, ?, ?, NULL, 1"));
        }
    }

    // ---- dropTableSql ----

    @Nested
    @DisplayName("dropTableSql")
    class DropTableSql {

        @Test
        @DisplayName("生成 DROP TABLE IF EXISTS 语句")
        void generatesDropTable() {
            String sql = builder.dropTableSql("test_table");

            assertEquals("DROP TABLE IF EXISTS test_table", sql);
        }

        @Test
        @DisplayName("空表名也能生成 SQL（不做校验）")
        void emptyTableName() {
            String sql = builder.dropTableSql("");

            assertEquals("DROP TABLE IF EXISTS ", sql);
        }
    }

    // ---- compactSql ----

    @Nested
    @DisplayName("compactSql")
    class CompactSql {

        @Test
        @DisplayName("生成包含 ROW_NUMBER 窗口函数的 DELETE 语句")
        void generatesCompactSql() {
            String sql = builder.compactSql("test_table");

            assertTrue(sql.startsWith("DELETE FROM test_table"));
            assertTrue(sql.contains("ROW_NUMBER()"));
            assertTrue(sql.contains("PARTITION BY row_key, family, qualifier"));
            assertTrue(sql.contains("ORDER BY timestamp DESC"));
            assertTrue(sql.contains("rn <= 3"));
        }

        @Test
        @DisplayName("SQL 中引用了两次表名（DELETE 和子查询）")
        void referencesTableNameInSubquery() {
            String sql = builder.compactSql("my_table");

            // 表名应出现在 DELETE FROM 和 FROM 子句中
            int count = 0;
            int idx = 0;
            String needle = "my_table";
            while ((idx = sql.indexOf(needle, idx)) != -1) {
                count++;
                idx += needle.length();
            }
            assertTrue(count >= 2, "compactSql should reference table name at least twice");
        }
    }

    // ---- buildPredicateScanSql ----

    @Nested
    @DisplayName("buildPredicateScanSql")
    class BuildPredicateScanSql {

        @Test
        @DisplayName("无谓词、无投影时生成基本 CTE 查询")
        void noPredicatesNoProjection() {
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("WITH row_deletes AS"));
            assertTrue(sql.contains("matching_rows AS"));
            assertTrue(sql.contains("SELECT DISTINCT row_key FROM t"));
            assertTrue(sql.endsWith("ORDER BY row_key, timestamp DESC, family, qualifier"));
            assertFalse(sql.contains("JOIN predicate_visible"));
        }

        @Test
        @DisplayName("单个谓词时生成 JOIN 查询并包含 qualifier 条件")
        void singlePredicate() {
            StorageColumnPredicate pred = new StorageColumnPredicate("name", "=", "alice".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(pred))
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("predicate_visible p0"));
            assertFalse(sql.contains("JOIN predicate_visible p1"));
            assertTrue(sql.contains("p0.qualifier = ? AND p0.value"));
            assertTrue(sql.contains("qualifier IN"));
        }

        @Test
        @DisplayName("多个谓词时生成多路 JOIN 查询")
        void multiplePredicates() {
            StorageColumnPredicate p1 = new StorageColumnPredicate("name", "=", "alice".getBytes());
            StorageColumnPredicate p2 = new StorageColumnPredicate("age", ">", "20".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(p1, p2))
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("JOIN predicate_visible p1 ON p1.row_key = p0.row_key"));
            assertTrue(sql.contains("p0.qualifier = ?"));
            assertTrue(sql.contains("p1.qualifier = ?"));
        }

        @Test
        @DisplayName("有投影限定符时 SQL 中包含 projection IN 条件")
        void withProjectedQualifiers() {
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .projectedQualifiers(java.util.List.of("name", "age"))
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("latest_cells AS"));
            // projection 占位符出现在 latest_cells CTE 的 qualifier IN 子句中
            int projectionInIdx = sql.indexOf("qualifier IN (", sql.indexOf("latest_cells"));
            assertTrue(projectionInIdx > 0, "Should have qualifier IN in latest_cells CTE");
        }

        @Test
        @DisplayName("无谓词且无投影时 matching_rows 使用 DISTINCT 扫描")
        void matchingRowsDistinctScan() {
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("SELECT DISTINCT row_key FROM t WHERE row_key >= ? AND row_key < ?"));
        }

        @Test
        @DisplayName("包含 visible_cells CTE 过滤已删除行")
        void includesVisibleCellsCte() {
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("visible_cells AS"));
            assertTrue(sql.contains("lc.is_deleted = 0"));
            assertTrue(sql.contains("rd.row_delete_ts IS NULL OR lc.timestamp > rd.row_delete_ts"));
        }
    }

    // ---- normalizeOperator (tested indirectly via buildPredicateScanSql) ----

    @Nested
    @DisplayName("normalizeOperator 间接测试")
    class NormalizeOperator {

        @Test
        @DisplayName("= 操作符正常生成 SQL")
        void equalsOperator() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "=", "val".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(pred))
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("p0.value = ?"));
        }

        @Test
        @DisplayName("== 操作符被归一化为 =")
        void doubleEqualsNormalized() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "==", "val".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(pred))
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("p0.value = ?"));
            assertFalse(sql.contains("p0.value == ?"));
        }

        @Test
        @DisplayName("> 操作符原样保留")
        void greaterThanOperator() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", ">", "val".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(pred))
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("p0.value > ?"));
        }

        @Test
        @DisplayName(">= 操作符原样保留")
        void greaterThanOrEqualOperator() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", ">=", "val".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(pred))
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("p0.value >= ?"));
        }

        @Test
        @DisplayName("< 操作符原样保留")
        void lessThanOperator() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "<", "val".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(pred))
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("p0.value < ?"));
        }

        @Test
        @DisplayName("<= 操作符原样保留")
        void lessThanOrEqualOperator() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "<=", "val".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(pred))
                .build();

            String sql = builder.buildPredicateScanSql("t", filter);

            assertTrue(sql.contains("p0.value <= ?"));
        }

        @Test
        @DisplayName("不支持的操作符抛出 IllegalArgumentException")
        void unsupportedOperatorThrows() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "!=", "val".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(pred))
                .build();

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> builder.buildPredicateScanSql("t", filter));
            assertTrue(ex.getMessage().contains("Unsupported storage predicate operator"));
        }

        @Test
        @DisplayName("LIKE 操作符不被支持")
        void likeOperatorThrows() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "LIKE", "val".getBytes());
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[]{0})
                .endKey(new byte[]{(byte) 0xFF})
                .columnPredicates(java.util.List.of(pred))
                .build();

            assertThrows(IllegalArgumentException.class,
                () -> builder.buildPredicateScanSql("t", filter));
        }
    }
}
