package com.minisql.storage;

import com.minisql.common.model.KeyValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisplayName("MySQLStorageEngine integration tests")
class MySQLStorageEngineTest {

    private static final String MYSQL_URL_ENV = "MINISQL_TEST_MYSQL_URL";
    private static final String MYSQL_USER_ENV = "MINISQL_TEST_MYSQL_USER";
    private static final String MYSQL_PASSWORD_ENV = "MINISQL_TEST_MYSQL_PASSWORD";
    private static final String TEST_REGION_ID = "test_region_001";

    private MySQLStorageEngine engine;

    @BeforeEach
    void setUp() {
        assumeTrue(hasMySqlIntegrationEnv(),
            () -> "Skipping MySQL storage integration test because "
                + MYSQL_URL_ENV + "/" + MYSQL_USER_ENV + "/" + MYSQL_PASSWORD_ENV + " are not set");

        MySQLConfig config = MySQLConfig.builder(
                System.getenv(MYSQL_URL_ENV),
                System.getenv(MYSQL_USER_ENV),
                System.getenv(MYSQL_PASSWORD_ENV))
            .maxPoolSize(5)
            .connectionTimeout(10000)
            .autoCreateSchema(true)
            .build();

        engine = new MySQLStorageEngine(config, TEST_REGION_ID);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.dropTable();
            engine.close();
        }
    }

    @Test
    @DisplayName("put and get return latest visible row values")
    void testPutAndGet() {
        byte[] rowKey = "row1".getBytes();
        KeyValue name = new KeyValue.Builder(rowKey)
            .family("cf")
            .qualifier("name")
            .value("zhangsan".getBytes())
            .build();
        KeyValue age = new KeyValue.Builder(rowKey)
            .family("cf")
            .qualifier("age")
            .value("18".getBytes())
            .build();

        engine.put(rowKey, name);
        engine.put(rowKey, age);

        List<KeyValue> results = engine.get(rowKey);
        assertNotNull(results);
        assertEquals(2, results.size());
    }

    @Test
    @DisplayName("batch put persists multiple rows")
    void testBatchPut() {
        List<KeyValue> values = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            byte[] rowKey = ("row" + i).getBytes();
            values.add(new KeyValue.Builder(rowKey)
                .family("cf")
                .qualifier("col")
                .value(("value" + i).getBytes())
                .build());
        }

        engine.batchPut(values);

        for (int i = 0; i < 5; i++) {
            List<KeyValue> results = engine.get(("row" + i).getBytes());
            assertEquals(1, results.size());
        }
    }

    @Test
    @DisplayName("scan iterates rows within range")
    void testScan() {
        List<KeyValue> values = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            byte[] rowKey = new byte[]{(byte) ('a' + i)};
            values.add(new KeyValue.Builder(rowKey)
                .family("cf")
                .qualifier("col")
                .value(("value" + i).getBytes())
                .build());
        }
        engine.batchPut(values);

        Iterator<KeyValue> iterator = engine.scan("c".getBytes(), "h".getBytes());
        int count = 0;
        while (iterator.hasNext()) {
            KeyValue kv = iterator.next();
            assertTrue(kv.getRowKey()[0] >= 'c');
            assertTrue(kv.getRowKey()[0] < 'h');
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    @DisplayName("scan with storage predicate returns latest visible rows for matching qualifier values")
    void testScanWithPredicatePushdown() {
        List<KeyValue> values = new ArrayList<>();
        values.add(new KeyValue.Builder("row1".getBytes())
            .family("")
            .qualifier("name")
            .value(com.minisql.common.utils.RowKeySerializer.serialize("A", com.minisql.common.model.Column.ColumnType.STRING))
            .build());
        values.add(new KeyValue.Builder("row1".getBytes())
            .family("")
            .qualifier("price")
            .value(com.minisql.common.utils.RowKeySerializer.serialize(10, com.minisql.common.model.Column.ColumnType.INT))
            .build());
        values.add(new KeyValue.Builder("row2".getBytes())
            .family("")
            .qualifier("name")
            .value(com.minisql.common.utils.RowKeySerializer.serialize("B", com.minisql.common.model.Column.ColumnType.STRING))
            .build());
        values.add(new KeyValue.Builder("row2".getBytes())
            .family("")
            .qualifier("price")
            .value(com.minisql.common.utils.RowKeySerializer.serialize(20, com.minisql.common.model.Column.ColumnType.INT))
            .build());
        engine.batchPut(values);

        StorageScanFilter filter = new StorageScanFilter(
            "row".getBytes(),
            "roz".getBytes(),
            List.of(new StorageColumnPredicate(
                "name",
                "=",
                com.minisql.common.utils.RowKeySerializer.serialize("A", com.minisql.common.model.Column.ColumnType.STRING))),
            List.of()
        );

        Iterator<KeyValue> iterator = engine.scan(filter);
        List<KeyValue> results = new ArrayList<>();
        iterator.forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(kv -> Arrays.equals("row1".getBytes(), kv.getRowKey())));
    }

    @Test
    @DisplayName("scan with projected qualifiers only returns requested columns")
    void testScanWithProjectionPushdown() {
        List<KeyValue> values = new ArrayList<>();
        values.add(new KeyValue.Builder("row1".getBytes())
            .family("")
            .qualifier("name")
            .value(com.minisql.common.utils.RowKeySerializer.serialize("A", com.minisql.common.model.Column.ColumnType.STRING))
            .build());
        values.add(new KeyValue.Builder("row1".getBytes())
            .family("")
            .qualifier("price")
            .value(com.minisql.common.utils.RowKeySerializer.serialize(10, com.minisql.common.model.Column.ColumnType.INT))
            .build());
        values.add(new KeyValue.Builder("row2".getBytes())
            .family("")
            .qualifier("name")
            .value(com.minisql.common.utils.RowKeySerializer.serialize("B", com.minisql.common.model.Column.ColumnType.STRING))
            .build());
        values.add(new KeyValue.Builder("row2".getBytes())
            .family("")
            .qualifier("price")
            .value(com.minisql.common.utils.RowKeySerializer.serialize(20, com.minisql.common.model.Column.ColumnType.INT))
            .build());
        engine.batchPut(values);

        StorageScanFilter filter = new StorageScanFilter(
            "row".getBytes(),
            "roz".getBytes(),
            List.of(),
            List.of("name")
        );

        Iterator<KeyValue> iterator = engine.scan(filter);
        List<KeyValue> results = new ArrayList<>();
        iterator.forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(kv -> "name".equals(kv.getQualifier())));
        assertTrue(results.stream().anyMatch(kv -> Arrays.equals("row1".getBytes(), kv.getRowKey())));
        assertTrue(results.stream().anyMatch(kv -> Arrays.equals("row2".getBytes(), kv.getRowKey())));
    }

    @Test
    @DisplayName("delete hides row from subsequent reads")
    void testDelete() {
        byte[] rowKey = "row-to-delete".getBytes();
        engine.put(rowKey, new KeyValue.Builder(rowKey)
            .family("cf")
            .qualifier("col")
            .value("original".getBytes())
            .build());

        assertFalse(engine.get(rowKey).isEmpty());
        engine.delete(rowKey);
        assertTrue(engine.get(rowKey).isEmpty());
    }

    @Test
    @DisplayName("latest timestamp wins for same row and qualifier")
    void testMvccLatestVersionWins() {
        byte[] rowKey = "mvcc-row".getBytes();
        engine.put(rowKey, new KeyValue.Builder(rowKey)
            .family("cf")
            .qualifier("col")
            .timestamp(1000L)
            .value("version1".getBytes())
            .build());
        engine.put(rowKey, new KeyValue.Builder(rowKey)
            .family("cf")
            .qualifier("col")
            .timestamp(2000L)
            .value("version2".getBytes())
            .build());

        List<KeyValue> results = engine.get(rowKey);
        assertEquals(1, results.size());
        assertEquals("version2", new String(results.get(0).getValue()));
        assertEquals(2000L, results.get(0).getTimestamp());
    }

    private boolean hasMySqlIntegrationEnv() {
        return !isBlank(System.getenv(MYSQL_URL_ENV))
            && !isBlank(System.getenv(MYSQL_USER_ENV))
            && !isBlank(System.getenv(MYSQL_PASSWORD_ENV));
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
