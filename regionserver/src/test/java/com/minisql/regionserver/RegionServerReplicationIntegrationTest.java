package com.minisql.regionserver;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.storage.RocksDBConfig;
import com.minisql.storage.RocksDBEngineFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionServer-Replication integration tests")
class RegionServerReplicationIntegrationTest {

    @TempDir
    Path tempDir;

    private RegionServer regionServer;

    @BeforeEach
    void setUp() {
        String testWalPath = tempDir.resolve("wal").toString();
        String dataDir = tempDir.resolve("rocksdb").toString();
        RocksDBEngineFactory engineFactory = new RocksDBEngineFactory(
            RocksDBConfig.builder(dataDir).enableWal(false).build()
        );

        regionServer = new RegionServer(
            "localhost", 16020,
            engineFactory,
            null, 3, testWalPath
        );
        regionServer.getReplicationCoordinator().start();
    }

    @AfterEach
    void tearDown() {
        try {
            regionServer.stop();
        } catch (Exception ignored) {
        }
    }

    private Region openTestRegion(String regionId) {
        Region region = new Region(regionId, "test_table", new byte[]{0x00}, new byte[]{0x7F});
        regionServer.getRegionManager().openRegion(region);
        return region;
    }

    @Test
    @DisplayName("put writes to local storage successfully")
    void putWritesToLocalStorage() {
        String regionId = "repl-integ-1";
        openTestRegion(regionId);

        KeyValue kv = new KeyValue();
        kv.setRowKey("row1".getBytes());
        kv.setFamily("");
        kv.setQualifier("name");
        kv.setValue("Alice".getBytes());
        kv.setType(KeyValue.Type.PUT);
        kv.setTimestamp(System.nanoTime());

        assertDoesNotThrow(() -> regionServer.put(regionId, List.of(kv), true));

        List<KeyValue> results = regionServer.get(regionId, "row1".getBytes());
        assertNotNull(results);
        assertFalse(results.isEmpty());
        assertEquals("Alice", new String(results.get(0).getValue()));
    }

    @Test
    @DisplayName("put with no replica group still commits locally")
    void putWithNoReplicaGroupStillCommitsLocally() {
        String regionId = "repl-integ-2";
        openTestRegion(regionId);

        // Remove the replica group created during openRegion
        ReplicationCoordinator coord = regionServer.getReplicationCoordinator();
        if (coord != null && coord.getReplicaGroup(regionId) != null) {
            coord.removeReplicaGroup(regionId);
        }

        KeyValue kv = new KeyValue();
        kv.setRowKey("row-solo".getBytes());
        kv.setFamily("");
        kv.setQualifier("val");
        kv.setValue("solo-data".getBytes());
        kv.setType(KeyValue.Type.PUT);
        kv.setTimestamp(System.nanoTime());

        assertDoesNotThrow(() -> regionServer.put(regionId, List.of(kv), true));

        List<KeyValue> results = regionServer.get(regionId, "row-solo".getBytes());
        assertNotNull(results);
        assertFalse(results.isEmpty());
    }

    @Test
    @DisplayName("multiple puts maintain data integrity")
    void fullReplicationLifecycle() {
        String regionId = "repl-integ-3";
        openTestRegion(regionId);

        int count = 20;
        for (int i = 0; i < count; i++) {
            KeyValue kv = new KeyValue();
            kv.setRowKey(("row-" + i).getBytes());
            kv.setFamily("");
            kv.setQualifier("idx");
            kv.setValue(String.valueOf(i).getBytes());
            kv.setType(KeyValue.Type.PUT);
            kv.setTimestamp(System.nanoTime());
            assertDoesNotThrow(() -> regionServer.put(regionId, List.of(kv), true));
        }

        // Verify all rows readable
        for (int i = 0; i < count; i++) {
            List<KeyValue> results = regionServer.get(regionId, ("row-" + i).getBytes());
            assertNotNull(results, "Should find row-" + i);
            assertFalse(results.isEmpty(), "row-" + i + " should have data");
            assertEquals(String.valueOf(i), new String(results.get(0).getValue()));
        }
    }
}
