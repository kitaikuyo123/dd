package com.minisql.regionserver;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.KeyValue;
import com.minisql.storage.StorageEngine;
import com.minisql.storage.StorageEngineFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionSplitService unit tests")
class RegionSplitServiceTest {

    // ---------- hand-written fakes ----------

    static class FakeStorageEngine implements StorageEngine {
        long estimatedSize;

        @Override public void put(byte[] key, KeyValue value) {}
        @Override public void batchPut(List<KeyValue> values) {}
        @Override public List<KeyValue> get(byte[] key) { return Collections.emptyList(); }
        @Override public Iterator<KeyValue> scan(byte[] startKey, byte[] endKey) {
            return Collections.<KeyValue>emptyList().iterator();
        }
        @Override public void delete(byte[] key) {}
        @Override public void flush() {}
        @Override public void compact(boolean major) {}
        @Override public void close() {}
        @Override public void dropData() {}
        @Override public long estimateSizeBytes() { return estimatedSize; }
    }

    static class FakeEngineFactory implements StorageEngineFactory {
        @Override
        public StorageEngine create(String regionId) {
            return new FakeStorageEngine();
        }
        @Override public void close() {}
    }

    static class FakeRegionServer extends RegionServer {
        private final ServerId serverId;
        private final StorageEngineFactory factory;

        FakeRegionServer() {
            super("localhost", 16020, null, new FakeEngineFactory(), null, 1);
            this.serverId = new ServerId("localhost", 16020);
            this.factory = super.getEngineFactory();
        }

        @Override
        public ServerId getServerId() {
            return serverId;
        }
    }

    // ---------- helpers ----------

    private RegionManager regionManager;
    private RegionSplitService splitService;

    /**
     * Registers a region as OPEN with a storage engine that has a given estimated size.
     */
    private FakeStorageEngine registerRegionWithSize(String regionId, String tableName,
                                                      byte[] startKey, byte[] endKey,
                                                      long estimatedSize) {
        Region region = new Region(regionId, tableName, startKey, endKey);
        FakeStorageEngine engine = new FakeStorageEngine();
        engine.estimatedSize = estimatedSize;
        RegionStorage storage = new RegionStorage(regionId, engine);
        regionManager.registerOpenedRegion(region, storage);
        return engine;
    }

    @BeforeEach
    void setUp() {
        FakeRegionServer fakeServer = new FakeRegionServer();
        regionManager = fakeServer.getRegionManager();
        splitService = new RegionSplitService(regionManager);
    }

    // ==================== shouldSplit ====================

    @Nested
    @DisplayName("shouldSplit")
    class ShouldSplitTests {

        @Test
        @DisplayName("returns false when region does not exist")
        void testShouldSplitUnknownRegion() {
            assertFalse(splitService.shouldSplit("nonexistent"));
        }

        @Test
        @DisplayName("returns false when region size is below minSplitSize")
        void testShouldSplitBelowMinSplitSize() {
            // Default minSplitSize = 1GB, give 500MB
            long size500MB = 500L * 1024 * 1024;
            registerRegionWithSize("small", "t", "a".getBytes(), "z".getBytes(), size500MB);

            assertFalse(splitService.shouldSplit("small"));
        }

        @Test
        @DisplayName("returns false when region size between minSplitSize and splitThreshold")
        void testShouldSplitBetweenMinAndThreshold() {
            // minSplitSize = 1GB, splitThreshold = 10GB; give 5GB
            long size5GB = 5L * 1024 * 1024 * 1024;
            registerRegionWithSize("mid", "t", "a".getBytes(), "z".getBytes(), size5GB);

            assertFalse(splitService.shouldSplit("mid"));
        }

        @Test
        @DisplayName("returns true when region size meets splitThreshold")
        void testShouldSplitAtThreshold() {
            // Default splitThreshold = 10GB
            long size10GB = 10L * 1024 * 1024 * 1024;
            registerRegionWithSize("big", "t", "a".getBytes(), "z".getBytes(), size10GB);

            assertTrue(splitService.shouldSplit("big"));
        }

        @Test
        @DisplayName("returns true when region size exceeds splitThreshold")
        void testShouldSplitAboveThreshold() {
            long size15GB = 15L * 1024 * 1024 * 1024;
            registerRegionWithSize("huge", "t", "a".getBytes(), "z".getBytes(), size15GB);

            assertTrue(splitService.shouldSplit("huge"));
        }

        @Test
        @DisplayName("custom threshold: under threshold returns false")
        void testCustomConfigUnderThreshold() {
            splitService.setConfig(100, 10); // 100MB threshold, 10MB min

            long size50MB = 50L * 1024 * 1024;
            registerRegionWithSize("custom-under", "t", "a".getBytes(), "z".getBytes(), size50MB);

            assertFalse(splitService.shouldSplit("custom-under"));
        }

        @Test
        @DisplayName("custom threshold: over threshold but below min returns false")
        void testCustomConfigOverThresholdButBelowMin() {
            splitService.setConfig(100, 10); // 100MB threshold, 10MB min

            long size5MB = 5L * 1024 * 1024;
            registerRegionWithSize("tiny", "t", "a".getBytes(), "z".getBytes(), size5MB);

            assertFalse(splitService.shouldSplit("tiny"),
                    "Region below minSplitSize should not split even if threshold is low");
        }

        @Test
        @DisplayName("custom threshold: above threshold and above min returns true")
        void testCustomConfigAboveThreshold() {
            splitService.setConfig(100, 10); // 100MB threshold, 10MB min

            long size150MB = 150L * 1024 * 1024;
            registerRegionWithSize("custom-over", "t", "a".getBytes(), "z".getBytes(), size150MB);

            assertTrue(splitService.shouldSplit("custom-over"));
        }

        @Test
        @DisplayName("returns false when region storage is null (region registered without storage)")
        void testShouldSplitNoStorage() {
            Region region = new Region("no-storage", "t", "a".getBytes(), "z".getBytes());
            regionManager.registerRegionInternal(region);
            regionManager.setRegionState("no-storage", RegionManager.RegionState.OPEN);

            assertFalse(splitService.shouldSplit("no-storage"));
        }
    }

    // ==================== findBestSplitPoint ====================

    @Nested
    @DisplayName("findBestSplitPoint")
    class FindBestSplitPointTests {

        @Test
        @DisplayName("throws IOException when region storage does not exist")
        void testFindSplitPointNoStorage() {
            Region region = new Region("ghost", "t", "a".getBytes(), "z".getBytes());
            regionManager.registerRegionInternal(region);

            IOException ex = assertThrows(IOException.class,
                    () -> splitService.findBestSplitPoint("ghost"));
            assertTrue(ex.getMessage().contains("storage not found"));
        }

        @Test
        @DisplayName("throws IOException when region metadata does not exist")
        void testFindSplitPointNoRegion() {
            FakeStorageEngine engine = new FakeStorageEngine();
            RegionStorage storage = new RegionStorage("orphan", engine);
            regionManager.registerRegionStorage("orphan", storage);

            IOException ex = assertThrows(IOException.class,
                    () -> splitService.findBestSplitPoint("orphan"));
            assertTrue(ex.getMessage().contains("Region not found"));
        }

        @Test
        @DisplayName("returns a split key between start and end keys")
        void testFindSplitPointBasic() throws IOException {
            byte[] start = new byte[]{0x10};
            byte[] end = new byte[]{0x20};
            registerRegionWithSize("r1", "t", start, end, 0);

            byte[] splitKey = splitService.findBestSplitPoint("r1");

            assertNotNull(splitKey);
            // The midpoint of 0x10 and 0x20 should be 0x18
            assertEquals(1, splitKey.length);
            assertEquals(0x18, splitKey[0] & 0xFF);
        }

        @Test
        @DisplayName("handles null start key by defaulting to 0x00")
        void testFindSplitPointNullStart() throws IOException {
            byte[] end = new byte[]{0x20};
            registerRegionWithSize("r2", "t", null, end, 0);

            byte[] splitKey = splitService.findBestSplitPoint("r2");

            assertNotNull(splitKey);
            // Midpoint of 0x00 and 0x20 = 0x10
            assertEquals(0x10, splitKey[0] & 0xFF);
        }

        @Test
        @DisplayName("handles null end key by defaulting to 0xFF")
        void testFindSplitPointNullEnd() throws IOException {
            byte[] start = new byte[]{(byte) 0x80};
            registerRegionWithSize("r3", "t", start, null, 0);

            byte[] splitKey = splitService.findBestSplitPoint("r3");

            assertNotNull(splitKey);
            // Midpoint of 0x80 and 0xFF = (0x80+0xFF)/2 = 0xBF (191)
            assertEquals(0xBF, splitKey[0] & 0xFF);
        }

        @Test
        @DisplayName("handles both null start and end keys")
        void testFindSplitPointBothNull() throws IOException {
            registerRegionWithSize("r4", "t", null, null, 0);

            byte[] splitKey = splitService.findBestSplitPoint("r4");

            assertNotNull(splitKey);
            // Midpoint of 0x00 and 0xFF = 0x7F (127)
            assertEquals(0x7F, splitKey[0] & 0xFF);
        }

        @Test
        @DisplayName("handles empty start and end key arrays")
        void testFindSplitPointEmptyArrays() throws IOException {
            registerRegionWithSize("r5", "t", new byte[0], new byte[0], 0);

            byte[] splitKey = splitService.findBestSplitPoint("r5");

            assertNotNull(splitKey);
            // Both treated as defaults: 0x00 and 0xFF -> midpoint 0x7F
            assertEquals(0x7F, splitKey[0] & 0xFF);
        }

        @Test
        @DisplayName("handles multi-byte keys with different lengths")
        void testFindSplitPointMultiByte() throws IOException {
            byte[] start = new byte[]{0x01, 0x00};
            byte[] end = new byte[]{0x01, 0x10};
            registerRegionWithSize("r6", "t", start, end, 0);

            byte[] splitKey = splitService.findBestSplitPoint("r6");

            assertNotNull(splitKey);
            assertEquals(2, splitKey.length);
            assertEquals(0x01, splitKey[0] & 0xFF);
            assertEquals(0x08, splitKey[1] & 0xFF);
        }
    }

    // ==================== setConfig ====================

    @Nested
    @DisplayName("setConfig")
    class SetConfigTests {

        @Test
        @DisplayName("setConfig applies custom threshold and minSplitSize")
        void testSetConfigAffectsThreshold() {
            splitService.setConfig(50, 5); // 50MB threshold, 5MB min

            // 30MB is above 5MB min but below 50MB threshold -> no split
            long size30MB = 30L * 1024 * 1024;
            registerRegionWithSize("cfg1", "t", "a".getBytes(), "z".getBytes(), size30MB);
            assertFalse(splitService.shouldSplit("cfg1"));

            // 60MB is above both -> should split
            long size60MB = 60L * 1024 * 1024;
            registerRegionWithSize("cfg2", "t", "a".getBytes(), "z".getBytes(), size60MB);
            assertTrue(splitService.shouldSplit("cfg2"));
        }

        @Test
        @DisplayName("setConfig with very large threshold means nothing splits")
        void testSetConfigLargeThreshold() {
            splitService.setConfig(1024 * 1024, 1); // 1TB threshold

            long size10GB = 10L * 1024 * 1024 * 1024;
            registerRegionWithSize("huge2", "t", "a".getBytes(), "z".getBytes(), size10GB);

            assertFalse(splitService.shouldSplit("huge2"));
        }
    }
}
