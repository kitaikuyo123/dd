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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionMergeService unit tests")
class RegionMergeServiceTest {

    // ---------- hand-written fakes ----------

    static class FakeStorageEngine implements StorageEngine {
        long storeFileSize;
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

        FakeRegionServer() {
            super("localhost", 16020, null, new FakeEngineFactory(), null, 1);
            this.serverId = new ServerId("localhost", 16020);
        }

        @Override
        public ServerId getServerId() {
            return serverId;
        }
    }

    // ---------- helpers ----------

    private RegionManager regionManager;
    private RegionMergeService mergeService;

    /**
     * Registers a region with a fake storage engine whose getActualTableSize / getStoreFileSize
     * returns the provided size.
     *
     * Because getStoreFileSize() in RegionStorage synchronizes and calls estimateSizeBytes(),
     * we set estimatedSize on the fake engine.
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
        mergeService = new RegionMergeService(regionManager);
    }

    // ==================== canMerge ====================

    @Nested
    @DisplayName("canMerge")
    class CanMergeTests {

        @Test
        @DisplayName("returns false when regions are not adjacent")
        void testCanMergeNotAdjacent() {
            // left endKey = "m", right startKey = "p" -- not equal, so not adjacent
            registerRegionWithSize("left", "t", "a".getBytes(), "m".getBytes(), 50L);
            registerRegionWithSize("right", "t", "p".getBytes(), "z".getBytes(), 50L);

            assertFalse(mergeService.canMerge("left", "right"));
        }

        @Test
        @DisplayName("returns false when left region storage is missing")
        void testCanMergeLeftStorageNull() {
            Region leftRegion = new Region("left", "t", "a".getBytes(), "m".getBytes());
            regionManager.registerRegionInternal(leftRegion);
            regionManager.setRegionState("left", RegionManager.RegionState.OPEN);

            registerRegionWithSize("right", "t", "m".getBytes(), "z".getBytes(), 50L);

            assertFalse(mergeService.canMerge("left", "right"));
        }

        @Test
        @DisplayName("returns false when right region storage is missing")
        void testCanMergeRightStorageNull() {
            registerRegionWithSize("left", "t", "a".getBytes(), "m".getBytes(), 50L);

            Region rightRegion = new Region("right", "t", "m".getBytes(), "z".getBytes());
            regionManager.registerRegionInternal(rightRegion);
            regionManager.setRegionState("right", RegionManager.RegionState.OPEN);

            assertFalse(mergeService.canMerge("left", "right"));
        }

        @Test
        @DisplayName("returns false when left region metadata is missing")
        void testCanMergeLeftRegionNull() {
            // Only register storage, not region metadata for left
            FakeStorageEngine leftEngine = new FakeStorageEngine();
            RegionStorage leftStorage = new RegionStorage("left", leftEngine);
            regionManager.registerRegionStorage("left", leftStorage);

            registerRegionWithSize("right", "t", "m".getBytes(), "z".getBytes(), 50L);

            assertFalse(mergeService.canMerge("left", "right"));
        }

        @Test
        @DisplayName("returns false when right region metadata is missing")
        void testCanMergeRightRegionNull() {
            registerRegionWithSize("left", "t", "a".getBytes(), "m".getBytes(), 50L);

            FakeStorageEngine rightEngine = new FakeStorageEngine();
            RegionStorage rightStorage = new RegionStorage("right", rightEngine);
            regionManager.registerRegionStorage("right", rightStorage);

            assertFalse(mergeService.canMerge("left", "right"));
        }

        @Test
        @DisplayName("returns false when combined size exceeds MAX_MERGE_SIZE")
        void testCanMergeExceedsMaxSize() {
            long size5GB = 5L * 1024 * 1024 * 1024;
            // 5GB + 5GB = 10GB > MAX_MERGE_SIZE(8GB)
            registerRegionWithSize("left-big", "t", "a".getBytes(), "m".getBytes(), size5GB);
            registerRegionWithSize("right-big", "t", "m".getBytes(), "z".getBytes(), size5GB);

            assertFalse(mergeService.canMerge("left-big", "right-big"));
        }

        @Test
        @DisplayName("returns true when both regions are below MERGE_THRESHOLD")
        void testCanMergeBothSmall() {
            // MERGE_THRESHOLD = 100MB; give 40MB each
            long size40MB = 40L * 1024 * 1024;
            registerRegionWithSize("left-sm", "t", "a".getBytes(), "m".getBytes(), size40MB);
            registerRegionWithSize("right-sm", "t", "m".getBytes(), "z".getBytes(), size40MB);

            assertTrue(mergeService.canMerge("left-sm", "right-sm"));
        }

        @Test
        @DisplayName("returns true when one region is below MIN_MERGE_SIZE (forced merge)")
        void testCanMergeOneTinyForced() {
            // One region is tiny (< 10MB = MIN_MERGE_SIZE), the other is large but under MAX
            long size5MB = 5L * 1024 * 1024;
            long size1GB = 1L * 1024 * 1024 * 1024;
            registerRegionWithSize("left-tiny", "t", "a".getBytes(), "m".getBytes(), size5MB);
            registerRegionWithSize("right-large", "t", "m".getBytes(), "z".getBytes(), size1GB);

            assertTrue(mergeService.canMerge("left-tiny", "right-large"));
        }

        @Test
        @DisplayName("returns true when right region is tiny and left is large (forced merge)")
        void testCanMergeRightTinyForced() {
            long size1GB = 1L * 1024 * 1024 * 1024;
            long size5MB = 5L * 1024 * 1024;
            registerRegionWithSize("left-large", "t", "a".getBytes(), "m".getBytes(), size1GB);
            registerRegionWithSize("right-tiny", "t", "m".getBytes(), "z".getBytes(), size5MB);

            assertTrue(mergeService.canMerge("left-large", "right-tiny"));
        }

        @Test
        @DisplayName("returns false when both regions are above MERGE_THRESHOLD and above MIN_MERGE_SIZE")
        void testCanMergeBothMedium() {
            // Each is 60MB: above MIN_MERGE_SIZE(10MB) and above MERGE_THRESHOLD(100MB)? No, 60MB < 100MB.
            // Let's use 150MB each: above MERGE_THRESHOLD but below MAX combined
            // 150MB + 150MB = 300MB < 8GB. Both above threshold, not forced.
            // Actually MERGE_THRESHOLD check: both < 100MB -> merge. If one >= 100MB and no tiny -> no merge.
            long size150MB = 150L * 1024 * 1024;
            registerRegionWithSize("left-med", "t", "a".getBytes(), "m".getBytes(), size150MB);
            registerRegionWithSize("right-med", "t", "m".getBytes(), "z".getBytes(), size150MB);

            // Both are > MERGE_THRESHOLD, neither is < MIN_MERGE_SIZE, combined < MAX_MERGE_SIZE
            assertFalse(mergeService.canMerge("left-med", "right-med"));
        }

        @Test
        @DisplayName("returns false for non-existent regions")
        void testCanMergeNonExistent() {
            assertFalse(mergeService.canMerge("ghost1", "ghost2"));
        }

        @Test
        @DisplayName("adjacent regions with same end/start key")
        void testCanMergeAdjacentExactMatch() {
            // left.endKey == right.startKey (both are "m")
            long size50MB = 50L * 1024 * 1024;
            registerRegionWithSize("left-exact", "t", "a".getBytes(), "m".getBytes(), size50MB);
            registerRegionWithSize("right-exact", "t", "m".getBytes(), "z".getBytes(), size50MB);

            assertTrue(mergeService.canMerge("left-exact", "right-exact"));
        }
    }

    // ==================== isAdjacent (via reflection for private method) ====================

    @Nested
    @DisplayName("isAdjacent")
    class IsAdjacentTests {

        private Method isAdjacentMethod;

        @BeforeEach
        void initReflection() throws Exception {
            isAdjacentMethod = RegionMergeService.class.getDeclaredMethod(
                    "isAdjacent", String.class, String.class);
            isAdjacentMethod.setAccessible(true);
        }

        private boolean invokeIsAdjacent(String left, String right) throws Exception {
            return (boolean) isAdjacentMethod.invoke(mergeService, left, right);
        }

        @Test
        @DisplayName("returns true when left.endKey equals right.startKey")
        void testIsAdjacentTrue() throws Exception {
            Region left = new Region("l", "t", "a".getBytes(), "m".getBytes());
            Region right = new Region("r", "t", "m".getBytes(), "z".getBytes());
            regionManager.registerRegionInternal(left);
            regionManager.registerRegionInternal(right);

            assertTrue(invokeIsAdjacent("l", "r"));
        }

        @Test
        @DisplayName("returns false when keys do not match")
        void testIsAdjacentFalse() throws Exception {
            Region left = new Region("l", "t", "a".getBytes(), "m".getBytes());
            Region right = new Region("r", "t", "n".getBytes(), "z".getBytes());
            regionManager.registerRegionInternal(left);
            regionManager.registerRegionInternal(right);

            assertFalse(invokeIsAdjacent("l", "r"));
        }

        @Test
        @DisplayName("returns true when both endKey and startKey are null")
        void testIsAdjacentBothNull() throws Exception {
            Region left = new Region("l", "t", "a".getBytes(), null);
            Region right = new Region("r", "t", null, "z".getBytes());
            regionManager.registerRegionInternal(left);
            regionManager.registerRegionInternal(right);

            assertTrue(invokeIsAdjacent("l", "r"));
        }

        @Test
        @DisplayName("returns false when left endKey is null but right startKey is not")
        void testIsAdjacentLeftNullRightNot() throws Exception {
            Region left = new Region("l", "t", "a".getBytes(), null);
            Region right = new Region("r", "t", "m".getBytes(), "z".getBytes());
            regionManager.registerRegionInternal(left);
            regionManager.registerRegionInternal(right);

            assertFalse(invokeIsAdjacent("l", "r"));
        }

        @Test
        @DisplayName("returns false when right startKey is null but left endKey is not")
        void testIsAdjacentRightNullLeftNot() throws Exception {
            Region left = new Region("l", "t", "a".getBytes(), "m".getBytes());
            Region right = new Region("r", "t", null, "z".getBytes());
            regionManager.registerRegionInternal(left);
            regionManager.registerRegionInternal(right);

            assertFalse(invokeIsAdjacent("l", "r"));
        }

        @Test
        @DisplayName("returns false when left region is not found")
        void testIsAdjacentLeftNotFound() throws Exception {
            Region right = new Region("r", "t", "m".getBytes(), "z".getBytes());
            regionManager.registerRegionInternal(right);

            assertFalse(invokeIsAdjacent("nonexistent", "r"));
        }

        @Test
        @DisplayName("returns false when right region is not found")
        void testIsAdjacentRightNotFound() throws Exception {
            Region left = new Region("l", "t", "a".getBytes(), "m".getBytes());
            regionManager.registerRegionInternal(left);

            assertFalse(invokeIsAdjacent("l", "nonexistent"));
        }

        @Test
        @DisplayName("returns false when both regions are not found")
        void testIsAdjacentBothNotFound() throws Exception {
            assertFalse(invokeIsAdjacent("ghost1", "ghost2"));
        }

        @Test
        @DisplayName("returns true when keys have multi-byte equality")
        void testIsAdjacentMultiByteKeys() throws Exception {
            byte[] boundary = new byte[]{0x01, 0x02, 0x03};
            Region left = new Region("l", "t", "a".getBytes(), boundary);
            Region right = new Region("r", "t", boundary.clone(), "z".getBytes());
            regionManager.registerRegionInternal(left);
            regionManager.registerRegionInternal(right);

            assertTrue(invokeIsAdjacent("l", "r"));
        }
    }

    // ==================== constants ====================

    @Nested
    @DisplayName("merge thresholds constants")
    class ConstantsTests {

        @Test
        @DisplayName("MERGE_THRESHOLD is 100MB")
        void testMergeThreshold() {
            assertEquals(100L * 1024 * 1024, RegionMergeService.MERGE_THRESHOLD);
        }

        @Test
        @DisplayName("MAX_MERGE_SIZE is 8GB")
        void testMaxMergeSize() {
            assertEquals(8L * 1024 * 1024 * 1024, RegionMergeService.MAX_MERGE_SIZE);
        }

        @Test
        @DisplayName("MIN_MERGE_SIZE is 10MB")
        void testMinMergeSize() {
            assertEquals(10L * 1024 * 1024, RegionMergeService.MIN_MERGE_SIZE);
        }
    }

    // ==================== canMerge edge cases ====================

    @Nested
    @DisplayName("canMerge edge cases")
    class CanMergeEdgeCaseTests {

        @Test
        @DisplayName("canMerge returns false for same regionId as both arguments")
        void testCanMergeSameRegion() {
            long size40MB = 40L * 1024 * 1024;
            // left endKey "m", right startKey "m" -- but same regionId
            // This depends on whether isAdjacent treats a region as adjacent to itself
            // A region with endKey="m" and startKey="a" -- not self-adjacent
            registerRegionWithSize("self", "t", "a".getBytes(), "m".getBytes(), size40MB);

            // Both arguments point to the same region, so endKey="m", startKey="a" -- not adjacent
            assertFalse(mergeService.canMerge("self", "self"));
        }

        @Test
        @DisplayName("canMerge handles zero-size regions")
        void testCanMergeZeroSize() {
            registerRegionWithSize("left-zero", "t", "a".getBytes(), "m".getBytes(), 0L);
            registerRegionWithSize("right-zero", "t", "m".getBytes(), "z".getBytes(), 0L);

            // Both are 0 < MERGE_THRESHOLD, combined 0 < MAX_MERGE_SIZE
            assertTrue(mergeService.canMerge("left-zero", "right-zero"));
        }
    }
}
