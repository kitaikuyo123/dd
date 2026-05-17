package com.minisql.regionserver;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.storage.StorageEngine;
import com.minisql.storage.StorageEngineFactory;
import com.minisql.storage.StorageScanFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionSplitService concurrency tests")
class RegionSplitServiceConcurrencyTest {

    private RegionManager regionManager;
    private RegionSplitService splitService;

    @BeforeEach
    void setUp() {
        FakeRegionServer fakeServer = new FakeRegionServer();
        regionManager = fakeServer.getRegionManager();
        splitService = new RegionSplitService(regionManager);
    }

    @Test
    @DisplayName("concurrent findBestSplitPoint on different regions succeeds")
    void concurrentFindSplitPointDifferentRegions() throws Exception {
        int regionCount = 4;
        for (int i = 0; i < regionCount; i++) {
            Region region = new Region("split-r" + i, "test_table",
                new byte[]{(byte) (0x10 * i)}, new byte[]{(byte) (0x10 * (i + 1))});
            FakeStorageEngine engine = new FakeStorageEngine();
            RegionStorage storage = new RegionStorage("split-r" + i, engine);
            regionManager.registerOpenedRegion(region, storage);
        }

        int threadCount = regionCount;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);
        List<byte[]> results = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    byte[] splitKey = splitService.findBestSplitPoint("split-r" + idx);
                    results.add(splitKey);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }, "split-finder-" + i).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        assertEquals(0, errors.get(), "No errors expected during concurrent findBestSplitPoint");
        assertEquals(regionCount, results.size(), "All threads should produce split keys");
    }

    @Test
    @DisplayName("blockWrites prevents writes during split window")
    void blockWritesDuringSplit() {
        String regionId = "split-block-test";
        Region region = new Region(regionId, "test_table",
            new byte[]{0x00}, new byte[]{0x7F});
        regionManager.openRegion(region);

        // Block writes (simulating split in progress)
        regionManager.blockWrites(regionId);
        assertTrue(regionManager.isWriteBlocked(regionId));

        // Verify writes are blocked
        assertTrue(regionManager.isWriteBlocked(regionId));

        // Unblock
        regionManager.unblockWrites(regionId);
        assertFalse(regionManager.isWriteBlocked(regionId));
    }

    // ================================
    // Fakes
    // ================================

    static class FakeStorageEngine implements StorageEngine {
        @Override public void put(byte[] key, KeyValue value) {}
        @Override public void batchPut(List<KeyValue> values) {}
        @Override public List<KeyValue> get(byte[] key) { return Collections.emptyList(); }
        @Override public Iterator<KeyValue> scan(byte[] startKey, byte[] endKey) {
            return Collections.<KeyValue>emptyList().iterator();
        }
        @Override public Iterator<KeyValue> scan(StorageScanFilter filter) {
            return Collections.<KeyValue>emptyList().iterator();
        }
        @Override public void delete(byte[] key) {}
        @Override public void flush() {}
        @Override public void compact(boolean major) {}
        @Override public void close() {}
        @Override public void dropData() {}
        @Override public long estimateSizeBytes() { return 0; }
    }

    static class FakeEngineFactory implements StorageEngineFactory {
        @Override public StorageEngine create(String regionId) { return new FakeStorageEngine(); }
        @Override public void close() {}
    }

    static class FakeRegionServer extends RegionServer {
        private final ServerId serverId;

        FakeRegionServer() {
            super("localhost", 16020, new FakeEngineFactory(), null, 1, "./data/test-wal-split-conc");
            this.serverId = new ServerId("localhost", 16020);
        }

        @Override
        public ServerId getServerId() { return serverId; }
    }
}
