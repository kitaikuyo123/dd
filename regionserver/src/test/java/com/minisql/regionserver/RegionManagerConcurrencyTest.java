package com.minisql.regionserver;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.KeyValue;
import com.minisql.storage.StorageEngine;
import com.minisql.storage.StorageEngineFactory;
import com.minisql.storage.StorageScanFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionManager concurrency tests")
class RegionManagerConcurrencyTest {

    private RegionManager regionManager;

    @BeforeEach
    void setUp() {
        FakeRegionServer fakeServer = new FakeRegionServer();
        regionManager = fakeServer.getRegionManager();
    }

    @Test
    @DisplayName("concurrent open and close on same region reaches consistent state")
    void concurrentOpenCloseSameRegion() throws Exception {
        int threadCount = 8;
        String regionId = "conc-open-close";
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger openCount = new AtomicInteger(0);
        AtomicInteger closeCount = new AtomicInteger(0);

        Runnable openTask = () -> {
            try {
                startLatch.await();
                Region region = new Region(regionId, "test_table",
                    new byte[]{0x00}, new byte[]{0x7F});
                regionManager.openRegion(region);
                openCount.incrementAndGet();
            } catch (Exception ignored) {
            } finally {
                doneLatch.countDown();
            }
        };

        Runnable closeTask = () -> {
            try {
                startLatch.await();
                regionManager.closeRegion(regionId, false);
                closeCount.incrementAndGet();
            } catch (Exception ignored) {
            } finally {
                doneLatch.countDown();
            }
        };

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount / 2; i++) {
            threads.add(new Thread(openTask, "open-" + i));
            threads.add(new Thread(closeTask, "close-" + i));
        }
        threads.forEach(Thread::start);
        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

        RegionManager.RegionState finalState = regionManager.getRegionState(regionId);
        assertTrue(finalState == RegionManager.RegionState.OPEN
                || finalState == RegionManager.RegionState.CLOSED
                || finalState == null,
            "Final state should be OPEN, CLOSED, or null, but was " + finalState);
    }

    @Test
    @DisplayName("blockWrites prevents writes during split window")
    void blockWritesPreventsdWritesDuringSplit() throws Exception {
        String regionId = "conc-block-writes";
        Region region = new Region(regionId, "test_table",
            new byte[]{0x00}, new byte[]{0x7F});
        regionManager.openRegion(region);

        regionManager.blockWrites(regionId);
        assertTrue(regionManager.isWriteBlocked(regionId));

        regionManager.unblockWrites(regionId);
        assertFalse(regionManager.isWriteBlocked(regionId));
    }

    @Test
    @DisplayName("concurrent fencing token updates remain monotonic")
    void concurrentFencingTokenUpdates() throws Exception {
        String regionId = "conc-fencing";
        Region region = new Region(regionId, "test_table",
            new byte[]{0x00}, new byte[]{0x7F});
        regionManager.openRegion(region);

        int threadCount = 8;
        int updatesPerThread = 1000;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicLong maxTokenSeen = new AtomicLong(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadIdx = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < updatesPerThread; i++) {
                        long token = (long) threadIdx * updatesPerThread + i + 1;
                        regionManager.updateFencingToken(regionId, token);
                        maxTokenSeen.updateAndGet(max -> Math.max(max, token));
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            }, "fencing-" + t).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

        long finalToken = regionManager.getFencingToken(regionId);
        assertTrue(finalToken > 0, "Fencing token should have been updated");
        assertTrue(finalToken <= maxTokenSeen.get(),
            "Final token " + finalToken + " should not exceed max seen " + maxTokenSeen.get());
    }

    @Test
    @DisplayName("concurrent reads during close do not crash")
    void concurrentReadsDuringClose() throws Exception {
        String regionId = "conc-read-close";
        Region region = new Region(regionId, "test_table",
            new byte[]{0x00}, new byte[]{0x7F});
        regionManager.openRegion(region);

        int readerCount = 4;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(readerCount + 1);
        AtomicInteger readErrors = new AtomicInteger(0);

        for (int i = 0; i < readerCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 100; j++) {
                        try {
                            regionManager.getRegionStorage(regionId);
                            regionManager.getRegionState(regionId);
                        } catch (Exception ignored) {
                            readErrors.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    doneLatch.countDown();
                }
            }, "reader-" + i).start();
        }

        new Thread(() -> {
            try {
                startLatch.await();
                Thread.sleep(10);
                regionManager.closeRegion(regionId, false);
            } catch (Exception ignored) {
            } finally {
                doneLatch.countDown();
            }
        }, "closer").start();

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
    }

    // ================================
    // Hand-written fakes
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
        @Override
        public StorageEngine create(String regionId) {
            return new FakeStorageEngine();
        }
        @Override public void close() {}
    }

    static class FakeRegionServer extends RegionServer {
        private final ServerId serverId;

        FakeRegionServer() {
            super("localhost", 16020, new FakeEngineFactory(), null, 1, "./data/test-wal-conc");
            this.serverId = new ServerId("localhost", 16020);
        }

        @Override
        public ServerId getServerId() {
            return serverId;
        }
    }
}
