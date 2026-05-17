package com.minisql.regionserver;

import com.google.protobuf.ByteString;
import com.minisql.common.model.Region;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.storage.RocksDBConfig;
import com.minisql.storage.RocksDBEngineFactory;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionServerServiceImpl concurrency tests")
class RegionServerServiceImplConcurrencyTest {

    private RegionServer regionServer;
    private RegionServerServiceImpl service;

    @TempDir
    Path tempDir;

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
        service = new RegionServerServiceImpl(regionServer);
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

    private RegionServerProto.PutRequest buildPutRequest(String regionId, String rowKey, String value) {
        return RegionServerProto.PutRequest.newBuilder()
            .setRegionId(regionId)
            .addKeyValues(CommonProto.KeyValue.newBuilder()
                .setRowKey(ByteString.copyFromUtf8(rowKey))
                .setColumnFamily("")
                .setQualifier("name")
                .setValue(ByteString.copyFromUtf8(value))
                .setType(CommonProto.KeyValueType.PUT)
                .setTimestamp(System.nanoTime())
                .build())
            .build();
    }

    private RegionServerProto.GetRequest buildGetRequest(String regionId, String rowKey) {
        return RegionServerProto.GetRequest.newBuilder()
            .setRegionId(regionId)
            .setRowKey(ByteString.copyFromUtf8(rowKey))
            .build();
    }

    // ================================
    // Tests
    // ================================

    @Test
    @DisplayName("concurrent put and get requests from multiple threads complete without errors")
    void concurrentPutAndGetRequests() throws Exception {
        String regionId = "conc-put-get";
        openTestRegion(regionId);

        int threadCount = 4;
        int rowsPerThread = 25;
        int totalRows = threadCount * rowsPerThread;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch putDoneLatch = new CountDownLatch(threadCount);
        CountDownLatch getDoneLatch = new CountDownLatch(threadCount);
        AtomicInteger putErrors = new AtomicInteger(0);
        AtomicInteger getErrors = new AtomicInteger(0);

        // 4 put threads, each inserts 25 unique rows
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int r = 0; r < rowsPerThread; r++) {
                        String rowKey = "put-" + threadId + "-" + r;
                        RegionServerProto.PutRequest req = buildPutRequest(regionId, rowKey, "value-" + threadId + "-" + r);
                        CapturingObserver<RegionServerProto.PutResponse> obs = new CapturingObserver<>();
                        service.put(req, obs);
                        if (obs.error != null || obs.value == null || !obs.value.getStatus().getSuccess()) {
                            putErrors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    putErrors.incrementAndGet();
                } finally {
                    putDoneLatch.countDown();
                }
            }, "put-thread-" + t).start();
        }

        // 4 get threads, each reads 25 rows (reads happen concurrently with writes;
        // a row might not exist yet, so we only count unexpected errors, not missing data)
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int r = 0; r < rowsPerThread; r++) {
                        String rowKey = "put-" + threadId + "-" + r;
                        RegionServerProto.GetRequest req = buildGetRequest(regionId, rowKey);
                        CapturingObserver<RegionServerProto.GetResponse> obs = new CapturingObserver<>();
                        service.get(req, obs);
                        if (obs.error != null) {
                            getErrors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    getErrors.incrementAndGet();
                } finally {
                    getDoneLatch.countDown();
                }
            }, "get-thread-" + t).start();
        }

        startLatch.countDown();
        assertTrue(putDoneLatch.await(30, TimeUnit.SECONDS), "All put threads should complete within timeout");
        assertTrue(getDoneLatch.await(30, TimeUnit.SECONDS), "All get threads should complete within timeout");

        assertEquals(0, putErrors.get(), "No put requests should encounter errors");
        assertEquals(0, getErrors.get(), "No get requests should encounter errors");

        // Verify total rows inserted by scanning
        RegionServerProto.ScanRequest scanReq = RegionServerProto.ScanRequest.newBuilder()
            .setRegionId(regionId)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.copyFrom(new byte[]{(byte) 0xFF}))
            .build();
        CapturingObserver<RegionServerProto.ScanResponse> scanObs = new CapturingObserver<>();
        service.scan(scanReq, scanObs);

        assertNotNull(scanObs.value, "Scan response should not be null");
        assertTrue(scanObs.value.getStatus().getSuccess(), "Scan should succeed");
        assertEquals(totalRows, scanObs.value.getKeyValuesCount(),
            "Total rows inserted should equal " + totalRows);
    }

    @Test
    @DisplayName("concurrent scans during writes do not crash or throw ConcurrentModificationException")
    void concurrentScanDuringWrites() throws Exception {
        String regionId = "conc-scan-write";
        openTestRegion(regionId);

        // Insert 10 initial rows
        for (int i = 0; i < 10; i++) {
            String rowKey = "init-" + i;
            RegionServerProto.PutRequest req = buildPutRequest(regionId, rowKey, "init-value-" + i);
            CapturingObserver<RegionServerProto.PutResponse> obs = new CapturingObserver<>();
            service.put(req, obs);
            assertTrue(obs.value.getStatus().getSuccess(), "Initial put should succeed for row " + rowKey);
        }

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch scanDoneLatch = new CountDownLatch(1);
        CountDownLatch writeDoneLatch = new CountDownLatch(4);
        AtomicInteger scanErrors = new AtomicInteger(0);
        AtomicInteger writeErrors = new AtomicInteger(0);
        AtomicInteger concurrentModificationCount = new AtomicInteger(0);
        AtomicInteger successfulScans = new AtomicInteger(0);

        // 1 scan thread: does 50 scans
        new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 50; i++) {
                    try {
                        RegionServerProto.ScanRequest req = RegionServerProto.ScanRequest.newBuilder()
                            .setRegionId(regionId)
                            .setStartKey(ByteString.EMPTY)
                            .setEndKey(ByteString.copyFrom(new byte[]{(byte) 0xFF}))
                            .build();
                        CapturingObserver<RegionServerProto.ScanResponse> obs = new CapturingObserver<>();
                        service.scan(req, obs);
                        if (obs.error != null) {
                            if (obs.error instanceof java.util.ConcurrentModificationException) {
                                concurrentModificationCount.incrementAndGet();
                            }
                            scanErrors.incrementAndGet();
                        } else if (obs.value != null && obs.value.getStatus().getSuccess()) {
                            successfulScans.incrementAndGet();
                        }
                    } catch (java.util.ConcurrentModificationException e) {
                        concurrentModificationCount.incrementAndGet();
                        scanErrors.incrementAndGet();
                    } catch (Exception e) {
                        scanErrors.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                scanErrors.incrementAndGet();
            } finally {
                scanDoneLatch.countDown();
            }
        }, "scan-thread").start();

        // 4 write threads, each writes 25 rows
        for (int t = 0; t < 4; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int r = 0; r < 25; r++) {
                        String rowKey = "write-" + threadId + "-" + r;
                        RegionServerProto.PutRequest req = buildPutRequest(regionId, rowKey, "write-value-" + threadId + "-" + r);
                        CapturingObserver<RegionServerProto.PutResponse> obs = new CapturingObserver<>();
                        service.put(req, obs);
                        if (obs.error != null || obs.value == null || !obs.value.getStatus().getSuccess()) {
                            writeErrors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    writeErrors.incrementAndGet();
                } finally {
                    writeDoneLatch.countDown();
                }
            }, "write-thread-" + t).start();
        }

        startLatch.countDown();
        assertTrue(scanDoneLatch.await(30, TimeUnit.SECONDS), "Scan thread should complete within timeout");
        assertTrue(writeDoneLatch.await(30, TimeUnit.SECONDS), "All write threads should complete within timeout");

        assertEquals(0, concurrentModificationCount.get(),
            "No ConcurrentModificationException should occur during concurrent scans and writes");
        assertEquals(0, scanErrors.get(),
            "Scan thread should complete without errors");
        assertEquals(0, writeErrors.get(),
            "Write threads should complete without errors");
        assertTrue(successfulScans.get() > 0,
            "At least some scans should succeed");
    }

    // ================================
    // Helper
    // ================================

    private static final class CapturingObserver<T> implements StreamObserver<T> {
        T value;
        Throwable error;
        boolean completed;

        @Override
        public void onNext(T value) { this.value = value; }
        @Override
        public void onError(Throwable t) { this.error = t; }
        @Override
        public void onCompleted() { this.completed = true; }
    }
}
