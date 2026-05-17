package com.minisql.stress;

import com.minisql.common.model.KeyValue;
import com.minisql.regionserver.RegionServer;
import com.minisql.regionserver.RegionStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class StressTestDriver {

    private static final Logger logger = LoggerFactory.getLogger(StressTestDriver.class);

    private final StressTestConfig config;
    private final RegionServer regionServer;
    private final String regionId;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    public StressTestDriver(StressTestConfig config, RegionServer regionServer, String regionId) {
        this.config = config;
        this.regionServer = regionServer;
        this.regionId = regionId;
    }

    public StressTestResult execute() throws InterruptedException {
        LatencyRecorder recorder = new LatencyRecorder();
        AtomicLong errors = new AtomicLong(0);
        AtomicLong totalOps = new AtomicLong(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        running.set(true);

        int threadCount = config.getThreadCount();
        executor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "Stress-Worker");
            t.setDaemon(true);
            return t;
        });

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    return;
                }

                while (running.get()) {
                    try {
                        long opStart = System.nanoTime();
                        performOperation(threadId, totalOps);
                        long durationMicros = (System.nanoTime() - opStart) / 1000;
                        recorder.record(durationMicros);
                        totalOps.incrementAndGet();
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }
            });
        }

        startLatch.countDown();

        // Let warmup run
        Thread.sleep(config.getWarmupSeconds() * 1000L);

        // Measure for the configured duration
        Thread.sleep(config.getDurationSeconds() * 1000L);

        running.set(false);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        long durationMs = System.currentTimeMillis() - startTime;

        StressTestResult result = new StressTestResult();
        result.totalOps = totalOps.get();
        result.durationMs = durationMs;
        result.throughputOpsPerSec = durationMs > 0 ? (totalOps.get() * 1000.0 / durationMs) : 0;
        result.p50Micros = recorder.getPercentile(0.50);
        result.p95Micros = recorder.getPercentile(0.95);
        result.p99Micros = recorder.getPercentile(0.99);
        result.minMicros = recorder.getMin();
        result.maxMicros = recorder.getMax();
        result.errorCount = errors.get();

        return result;
    }

    private void performOperation(int threadId, AtomicLong counter) throws Exception {
        RegionStorage storage = regionServer.getRegionManager().getRegionStorage(regionId);
        if (storage == null) return;

        int keyIndex = (int) (counter.get() % config.getKeySpaceSize());
        byte[] rowKey = ("stress-key-" + keyIndex).getBytes();
        byte[] value = new byte[config.getValueSizeBytes()];
        java.util.Arrays.fill(value, (byte) threadId);

        switch (config.getOperationType()) {
            case PUT: {
                KeyValue kv = new KeyValue();
                kv.setRowKey(rowKey);
                kv.setFamily("");
                kv.setQualifier("val");
                kv.setTimestamp(System.nanoTime());
                kv.setValue(value);
                kv.setType(KeyValue.Type.PUT);
                storage.put(kv);
                break;
            }
            case GET: {
                storage.get(rowKey);
                break;
            }
            case SCAN: {
                storage.scan(new byte[]{0x00}, new byte[]{(byte) 0xFF});
                break;
            }
            case MIXED: {
                int mode = keyIndex % 3;
                if (mode == 0) {
                    KeyValue kv = new KeyValue();
                    kv.setRowKey(rowKey);
                    kv.setFamily("");
                    kv.setQualifier("val");
                    kv.setTimestamp(System.nanoTime());
                    kv.setValue(value);
                    kv.setType(KeyValue.Type.PUT);
                    storage.put(kv);
                } else if (mode == 1) {
                    storage.get(rowKey);
                } else {
                    storage.scan(new byte[]{0x00}, new byte[]{(byte) 0x7F});
                }
                break;
            }
        }
    }

    public void shutdown() {
        running.set(false);
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
