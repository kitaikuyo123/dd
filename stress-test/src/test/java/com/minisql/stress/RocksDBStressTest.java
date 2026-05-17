package com.minisql.stress;

import com.minisql.common.model.KeyValue;
import com.minisql.storage.RocksDBConfig;
import com.minisql.storage.RocksDBStorageEngine;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RocksDB Stress Test")
class RocksDBStressTest {

    private RocksDBStorageEngine engine;
    private LatencyRecorder recorder;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        RocksDBConfig config = RocksDBConfig.builder(tempDir.toString())
            .enableWal(false)
            .build();
        engine = new RocksDBStorageEngine(config, "stress-region");
        recorder = new LatencyRecorder();
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            try { engine.close(); } catch (Exception ignored) {}
        }
    }

    private KeyValue makeKv(byte[] key, int valueSize) {
        KeyValue kv = new KeyValue();
        kv.setRowKey(key);
        kv.setFamily("");
        kv.setQualifier("v");
        kv.setTimestamp(System.nanoTime());
        kv.setValue(new byte[valueSize]);
        Arrays.fill(kv.getValue(), (byte) 0x42);
        kv.setType(KeyValue.Type.PUT);
        return kv;
    }

    @Nested
    @DisplayName("PUT throughput")
    class PutThroughput {

        @Test
        @DisplayName("4 threads x 50k puts against RocksDB")
        void putThroughput() throws Exception {
            int threads = 4;
            int opsPerThread = 50_000;

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicLong errors = new AtomicLong(0);

            for (int t = 0; t < threads; t++) {
                final int tid = t;
                new Thread(() -> {
                    try { start.await(); } catch (InterruptedException e) { return; }
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            long s = System.nanoTime();
                            byte[] key = ("rkey-" + tid + "-" + i).getBytes();
                            engine.put(key, makeKv(key, 64));
                            recorder.record((System.nanoTime() - s) / 1000);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "RPut-" + tid).start();
            }

            long wallStart = System.nanoTime();
            start.countDown();
            done.await(120, TimeUnit.SECONDS);
            long wallMicros = (System.nanoTime() - wallStart) / 1000;

            assertEquals(0, errors.get(), "no errors expected");
            double throughput = recorder.getCount() * 1_000_000.0 / wallMicros;
            assertTrue(throughput > 1_000, "throughput should exceed 1k ops/sec, got " + throughput);
            System.out.printf("[ROCKS PUT] %.0f ops/sec | p50=%.0fus p99=%.0fus | errors=%d%n",
                throughput, recorder.getPercentile(0.50), recorder.getPercentile(0.99), errors.get());
        }
    }

    @Nested
    @DisplayName("GET throughput")
    class GetThroughput {

        @Test
        @DisplayName("pre-populate 20k keys, 4 threads read back")
        void getThroughput() throws Exception {
            int keyCount = 20_000;
            int threads = 4;
            int readsPerThread = 20_000;

            for (int i = 0; i < keyCount; i++) {
                byte[] key = ("gkey-" + i).getBytes();
                engine.put(key, makeKv(key, 64));
            }

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicLong errors = new AtomicLong(0);

            for (int t = 0; t < threads; t++) {
                final int tid = t;
                new Thread(() -> {
                    try { start.await(); } catch (InterruptedException e) { return; }
                    try {
                        Random rng = new Random(tid);
                        for (int i = 0; i < readsPerThread; i++) {
                            long s = System.nanoTime();
                            int ki = rng.nextInt(keyCount);
                            engine.get(("gkey-" + ki).getBytes());
                            recorder.record((System.nanoTime() - s) / 1000);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "RGet-" + tid).start();
            }

            long wallStart = System.nanoTime();
            start.countDown();
            done.await(120, TimeUnit.SECONDS);
            long wallMicros = (System.nanoTime() - wallStart) / 1000;

            assertEquals(0, errors.get());
            double throughput = recorder.getCount() * 1_000_000.0 / wallMicros;
            assertTrue(throughput > 2_000, "throughput should exceed 2k ops/sec, got " + throughput);
            System.out.printf("[ROCKS GET] %.0f ops/sec | p50=%.0fus p99=%.0fus | errors=%d%n",
                throughput, recorder.getPercentile(0.50), recorder.getPercentile(0.99), errors.get());
        }
    }

    @Nested
    @DisplayName("MIXED workload")
    class MixedWorkload {

        @Test
        @DisplayName("4 threads mixed put/get/scan against RocksDB")
        void mixedWorkload() throws Exception {
            int threads = 4;
            int opsPerThread = 20_000;
            AtomicLong counter = new AtomicLong(0);

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicLong errors = new AtomicLong(0);

            for (int t = 0; t < threads; t++) {
                final int tid = t;
                new Thread(() -> {
                    try { start.await(); } catch (InterruptedException e) { return; }
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            long s = System.nanoTime();
                            int mode = (int) (counter.getAndIncrement() % 10);
                            if (mode < 5) {
                                byte[] key = ("mkey-" + tid + "-" + i).getBytes();
                                engine.put(key, makeKv(key, 64));
                            } else if (mode < 8) {
                                engine.get(("mkey-" + tid + "-" + (i % 500)).getBytes());
                            } else {
                                engine.scan("mkey-0-0".getBytes(), "mkey-z".getBytes());
                            }
                            recorder.record((System.nanoTime() - s) / 1000);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "RMix-" + tid).start();
            }

            long wallStart = System.nanoTime();
            start.countDown();
            done.await(120, TimeUnit.SECONDS);
            long wallMicros = (System.nanoTime() - wallStart) / 1000;

            assertEquals(0, errors.get());
            double throughput = recorder.getCount() * 1_000_000.0 / wallMicros;
            assertTrue(throughput > 500, "mixed throughput should exceed 500 ops/sec, got " + throughput);
            System.out.printf("[ROCKS MIXED] %.0f ops/sec | p50=%.0fus p99=%.0fus | errors=%d%n",
                throughput, recorder.getPercentile(0.50), recorder.getPercentile(0.99), errors.get());
        }
    }

    @Nested
    @DisplayName("SCAN latency")
    class ScanLatency {

        @Test
        @DisplayName("scan 5k keys latency p99 < 100ms")
        void scanLatency() throws Exception {
            int keyCount = 5_000;
            for (int i = 0; i < keyCount; i++) {
                byte[] key = String.format("skey-%05d", i).getBytes();
                engine.put(key, makeKv(key, 64));
            }

            LatencyRecorder scanRecorder = new LatencyRecorder();
            int scans = 50;
            for (int i = 0; i < scans; i++) {
                long s = System.nanoTime();
                var it = engine.scan("skey-00000".getBytes(), "skey-10000".getBytes());
                long count = 0;
                while (it.hasNext()) { it.next(); count++; }
                scanRecorder.record((System.nanoTime() - s) / 1000);
                assertEquals(keyCount, count);
            }

            double p99 = scanRecorder.getPercentile(0.99);
            System.out.printf("[ROCKS SCAN] p50=%.0fus p99=%.0fus min=%d max=%d%n",
                scanRecorder.getPercentile(0.50), p99,
                scanRecorder.getMin(), scanRecorder.getMax());
            assertTrue(p99 < 100_000, "p99 scan latency should be < 100ms, got " + p99 + "us");
        }
    }

    @Nested
    @DisplayName("Batch PUT")
    class BatchPut {

        @Test
        @DisplayName("batchPut 1000 entries x 100 batches")
        void batchPutStress() throws Exception {
            int batchSize = 1000;
            int batches = 100;
            LatencyRecorder batchRecorder = new LatencyRecorder();

            for (int b = 0; b < batches; b++) {
                List<KeyValue> kvs = new ArrayList<>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    byte[] key = ("bkey-" + b + "-" + i).getBytes();
                    kvs.add(makeKv(key, 64));
                }
                long s = System.nanoTime();
                engine.batchPut(kvs);
                batchRecorder.record((System.nanoTime() - s) / 1000);
            }

            double p99 = batchRecorder.getPercentile(0.99);
            System.out.printf("[ROCKS BATCH] p50=%.0fus p99=%.0fus min=%d max=%d count=%d%n",
                batchRecorder.getPercentile(0.50), p99,
                batchRecorder.getMin(), batchRecorder.getMax(), batchRecorder.getCount());
            assertTrue(batchRecorder.getCount() == batches);
        }
    }

    @Nested
    @DisplayName("Concurrent put+get")
    class ConcurrentPutGet {

        @Test
        @DisplayName("8 writers + 8 readers concurrent on same key space")
        void concurrentClientsStress() throws Exception {
            int writers = 8;
            int readers = 8;
            int opsPerThread = 10_000;
            int keySpace = 500;

            // Pre-populate
            for (int i = 0; i < keySpace; i++) {
                byte[] key = ("ckey-" + i).getBytes();
                engine.put(key, makeKv(key, 64));
            }

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(writers + readers);
            AtomicLong errors = new AtomicLong(0);

            for (int t = 0; t < writers; t++) {
                final int tid = t;
                new Thread(() -> {
                    try { start.await(); } catch (InterruptedException e) { return; }
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            byte[] key = ("ckey-" + (i % keySpace)).getBytes();
                            engine.put(key, makeKv(key, 64));
                            recorder.record(0);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "Writer-" + tid).start();
            }

            for (int t = 0; t < readers; t++) {
                final int tid = t;
                new Thread(() -> {
                    try { start.await(); } catch (InterruptedException e) { return; }
                    try {
                        Random rng = new Random(tid * 17L);
                        for (int i = 0; i < opsPerThread; i++) {
                            int ki = rng.nextInt(keySpace);
                            engine.get(("ckey-" + ki).getBytes());
                            recorder.record(0);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "Reader-" + tid).start();
            }

            start.countDown();
            assertTrue(done.await(120, TimeUnit.SECONDS), "should complete within 120s");

            assertEquals(0, errors.get());
            long totalOps = (long) (writers + readers) * opsPerThread;
            assertTrue(recorder.getCount() >= totalOps * 0.99);
            System.out.printf("[ROCKS CONCURRENT] total=%d errors=%d%n", recorder.getCount(), errors.get());
        }
    }
}
