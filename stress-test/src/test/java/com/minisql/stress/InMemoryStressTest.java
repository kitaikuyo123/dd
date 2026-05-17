package com.minisql.stress;

import com.minisql.common.model.KeyValue;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("In-Memory Stress Test")
class InMemoryStressTest {

    /** Simple in-memory StorageEngine for stress testing without RocksDB overhead */
    private static class InMemoryEngine {
        private final ConcurrentHashMap<String, List<KeyValue>> data = new ConcurrentHashMap<>();
        private volatile boolean closed = false;

        void put(byte[] key, KeyValue kv) {
            if (closed) throw new RuntimeException("closed");
            data.compute(new String(key), (k, existing) -> {
                List<KeyValue> list = existing != null ? new ArrayList<>(existing) : new ArrayList<>();
                list.add(kv);
                return Collections.unmodifiableList(list);
            });
        }

        List<KeyValue> get(byte[] key) {
            if (closed) throw new RuntimeException("closed");
            return data.getOrDefault(new String(key), Collections.emptyList());
        }

        long scan(byte[] startKey, byte[] endKey) {
            if (closed) throw new RuntimeException("closed");
            String start = new String(startKey);
            String end = new String(endKey);
            return data.entrySet().stream()
                .filter(e -> e.getKey().compareTo(start) >= 0 && e.getKey().compareTo(end) < 0)
                .count();
        }

        int size() { return data.size(); }

        void close() { closed = true; }
    }

    private InMemoryEngine engine;
    private LatencyRecorder recorder;

    @BeforeEach
    void setUp() {
        engine = new InMemoryEngine();
        recorder = new LatencyRecorder();
    }

    @AfterEach
    void tearDown() {
        if (engine != null) engine.close();
    }

    @Nested
    @DisplayName("PUT throughput")
    class PutThroughput {

        @Test
        @DisplayName("8 threads x 100k puts completes with >10k ops/sec")
        void putThroughput() throws Exception {
            int threads = 8;
            int opsPerThread = 100_000;
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
                            byte[] key = ("key-" + tid + "-" + i).getBytes();
                            KeyValue kv = new KeyValue();
                            kv.setRowKey(key);
                            kv.setFamily("");
                            kv.setQualifier("v");
                            kv.setTimestamp(System.nanoTime());
                            kv.setValue(new byte[64]);
                            kv.setType(KeyValue.Type.PUT);
                            engine.put(key, kv);
                            recorder.record((System.nanoTime() - s) / 1000);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "Put-" + tid).start();
            }

            long wallStart = System.nanoTime();
            start.countDown();
            done.await(60, TimeUnit.SECONDS);
            long wallMicros = (System.nanoTime() - wallStart) / 1000;

            assertEquals(0, errors.get(), "no errors expected");
            assertTrue(recorder.getCount() >= threads * opsPerThread * 0.99, "should complete >99% of ops");
            double throughput = recorder.getCount() * 1_000_000.0 / wallMicros;
            assertTrue(throughput > 10_000, "throughput should exceed 10k ops/sec, got " + throughput);
            System.out.printf("[PUT] %.0f ops/sec | p50=%.0fus p99=%.0fus | errors=%d%n",
                throughput, recorder.getPercentile(0.50), recorder.getPercentile(0.99), errors.get());
        }
    }

    @Nested
    @DisplayName("GET throughput")
    class GetThroughput {

        @Test
        @DisplayName("pre-populate 50k keys, 8 threads read back")
        void getThroughput() throws Exception {
            int keyCount = 50_000;
            int threads = 8;
            int readsPerThread = 50_000;

            // Pre-populate
            for (int i = 0; i < keyCount; i++) {
                byte[] key = ("key-" + i).getBytes();
                KeyValue kv = new KeyValue();
                kv.setRowKey(key);
                kv.setFamily("");
                kv.setQualifier("v");
                kv.setTimestamp(i);
                kv.setValue(new byte[64]);
                kv.setType(KeyValue.Type.PUT);
                engine.put(key, kv);
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
                            List<KeyValue> result = engine.get(("key-" + ki).getBytes());
                            recorder.record((System.nanoTime() - s) / 1000);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "Get-" + tid).start();
            }

            long wallStart = System.nanoTime();
            start.countDown();
            done.await(60, TimeUnit.SECONDS);
            long wallMicros = (System.nanoTime() - wallStart) / 1000;

            assertEquals(0, errors.get());
            double throughput = recorder.getCount() * 1_000_000.0 / wallMicros;
            assertTrue(throughput > 10_000, "throughput should exceed 10k ops/sec, got " + throughput);
            System.out.printf("[GET] %.0f ops/sec | p50=%.0fus p99=%.0fus | errors=%d%n",
                throughput, recorder.getPercentile(0.50), recorder.getPercentile(0.99), errors.get());
        }
    }

    @Nested
    @DisplayName("MIXED workload")
    class MixedWorkload {

        @Test
        @DisplayName("8 threads 50/30/20 put/get/scan mix")
        void mixedWorkload() throws Exception {
            int threads = 8;
            int opsPerThread = 50_000;
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
                                KeyValue kv = new KeyValue();
                                kv.setRowKey(key); kv.setFamily(""); kv.setQualifier("v");
                                kv.setTimestamp(System.nanoTime());
                                kv.setValue(new byte[64]); kv.setType(KeyValue.Type.PUT);
                                engine.put(key, kv);
                            } else if (mode < 8) {
                                engine.get(("mkey-" + tid + "-" + (i % 1000)).getBytes());
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
                }, "Mix-" + tid).start();
            }

            long wallStart = System.nanoTime();
            start.countDown();
            done.await(60, TimeUnit.SECONDS);
            long wallMicros = (System.nanoTime() - wallStart) / 1000;

            assertEquals(0, errors.get());
            double throughput = recorder.getCount() * 1_000_000.0 / wallMicros;
            assertTrue(throughput > 5_000, "mixed throughput should exceed 5k ops/sec, got " + throughput);
            System.out.printf("[MIXED] %.0f ops/sec | p50=%.0fus p99=%.0fus | errors=%d%n",
                throughput, recorder.getPercentile(0.50), recorder.getPercentile(0.99), errors.get());
        }
    }

    @Nested
    @DisplayName("SCAN latency")
    class ScanLatency {

        @Test
        @DisplayName("scan 10k keys latency < 50ms p99")
        void scanLatency() throws Exception {
            int keyCount = 10_000;
            for (int i = 0; i < keyCount; i++) {
                byte[] key = String.format("scan-key-%05d", i).getBytes();
                KeyValue kv = new KeyValue();
                kv.setRowKey(key); kv.setFamily(""); kv.setQualifier("v");
                kv.setTimestamp(i); kv.setValue(new byte[64]); kv.setType(KeyValue.Type.PUT);
                engine.put(key, kv);
            }

            LatencyRecorder scanRecorder = new LatencyRecorder();
            int scans = 100;
            for (int i = 0; i < scans; i++) {
                long s = System.nanoTime();
                long count = engine.scan("scan-key-00000".getBytes(), "scan-key-10000".getBytes());
                scanRecorder.record((System.nanoTime() - s) / 1000);
                assertEquals(keyCount, count);
            }

            double p99 = scanRecorder.getPercentile(0.99);
            System.out.printf("[SCAN] p50=%.0fus p99=%.0fus min=%d max=%d%n",
                scanRecorder.getPercentile(0.50), p99,
                scanRecorder.getMin(), scanRecorder.getMax());
            assertTrue(p99 < 50_000, "p99 scan latency should be < 50ms, got " + p99 + "us");
        }
    }

    @Nested
    @DisplayName("Concurrent clients")
    class ConcurrentClients {

        @Test
        @DisplayName("16 threads concurrent put+get with shared key space")
        void concurrentClientsStress() throws Exception {
            int threads = 16;
            int opsPerThread = 20_000;
            int keySpace = 1000;

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicLong errors = new AtomicLong(0);
            AtomicLong puts = new AtomicLong(0);
            AtomicLong gets = new AtomicLong(0);

            for (int t = 0; t < threads; t++) {
                final int tid = t;
                new Thread(() -> {
                    try { start.await(); } catch (InterruptedException e) { return; }
                    try {
                        Random rng = new Random(tid * 31L);
                        for (int i = 0; i < opsPerThread; i++) {
                            int ki = rng.nextInt(keySpace);
                            byte[] key = ("ckey-" + ki).getBytes();
                            if (rng.nextBoolean()) {
                                KeyValue kv = new KeyValue();
                                kv.setRowKey(key); kv.setFamily(""); kv.setQualifier("v");
                                kv.setTimestamp(System.nanoTime());
                                kv.setValue(new byte[32]); kv.setType(KeyValue.Type.PUT);
                                engine.put(key, kv);
                                puts.incrementAndGet();
                            } else {
                                engine.get(key);
                                gets.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }, "Client-" + tid).start();
            }

            start.countDown();
            assertTrue(done.await(60, TimeUnit.SECONDS), "should complete within 60s");

            assertEquals(0, errors.get());
            assertTrue(puts.get() > 0);
            assertTrue(gets.get() > 0);
            System.out.printf("[CONCURRENT] puts=%d gets=%d errors=%d total=%d%n",
                puts.get(), gets.get(), errors.get(), puts.get() + gets.get());
        }
    }
}
