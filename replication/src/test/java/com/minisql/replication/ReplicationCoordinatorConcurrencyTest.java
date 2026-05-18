package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicationCoordinator concurrency tests")
class ReplicationCoordinatorConcurrencyTest {

    // ------------------------------------------------------------------ helpers

    private KeyValue sampleMutation() {
        KeyValue kv = new KeyValue();
        kv.setRowKey(new byte[]{0x01});
        kv.setFamily("cf");
        kv.setQualifier("q");
        kv.setTimestamp(1L);
        kv.setValue(new byte[]{0x02});
        kv.setType(KeyValue.Type.PUT);
        return kv;
    }

    private ReplicationConfig testConfig() {
        return ReplicationConfig.builder(3)
            .ackTimeoutMs(200)
            .replicationTimeoutMs(500)
            .healthCheckIntervalMs(60000)
            .catchUpLagThreshold(100)
            .build();
    }

    // ------------------------------------------------------------------ fakes

    private static final class FakeTransportClient implements ReplicationTransportClient {
        private final Map<ServerId, Boolean> replicateResults = new java.util.concurrent.ConcurrentHashMap<>();
        private List<KeyValue> snapshot = Collections.emptyList();

        void setReplicateResult(ServerId replica, boolean success) {
            replicateResults.put(replica, success);
        }

        void setSnapshot(List<KeyValue> snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public boolean replicate(ServerId replica, String regionId, ReplicationLogEntry entry, long timeoutMs) {
            return replicateResults.getOrDefault(replica, true);
        }

        @Override
        public boolean replicateBatch(ServerId replica, String regionId, List<ReplicationLogEntry> entries, long timeoutMs) {
            return replicateResults.getOrDefault(replica, true);
        }

        @Override
        public List<KeyValue> fetchSnapshot(ServerId primary, String regionId, long timeoutMs) {
            return snapshot;
        }

        @Override
        public boolean streamSnapshotDirect(ServerId primary, ServerId replica, String regionId,
                                            int batchSize, long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public boolean sendSnapshot(ServerId replica, String regionId, List<KeyValue> snapshot,
                                    int batchSize, long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public boolean sendSnapshotStreaming(ServerId replica, String regionId, List<KeyValue> snapshot,
                                            int batchSize, long timeoutMs, long finalSequenceId) {
            return true;
        }

        @Override
        public void close() {
        }
    }

    private static final class FakeWal extends ReplicationWAL {
        private final AtomicLong sequence = new AtomicLong();
        private final Map<String, Long> appliedProgress = new java.util.concurrent.ConcurrentHashMap<>();
        private final List<ReplicationLogEntry> allEntries = Collections.synchronizedList(new ArrayList<>());

        private FakeWal() {
            super();
        }

        @Override
        public void initialize() {
        }

        @Override
        public long getCurrentSequenceId(String regionId) {
            return sequence.get();
        }

        @Override
        public ReplicationLogEntry append(String regionId, List<KeyValue> mutations) {
            ReplicationLogEntry entry = new ReplicationLogEntry(
                sequence.incrementAndGet(), System.currentTimeMillis(), mutations);
            allEntries.add(entry);
            return entry;
        }

        @Override
        public void markAsApplied(String regionId, long sequenceId, String replicaAddress) {
            String key = regionId + ":" + replicaAddress;
            appliedProgress.merge(key, sequenceId, Math::max);
        }

        @Override
        public long getAppliedProgress(String regionId, String replicaAddress) {
            return appliedProgress.getOrDefault(regionId + ":" + replicaAddress, 0L);
        }

        @Override
        public List<ReplicationLogEntry> getEntries(String regionId, long fromSequenceId) {
            List<ReplicationLogEntry> result = new ArrayList<>();
            for (ReplicationLogEntry entry : allEntries) {
                if (entry.getSequenceId() >= fromSequenceId) {
                    result.add(entry);
                }
            }
            return result;
        }

        @Override
        public void cleanup(String regionId, int maxRetention) {
        }

        @Override
        public void cleanup(String regionId, int maxRetention, long minConfirmedSeqId) {
        }

        @Override
        public void close() {
        }
    }

    // ------------------------------------------------------------------ tests

    @Test
    @DisplayName("8 threads each write 10 mutations to the same region; all 80 are recorded")
    void concurrentWritesToSameRegionAllReplicated() throws Exception {
        FakeWal wal = new FakeWal();
        FakeTransportClient transport = new FakeTransportClient();
        ReplicationCoordinator coordinator = new ReplicationCoordinator(testConfig(), wal, transport);
        coordinator.start();

        try {
            Region region = new Region("region-conc", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId secondary1 = new ServerId("secondary-1", 16021);
            ServerId secondary2 = new ServerId("secondary-2", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, secondary1, secondary2));

            int threadCount = 8;
            int writesPerThread = 10;
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            AtomicInteger errorCount = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            List<Runnable> tasks = new ArrayList<>();
            for (int t = 0; t < threadCount; t++) {
                final int threadIdx = t;
                tasks.add(() -> {
                    try {
                        barrier.await(5, TimeUnit.SECONDS);
                        for (int i = 0; i < writesPerThread; i++) {
                            KeyValue kv = new KeyValue();
                            kv.setRowKey(new byte[]{(byte) threadIdx, (byte) i});
                            kv.setFamily("cf");
                            kv.setQualifier("q");
                            kv.setTimestamp(System.currentTimeMillis());
                            kv.setValue(new byte[]{0x01});
                            kv.setType(KeyValue.Type.PUT);
                            boolean ok = coordinator.replicateSync(region.getRegionId(), List.of(kv));
                            if (!ok) {
                                errorCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    }
                });
            }

            executor.invokeAll(tasks.stream().map(Executors::callable).collect(Collectors.toList()));
            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

            assertEquals(0, errorCount.get(), "No write should have failed");
            assertEquals(threadCount * writesPerThread, wal.allEntries.size(),
                "All 80 mutations should be recorded in the WAL");
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("failover during ongoing writes does not corrupt coordinator state")
    void concurrentFailoverDuringWritesDoesNotCorruptState() throws Exception {
        FakeWal wal = new FakeWal();
        FakeTransportClient transport = new FakeTransportClient();
        ReplicationCoordinator coordinator = new ReplicationCoordinator(testConfig(), wal, transport);
        coordinator.start();

        try {
            Region region = new Region("region-fo", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId secondary1 = new ServerId("secondary-1", 16021);
            ServerId secondary2 = new ServerId("secondary-2", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, secondary1, secondary2));

            // Give secondaries different progress levels so failover picks the best one
            coordinator.getReplicaGroup(region.getRegionId())
                .updateReplicaState(secondary1, 5L, 0L);
            coordinator.getReplicaGroup(region.getRegionId())
                .updateReplicaState(secondary2, 10L, 0L);

            ServerId originalPrimary = coordinator.getReplicaGroup(region.getRegionId()).getPrimary();
            assertEquals(primary, originalPrimary);

            CountDownLatch writesStarted = new CountDownLatch(1);
            AtomicInteger writeErrors = new AtomicInteger(0);

            // Writer thread: loop 50 times, writing mutations
            Thread writer = new Thread(() -> {
                writesStarted.countDown();
                for (int i = 0; i < 50; i++) {
                    try {
                        KeyValue kv = new KeyValue();
                        kv.setRowKey(new byte[]{(byte) i});
                        kv.setFamily("cf");
                        kv.setQualifier("q");
                        kv.setTimestamp(System.currentTimeMillis());
                        kv.setValue(new byte[]{0x01});
                        kv.setType(KeyValue.Type.PUT);
                        coordinator.replicateSync(region.getRegionId(), List.of(kv));
                    } catch (Exception e) {
                        writeErrors.incrementAndGet();
                    }
                }
            }, "writer");

            writer.start();
            assertTrue(writesStarted.await(5, TimeUnit.SECONDS));

            // Sleep a little so some writes go through before failover
            Thread.sleep(10);
            coordinator.failover(region.getRegionId());

            writer.join(30_000);
            assertFalse(writer.isAlive(), "Writer thread should have completed");

            // Failover should have promoted a secondary
            ServerId newPrimary = coordinator.getReplicaGroup(region.getRegionId()).getPrimary();
            assertNotEquals(originalPrimary, newPrimary, "Primary should have changed after failover");
            assertNotNull(newPrimary, "New primary should be non-null");

            // Coordinator should still accept writes after failover
            KeyValue postFailover = new KeyValue();
            postFailover.setRowKey(new byte[]{(byte) 0xFF});
            postFailover.setFamily("cf");
            postFailover.setQualifier("q");
            postFailover.setTimestamp(System.currentTimeMillis());
            postFailover.setValue(new byte[]{0x01});
            postFailover.setType(KeyValue.Type.PUT);
            boolean ok = coordinator.replicateSync(region.getRegionId(), List.of(postFailover));
            assertTrue(ok, "Write should succeed after failover");
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("writes succeed in degraded mode when all secondaries are down")
    void degradedModeWritesSucceedWithAllSecondariesDown() throws Exception {
        FakeWal wal = new FakeWal();
        FakeTransportClient transport = new FakeTransportClient();
        ReplicationCoordinator coordinator = new ReplicationCoordinator(testConfig(), wal, transport);
        coordinator.start();

        try {
            Region region = new Region("region-deg", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId secondary1 = new ServerId("secondary-1", 16021);
            ServerId secondary2 = new ServerId("secondary-2", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, secondary1, secondary2));

            // All secondaries return false (unreachable)
            transport.setReplicateResult(secondary1, false);
            transport.setReplicateResult(secondary2, false);

            // Write 5 entries — primary-only write should still succeed (degraded mode)
            for (int i = 0; i < 5; i++) {
                KeyValue kv = new KeyValue();
                kv.setRowKey(new byte[]{(byte) i});
                kv.setFamily("cf");
                kv.setQualifier("q");
                kv.setTimestamp(System.currentTimeMillis());
                kv.setValue(new byte[]{0x01});
                kv.setType(KeyValue.Type.PUT);
                boolean ok = coordinator.replicateSync(region.getRegionId(), List.of(kv));
                assertTrue(ok, "replicateSync should return true in degraded mode (primary-only write)");
            }

            assertTrue(coordinator.isDegraded(region.getRegionId()),
                "Region should be marked as degraded when all secondaries are unreachable");
            assertEquals(5, wal.allEntries.size(), "All 5 writes should be recorded in WAL");
        } finally {
            coordinator.stop();
        }
    }

    @Test
    @DisplayName("concurrent health checks and writes complete without deadlock or CME")
    void concurrentHealthCheckAndWrite() throws Exception {
        FakeWal wal = new FakeWal();
        FakeTransportClient transport = new FakeTransportClient();

        // Use a short health-check interval so the scheduler fires repeatedly
        ReplicationConfig config = ReplicationConfig.builder(3)
            .ackTimeoutMs(200)
            .replicationTimeoutMs(500)
            .healthCheckIntervalMs(50)
            .catchUpLagThreshold(100)
            .build();

        ReplicationCoordinator coordinator = new ReplicationCoordinator(config, wal, transport);
        coordinator.start();

        try {
            Region region = new Region("region-hc", "orders", new byte[]{0x00}, new byte[]{0x7F});
            ServerId primary = new ServerId("primary", 16020);
            ServerId secondary1 = new ServerId("secondary-1", 16021);
            ServerId secondary2 = new ServerId("secondary-2", 16022);
            coordinator.createReplicaGroup(region, List.of(primary, secondary1, secondary2));

            int writeCount = 20;
            CountDownLatch ready = new CountDownLatch(1);
            AtomicInteger writeErrors = new AtomicInteger(0);

            // Writer thread
            Thread writer = new Thread(() -> {
                ready.countDown();
                for (int i = 0; i < writeCount; i++) {
                    try {
                        KeyValue kv = new KeyValue();
                        kv.setRowKey(new byte[]{(byte) i});
                        kv.setFamily("cf");
                        kv.setQualifier("q");
                        kv.setTimestamp(System.currentTimeMillis());
                        kv.setValue(new byte[]{0x01});
                        kv.setType(KeyValue.Type.PUT);
                        coordinator.replicateSync(region.getRegionId(), List.of(kv));
                    } catch (Exception e) {
                        writeErrors.incrementAndGet();
                    }
                }
            }, "writer");

            writer.start();
            assertTrue(ready.await(5, TimeUnit.SECONDS));

            // Concurrently trigger several synchronous health checks while writes are in flight
            int healthCheckCount = 10;
            Thread[] hcThreads = new Thread[healthCheckCount];
            for (int i = 0; i < healthCheckCount; i++) {
                hcThreads[i] = new Thread(() -> {
                    try {
                        coordinator.triggerHealthCheckNow();
                    } catch (Exception e) {
                        // Swallow — we only care about the writer thread
                    }
                }, "health-check-" + i);
                hcThreads[i].start();
            }

            // Wait for all health-check threads
            for (Thread t : hcThreads) {
                t.join(10_000);
            }

            // Wait for writer to complete (no deadlock)
            writer.join(30_000);
            assertFalse(writer.isAlive(), "Writer thread should have finished without deadlock");

            assertEquals(0, writeErrors.get(), "No write errors should have occurred");
            assertEquals(writeCount, wal.allEntries.size(),
                "All 20 writes should be recorded in the WAL");
        } finally {
            coordinator.stop();
        }
    }
}
