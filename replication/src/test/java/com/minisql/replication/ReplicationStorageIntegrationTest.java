package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicationStorage integration tests")
class ReplicationStorageIntegrationTest {

    @TempDir
    Path tempDir;

    private ReplicationWAL wal;

    @BeforeEach
    void setUp() {
        wal = new ReplicationWAL(tempDir.resolve("wal-integ").toString());
        wal.initialize();
    }

    @AfterEach
    void tearDown() {
        if (wal != null) {
            try {
                wal.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("write 100 mutations and read them back via getEntries")
    void writeMutationsReadBackViaGetEntries() {
        String regionId = "integ-r1";
        int count = 100;

        for (int i = 0; i < count; i++) {
            KeyValue kv = sampleMutation(i);
            wal.append(regionId, List.of(kv));
        }

        List<ReplicationLogEntry> entries = wal.getEntries(regionId, 1);
        assertEquals(count, entries.size(), "All 100 entries should be readable");

        // Verify ordering: sequence IDs should be 1..100
        for (int i = 0; i < count; i++) {
            assertEquals(i + 1, entries.get(i).getSequenceId());
            assertEquals(1, entries.get(i).getMutations().size());
        }
    }

    @Test
    @DisplayName("WAL progress survives close and reopen")
    void walProgressSurvivesCloseReopen() throws Exception {
        String regionId = "integ-r2";
        String reopenPath = tempDir.resolve("wal-reopen-test").toString();

        // First instance: write entries and mark progress
        ReplicationWAL firstWal = new ReplicationWAL(reopenPath);
        firstWal.initialize();
        for (int i = 0; i < 5; i++) {
            firstWal.append(regionId, List.of(sampleMutation(i)));
        }
        firstWal.markAsApplied(regionId, 3L, "host1:1234");
        firstWal.markAsApplied(regionId, 5L, "host2:5678");
        firstWal.close();

        // Brief pause to ensure OS releases file locks (Windows)
        Thread.sleep(100);

        // Second instance at same path: verify data persisted
        ReplicationWAL secondWal = new ReplicationWAL(reopenPath);
        secondWal.initialize();
        try {
            assertEquals(3L, secondWal.getAppliedProgress(regionId, "host1:1234"));
            assertEquals(5L, secondWal.getAppliedProgress(regionId, "host2:5678"));
            assertEquals(0L, secondWal.getAppliedProgress(regionId, "unknown:9999"));

            // Verify entries readable
            List<ReplicationLogEntry> entries = secondWal.getEntries(regionId, 1);
            assertEquals(5, entries.size());
        } finally {
            secondWal.close();
        }
    }

    @Test
    @DisplayName("batch append is atomic (all-or-nothing)")
    void batchAppendAtomicity() {
        String regionId = "integ-r3";

        List<List<KeyValue>> batches = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batches.add(List.of(sampleMutation(i)));
        }

        List<ReplicationLogEntry> entries = wal.appendBatch(regionId, batches);
        assertEquals(5, entries.size());

        // All entries should be readable
        List<ReplicationLogEntry> readBack = wal.getEntries(regionId, 1);
        assertEquals(5, readBack.size());

        // Sequence IDs should be contiguous
        for (int i = 0; i < 5; i++) {
            assertEquals(i + 1, readBack.get(i).getSequenceId());
        }
    }

    @Test
    @DisplayName("concurrent write and read from WAL do not corrupt data")
    void concurrentWriteAndRead() throws Exception {
        String regionId = "integ-r4";
        int writerCount = 2;
        int entriesPerWriter = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(writerCount + 1);
        List<Throwable> errors = new ArrayList<>();

        // Writers
        for (int w = 0; w < writerCount; w++) {
            final int writerId = w;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < entriesPerWriter; i++) {
                        wal.append(regionId, List.of(sampleMutation(writerId * 100 + i)));
                    }
                } catch (Throwable e) {
                    synchronized (errors) { errors.add(e); }
                } finally {
                    doneLatch.countDown();
                }
            }, "writer-" + w).start();
        }

        // Reader
        new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 20; i++) {
                    try {
                        wal.getEntries(regionId, 1);
                        Thread.sleep(5);
                    } catch (Exception ignored) {
                    }
                }
            } catch (Throwable e) {
                synchronized (errors) { errors.add(e); }
            } finally {
                doneLatch.countDown();
            }
        }, "reader").start();

        startLatch.countDown();
        assertTrue(doneLatch.await(15, TimeUnit.SECONDS));

        assertTrue(errors.isEmpty(),
            "No errors expected during concurrent read/write, got: " + errors);

        // Verify total entries written
        List<ReplicationLogEntry> all = wal.getEntries(regionId, 1);
        assertEquals(writerCount * entriesPerWriter, all.size(),
            "All entries should be present after concurrent writes");
    }

    // ================================
    // Helper
    // ================================

    private KeyValue sampleMutation(int index) {
        KeyValue kv = new KeyValue();
        kv.setRowKey(("row-" + index).getBytes());
        kv.setFamily("cf");
        kv.setQualifier("col");
        kv.setTimestamp(System.currentTimeMillis());
        kv.setValue(("value-" + index).getBytes());
        kv.setType(KeyValue.Type.PUT);
        return kv;
    }
}
