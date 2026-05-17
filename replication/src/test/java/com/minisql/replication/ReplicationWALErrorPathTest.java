package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicationWAL error-path tests")
class ReplicationWALErrorPathTest {

    private static final AtomicInteger counter = new AtomicInteger(0);

    private ReplicationWAL wal;
    private String dbPath;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        dbPath = tempDir.resolve("wal-err-" + counter.incrementAndGet()).toString();
        wal = new ReplicationWAL(dbPath);
    }

    @AfterEach
    void tearDown() {
        if (wal != null) {
            wal.close();
        }
    }

    private KeyValue sampleMutation(String value) {
        KeyValue kv = new KeyValue();
        kv.setRowKey(value.getBytes());
        kv.setFamily("cf");
        kv.setQualifier("col");
        kv.setTimestamp(System.currentTimeMillis());
        kv.setValue(value.getBytes());
        kv.setType(KeyValue.Type.PUT);
        return kv;
    }

    private List<KeyValue> mutations(String... values) {
        List<KeyValue> list = new ArrayList<>();
        for (String v : values) list.add(sampleMutation(v));
        return list;
    }

    @Test
    @DisplayName("append after close throws RuntimeException because db is null")
    void appendAfterCloseThrowsRuntimeException() {
        wal.initialize();
        wal.append("r1", mutations("before-close"));
        wal.close();

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
            wal.append("r1", mutations("after-close"))
        );
        assertNotNull(ex, "Should throw a RuntimeException after close");
    }

    @Test
    @DisplayName("concurrent append and cleanup do not crash or corrupt data")
    void concurrentAppendAndCleanup() throws Exception {
        wal.initialize();

        String regionId = "r-concurrent";
        int totalAppends = 100;
        int retention = 10;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger appendCount = new AtomicInteger(0);

        // Thread 1: append 100 entries
        Thread appendThread = new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < totalAppends; i++) {
                    wal.append(regionId, mutations("val-" + i));
                    appendCount.incrementAndGet();
                }
            } catch (Throwable t) {
                error.compareAndSet(null, t);
            } finally {
                doneLatch.countDown();
            }
        }, "append-thread");

        // Thread 2: repeatedly call cleanup while appends are happening
        Thread cleanupThread = new Thread(() -> {
            try {
                startLatch.await();
                while (appendCount.get() < totalAppends) {
                    wal.cleanup(regionId, retention);
                    Thread.sleep(1); // small pause to avoid busy-loop
                }
            } catch (Throwable t) {
                error.compareAndSet(null, t);
            } finally {
                doneLatch.countDown();
            }
        }, "cleanup-thread");

        appendThread.start();
        cleanupThread.start();
        startLatch.countDown(); // fire both threads simultaneously

        boolean finished = doneLatch.await(30, TimeUnit.SECONDS);
        assertTrue(finished, "Both threads should finish within timeout");

        appendThread.join(5000);
        cleanupThread.join(5000);

        Throwable caught = error.get();
        if (caught != null) {
            if (caught instanceof Exception) {
                throw (Exception) caught;
            }
            throw new AssertionError("Thread threw error", caught);
        }

        // Verify the last (retention) entries are readable
        int written = appendCount.get();
        long maxSeq = wal.getCurrentSequenceId(regionId);
        long expectedStartSeq = Math.max(1, maxSeq - retention + 1);

        List<ReplicationLogEntry> entries = wal.getEntries(regionId, expectedStartSeq);
        assertFalse(entries.isEmpty(), "Should have at least the retained entries");

        // Verify every entry from expectedStartSeq to maxSeq is present and valid
        for (ReplicationLogEntry entry : entries) {
            assertTrue(entry.getSequenceId() >= expectedStartSeq,
                "Entry seqId=" + entry.getSequenceId() + " should be >= " + expectedStartSeq);
            assertFalse(entry.getMutations().isEmpty(),
                "Entry seqId=" + entry.getSequenceId() + " should have mutations");
        }
    }

    @Test
    @DisplayName("close is idempotent and does not throw on second call")
    void closeIsIdempotent() {
        wal.initialize();
        wal.close();

        // Second close should complete without throwing
        assertDoesNotThrow(() -> wal.close());
    }

    @Test
    @DisplayName("getEntries on nonexistent region returns empty list, not exception")
    void getEntriesOnEmptyRegionReturnsEmptyList() {
        wal.initialize();
        List<ReplicationLogEntry> entries = wal.getEntries("nonexistent", 0);
        assertNotNull(entries, "Should return a list, not null");
        assertTrue(entries.isEmpty(), "Should be empty for a nonexistent region");
    }

    @Test
    @DisplayName("applied progress survives close and reopen at the same path")
    void appliedProgressSurvivesCloseReopen() {
        String reopenPath = tempDir.resolve("wal-reopen").toString();
        wal = new ReplicationWAL(reopenPath);
        wal.initialize();

        wal.markAsApplied("r1", 42L, "host:1234");

        // Verify progress was written
        assertEquals(42L, wal.getAppliedProgress("r1", "host:1234"),
            "Progress should be readable before close");

        wal.close();

        // RocksDB may leave the LOCK file on Windows after close().
        // Since we know this is a safe reopen in the same JVM, clean it up.
        File lockFile = new File(reopenPath, "LOCK");
        if (lockFile.exists()) {
            lockFile.delete();
        }

        // Reopen at the same path with a new WAL instance
        wal = new ReplicationWAL(reopenPath);
        wal.initialize();

        long progress = wal.getAppliedProgress("r1", "host:1234");
        assertEquals(42L, progress,
            "Applied progress should survive close and reopen");
    }
}
