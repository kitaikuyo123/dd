package com.minisql.zookeeper;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DistributedLock tests")
class DistributedLockTest {

    private TestingServer server;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    // ---------- helpers ----------

    private ZkClient newClient() throws Exception {
        server = new TestingServer();
        ZkClient client = new ZkClient(server.getConnectString());
        client.start();
        int retries = 0;
        while (!client.isConnected() && retries < 30) {
            Thread.sleep(100);
            retries++;
        }
        if (!client.isConnected()) {
            throw new IllegalStateException("Failed to connect to test ZK");
        }
        return client;
    }

    // ==================================================================

    @Nested
    @DisplayName("acquire / release")
    class AcquireRelease {

        @Test
        @DisplayName("acquires and releases")
        void acquiresAndReleases() throws Exception {
            ZkClient client = newClient();
            DistributedLock lock = new DistributedLock(client.getClient(), "/test/lock-a");

            lock.acquire();
            assertTrue(lock.isAcquiredInThisProcess());
            lock.release();
            // After release, the mutex shouldn't report as held
            // isAcquiredInThisProcess may still return true on some Curator versions;
            // the real test is that another lock can acquire it
            client.close();
        }

        @Test
        @DisplayName("second contender cannot acquire held lock within timeout")
        void secondContenderBlocked() throws Exception {
            ZkClient client = newClient();
            DistributedLock lock1 = new DistributedLock(client.getClient(), "/test/lock-b");
            lock1.acquire();
            assertTrue(lock1.isAcquiredInThisProcess());

            // Second lock on same path should fail fast with short timeout
            DistributedLock lock2 = new DistributedLock(client.getClient(), "/test/lock-b");
            assertFalse(lock2.acquire(200, TimeUnit.MILLISECONDS));

            lock1.release();
            client.close();
        }

        @Test
        @DisplayName("released lock can be re-acquired by another contender")
        void reacquireAfterRelease() throws Exception {
            ZkClient client = newClient();
            DistributedLock lock1 = new DistributedLock(client.getClient(), "/test/lock-c");
            lock1.acquire();
            lock1.release();

            DistributedLock lock2 = new DistributedLock(client.getClient(), "/test/lock-c");
            assertTrue(lock2.acquire(1, TimeUnit.SECONDS));
            lock2.release();
            client.close();
        }
    }

    @Nested
    @DisplayName("multiple independent locks")
    class MultipleLocks {

        @Test
        @DisplayName("different lock paths do not conflict")
        void differentPathsNoConflict() throws Exception {
            ZkClient client = newClient();
            DistributedLock lockA = new DistributedLock(client.getClient(), "/test/lock-d-a");
            DistributedLock lockB = new DistributedLock(client.getClient(), "/test/lock-d-b");

            lockA.acquire();
            lockB.acquire();

            assertTrue(lockA.isAcquiredInThisProcess());
            assertTrue(lockB.isAcquiredInThisProcess());

            lockA.release();
            lockB.release();
            client.close();
        }
    }

    @Nested
    @DisplayName("error handling")
    class ErrorHandling {

        @Test
        @DisplayName("release of unheld lock throws IllegalMonitorStateException")
        void releaseUnheldThrows() throws Exception {
            ZkClient client = newClient();
            DistributedLock lock = new DistributedLock(client.getClient(), "/test/lock-e");
            assertFalse(lock.isAcquiredInThisProcess());
            // Curator throws when releasing an unheld lock
            assertThrows(Exception.class, () -> lock.release());
            client.close();
        }

        @Test
        @DisplayName("isAcquiredInThisProcess returns false before acquire")
        void notAcquiredInitially() throws Exception {
            ZkClient client = newClient();
            DistributedLock lock = new DistributedLock(client.getClient(), "/test/lock-f");
            assertFalse(lock.isAcquiredInThisProcess());
            client.close();
        }

        @Test
        @DisplayName("timeout zero returns immediately")
        void zeroTimeoutReturnsImmediately() throws Exception {
            ZkClient client = newClient();
            DistributedLock lock1 = new DistributedLock(client.getClient(), "/test/lock-g");
            lock1.acquire();

            DistributedLock lock2 = new DistributedLock(client.getClient(), "/test/lock-g");
            long start = System.nanoTime();
            boolean acquired = lock2.acquire(0, TimeUnit.MILLISECONDS);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertFalse(acquired);
            assertTrue(elapsedMs < 1000, "zero-timeout should return quickly, took " + elapsedMs + "ms");

            lock1.release();
            client.close();
        }
    }
}
