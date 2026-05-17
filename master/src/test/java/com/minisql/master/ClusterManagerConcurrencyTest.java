package com.minisql.master;

import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.state.ClusterManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ClusterManager concurrency tests")
class ClusterManagerConcurrencyTest {

    private ClusterManager clusterManager;

    @BeforeEach
    void setUp() {
        clusterManager = new ClusterManager(new LoadBalancer());
    }

    @Test
    @DisplayName("concurrent register and unregister servers leaves consistent active list")
    void concurrentRegisterAndUnregisterServers() throws Exception {
        // 4 servers to register, 2 of which will also be unregistered
        ServerId s0 = new ServerId("host-0", 16020, 1L);
        ServerId s1 = new ServerId("host-1", 16021, 2L);
        ServerId s2 = new ServerId("host-2", 16022, 3L);
        ServerId s3 = new ServerId("host-3", 16023, 4L);

        int totalThreads = 6; // 4 register + 2 unregister
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalThreads);
        AtomicInteger errors = new AtomicInteger(0);

        // 4 threads: each registers a different server
        ServerId[] servers = {s0, s1, s2, s3};
        for (int i = 0; i < 4; i++) {
            final ServerId server = servers[i];
            new Thread(() -> {
                try {
                    startLatch.await();
                    clusterManager.registerServer(server);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }, "register-" + i).start();
        }

        // 2 threads: unregister s1 and s3 (after they have been registered by racing)
        // We register s1 and s3 first, then start all threads so unregister can find them.
        clusterManager.registerServer(s1);
        clusterManager.registerServer(s3);

        ServerId[] toRemove = {s1, s3};
        for (int i = 0; i < 2; i++) {
            final ServerId server = toRemove[i];
            new Thread(() -> {
                try {
                    startLatch.await();
                    clusterManager.removeServer(server);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }, "unregister-" + i).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All threads should finish within timeout");
        assertEquals(0, errors.get(), "No exceptions should be thrown during concurrent operations");

        // s1 and s3 were registered then removed; s0 and s2 remain active
        assertFalse(clusterManager.isServerActive(s1), "s1 should be removed");
        assertFalse(clusterManager.isServerActive(s3), "s3 should be removed");
        assertTrue(clusterManager.isServerActive(s0), "s0 should be active");
        assertTrue(clusterManager.isServerActive(s2), "s2 should be active");

        Collection<ClusterManager.ServerInfo> active = clusterManager.getActiveServers();
        assertEquals(2, active.size(), "Exactly 2 servers should remain active");
    }

    @Test
    @DisplayName("concurrent region assignment updates produce correct final state")
    void concurrentRegionAssignmentUpdates() throws Exception {
        // Register a target server for assignments
        ServerId target = new ServerId("target-host", 16020, 1L);
        clusterManager.registerServer(target);

        int threadCount = 8;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final String regionId = "region-" + i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    clusterManager.assignRegionToServer(regionId, target);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }, "assign-" + i).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All threads should finish within timeout");
        assertEquals(0, errors.get(), "No ConcurrentModificationException or other errors should occur");

        Map<String, ServerId> assignments = clusterManager.getRegionAssignments();
        assertEquals(threadCount, assignments.size(), "All 8 regions should be assigned");
        for (int i = 0; i < threadCount; i++) {
            String regionId = "region-" + i;
            assertTrue(assignments.containsKey(regionId),
                "Region " + regionId + " should be in assignments");
            assertEquals(target, assignments.get(regionId),
                "Region " + regionId + " should be assigned to target server");
        }
    }

    @Test
    @DisplayName("concurrent load updates do not crash")
    void concurrentLoadUpdates() throws Exception {
        // Register servers for load updates
        int serverCount = 8;
        ServerId[] servers = new ServerId[serverCount];
        for (int i = 0; i < serverCount; i++) {
            servers[i] = new ServerId("load-host-" + i, 16020 + i, (long) i);
            clusterManager.registerServer(servers[i]);
        }

        int updatesPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(serverCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < serverCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < updatesPerThread; j++) {
                        ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
                        load.setRegionId("region-" + idx);
                        load.setStoreFileSize((long) (idx + 1) * (j + 1) * 1024);
                        load.setMemStoreSize((long) (j + 1) * 64);
                        load.setReadRequests(j * 10L);
                        load.setWriteRequests(j * 5L);
                        clusterManager.updateRegionLoad(servers[idx], "region-" + idx, load);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }, "load-update-" + i).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "All threads should finish within timeout");
        assertEquals(0, errors.get(), "No exceptions should occur during concurrent load updates");

        // Verify the final load state is accessible for each server
        for (int i = 0; i < serverCount; i++) {
            final int idx = i;
            ClusterManager.ServerInfo info = clusterManager.getActiveServersList()
                .stream()
                .filter(s -> s.getServerId().equals(servers[idx]))
                .findFirst()
                .orElse(null);
            assertNotNull(info, "Server info for server " + i + " should exist");
            assertTrue(info.getRegionLoads().containsKey("region-" + idx),
                "Server " + i + " should have load for region-" + idx);
        }
    }
}
