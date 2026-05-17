package com.minisql.client;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.utils.BytesUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Router concurrency tests")
class RouterConcurrencyTest {

    private Router router;

    @BeforeEach
    void setUp() {
        router = new Router();
    }

    @Test
    @DisplayName("concurrent route lookups during cache update do not crash")
    void concurrentRouteLookupsDuringCacheUpdate() throws Exception {
        // Pre-populate with initial regions
        for (int i = 0; i < 4; i++) {
            Region region = buildRegion("r" + i, "orders",
                String.valueOf((char) ('a' + i)), String.valueOf((char) ('b' + i)));
            router.addRoute("orders", region, new ServerId("host" + i, 16020 + i));
        }

        int readerCount = 8;
        int writerCount = 1;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(readerCount + writerCount);
        AtomicInteger errors = new AtomicInteger(0);

        // Readers: look up routes continuously
        for (int i = 0; i < readerCount; i++) {
            final int readerId = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 200; j++) {
                        router.route("orders", BytesUtil.toBytes("key" + j));
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }, "reader-" + i).start();
        }

        // Writer: continuously updates routes
        new Thread(() -> {
            try {
                startLatch.await();
                for (int j = 0; j < 100; j++) {
                    String regionId = "r" + (j % 4);
                    Region region = buildRegion(regionId, "orders",
                        String.valueOf((char) ('a' + (j % 4))),
                        String.valueOf((char) ('b' + (j % 4))));
                    router.addRoute("orders", region,
                        new ServerId("host-updated-" + j, 16020 + j));
                }
            } catch (Exception e) {
                errors.incrementAndGet();
            } finally {
                doneLatch.countDown();
            }
        }, "writer").start();

        startLatch.countDown();
        assertTrue(doneLatch.await(15, TimeUnit.SECONDS));
        assertEquals(0, errors.get(), "No errors expected during concurrent route operations");

        // Verify final state is usable
        List<Router.RegionRouteInfo> regions = router.getAllRegionLocations("orders");
        assertNotNull(regions);
        assertFalse(regions.isEmpty());
    }

    @Test
    @DisplayName("concurrent refresh of route cache maintains sorted order")
    void concurrentRefreshRouteCache() throws Exception {
        // Pre-populate
        for (int i = 0; i < 4; i++) {
            Region region = buildRegion("r" + i, "items",
                String.valueOf((char) ('a' + i)), String.valueOf((char) ('b' + i)));
            router.addRoute("items", region, new ServerId("host" + i, 16020 + i));
        }

        int threadCount = 4;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 50; j++) {
                        String regionId = "r" + (j % 4);
                        Region region = buildRegion(regionId, "items",
                            String.valueOf((char) ('a' + (j % 4))),
                            String.valueOf((char) ('b' + (j % 4))));
                        router.addRoute("items", region,
                            new ServerId("h" + threadId + "-" + j, 16020 + threadId));
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }, "updater-" + t).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(15, TimeUnit.SECONDS));
        assertEquals(0, errors.get());

        // After all updates, cache should still be sorted
        List<Router.RegionRouteInfo> regions = router.getAllRegionLocations("items");
        assertNotNull(regions);
        assertFalse(regions.isEmpty());
        for (int i = 1; i < regions.size(); i++) {
            byte[] prev = regions.get(i - 1).getStartKey();
            byte[] curr = regions.get(i).getStartKey();
            if (prev != null && curr != null && prev.length > 0 && curr.length > 0) {
                assertTrue(BytesUtil.compareTo(prev, curr) <= 0,
                    "Regions should be sorted by startKey");
            }
        }
    }

    private Region buildRegion(String regionId, String tableName, String start, String end) {
        Region region = new Region();
        region.setRegionId(regionId);
        region.setTableName(tableName);
        region.setStartKey(start.isEmpty() ? new byte[0] : start.getBytes());
        region.setEndKey(end.isEmpty() ? new byte[0] : end.getBytes());
        return region;
    }
}
