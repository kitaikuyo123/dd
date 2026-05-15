package com.minisql.client;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.utils.BytesUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Router tests")
class RouterTest {

    private Router router;

    @BeforeEach
    void setUp() {
        router = new Router();
    }

    // --- addRoute + route ---

    @Test
    @DisplayName("addRoute stores region and route returns correct primary")
    void testAddRouteAndRoute() {
        Region region = buildRegion("region-1", "users", "a", "z");
        router.addRoute("users", region, new ServerId("host1", 16020));

        Router.ServerAddress addr = router.route("users", BytesUtil.toBytes("m"));
        assertNotNull(addr);
        assertEquals("host1", addr.getHost());
        assertEquals(16020, addr.getPort());
    }

    @Test
    @DisplayName("route returns null for empty cache")
    void testRouteEmptyCache() {
        Router.ServerAddress addr = router.route("users", BytesUtil.toBytes("a"));
        assertNull(addr);
    }

    @Test
    @DisplayName("addRoute updates existing region")
    void testAddRouteUpdatesExisting() {
        Region region = buildRegion("region-1", "users", "a", "z");
        router.addRoute("users", region, new ServerId("host1", 16020));

        // Update same region with new primary
        router.addRoute("users", region, new ServerId("host2", 16021));

        Router.ServerAddress addr = router.route("users", BytesUtil.toBytes("m"));
        assertNotNull(addr);
        assertEquals("host2", addr.getHost());
        assertEquals(16021, addr.getPort());
    }

    @Test
    @DisplayName("addRoute with multiple regions routes to correct one")
    void testMultipleRegions() {
        Region regionA = buildRegion("r1", "users", "a", "m");
        Region regionM = buildRegion("r2", "users", "m", "z");

        router.addRoute("users", regionA, new ServerId("hostA", 16020));
        router.addRoute("users", regionM, new ServerId("hostM", 16021));

        // "b" falls in [a, m)
        Router.ServerAddress addrB = router.route("users", BytesUtil.toBytes("b"));
        assertNotNull(addrB);
        assertEquals("hostA", addrB.getHost());

        // "x" falls in [m, z)
        Router.ServerAddress addrX = router.route("users", BytesUtil.toBytes("x"));
        assertNotNull(addrX);
        assertEquals("hostM", addrX.getHost());
    }

    // --- findRegionByKey (binary search) ---

    @Test
    @DisplayName("binary search handles empty startKey (negative infinity)")
    void testBinarySearchEmptyStartKey() {
        Region region = buildRegion("r1", "t", "", "m");
        router.addRoute("t", region, new ServerId("host", 16020));

        // Any key should match since start is empty
        Router.ServerAddress addr = router.route("t", BytesUtil.toBytes("a"));
        assertNotNull(addr);
    }

    @Test
    @DisplayName("binary search handles empty endKey (positive infinity)")
    void testBinarySearchEmptyEndKey() {
        Region region = buildRegion("r1", "t", "m", "");
        router.addRoute("t", region, new ServerId("host", 16020));

        // Key "z" should match since end is empty
        Router.ServerAddress addr = router.route("t", BytesUtil.toBytes("z"));
        assertNotNull(addr);
    }

    @Test
    @DisplayName("binary search returns null for key outside all ranges")
    void testBinarySearchNoMatch() {
        Region region = buildRegion("r1", "t", "m", "z");
        router.addRoute("t", region, new ServerId("host", 16020));

        // "a" < "m", so no match
        Router.ServerAddress addr = router.route("t", BytesUtil.toBytes("a"));
        assertNull(addr);
    }

    @Test
    @DisplayName("binary search boundary: startKey inclusive, endKey exclusive")
    void testBinarySearchBoundary() {
        Region region = buildRegion("r1", "t", "a", "z");
        router.addRoute("t", region, new ServerId("host", 16020));

        // startKey inclusive
        assertNotNull(router.route("t", BytesUtil.toBytes("a")));
        // endKey exclusive
        assertNull(router.route("t", BytesUtil.toBytes("z")));
    }

    // --- getAllRegionLocations / getTargetRegionLocation ---

    @Test
    @DisplayName("getAllRegionLocations returns all regions")
    void testGetAllRegionLocations() {
        Region r1 = buildRegion("r1", "t", "a", "m");
        Region r2 = buildRegion("r2", "t", "m", "z");
        router.addRoute("t", r1, new ServerId("h1", 16020));
        router.addRoute("t", r2, new ServerId("h2", 16021));

        List<Router.RegionRouteInfo> locations = router.getAllRegionLocations("t");
        assertNotNull(locations);
        assertEquals(2, locations.size());
    }

    @Test
    @DisplayName("getTargetRegionLocation returns correct region")
    void testGetTargetRegionLocation() {
        Region r1 = buildRegion("r1", "t", "a", "m");
        Region r2 = buildRegion("r2", "t", "m", "z");
        router.addRoute("t", r1, new ServerId("h1", 16020));
        router.addRoute("t", r2, new ServerId("h2", 16021));

        Router.RegionRouteInfo info = router.getTargetRegionLocation("t", BytesUtil.toBytes("x"));
        assertNotNull(info);
        assertEquals("r2", info.getRegionId());
    }

    // --- clearCache ---

    @Test
    @DisplayName("clearCache removes cached routes")
    void testClearCache() {
        router.addRoute("users", buildRegion("r1", "users", "a", "z"), new ServerId("host", 16020));
        router.clearCache();
        assertNull(router.getRouteCache("users"));
    }

    // --- TTL ---

    @Test
    @DisplayName("expired cache entry triggers refresh (returns null when no ZK)")
    void testTtlExpiry() {
        Region region = buildRegion("r1", "t", "a", "z");
        router.addRoute("t", region, new ServerId("host", 16020));

        // Manually corrupt the cache timestamp to simulate expiry
        // Access internal cache via getRouteCache to verify data exists
        assertNotNull(router.getRouteCache("t"));

        // Force TTL expiry by writing a stale entry directly
        java.lang.reflect.Field field;
        try {
            field = Router.class.getDeclaredField("routeCache");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> cache = (java.util.Map<String, Object>) field.get(router);
            Object entry = cache.get("t");
            java.lang.reflect.Field nanosField = entry.getClass().getDeclaredField("cachedAtNanos");
            nanosField.setAccessible(true);
            // Set to 0 so it's definitely expired
            nanosField.set(entry, 0L);
        } catch (Exception e) {
            fail("Failed to manipulate cache for TTL test: " + e.getMessage());
        }

        // No ZK available, so refresh fails, getOrRefreshRegions returns null
        List<Router.RegionRouteInfo> result = router.getAllRegionLocations("t");
        // With expired cache and no ZK, should still return stale data as fallback
        assertNotNull(result);
    }

    // --- getMaster removed ---

    @Test
    @DisplayName("route returns null when no regions found (no master fallback)")
    void testNoMasterFallback() {
        Router.ServerAddress addr = router.route("nonexistent", BytesUtil.toBytes("key"));
        assertNull(addr);
    }

    // --- concurrency ---

    @Test
    @DisplayName("concurrent route and addRoute do not throw")
    void testConcurrentAccess() throws Exception {
        int threadCount = 10;
        int iterations = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            pool.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < iterations; j++) {
                        if (threadId % 2 == 0) {
                            String regionId = "r-" + (j % 3);
                            Region region = buildRegion(regionId, "t",
                                String.valueOf((char) ('a' + (j % 3))),
                                String.valueOf((char) ('b' + (j % 3))));
                            router.addRoute("t", region, new ServerId("h" + threadId, 16020 + threadId));
                        } else {
                            router.route("t", BytesUtil.toBytes("key" + j));
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        assertEquals(0, errors.get());
        pool.shutdown();
    }

    // --- helpers ---

    private Region buildRegion(String regionId, String tableName, String start, String end) {
        Region region = new Region();
        region.setRegionId(regionId);
        region.setTableName(tableName);
        region.setStartKey(start.isEmpty() ? new byte[0] : start.getBytes());
        region.setEndKey(end.isEmpty() ? new byte[0] : end.getBytes());
        return region;
    }
}
