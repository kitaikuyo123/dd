package com.minisql.client;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Router tests")
class RouterTest {

    @Test
    @DisplayName("setMasterAddress updates fallback master")
    void testSetMasterAddress() {
        Router router = new Router();
        Router.ServerAddress master = new Router.ServerAddress("localhost", 16000);

        router.setMasterAddress(master);

        assertEquals(master, router.getMaster());
    }

    @Test
    @DisplayName("addRoute stores region route info")
    void testAddRoute() {
        Router router = new Router();
        Region region = buildRegion("region-1", "users", "a", "z");
        ServerId primary = new ServerId("primary", 16020);

        router.addRoute("users", region, primary);

        List<Router.RegionRouteInfo> routes = router.getRouteCache("users");
        assertEquals(1, routes.size());
        assertEquals("region-1", routes.get(0).getRegionId());
        assertEquals("primary", routes.get(0).getPrimaryServer().getHost());
    }

    @Test
    @DisplayName("route returns the primary even when read preference prefers secondaries")
    void testWriteRouteAlwaysUsesPrimary() {
        Router router = new Router();
        router.setDefaultReadConsistency(Router.ReadConsistency.PREFER_SECONDARY);

        Region region = buildRegion("region-1", "users", "a", "z");
        region.addReplica(new ServerId("secondary", 16021));
        ServerId primary = new ServerId("primary", 16020);
        router.addRoute("users", region, primary);

        Router.ServerAddress target = router.route("users", "m".getBytes());

        assertEquals("primary", target.getHost());
        assertEquals(16020, target.getPort());
    }

    @Test
    @DisplayName("routeForRead can use primary only consistency")
    void testRouteForReadPrimaryOnly() {
        Router router = new Router();
        Region region = buildRegion("region-1", "users", "a", "z");
        region.addReplica(new ServerId("secondary", 16021));
        ServerId primary = new ServerId("primary", 16020);
        router.addRoute("users", region, primary);

        Router.ServerAddress target = router.routeForRead("users", "m".getBytes(), Router.ReadConsistency.PRIMARY_ONLY);

        assertEquals("primary", target.getHost());
        assertEquals(16020, target.getPort());
    }

    @Test
    @DisplayName("clearCache removes cached routes")
    void testClearCache() {
        Router router = new Router();
        router.addRoute("users", buildRegion("region-1", "users", "a", "z"), new ServerId("primary", 16020));

        router.clearCache();

        assertTrue(router.getRouteCache("users") == null || router.getRouteCache("users").isEmpty());
    }

    private Region buildRegion(String regionId, String tableName, String start, String end) {
        Region region = new Region();
        region.setRegionId(regionId);
        region.setTableName(tableName);
        region.setStartKey(start.getBytes());
        region.setEndKey(end.getBytes());
        return region;
    }
}
