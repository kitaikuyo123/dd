package com.minisql.client;

import com.minisql.common.Constants;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.utils.BytesUtil;
import com.minisql.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 请求路由器，负责根据 rowKey 路由到正确的 RegionServer。
 *
 * <p>ZK 是唯一的路由数据源。缓存通过 ZK watcher 失效 + TTL 双重保障。
 */
public class Router {

    private static final Logger logger = LoggerFactory.getLogger(Router.class);

    static final long ROUTE_CACHE_TTL_NANOS = 30_000_000_000L; // 30 seconds

    // 本地路由缓存: tableName -> CachedRouteEntry
    private final Map<String, CachedRouteEntry> routeCache = new ConcurrentHashMap<>();

    // ZooKeeper 客户端
    private volatile ZkClient zkClient;
    private final Set<String> watchedTables = ConcurrentHashMap.newKeySet();
    private final Set<String> watchedRegionPaths = ConcurrentHashMap.newKeySet();

    public Router() {
    }

    public Router(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
        this.watchedTables.clear();
        this.watchedRegionPaths.clear();
    }

    /**
     * 根据表名和 rowKey 定位 RegionServer 的 Primary。
     */
    public ServerAddress route(String tableName, byte[] rowKey) {
        List<RegionRouteInfo> regions = getOrRefreshRegions(tableName);
        if (regions == null || regions.isEmpty()) {
            return null;
        }

        RegionRouteInfo targetRegion = findRegionByKey(regions, rowKey);
        if (targetRegion != null) {
            return targetRegion.getPrimaryServer();
        }

        return null;
    }

    /**
     * 获取表的所有 region 路由信息。
     */
    public List<RegionRouteInfo> getAllRegionLocations(String tableName) {
        return getOrRefreshRegions(tableName);
    }

    /**
     * 获取表的单个 region 路由信息（按 rowKey 定位）。
     */
    public RegionRouteInfo getTargetRegionLocation(String tableName, byte[] rowKey) {
        List<RegionRouteInfo> regions = getOrRefreshRegions(tableName);
        if (regions == null || regions.isEmpty()) {
            return null;
        }
        return findRegionByKey(regions, rowKey);
    }

    /**
     * 获取缓存或从 ZK 刷新。
     */
    private List<RegionRouteInfo> getOrRefreshRegions(String tableName) {
        CachedRouteEntry entry = routeCache.get(tableName);
        if (entry != null && !entry.isExpired()) {
            return entry.regions;
        }

        // TTL 过期或缓存 miss，同步刷新
        refreshRouteCache(tableName);

        entry = routeCache.get(tableName);
        if (entry != null && !entry.isExpired()) {
            return entry.regions;
        }
        // 退回 stale 数据（如果有）
        if (entry != null) {
            return entry.regions;
        }
        return null;
    }

    /**
     * 根据 rowKey 用二分查找定位 Region。
     */
    private RegionRouteInfo findRegionByKey(List<RegionRouteInfo> regions, byte[] rowKey) {
        if (rowKey == null) {
            return null;
        }
        int lo = 0, hi = regions.size() - 1;
        RegionRouteInfo result = null;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            RegionRouteInfo candidate = regions.get(mid);
            byte[] startKey = candidate.getStartKey();
            int cmp = (startKey == null || startKey.length == 0)
                ? 1  // empty startKey = negative infinity, rowKey >= it
                : BytesUtil.compareTo(rowKey, startKey);
            if (cmp >= 0) {
                result = candidate;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        // Verify rowKey < endKey
        if (result != null) {
            byte[] endKey = result.getEndKey();
            if (endKey != null && endKey.length > 0 && BytesUtil.compareTo(rowKey, endKey) >= 0) {
                return null;
            }
        }
        return result;
    }

    /**
     * 从 ZK 刷新路由缓存。
     */
    public void refreshRouteCache(String tableName) {
        if (!isZkUsable()) {
            logger.warn("No ZooKeeper connection available");
            return;
        }

        try {
            String tablePath = "/minisql/tables/" + tableName;

            if (!zkClient.exists(tablePath)) {
                logger.warn("Table not found in ZooKeeper: {}", tableName);
                return;
            }

            String regionsPath = tablePath + "/regions";
            if (!zkClient.exists(regionsPath)) {
                logger.warn("No regions found for table: {}", tableName);
                return;
            }
            ensureTableWatcher(tableName, regionsPath);

            List<String> regionIds = zkClient.getChildren(regionsPath);

            List<RegionRouteInfo> regions = new ArrayList<>();

            for (String regionId : regionIds) {
                try {
                    String regionPath = regionsPath + "/" + regionId;
                    byte[] regionData = zkClient.getData(regionPath);

                    Region region = parseRegionData(regionId, tableName, regionData);

                    if (region != null) {
                        String primaryPath = regionPath + "/primary";
                        String replicasPath = regionPath + "/replicas";
                        ServerAddress primaryServer = null;

                        ensureRegionWatch(tableName, regionPath, primaryPath, replicasPath);

                        if (zkClient.exists(primaryPath)) {
                            byte[] primaryData = zkClient.getData(primaryPath);
                            primaryServer = parseServerAddress(new String(primaryData, java.nio.charset.StandardCharsets.UTF_8));
                        }

                        if (primaryServer != null) {
                            regions.add(new RegionRouteInfo(region, primaryServer));
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to load region {}: {}", regionId, e.getMessage());
                }
            }

            if (!regions.isEmpty()) {
                // Sort by startKey for binary search
                regions.sort(Comparator.comparing(
                    RegionRouteInfo::getStartKey,
                    (a, b) -> {
                        if (a == null || a.length == 0) return -1;
                        if (b == null || b.length == 0) return 1;
                        return BytesUtil.compareTo(a, b);
                    }
                ));
                routeCache.put(tableName, new CachedRouteEntry(
                    Collections.unmodifiableList(regions), System.nanoTime()));
            }
        } catch (Exception e) {
            if (isZkStoppedError(e)) {
                return;
            }
            logger.warn("Failed to refresh route cache: {}", e.getMessage(), e);
        }
    }

    private Region parseRegionData(String regionId, String tableName, byte[] data) {
        try {
            String dataStr = new String(data, "UTF-8");
            String[] parts = dataStr.split("\\|", -1);

            Region region = new Region();

            if (parts.length >= 4) {
                region.setRegionId(parts[0]);
                region.setTableName(parts[1]);
                if (!parts[2].isEmpty()) {
                    region.setStartKey(Base64.getDecoder().decode(parts[2]));
                } else {
                    region.setStartKey(new byte[0]);
                }
                if (!parts[3].isEmpty()) {
                    region.setEndKey(Base64.getDecoder().decode(parts[3]));
                } else {
                    region.setEndKey(new byte[0]);
                }
            } else {
                logger.warn("Invalid region data format for {}/{}: expected 4 parts, got {}, data: {}",
                    tableName, regionId, parts.length, dataStr);
                return null;
            }

            return region;
        } catch (Exception e) {
            logger.warn("Failed to parse region data for {}/{}: {}", tableName, regionId, e.getMessage(), e);
            return null;
        }
    }

    private ServerAddress parseServerAddress(String address) {
        if (address == null || address.isEmpty()) {
            return null;
        }

        String[] parts = address.split(":");
        if (parts[0].isEmpty()) {
            return null;
        }
        String host = parts[0];
        int port = Constants.DEFAULT_REGIONSERVER_PORT;
        if (parts.length > 1) {
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                logger.warn("Invalid port in address '{}', using default {}", address, port);
            }
        }
        return new ServerAddress(host, port);
    }

    void addRoute(String tableName, Region region, ServerId primaryServer) {
        RegionRouteInfo newRoute = new RegionRouteInfo(region, primaryServer);
        routeCache.compute(tableName, (key, existing) -> {
            List<RegionRouteInfo> updated = existing == null
                ? new ArrayList<>()
                : new ArrayList<>(existing.regions);
            updated.removeIf(route -> route.getRegionId().equals(newRoute.getRegionId()));
            updated.add(newRoute);
            updated.sort(Comparator.comparing(
                RegionRouteInfo::getStartKey,
                (a, b) -> {
                    if (a == null || a.length == 0) return -1;
                    if (b == null || b.length == 0) return 1;
                    return BytesUtil.compareTo(a, b);
                }
            ));
            return new CachedRouteEntry(Collections.unmodifiableList(updated), System.nanoTime());
        });
    }

    List<RegionRouteInfo> getRouteCache(String tableName) {
        CachedRouteEntry entry = routeCache.get(tableName);
        return entry != null ? entry.regions : null;
    }

    void clearCache() {
        routeCache.clear();
        watchedTables.clear();
        watchedRegionPaths.clear();
    }

    // --- ZK watchers ---

    private void ensureTableWatcher(String tableName, String regionsPath) {
        if (watchedTables.add(tableName)) {
            try {
                zkClient.watchChildren(regionsPath, (path, children) -> {
                    if (!isZkUsable()) {
                        return;
                    }
                    routeCache.remove(tableName);
                    watchedRegionPaths.removeIf(watchedPath -> watchedPath.startsWith(path));
                    try {
                        ensureTableWatcher(tableName, regionsPath);
                        refreshRouteCache(tableName);
                    } catch (Exception e) {
                        if (isZkStoppedError(e)) {
                            return;
                        }
                        logger.warn("Failed to refresh table watcher cache for {}: {}", tableName, e.getMessage());
                    }
                });
            } catch (Exception e) {
                watchedTables.remove(tableName);
                if (isZkStoppedError(e)) {
                    return;
                }
                logger.warn("Failed to watch regions for table {}: {}", tableName, e.getMessage());
            }
        }
    }

    private void ensureRegionWatch(String tableName, String regionPath, String primaryPath, String replicasPath) {
        if (watchedRegionPaths.add(regionPath)) {
            try {
                zkClient.watchChildren(regionPath, (path, children) -> {
                    if (!isZkUsable()) {
                        return;
                    }
                    routeCache.remove(tableName);
                    watchedRegionPaths.remove(regionPath);
                    refreshRouteCache(tableName);
                });
            } catch (Exception e) {
                if (isZkStoppedError(e)) {
                    return;
                }
                logger.warn("Failed to watch region path {}: {}", regionPath, e.getMessage());
            }
            registerNodeWatcher(primaryPath, tableName, regionPath);
            registerNodeWatcher(replicasPath, tableName, regionPath);
        }
    }

    private void registerNodeWatcher(String nodePath, String tableName, String regionPath) {
        try {
            if (!isZkUsable()) {
                return;
            }
            if (!zkClient.exists(nodePath)) {
                return;
            }
            zkClient.watchNode(nodePath, (path, type) -> {
                if (!isZkUsable()) {
                    return;
                }
                routeCache.remove(tableName);
                watchedRegionPaths.remove(regionPath);
                try {
                    refreshRouteCache(tableName);
                } catch (Exception e) {
                    if (isZkStoppedError(e)) {
                        return;
                    }
                    logger.warn("Failed to refresh route cache for watcher {}: {}", nodePath, e.getMessage());
                }
            });
        } catch (Exception e) {
            if (isZkStoppedError(e)) {
                return;
            }
            logger.warn("Failed to register watcher for {}: {}", nodePath, e.getMessage());
        }
    }

    private boolean isZkUsable() {
        return zkClient != null && zkClient.isStarted();
    }

    private boolean isZkStoppedError(Exception e) {
        return e instanceof IllegalStateException
            && e.getMessage() != null
            && e.getMessage().contains("Expected state [STARTED] was [STOPPED]");
    }

    // --- Inner types ---

    private static class CachedRouteEntry {
        final List<RegionRouteInfo> regions;
        final long cachedAtNanos;

        CachedRouteEntry(List<RegionRouteInfo> regions, long cachedAtNanos) {
            this.regions = regions;
            this.cachedAtNanos = cachedAtNanos;
        }

        boolean isExpired() {
            return System.nanoTime() - cachedAtNanos > ROUTE_CACHE_TTL_NANOS;
        }
    }

    /**
     * 路由信息
     */
    static class RegionRouteInfo {
        private final String regionId;
        private final byte[] startKey;
        private final byte[] endKey;
        private final ServerAddress primaryServer;

        public RegionRouteInfo(Region region, ServerAddress primaryServer) {
            this.regionId = region.getRegionId();
            this.startKey = region.getStartKey();
            this.endKey = region.getEndKey();
            this.primaryServer = primaryServer;
        }

        public RegionRouteInfo(Region region, ServerId primaryServer) {
            this(region, new ServerAddress(primaryServer.getHost(), primaryServer.getPort()));
        }

        public String getRegionId() {
            return regionId;
        }

        public byte[] getStartKey() {
            return startKey;
        }

        public byte[] getEndKey() {
            return endKey;
        }

        public ServerAddress getPrimaryServer() {
            return primaryServer;
        }
    }

    /**
     * 服务器地址
     */
    public static class ServerAddress {
        private final String host;
        private final int port;

        public ServerAddress(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ServerAddress that = (ServerAddress) o;
            return port == that.port && Objects.equals(host, that.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }
    }
}
