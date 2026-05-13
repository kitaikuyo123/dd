package com.minisql.client;

import com.minisql.common.Constants;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.MasterProto;
import com.minisql.common.proto.MasterServiceGrpc;
import com.minisql.common.utils.BytesUtil;
import com.minisql.zookeeper.ZkClient;
import com.minisql.zookeeper.ZkPayloads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 请求路由器，负责根据 rowKey 路由到正确的 RegionServer。
 *
 * <p>所有读写请求均路由到 Primary。副本仅用于故障转移，不参与读请求分发。
 */
public class Router {

    private static final Logger logger = LoggerFactory.getLogger(Router.class);

    // 本地路由缓存: tableName -> List<RegionRouteInfo>
    private final Map<String, List<RegionRouteInfo>> routeCache = new ConcurrentHashMap<>();

    // Master 地址缓存
    private volatile ServerAddress masterAddress;

    // ZooKeeper 客户端（用于获取最新路由信息）
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
        // 1. 从缓存获取该表的所有 Region
        List<RegionRouteInfo> regions = routeCache.get(tableName);
        if (regions == null || regions.isEmpty()) {
            refreshRouteCache(tableName);
            regions = routeCache.get(tableName);
        }

        if (regions == null || regions.isEmpty()) {
            return getMaster();
        }

        // 2. 根据 rowKey 找到对应的 Region
        RegionRouteInfo targetRegion = findRegionByKey(regions, rowKey);
        if (targetRegion != null) {
            return targetRegion.getPrimaryServer();
        }

        // 3. 没找到，返回 Master
        return getMaster();
    }

    /**
     * 根据 rowKey 查找对应的 Region
     */
    private RegionRouteInfo findRegionByKey(List<RegionRouteInfo> regions, byte[] rowKey) {
        for (RegionRouteInfo region : regions) {
            if (BytesUtil.isKeyInRange(rowKey, region.getStartKey(), region.getEndKey())) {
                return region;
            }
        }
        return null;
    }

    /**
     * 刷新路由缓存
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
                routeCache.put(tableName, Collections.unmodifiableList(regions));
                return;
            }

            if (refreshRouteCacheFromMaster(tableName)) {
                return;
            }

        } catch (Exception e) {
            if (isZkStoppedError(e)) {
                return;
            }
            logger.warn("Failed to refresh route cache: {}", e.getMessage(), e);
        }
    }

    private boolean refreshRouteCacheFromMaster(String tableName) {
        ServerAddress master = getMaster();
        if (master == null) {
            return false;
        }

        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(master.getHost(), master.getPort())
                .usePlaintext()
                .build();

            MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(3, TimeUnit.SECONDS);

            MasterProto.GetTableRegionsResponse response = stub.getTableRegions(
                MasterProto.GetTableRegionsRequest.newBuilder()
                    .setTableName(tableName)
                    .build()
            );

            if (!response.getStatus().getSuccess()) {
                return false;
            }

            List<RegionRouteInfo> regions = new ArrayList<>();
            for (CommonProto.RegionInfo regionInfo : response.getRegionsList()) {
                Region region = new Region();
                region.setRegionId(regionInfo.getRegionId());
                region.setTableName(regionInfo.getTableName());
                region.setStartKey(regionInfo.getStartKey().toByteArray());
                region.setEndKey(regionInfo.getEndKey().toByteArray());

                ServerAddress primary = regionInfo.hasPrimary()
                    ? new ServerAddress(regionInfo.getPrimary().getHost(), regionInfo.getPrimary().getPort())
                    : null;

                if (primary != null) {
                    regions.add(new RegionRouteInfo(region, primary));
                }
            }

            if (!regions.isEmpty()) {
                routeCache.put(tableName, Collections.unmodifiableList(regions));
                return true;
            }
        } catch (Exception e) {
            logger.warn("Failed to refresh route cache from Master: {}", e.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdown();
                try {
                    if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    channel.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }

        return false;
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

    /**
     * 获取 Master 地址
     */
    public ServerAddress getMaster() {
        if (masterAddress != null) {
            return masterAddress;
        }

        if (isZkUsable()) {
            try {
                String masterPath = Constants.ZK_MASTER_LEADER_PATH;
                if (zkClient.exists(masterPath)) {
                    byte[] masterData = zkClient.getData(masterPath);
                    String masterInfo = ZkPayloads.decodeLeaderAddress(masterData);
                    masterAddress = parseServerAddress(masterInfo);
                    return masterAddress;
                }
            } catch (Exception e) {
                if (isZkStoppedError(e)) {
                    return masterAddress;
                }
                logger.warn("Failed to get master from ZooKeeper: {}", e.getMessage());
            }
        }

        return new ServerAddress("localhost", 16000);
    }

    private ServerAddress parseServerAddress(String address) {
        if (address == null || address.isEmpty()) {
            return null;
        }

        String[] parts = address.split(":");
        String host = parts[0];
        int port = 16020;
        if (parts.length > 1) {
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                logger.warn("Invalid port in address '{}', using default {}", address, port);
            }
        }
        return new ServerAddress(host, port);
    }

    public void setMasterAddress(ServerAddress masterAddress) {
        this.masterAddress = masterAddress;
    }

    public void addRoute(String tableName, Region region, ServerId primaryServer) {
        RegionRouteInfo newRoute = new RegionRouteInfo(region, primaryServer);
        routeCache.compute(tableName, (key, existing) -> {
            List<RegionRouteInfo> updated = existing == null
                ? new ArrayList<>()
                : new ArrayList<>(existing);
            updated.removeIf(route -> route.getRegionId().equals(newRoute.getRegionId()));
            updated.add(newRoute);
            return Collections.unmodifiableList(updated);
        });
    }

    List<RegionRouteInfo> getRouteCache(String tableName) {
        return routeCache.get(tableName);
    }

    public void clearCache() {
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
