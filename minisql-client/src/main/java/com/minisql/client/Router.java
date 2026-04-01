package com.minisql.client;

import com.minisql.common.Constants;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.MasterProto;
import com.minisql.common.proto.MasterServiceGrpc;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.common.proto.RegionServerServiceGrpc;
import com.minisql.zookeeper.ZkClient;
import com.minisql.zookeeper.ZkPayloads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 请求路由器
 * 负责根据rowKey路由到正确的RegionServer
 * 负责模块: 开发者C
 */
public class Router {

    private static final long DEFAULT_LAG_CACHE_TTL_MS = 2000L;
    private static final long DEFAULT_CIRCUIT_BREAKER_MS = 5000L;
    private static final long DEFAULT_WRITE_PRIMARY_PIN_MS = 5000L;
    private static final long DEFAULT_FRESH_REPLICA_MAX_LAG = 10L;

    // 本地路由缓存: tableName -> List<RegionRouteInfo>
    private final Map<String, List<RegionRouteInfo>> routeCache = new ConcurrentHashMap<>();

    // Master 地址缓存
    private volatile ServerAddress masterAddress;

    // ZooKeeper 客户端（用于获取最新路由信息）
    private volatile ZkClient zkClient;
    private volatile ReadConsistency defaultReadConsistency = ReadConsistency.SECONDARY_IF_FRESH;
    private volatile long freshReplicaMaxLag = DEFAULT_FRESH_REPLICA_MAX_LAG;
    private volatile long lagCacheTtlMs = DEFAULT_LAG_CACHE_TTL_MS;
    private volatile long circuitBreakerWindowMs = DEFAULT_CIRCUIT_BREAKER_MS;
    private volatile long writePrimaryPinMs = DEFAULT_WRITE_PRIMARY_PIN_MS;

    private final Map<String, LagSnapshot> lagCache = new ConcurrentHashMap<>();
    private final Map<String, Long> circuitBreakerUntil = new ConcurrentHashMap<>();
    private final Map<String, Long> recentWrites = new ConcurrentHashMap<>();
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

    public void setDefaultReadConsistency(ReadConsistency defaultReadConsistency) {
        this.defaultReadConsistency = defaultReadConsistency == null
            ? ReadConsistency.SECONDARY_IF_FRESH
            : defaultReadConsistency;
    }

    public void setFreshReplicaMaxLag(long freshReplicaMaxLag) {
        this.freshReplicaMaxLag = Math.max(0L, freshReplicaMaxLag);
    }

    public void setLagCacheTtlMs(long lagCacheTtlMs) {
        this.lagCacheTtlMs = Math.max(0L, lagCacheTtlMs);
    }

    public void setCircuitBreakerWindowMs(long circuitBreakerWindowMs) {
        this.circuitBreakerWindowMs = Math.max(0L, circuitBreakerWindowMs);
    }

    public void setWritePrimaryPinMs(long writePrimaryPinMs) {
        this.writePrimaryPinMs = Math.max(0L, writePrimaryPinMs);
    }

    public void recordWrite(String tableName, byte[] rowKey) {
        if (tableName == null || rowKey == null) {
            return;
        }
        recentWrites.put(buildRecentWriteKey(tableName, rowKey), System.currentTimeMillis() + writePrimaryPinMs);
    }

    /**
     * 根据表名和rowKey定位RegionServer（改进版）
     * 支持基于负载的选择（当多个副本存在时）
     */
    public ServerAddress route(String tableName, byte[] rowKey) {
        // 1. 从缓存获取该表的所有 Region
        List<RegionRouteInfo> regions = routeCache.get(tableName);
        if (regions == null || regions.isEmpty()) {
            // 如果没有缓存，刷新路由信息
            refreshRouteCache(tableName);
            regions = routeCache.get(tableName);
        }

        if (regions == null || regions.isEmpty()) {
            // 仍然没有，返回 Master 地址
            return getMaster();
        }

        // 2. 根据 rowKey 找到对应的 Region
        RegionRouteInfo targetRegion = findRegionByKey(regions, rowKey);
        if (targetRegion != null) {
            // 如果有多个副本，选择负载最低的
            return selectBestServer(targetRegion);
        }

        // 3. 没找到，返回 Master
        return getMaster();
    }

    /**
     * 选择最佳的服务器（考虑负载）
     */
    private ServerAddress selectBestServer(RegionRouteInfo region) {
        // 目前只返回主副本，后续可以扩展为从多个副本中选择
        // 可以从 ZooKeeper 获取副本列表和负载信息
        return region.getPrimaryServer();
    }

    /**
     * 获取表的任意一个RegionServer（改进版）
     * 基于负载选择而非随机
     */
    public ServerAddress routeForRead(String tableName, byte[] rowKey) {
        return routeForRead(tableName, rowKey, defaultReadConsistency);
    }

    public ServerAddress routeForRead(String tableName, byte[] rowKey, ReadConsistency consistency) {
        List<RegionRouteInfo> regions = routeCache.get(tableName);
        if (regions == null || regions.isEmpty()) {
            refreshRouteCache(tableName);
            regions = routeCache.get(tableName);
        }

        if (regions == null || regions.isEmpty()) {
            return getMaster();
        }

        RegionRouteInfo targetRegion = findRegionByKey(regions, rowKey);
        if (targetRegion != null) {
            return selectBestReadServer(targetRegion, tableName, rowKey, consistency);
        }

        return getMaster();
    }

    private ServerAddress selectBestReadServer(RegionRouteInfo region) {
        return selectBestReadServer(region, region.getRegionId(), null, defaultReadConsistency);
    }

    private ServerAddress selectBestReadServer(RegionRouteInfo region,
                                               String tableName,
                                               byte[] rowKey,
                                               ReadConsistency consistency) {
        if (consistency == null) {
            consistency = defaultReadConsistency;
        }
        if (shouldPinToPrimary(tableName, rowKey) || consistency == ReadConsistency.PRIMARY_ONLY) {
            return region.getPrimaryServer();
        }

        List<ServerAddress> candidates = region.getReadableServers();
        if (candidates.isEmpty()) {
            return region.getPrimaryServer();
        }

        ServerAddress bestServer = consistency == ReadConsistency.PREFER_SECONDARY
            ? null
            : region.getPrimaryServer();
        long lowestLag = Long.MAX_VALUE;
        boolean allowStaleReplica = consistency == ReadConsistency.PREFER_SECONDARY
            || consistency == ReadConsistency.ANY_REPLICA;

        for (ServerAddress candidate : candidates) {
            if (candidate.equals(region.getPrimaryServer()) && consistency == ReadConsistency.PREFER_SECONDARY) {
                continue;
            }
            if (isCircuitOpen(region.getRegionId(), candidate)) {
                continue;
            }

            long lag = fetchReplicationLag(region.getRegionId(), candidate);
            if (!allowStaleReplica && !candidate.equals(region.getPrimaryServer()) && lag > freshReplicaMaxLag) {
                continue;
            }
            if (lag < lowestLag) {
                lowestLag = lag;
                bestServer = candidate;
            }
        }

        if (bestServer != null) {
            return bestServer;
        }
        if (consistency == ReadConsistency.PREFER_SECONDARY) {
            return region.getPrimaryServer();
        }
        return region.getPrimaryServer();
    }

    public ServerAddress routeToAny(String tableName) {
        List<RegionRouteInfo> regions = routeCache.get(tableName);
        if (regions == null || regions.isEmpty()) {
            refreshRouteCache(tableName);
            regions = routeCache.get(tableName);
        }

        if (regions != null && !regions.isEmpty()) {
            // 策略1：简单的轮询
            // return roundRobinSelect(regions);

            // 策略2：随机选择（当前实现，最简单）
            RegionRouteInfo region = regions.get(new Random().nextInt(regions.size()));
            return selectBestReadServer(region);

            // 策略3：选择Region数据量最小的（需要ZK支持）
            // return selectByRegionSize(regions);
        }

        // 返回默认地址
        return new ServerAddress("localhost", 16020);
    }

    /**
     * 轮询选择（均匀分布请求）
     */
    private int roundRobinIndex = 0;
    private ServerAddress roundRobinSelect(List<RegionRouteInfo> regions) {
        if (regions.isEmpty()) return null;
        int index = (roundRobinIndex++) % regions.size();
        return regions.get(index).getPrimaryServer();
    }

    /**
     * 根据 rowKey 查找对应的 Region
     */
    private RegionRouteInfo findRegionByKey(List<RegionRouteInfo> regions, byte[] rowKey) {
        for (RegionRouteInfo region : regions) {
            if (isKeyInRange(rowKey, region.getStartKey(), region.getEndKey())) {
                return region;
            }
        }
        return null;
    }

    /**
     * 检查 rowKey 是否在指定范围内
     */
    private boolean isKeyInRange(byte[] rowKey, byte[] startKey, byte[] endKey) {
        // startKey <= rowKey < endKey
        if (startKey != null && compareBytes(rowKey, startKey) < 0) {
            return false;
        }
        if (endKey != null && compareBytes(rowKey, endKey) >= 0) {
            return false;
        }
        return true;
    }

    /**
     * 比较字节数组
     */
    private int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    /**
     * 刷新路由缓存
     */
    public void refreshRouteCache(String tableName) {
        if (zkClient == null) {
            System.err.println("No ZooKeeper connection available");
            return;
        }

        try {
            // 从 ZooKeeper 获取该表的所有 Region 信息
            String tablePath = "/minisql/tables/" + tableName;

            // 检查表是否存在
            if (!zkClient.exists(tablePath)) {
                System.err.println("Table not found in ZooKeeper: " + tableName);
                return;
            }

            // 获取所有 Region
            String regionsPath = tablePath + "/regions";
            if (!zkClient.exists(regionsPath)) {
                System.err.println("No regions found for table: " + tableName);
                return;
            }
            ensureTableWatcher(tableName, regionsPath);

            List<String> regionIds = zkClient.getChildren(regionsPath);

            List<RegionRouteInfo> regions = new ArrayList<>();

            for (String regionId : regionIds) {
                try {
                    // 获取 Region 元数据
                    String regionPath = regionsPath + "/" + regionId;
                    byte[] regionData = zkClient.getData(regionPath);

                    Region region = parseRegionData(regionId, tableName, regionData);

                    if (region != null) {
                        // 获取 Region 的主副本服务器
                        String primaryPath = regionPath + "/primary";
                        String replicasPath = regionPath + "/replicas";
                        ServerAddress primaryServer = null;
                        List<ServerAddress> replicas = Collections.emptyList();

                        ensureRegionWatch(tableName, regionPath, primaryPath, replicasPath);

                        if (zkClient.exists(primaryPath)) {
                            byte[] primaryData = zkClient.getData(primaryPath);
                            primaryServer = parseServerAddress(new String(primaryData, java.nio.charset.StandardCharsets.UTF_8));
                        }

                        if (zkClient.exists(replicasPath)) {
                            byte[] replicasData = zkClient.getData(replicasPath);
                            replicas = parseReplicaAddresses(new String(replicasData, java.nio.charset.StandardCharsets.UTF_8));
                        }

                        if (primaryServer != null) {
                            regions.add(new RegionRouteInfo(region, primaryServer, replicas));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Failed to load region " + regionId + ": " + e.getMessage());
                }
            }

            // 更新缓存
            if (!regions.isEmpty()) {
                routeCache.put(tableName, regions);
                return;
            }

            if (refreshRouteCacheFromMaster(tableName)) {
                return;
            }

        } catch (Exception e) {
            System.err.println("Failed to refresh route cache: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 获取表的所有 Region
     */
    public List<Region> getRegionsForTable(String tableName) {
        // 确保缓存是最新的
        refreshRouteCache(tableName);

        List<RegionRouteInfo> routeInfos = routeCache.get(tableName);
        if (routeInfos == null) {
            return new ArrayList<>();
        }

        List<Region> regions = new ArrayList<>();
        for (RegionRouteInfo info : routeInfos) {
            Region region = new Region();
            region.setRegionId(info.getRegionId());
            region.setTableName(tableName);
            region.setStartKey(info.getStartKey());
            region.setEndKey(info.getEndKey());
            regions.add(region);
        }
        return regions;
    }

    /**
     * 解析 Region 数据
     * 格式：regionId|tableName|startKeyBase64|endKeyBase64
     * 注意：split 需要 limit=-1 来保留尾部的空字符串
     */
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

                List<ServerAddress> replicas = new ArrayList<>();
                for (CommonProto.ServerId replica : regionInfo.getReplicasList()) {
                    replicas.add(new ServerAddress(replica.getHost(), replica.getPort()));
                }

                ServerAddress primary = regionInfo.hasPrimary()
                    ? new ServerAddress(regionInfo.getPrimary().getHost(), regionInfo.getPrimary().getPort())
                    : null;
                if (primary == null && !replicas.isEmpty()) {
                    primary = replicas.get(0);
                }

                if (primary != null) {
                    regions.add(new RegionRouteInfo(region, primary, replicas));
                }
            }

            if (!regions.isEmpty()) {
                routeCache.put(tableName, regions);
                return true;
            }
        } catch (Exception e) {
            System.err.println("Failed to refresh route cache from Master: " + e.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }

        return false;
    }

    private long fetchReplicationLag(String regionId, ServerAddress candidate) {
        String cacheKey = buildLagCacheKey(regionId, candidate);
        long now = System.currentTimeMillis();
        LagSnapshot cached = lagCache.get(cacheKey);
        if (cached != null && cached.expiresAt >= now) {
            return cached.lagInEntries;
        }

        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(candidate.getHost(), candidate.getPort())
                .usePlaintext()
                .build();

            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                RegionServerServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(2, TimeUnit.SECONDS);

            RegionServerProto.GetReplicationLagResponse response = stub.getReplicationLag(
                RegionServerProto.GetReplicationLagRequest.newBuilder()
                    .setRegionId(regionId)
                    .build()
            );

            if (response.getStatus().getSuccess()) {
                lagCache.put(cacheKey, new LagSnapshot(response.getLagInEntries(), now + lagCacheTtlMs));
                circuitBreakerUntil.remove(cacheKey);
                return response.getLagInEntries();
            }
        } catch (Exception e) {
            circuitBreakerUntil.put(cacheKey, now + circuitBreakerWindowMs);
            System.err.println("Failed to fetch replication lag from " + candidate +
                " for region " + regionId + ": " + e.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }

        return Long.MAX_VALUE;
    }

    private boolean isCircuitOpen(String regionId, ServerAddress candidate) {
        Long until = circuitBreakerUntil.get(buildLagCacheKey(regionId, candidate));
        if (until == null) {
            return false;
        }
        if (until < System.currentTimeMillis()) {
            circuitBreakerUntil.remove(buildLagCacheKey(regionId, candidate));
            return false;
        }
        return true;
    }

    private boolean shouldPinToPrimary(String tableName, byte[] rowKey) {
        if (tableName == null || rowKey == null || writePrimaryPinMs <= 0) {
            return false;
        }
        String key = buildRecentWriteKey(tableName, rowKey);
        Long until = recentWrites.get(key);
        if (until == null) {
            return false;
        }
        if (until < System.currentTimeMillis()) {
            recentWrites.remove(key);
            return false;
        }
        return true;
    }

    private String buildRecentWriteKey(String tableName, byte[] rowKey) {
        return tableName + "|" + Base64.getEncoder().encodeToString(rowKey);
    }

    private String buildLagCacheKey(String regionId, ServerAddress candidate) {
        return regionId + "|" + candidate;
    }

    private Region parseRegionData(String regionId, String tableName, byte[] data) {
        try {
            String dataStr = new String(data, "UTF-8");
            // 使用 limit=-1 保留尾部的空字符串
            String[] parts = dataStr.split("\\|", -1);

            Region region = new Region();

            // 新格式：regionId|tableName|startKeyBase64|endKeyBase64
            if (parts.length >= 4) {
                region.setRegionId(parts[0]);
                region.setTableName(parts[1]);
                // 空字符串表示空字节数组
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
                // 数据格式异常，记录日志并返回 null
                System.err.println("Invalid region data format for " + tableName + "/" + regionId +
                                  ": expected 4 parts, got " + parts.length +
                                  ", data: " + dataStr);
                return null;
            }

            return region;
        } catch (Exception e) {
            System.err.println("Failed to parse region data for " + tableName + "/" + regionId + ": " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取Master地址
     */
    public ServerAddress getMaster() {
        if (masterAddress != null) {
            return masterAddress;
        }

        if (zkClient != null) {
            try {
                // 从 ZooKeeper 获取当前 Master
                String masterPath = Constants.ZK_MASTER_LEADER_PATH;
                if (zkClient.exists(masterPath)) {
                    byte[] masterData = zkClient.getData(masterPath);
                    String masterInfo = ZkPayloads.decodeLeaderAddress(masterData);
                    masterAddress = parseServerAddress(masterInfo);
                    return masterAddress;
                }
            } catch (Exception e) {
                System.err.println("Failed to get master from ZooKeeper: " + e.getMessage());
            }
        }

        // 默认地址
        return new ServerAddress("localhost", 16000);
    }

    /**
     * 解析服务器地址
     */
    private ServerAddress parseServerAddress(String address) {
        if (address == null || address.isEmpty()) {
            return null;
        }

        String[] parts = address.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 16020;
        return new ServerAddress(host, port);
    }

    /**
     * 设置 Master 地址
     */
    public void setMasterAddress(ServerAddress masterAddress) {
        this.masterAddress = masterAddress;
    }

    /**
     * 添加路由信息（用于测试或本地缓存）
     */
    public void addRoute(String tableName, Region region, ServerId primaryServer) {
        RegionRouteInfo newRoute = new RegionRouteInfo(region, primaryServer);
        routeCache.compute(tableName, (key, existing) -> {
            List<RegionRouteInfo> updated = existing == null
                ? new ArrayList<>()
                : new ArrayList<>(existing);
            updated.removeIf(route -> route.getRegionId().equals(newRoute.getRegionId()));
            updated.add(newRoute);
            return updated;
        });
    }

    /**
     * 获取路由缓存（包可见）
     */
    List<RegionRouteInfo> getRouteCache(String tableName) {
        return routeCache.get(tableName);
    }

    /**
     * 清除路由缓存
     */
    public void clearCache() {
        routeCache.clear();
        watchedTables.clear();
        watchedRegionPaths.clear();
    }

    private void ensureTableWatcher(String tableName, String regionsPath) {
        if (watchedTables.add(tableName)) {
            try {
                zkClient.watchChildren(regionsPath, (path, children) -> {
                    routeCache.remove(tableName);
                    watchedRegionPaths.removeIf(watchedPath -> watchedPath.startsWith(path));
                    try {
                        ensureTableWatcher(tableName, regionsPath);
                        refreshRouteCache(tableName);
                    } catch (Exception e) {
                        System.err.println("Failed to refresh table watcher cache for " + tableName + ": " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                watchedTables.remove(tableName);
                System.err.println("Failed to watch regions for table " + tableName + ": " + e.getMessage());
            }
        }
    }

    private void ensureRegionWatch(String tableName, String regionPath, String primaryPath, String replicasPath) {
        if (watchedRegionPaths.add(regionPath)) {
            try {
                zkClient.watchChildren(regionPath, (path, children) -> {
                    routeCache.remove(tableName);
                    watchedRegionPaths.remove(regionPath);
                    refreshRouteCache(tableName);
                });
            } catch (Exception e) {
                System.err.println("Failed to watch region path " + regionPath + ": " + e.getMessage());
            }
            registerNodeWatcher(primaryPath, tableName, regionPath);
            registerNodeWatcher(replicasPath, tableName, regionPath);
        }
    }

    private void registerNodeWatcher(String nodePath, String tableName, String regionPath) {
        try {
            if (!zkClient.exists(nodePath)) {
                return;
            }
            zkClient.watchNode(nodePath, (path, type) -> {
                routeCache.remove(tableName);
                watchedRegionPaths.remove(regionPath);
                try {
                    refreshRouteCache(tableName);
                } catch (Exception e) {
                    System.err.println("Failed to refresh route cache for watcher " + nodePath + ": " + e.getMessage());
                }
            });
        } catch (Exception e) {
            System.err.println("Failed to register watcher for " + nodePath + ": " + e.getMessage());
        }
    }

    private List<ServerAddress> parseReplicaAddresses(String encodedReplicas) {
        if (encodedReplicas == null || encodedReplicas.isBlank()) {
            return Collections.emptyList();
        }
        List<ServerAddress> replicas = new ArrayList<>();
        for (String address : encodedReplicas.split(",")) {
            ServerAddress replica = parseServerAddress(address.trim());
            if (replica != null) {
                replicas.add(replica);
            }
        }
        return replicas;
    }

    /**
     * 路由信息（包可见）
     */
    static class RegionRouteInfo {
        private final String regionId;
        private final byte[] startKey;
        private final byte[] endKey;
        private final ServerAddress primaryServer;
        private final List<ServerAddress> replicaServers;

        public RegionRouteInfo(Region region, ServerAddress primaryServer) {
            this(region, primaryServer, Collections.emptyList());
        }

        public RegionRouteInfo(Region region, ServerAddress primaryServer, List<ServerAddress> replicaServers) {
            this.regionId = region.getRegionId();
            this.startKey = region.getStartKey();
            this.endKey = region.getEndKey();
            this.primaryServer = primaryServer;
            this.replicaServers = normalizeReplicas(primaryServer, replicaServers);
        }

        public RegionRouteInfo(Region region, ServerId primaryServer) {
            this(region, new ServerAddress(primaryServer.getHost(), primaryServer.getPort()), toReplicaAddresses(region));
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

        public List<ServerAddress> getReplicaServers() {
            return replicaServers;
        }

        public List<ServerAddress> getReadableServers() {
            return replicaServers.isEmpty()
                ? Collections.singletonList(primaryServer)
                : replicaServers;
        }

        private static List<ServerAddress> toReplicaAddresses(Region region) {
            if (region.getReplicas() == null || region.getReplicas().isEmpty()) {
                return Collections.emptyList();
            }

            List<ServerAddress> replicas = new ArrayList<>();
            for (ServerId replica : region.getReplicas()) {
                replicas.add(new ServerAddress(replica.getHost(), replica.getPort()));
            }
            return replicas;
        }

        private static List<ServerAddress> normalizeReplicas(ServerAddress primaryServer, List<ServerAddress> replicaServers) {
            LinkedHashSet<ServerAddress> normalized = new LinkedHashSet<>();
            if (replicaServers != null) {
                normalized.addAll(replicaServers);
            }
            if (primaryServer != null) {
                normalized.add(primaryServer);
            }
            return new ArrayList<>(normalized);
        }
    }

    /**
     * 服务器地址
     */
    public enum ReadConsistency {
        PRIMARY_ONLY,
        PREFER_SECONDARY,
        SECONDARY_IF_FRESH,
        ANY_REPLICA
    }

    private static final class LagSnapshot {
        private final long lagInEntries;
        private final long expiresAt;

        private LagSnapshot(long lagInEntries, long expiresAt) {
            this.lagInEntries = lagInEntries;
            this.expiresAt = expiresAt;
        }
    }

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
