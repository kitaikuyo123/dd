package com.minisql.master.state;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群管理器
 * Region 放置数据（primary、replicas）统一委托给 MetadataManager 中的 Region 模型。
 */
public class ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private String serverKey(ServerId serverId) {
        return serverId.getHost() + ":" + serverId.getPort();
    }

    // 活跃的 RegionServer: serverKey -> 服务器信息
    private final Map<String, ServerInfo> activeServers = new ConcurrentHashMap<>();

    // 已下线的 RegionServer（墓地），保留 5 分钟用于前端展示
    private final Map<String, GraveyardEntry> removedServers = new ConcurrentHashMap<>();
    private static final long GRAVEYARD_TTL_MS = 5L * 60L * 1000L;

    // 负载均衡器
    private final LoadBalancer loadBalancer;

    // 元数据管理器 —— Region 放置数据的唯一真相源
    private MetadataManager metadataManager;

    // ZooKeeper 客户端
    private ZkClient zkClient;

    // 副本序列号跟踪：regionId -> Map<ServerId, sequenceId>
    private final Map<String, Map<ServerId, Long>> replicaSequenceIds = new ConcurrentHashMap<>();

    // Fencing Token 管理：regionId -> fencingToken
    private final Map<String, Long> regionFencingTokens = new ConcurrentHashMap<>();

    public ClusterManager(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
        this.metadataManager = new MetadataManager();
    }

    public ClusterManager(LoadBalancer loadBalancer, MetadataManager metadataManager) {
        this.loadBalancer = loadBalancer;
        this.metadataManager = metadataManager;
    }

    public void setMetadataManager(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
        initializeZkPaths();
    }

    private void initializeZkPaths() {
        if (zkClient == null) return;
        try {
            String regionServersPath = "/minisql/regionservers";
            if (!zkClient.exists(regionServersPath)) {
                zkClient.createPersistent(regionServersPath, new byte[0]);
            }
        } catch (Exception e) {
            logger.warn("Failed to initialize ZK paths: {}", e.getMessage(), e);
        }
    }

    // ==================== RegionServer 管理 ====================

    public void registerServer(ServerId serverId, long timestamp) {
        removedServers.remove(serverKey(serverId));
        ServerInfo info = new ServerInfo(serverId, timestamp);
        activeServers.put(serverKey(serverId), info);
        logger.info("RegionServer registered: {}", serverId);
    }

    public void registerServer(ServerId serverId) {
        removedServers.remove(serverKey(serverId));
        ServerInfo info = new ServerInfo(serverId, System.currentTimeMillis());
        activeServers.put(serverKey(serverId), info);
        logger.info("RegionServer registered: {}", serverId);
    }

    public void handleHeartbeat(ServerId serverId, long timestamp) {
        ServerInfo info = activeServers.get(serverKey(serverId));
        if (info != null) {
            info.setLastHeartbeat(timestamp);
        }
    }

    public void removeServer(ServerId serverId) {
        String key = serverKey(serverId);
        ServerInfo removed = activeServers.remove(key);
        if (removed != null) {
            removedServers.put(key, new GraveyardEntry(
                removed.getServerId(),
                System.currentTimeMillis(),
                removed.getLastHeartbeat(),
                removed.getMetrics(),
                removed.getRegionLoads().size()
            ));
        }
        purgeGraveyard();
        logger.info("RegionServer removed: {}", serverId);
    }

    private void purgeGraveyard() {
        long cutoff = System.currentTimeMillis() - GRAVEYARD_TTL_MS;
        removedServers.entrySet().removeIf(e -> e.getValue().getRemovedAt() < cutoff);
    }

    public List<GraveyardEntry> getRemovedServers() {
        purgeGraveyard();
        return new ArrayList<>(removedServers.values());
    }

    public boolean isServerActive(ServerId serverId) {
        return activeServers.containsKey(serverKey(serverId));
    }

    public Collection<ServerInfo> getActiveServers() {
        return new ArrayList<>(activeServers.values());
    }

    public List<ServerInfo> getActiveServersList() {
        return new ArrayList<>(activeServers.values());
    }

    // ==================== Region 放置 —— 委托给 Region 模型 ====================

    private Region requireRegion(String regionId) {
        if (metadataManager == null) {
            logger.warn("MetadataManager not set, cannot access region {}", regionId);
            return null;
        }
        return metadataManager.getRegion(regionId);
    }

    private Region ensureRegion(String regionId) {
        Region region = requireRegion(regionId);
        if (region == null) {
            region = new Region();
            region.setRegionId(regionId);
            metadataManager.registerRegion(region);
        }
        return region;
    }

    public ServerId getPrimaryServerForRegion(String regionId) {
        Region region = requireRegion(regionId);
        return region != null ? region.getPrimary() : null;
    }

    /**
     * 获取 Region 的所有副本服务器（包含 primary）。
     * 这是拥有该 region 的完整服务器列表。
     */
    public List<ServerId> getReplicaServers(String regionId) {
        Region region = requireRegion(regionId);
        if (region == null || region.getReplicas() == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(region.getReplicas());
    }

    /**
     * 获取 Region 的次级副本服务器（不含 primary），用于前端展示等场景。
     */
    public List<ServerId> getSecondaryServers(String regionId) {
        Region region = requireRegion(regionId);
        if (region == null || region.getReplicas() == null) {
            return Collections.emptyList();
        }
        ServerId primary = region.getPrimary();
        List<ServerId> result = new ArrayList<>();
        for (ServerId s : region.getReplicas()) {
            if (!s.equals(primary)) {
                result.add(s);
            }
        }
        return result;
    }

    /**
     * 设置 region 的 primary server（仅修改内存）。
     * 调用方必须随后调用 metadataManager.registerRegionForTable() 持久化到 ZK。
     */
    public void assignRegionToServer(String regionId, ServerId serverId) {
        Region region = ensureRegion(regionId);
        region.setPrimary(serverId);
        region.addReplica(serverId);
    }

    public void unassignRegion(String regionId) {
        Region region = requireRegion(regionId);
        if (region != null) {
            region.setPrimary(null);
            region.setState(Region.State.OFFLINE);
        }
    }

    public void removeRegionMetadata(String tableName, String regionId) {
        Region region = requireRegion(regionId);
        if (region != null) {
            region.setPrimary(null);
            region.getReplicas().clear();
            region.setState(Region.State.CLOSED);
        }
        replicaSequenceIds.remove(regionId);
        regionFencingTokens.remove(regionId);
        for (ServerInfo info : activeServers.values()) {
            info.removeRegionLoad(regionId);
        }
        if (metadataManager != null) {
            metadataManager.removeRegion(regionId);
        }
    }

    public void updateRegionAssignment(String regionId, ServerId serverId) {
        Region region = ensureRegion(regionId);
        region.setPrimary(serverId);
        region.addReplica(serverId);
        logger.info("Region assignment updated: {} -> {}", regionId, serverId);
    }

    /**
     * 选择目标 server 并设置 primary（仅修改内存）。
     * 调用方必须随后调用 metadataManager.registerRegionForTable() 持久化到 ZK。
     */
    public ServerId assignRegion(Region region) {
        if (metadataManager != null && metadataManager.getRegion(region.getRegionId()) == null) {
            metadataManager.registerRegion(region);
        }
        ServerId targetServer = loadBalancer.selectServerForRegion(region, new ArrayList<>(activeServers.values()));
        if (targetServer != null) {
            region.setPrimary(targetServer);
            region.addReplica(targetServer);
            logger.info("Region {} assigned to {}", region.getRegionId(), targetServer);
        }
        return targetServer;
    }

    /**
     * 重新选择 server 并设置 primary（仅修改内存）。
     * 调用方必须随后调用 metadataManager.registerRegionForTable() 持久化到 ZK。
     */
    public ServerId reassignRegion(String regionId) {
        Region region = requireRegion(regionId);
        if (region == null) {
            return null;
        }
        ServerId newServer = loadBalancer.selectServerForRegion(region, new ArrayList<>(activeServers.values()));
        if (newServer != null) {
            region.setPrimary(newServer);
            region.addReplica(newServer);
            logger.info("Region {} reassigned to {}", regionId, newServer);
        }
        return newServer;
    }

    public Map<String, ServerId> getRegionAssignments() {
        Map<String, ServerId> result = new LinkedHashMap<>();
        if (metadataManager == null) return result;
        for (Region region : metadataManager.getAllRegions()) {
            if (region.getPrimary() != null) {
                result.put(region.getRegionId(), region.getPrimary());
            }
        }
        return result;
    }

    public ServerId getRegionAssignment(String regionId) {
        return getPrimaryServerForRegion(regionId);
    }

    public List<String> getRegionsAssignedToServer(ServerId serverId) {
        List<String> regionIds = new ArrayList<>();
        if (serverId == null || metadataManager == null) return regionIds;
        for (Region region : metadataManager.getAllRegions()) {
            if (serverId.equals(region.getPrimary())) {
                regionIds.add(region.getRegionId());
            }
        }
        return regionIds;
    }

    public List<String> getRegionsReplicatedOnServer(ServerId serverId) {
        List<String> regionIds = new ArrayList<>();
        if (serverId == null || metadataManager == null) return regionIds;
        for (Region region : metadataManager.getAllRegions()) {
            if (region.getReplicas() != null && region.getReplicas().contains(serverId)) {
                regionIds.add(region.getRegionId());
            }
        }
        return regionIds;
    }

    // ==================== Region 状态 ====================

    public void updateRegionState(String regionId, Region.State state) {
        Region region = ensureRegion(regionId);
        region.setState(state);
    }

    public Region.State getRegionState(String regionId) {
        Region region = requireRegion(regionId);
        return region != null ? region.getState() : null;
    }

    // ==================== 表-Region 索引 ====================

    public Set<String> getTableRegions(String tableName) {
        if (metadataManager == null) return Collections.emptySet();
        Set<String> result = new LinkedHashSet<>();
        for (Region region : metadataManager.getRegionsForTable(tableName)) {
            result.add(region.getRegionId());
        }
        return result;
    }

    // ==================== 副本管理 ====================

    public void addReplica(String regionId, ServerId replica) {
        Region region = ensureRegion(regionId);
        region.addReplica(replica);
    }

    public void removeReplica(String regionId, ServerId replica) {
        Region region = requireRegion(regionId);
        if (region != null) {
            region.removeReplica(replica);
        }
    }

    public void promoteReplicaToPrimary(String regionId, ServerId newPrimary) {
        Region region = ensureRegion(regionId);
        region.setPrimary(newPrimary);
        region.addReplica(newPrimary);
        logger.info("Promoted {} to primary for region {}", newPrimary, regionId);
    }

    // ==================== Fencing Token ====================

    public void updateFencingToken(String regionId, long fencingToken) {
        regionFencingTokens.put(regionId, fencingToken);
        logger.info("Updated fencing token for region {} to {}", regionId, fencingToken);
    }

    public long getFencingToken(String regionId) {
        return regionFencingTokens.getOrDefault(regionId, 0L);
    }

    public boolean validateFencingToken(String regionId, long token) {
        long currentToken = regionFencingTokens.getOrDefault(regionId, 0L);
        return token >= currentToken;
    }

    // ==================== 副本序列号 ====================

    public long getReplicaSequenceId(String regionId, ServerId replica) {
        Map<ServerId, Long> sequences = replicaSequenceIds.get(regionId);
        return sequences != null ? sequences.getOrDefault(replica, 0L) : 0L;
    }

    public void updateReplicaSequenceId(String regionId, ServerId replica, long sequenceId) {
        replicaSequenceIds.computeIfAbsent(regionId, k -> new ConcurrentHashMap<>())
                .put(replica, sequenceId);
    }

    // ==================== 服务器负载信息 ====================

    public void updateRegionLoad(ServerId serverId, String regionId, RegionLoad load) {
        ServerInfo info = activeServers.get(serverKey(serverId));
        if (info != null) {
            info.updateRegionLoad(regionId, load);
        }
    }

    public void removeRegionLoad(ServerId serverId, String regionId) {
        ServerInfo info = activeServers.get(serverKey(serverId));
        if (info != null) {
            info.removeRegionLoad(regionId);
        }
    }

    public void updateServerMetrics(ServerId serverId, ServerMetrics metrics) {
        ServerInfo info = activeServers.get(serverKey(serverId));
        if (info != null) {
            info.setMetrics(metrics);
        }
    }

    // ==================== 内部类 ====================

    public static class ServerInfo {
        private final ServerId serverId;
        private volatile long lastHeartbeat;
        private final Map<String, RegionLoad> regionLoads = new ConcurrentHashMap<>();
        private volatile ServerMetrics metrics;

        public ServerInfo(ServerId serverId, long lastHeartbeat) {
            this.serverId = serverId;
            this.lastHeartbeat = lastHeartbeat;
        }

        public ServerId getServerId() { return serverId; }
        public long getLastHeartbeat() { return lastHeartbeat; }
        public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
        public Map<String, RegionLoad> getRegionLoads() { return Collections.unmodifiableMap(regionLoads); }
        public void updateRegionLoad(String regionId, RegionLoad load) { regionLoads.put(regionId, load); }
        public void removeRegionLoad(String regionId) { regionLoads.remove(regionId); }
        public ServerMetrics getMetrics() { return metrics; }
        public void setMetrics(ServerMetrics metrics) { this.metrics = metrics; }
    }

    public static class RegionLoad {
        private String regionId;
        private long readRequests;
        private long writeRequests;
        private long storeFileSize;
        private long memStoreSize;

        public String getRegionId() { return regionId; }
        public void setRegionId(String regionId) { this.regionId = regionId; }
        public long getReadRequests() { return readRequests; }
        public void setReadRequests(long readRequests) { this.readRequests = readRequests; }
        public long getWriteRequests() { return writeRequests; }
        public void setWriteRequests(long writeRequests) { this.writeRequests = writeRequests; }
        public long getStoreFileSize() { return storeFileSize; }
        public void setStoreFileSize(long storeFileSize) { this.storeFileSize = storeFileSize; }
        public long getMemStoreSize() { return memStoreSize; }
        public void setMemStoreSize(long memStoreSize) { this.memStoreSize = memStoreSize; }
    }

    public static class ServerMetrics {
        private double cpuUsage;
        private double memoryUsage;
        private long availableSpace;
        private long totalSpace;

        public double getCpuUsage() { return cpuUsage; }
        public void setCpuUsage(double cpuUsage) { this.cpuUsage = cpuUsage; }
        public double getMemoryUsage() { return memoryUsage; }
        public void setMemoryUsage(double memoryUsage) { this.memoryUsage = memoryUsage; }
        public long getAvailableSpace() { return availableSpace; }
        public void setAvailableSpace(long availableSpace) { this.availableSpace = availableSpace; }
        public long getTotalSpace() { return totalSpace; }
        public void setTotalSpace(long totalSpace) { this.totalSpace = totalSpace; }
    }

    public static class GraveyardEntry {
        private final ServerId serverId;
        private final long removedAt;
        private final long lastHeartbeat;
        private final ServerMetrics lastMetrics;
        private final int regionCount;

        public GraveyardEntry(ServerId serverId, long removedAt, long lastHeartbeat,
                              ServerMetrics lastMetrics, int regionCount) {
            this.serverId = serverId;
            this.removedAt = removedAt;
            this.lastHeartbeat = lastHeartbeat;
            this.lastMetrics = lastMetrics;
            this.regionCount = regionCount;
        }

        public ServerId getServerId() { return serverId; }
        public long getRemovedAt() { return removedAt; }
        public long getLastHeartbeat() { return lastHeartbeat; }
        public ServerMetrics getLastMetrics() { return lastMetrics; }
        public int getRegionCount() { return regionCount; }
    }
}
