package com.minisql.master.state;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.storage.MySQLConfig;
import com.minisql.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群管理器
 * 负责模块：开发者 A
 */
public class ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private String serverKey(ServerId serverId) {
        return serverId.getHost() + ":" + serverId.getPort();
    }

    // 活跃的 RegionServer: serverId -> 服务器信息
    private final Map<String, ServerInfo> activeServers = new ConcurrentHashMap<>();

    // Region 分配信息：regionId -> 分配的 server
    private final Map<String, RegionAssignment> regionAssignments = new ConcurrentHashMap<>();

    // 表到 Region 的映射：tableName -> Set<regionId>
    private final Map<String, Set<String>> tableRegions = new ConcurrentHashMap<>();

    // Region 状态：regionId -> State
    private final Map<String, Region.State> regionStates = new ConcurrentHashMap<>();

    private final LoadBalancer loadBalancer;

    // ZooKeeper 客户端
    private ZkClient zkClient;

    // 副本管理：regionId -> List<ServerId>
    private final Map<String, List<ServerId>> regionReplicas = new ConcurrentHashMap<>();

    // 副本序列号跟踪：regionId -> Map<ServerId, sequenceId>
    private final Map<String, Map<ServerId, Long>> replicaSequenceIds = new ConcurrentHashMap<>();

    // Fencing Token 管理：regionId -> fencingToken（用于防止脑裂）
    private final Map<String, Long> regionFencingTokens = new ConcurrentHashMap<>();

    // RegionServer 的 MySQL 配置：serverId -> MySQL 配置
    private final Map<String, MySQLConfig> serverMySQLConfigs = new ConcurrentHashMap<>();

    public ClusterManager(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
    }

    /**
     * 设置 ZooKeeper 客户端
     */
    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
        initializeZkPaths();
    }

    /**
     * 初始化 ZooKeeper 路径
     */
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

    /**
     * 注册 RegionServer（带时间戳）
     */
    public void registerServer(ServerId serverId, long timestamp) {
        ServerInfo info = new ServerInfo(serverId, timestamp);
        activeServers.put(serverKey(serverId), info);
        logger.info("RegionServer registered: {}", serverId);
    }

    /**
     * 更新 Region 负载信息
     */
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

    /**
     * 更新服务器指标
     */
    public void updateServerMetrics(ServerId serverId, ServerMetrics metrics) {
        ServerInfo info = activeServers.get(serverKey(serverId));
        if (info != null) {
            info.setMetrics(metrics);
        }
    }

    /**
     * 获取 Region 的主服务器
     */
    public ServerId getPrimaryServerForRegion(String regionId) {
        RegionAssignment assignment = regionAssignments.get(regionId);
        return assignment != null ? assignment.getServerId() : null;
    }

    /**
     * 分配 Region 到指定服务器
     */
    public void assignRegionToServer(String regionId, ServerId serverId) {
        RegionAssignment assignment = new RegionAssignment(regionId, serverId);
        regionAssignments.put(regionId, assignment);
    }

    /**
     * 取消 Region 分配
     */
    public void unassignRegion(String regionId) {
        regionAssignments.remove(regionId);
        regionStates.remove(regionId);
    }

    public void removeRegionMetadata(String tableName, String regionId) {
        regionAssignments.remove(regionId);
        regionStates.remove(regionId);
        regionReplicas.remove(regionId);
        replicaSequenceIds.remove(regionId);
        regionFencingTokens.remove(regionId);
        for (ServerInfo info : activeServers.values()) {
            info.removeRegionLoad(regionId);
        }

        if (tableName != null) {
            Set<String> regions = tableRegions.get(tableName);
            if (regions != null) {
                regions.remove(regionId);
                if (regions.isEmpty()) {
                    tableRegions.remove(tableName);
                }
            }
        }
    }

    /**
     * 更新 Region 状态
     */
    public void updateRegionState(String regionId, Region.State state) {
        regionStates.put(regionId, state);
    }

    /**
     * 获取活跃服务器列表
     */
    public List<ServerInfo> getActiveServersList() {
        return new ArrayList<>(activeServers.values());
    }

    /**
     * 注册 RegionServer
     */
    public void registerServer(ServerId serverId) {
        ServerInfo info = new ServerInfo(serverId, System.currentTimeMillis());
        activeServers.put(serverKey(serverId), info);
        logger.info("RegionServer registered: {}", serverId);
    }

    /**
     * 处理心跳
     */
    public void handleHeartbeat(ServerId serverId, long timestamp) {
        ServerInfo info = activeServers.get(serverKey(serverId));
        if (info != null) {
            info.setLastHeartbeat(timestamp);
        }
    }

    /**
     * 分配 Region 到 RegionServer
     */
    public ServerId assignRegion(Region region) {
        // 使用负载均衡器选择最优服务器
        ServerId targetServer = loadBalancer.selectServerForRegion(region, new ArrayList<>(activeServers.values()));

        if (targetServer != null) {
            RegionAssignment assignment = new RegionAssignment(region.getRegionId(), targetServer);
            regionAssignments.put(region.getRegionId(), assignment);

            Set<String> regions = tableRegions.computeIfAbsent(region.getTableName(), k -> ConcurrentHashMap.newKeySet());
            regions.add(region.getRegionId());

            logger.info("Region {} assigned to {}", region.getRegionId(), targetServer);
        }

        return targetServer;
    }

    /**
     * 重新分配 Region（用于故障恢复）
     */
    public ServerId reassignRegion(String regionId) {
        RegionAssignment oldAssignment = regionAssignments.get(regionId);
        if (oldAssignment == null) {
            return null;
        }

        // 从故障节点移除
        Region region = new Region();  // 需要查询实际 Region 信息
        region.setRegionId(regionId);

        ServerId newServer = loadBalancer.selectServerForRegion(region, new ArrayList<>(activeServers.values()));
        if (newServer != null) {
            oldAssignment.setServerId(newServer);
            logger.info("Region {} reassigned to {}", regionId, newServer);
        }

        return newServer;
    }

    /**
     * 移除故障服务器
     */
    public void removeServer(ServerId serverId) {
        activeServers.remove(serverKey(serverId));
        logger.info("RegionServer removed: {}", serverId);
    }

    /**
     * 获取所有 Region 分配信息
     */
    public Map<String, RegionAssignment> getRegionAssignments() {
        return new ConcurrentHashMap<>(regionAssignments);
    }

    public List<String> getRegionsAssignedToServer(ServerId serverId) {
        List<String> regionIds = new ArrayList<>();
        if (serverId == null) {
            return regionIds;
        }
        for (Map.Entry<String, RegionAssignment> entry : regionAssignments.entrySet()) {
            RegionAssignment assignment = entry.getValue();
            if (assignment != null && serverId.equals(assignment.getServerId())) {
                regionIds.add(entry.getKey());
            }
        }
        return regionIds;
    }

    public List<String> getRegionsReplicatedOnServer(ServerId serverId) {
        List<String> regionIds = new ArrayList<>();
        if (serverId == null) {
            return regionIds;
        }
        for (Map.Entry<String, List<ServerId>> entry : regionReplicas.entrySet()) {
            List<ServerId> replicas = entry.getValue();
            if (replicas != null && replicas.contains(serverId)) {
                regionIds.add(entry.getKey());
            }
        }
        return regionIds;
    }

    /**
     * 获取单个 Region 分配信息
     */
    public RegionAssignment getRegionAssignment(String regionId) {
        return regionAssignments.get(regionId);
    }

    /**
     * 检查服务器是否活跃。
     * 由 ZooKeeper 临时节点事件驱动：
     * - onServerAdded → registerServer() → true
     * - onServerRemoved → removeServer() → false
     * 心跳不参与存活判断，只更新 metrics。
     */
    public boolean isServerActive(com.minisql.common.model.ServerId serverId) {
        return activeServers.containsKey(serverKey(serverId));
    }

    /**
     * 更新 Region 分配
     * 如果 assignment 不存在，则创建新的 assignment
     */
    public void updateRegionAssignment(String regionId, com.minisql.common.model.ServerId serverId) {
        RegionAssignment assignment = regionAssignments.get(regionId);
        if (assignment != null) {
            assignment.setServerId(serverId);
            logger.info("Region assignment updated: {} -> {}", regionId, serverId);
        } else {
            // assignment 不存在（可能在 failover 前被 unassignRegion 移除），创建新的
            regionAssignments.put(regionId, new RegionAssignment(regionId, serverId));
            logger.info("Region assignment created: {} -> {}", regionId, serverId);
        }
    }

    /**
     * 获取 Region 状态
     */
    public Region.State getRegionState(String regionId) {
        return regionStates.get(regionId);
    }

    /**
     * 获取所有活跃的 RegionServer
     */
    public Collection<ServerInfo> getActiveServers() {
        return activeServers.values();
    }

    /**
     * 获取表的所有 Region
     */
    public Set<String> getTableRegions(String tableName) {
        return tableRegions.getOrDefault(tableName, Collections.emptySet());
    }

    /**
     * 获取 Region 的所有副本服务器
     */
    public List<ServerId> getReplicaServers(String regionId) {
        return regionReplicas.getOrDefault(regionId, Collections.emptyList());
    }

    /**
     * 获取副本的序列号
     */
    public long getReplicaSequenceId(String regionId, ServerId replica) {
        Map<ServerId, Long> sequences = replicaSequenceIds.get(regionId);
        return sequences != null ? sequences.getOrDefault(replica, 0L) : 0L;
    }

    /**
     * 更新副本序列号
     */
    public void updateReplicaSequenceId(String regionId, ServerId replica, long sequenceId) {
        replicaSequenceIds.computeIfAbsent(regionId, k -> new ConcurrentHashMap<>())
                .put(replica, sequenceId);
    }

    /**
     * 提升副本为主副本
     */
    public void promoteReplicaToPrimary(String regionId, ServerId newPrimary) {
        RegionAssignment assignment = regionAssignments.get(regionId);
        if (assignment != null) {
            assignment.setServerId(newPrimary);
            logger.info("Promoted {} to primary for region {}", newPrimary, regionId);
        }
    }

    /**
     * 添加副本到 Region
     */
    public void addReplica(String regionId, ServerId replica) {
        List<ServerId> replicas = regionReplicas.computeIfAbsent(regionId, k -> Collections.synchronizedList(new ArrayList<>()));
        if (!replicas.contains(replica)) {
            replicas.add(replica);
        }
    }

    /**
     * 移除副本
     */
    public void removeReplica(String regionId, ServerId replica) {
        List<ServerId> replicas = regionReplicas.get(regionId);
        if (replicas != null) {
            replicas.remove(replica);
        }
    }

    /**
     * 更新 Fencing Token（故障转移时调用）
     */
    public void updateFencingToken(String regionId, long fencingToken) {
        regionFencingTokens.put(regionId, fencingToken);
        logger.info("Updated fencing token for region {} to {}", regionId, fencingToken);
    }

    /**
     * 获取当前 Fencing Token
     */
    public long getFencingToken(String regionId) {
        return regionFencingTokens.getOrDefault(regionId, 0L);
    }

    /**
     * 验证 Fencing Token 是否有效
     */
    public boolean validateFencingToken(String regionId, long token) {
        long currentToken = regionFencingTokens.getOrDefault(regionId, 0L);
        return token >= currentToken;
    }

    // ==================== MySQL 配置管理 ====================

    /**
     * 注册 RegionServer 的 MySQL 配置
     */
    public void registerMySQLConfig(ServerId serverId, MySQLConfig config) {
        serverMySQLConfigs.put(serverKey(serverId), config);
        logger.info("MySQL config registered for server: {}", serverId);
    }

    /**
     * 获取 RegionServer 的 MySQL 配置
     */
    public MySQLConfig getMySQLConfig(ServerId serverId) {
        return serverMySQLConfigs.get(serverKey(serverId));
    }

    /**
     * 移除 RegionServer 的 MySQL 配置
     */
    public void removeMySQLConfig(ServerId serverId) {
        serverMySQLConfigs.remove(serverKey(serverId));
    }

    /**
     * 服务器信息
     */
    public static class ServerInfo {
        private final ServerId serverId;
        private volatile long lastHeartbeat;
        private final Map<String, RegionLoad> regionLoads = new ConcurrentHashMap<>();
        private volatile ServerMetrics metrics;

        public ServerInfo(ServerId serverId, long lastHeartbeat) {
            this.serverId = serverId;
            this.lastHeartbeat = lastHeartbeat;
        }

        public ServerId getServerId() {
            return serverId;
        }

        public long getLastHeartbeat() {
            return lastHeartbeat;
        }

        public void setLastHeartbeat(long lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
        }

        public Map<String, RegionLoad> getRegionLoads() {
            return regionLoads;
        }

        public void updateRegionLoad(String regionId, RegionLoad load) {
            regionLoads.put(regionId, load);
        }

        public void removeRegionLoad(String regionId) {
            regionLoads.remove(regionId);
        }

        public ServerMetrics getMetrics() {
            return metrics;
        }

        public void setMetrics(ServerMetrics metrics) {
            this.metrics = metrics;
        }
    }

    /**
     * Region 负载信息
     */
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

    /**
     * 服务器指标
     */
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

    /**
     * Region 分配信息
     */
    public static class RegionAssignment {
        private final String regionId;
        private volatile ServerId serverId;

        public RegionAssignment(String regionId, ServerId serverId) {
            this.regionId = regionId;
            this.serverId = serverId;
        }

        public String getRegionId() {
            return regionId;
        }

        public ServerId getServerId() {
            return serverId;
        }

        public void setServerId(ServerId serverId) {
            this.serverId = serverId;
        }
    }
}
