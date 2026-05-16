package com.minisql.master.monitoring;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.master.rebalance.HotSpotCoordinator;
import com.minisql.master.rebalance.LoadBalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 集群监控服务
 *
 * 聚合集群状态信息，为监控 HTTP 接口提供数据。
 * 职责包括:
 *   - 集群概览（服务器数、Region数、QPS、告警数）
 *   - 服务器列表及其负载指标
 *   - Region 列表及其读写统计、复制延迟、热点评分
 *   - 表级汇总（Region数、读写量、错误数、热点分数）
 *   - SQL 指标统计（QPS、延迟分位数、错误率）
 *   - 集群事件时间线
 */
public class MonitoringService {

    private static final long FIVE_MINUTES_MS = 5L * 60L * 1000L;
    private static final long DAY_MS = 24L * 60L * 60L * 1000L;
    private static final long HEARTBEAT_STALE_THRESHOLD_MS = 10_000L;

    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final ReplicaMonitor replicaMonitor;
    private final ReplicaLifecycleManager lifecycleManager;
    private final SqlMetricsRegistry sqlMetricsRegistry;
    private final ClusterEventTimeline eventTimeline;
    private final LoadBalancer.LoadCalculator displayLoadCalculator;
    private volatile HotSpotCoordinator hotSpotCoordinator;

    public MonitoringService(ClusterManager clusterManager,
                             MetadataManager metadataManager,
                             ReplicaMonitor replicaMonitor,
                             ReplicaLifecycleManager lifecycleManager) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.replicaMonitor = replicaMonitor;
        this.lifecycleManager = lifecycleManager;
        this.sqlMetricsRegistry = new SqlMetricsRegistry();
        this.eventTimeline = new ClusterEventTimeline();
        this.displayLoadCalculator = new LoadBalancer.LoadCalculator();
        this.displayLoadCalculator.setClusterManager(clusterManager);
    }

    public void setHotSpotCoordinator(HotSpotCoordinator hotSpotCoordinator) {
        this.hotSpotCoordinator = hotSpotCoordinator;
    }

    public void recordSqlMetric(String sqlType, String tableName, boolean success, long latencyMs,
                                List<String> regionIds, String errorMessage, String source) {
        sqlMetricsRegistry.record(sqlType, tableName, success, latencyMs);
        if (!success) {
            recordEvent(
                "SQL_ERROR",
                "WARN",
                regionIds != null && !regionIds.isEmpty() ? regionIds.get(0) : null,
                tableName,
                source,
                null,
                "SQL request failed",
                errorMessage
            );
        }
    }

    public void recordEvent(String type, String severity, String regionId, String tableName,
                            String sourceServer, String targetServer, String message, String details) {
        eventTimeline.record(type, severity, regionId, tableName, sourceServer, targetServer, message, details);
    }

    public Map<String, Object> overview() {
        List<Map<String, Object>> serverList = servers();
        List<Map<String, Object>> regionList = regions();
        SqlMetricsRegistry.SqlMetricSummary summary = sqlMetricsRegistry.summarize(FIVE_MINUTES_MS);
        long offlineServers = serverList.stream().filter(server -> "offline".equals(server.get("status"))).count();
        long warningServers = serverList.stream().filter(server -> "warning".equals(server.get("status"))).count();
        long replicaAlerts = regionList.stream().filter(region ->
            ((Number) region.getOrDefault("replicationLag", 0L)).longValue() > 0L
        ).count();

        Map<String, Object> result = new HashMap<>();
        result.put("activeServers", serverList.size());
        result.put("offlineServers", offlineServers);
        result.put("warningServers", warningServers);
        result.put("regionCount", regionList.size());
        result.put("tableCount", metadataManager.getAllTables().size());
        result.put("replicaAlerts", replicaAlerts);
        result.put("currentQps", summary.getQps());
        result.put("errors24h", sqlMetricsRegistry.summarize(DAY_MS).getErrorCount());
        result.put("recentEvents", events(Collections.emptySet(), 10));
        return result;
    }

    public List<Map<String, Object>> servers() {
        Collection<ClusterManager.ServerInfo> activeServers = clusterManager.getActiveServers();
        List<Map<String, Object>> result = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (ClusterManager.ServerInfo info : activeServers) {
            Map<String, Object> row = new HashMap<>();
            row.put("serverId", info.getServerId() != null ? info.getServerId().getServerName() : null);
            row.put("lastHeartbeat", info.getLastHeartbeat());
            row.put("online", true);

            long staleMs = now - info.getLastHeartbeat();
            if (info.getLastHeartbeat() > 0 && staleMs > HEARTBEAT_STALE_THRESHOLD_MS) {
                row.put("status", "warning");
            } else {
                row.put("status", "online");
            }

            ClusterManager.ServerMetrics metrics = info.getMetrics();
            row.put("cpuUsage", metrics != null ? metrics.getCpuUsage() : 0.0);
            row.put("memoryUsage", metrics != null ? metrics.getMemoryUsage() : 0.0);
            row.put("availableSpace", metrics != null ? metrics.getAvailableSpace() : 0L);
            row.put("totalSpace", metrics != null ? metrics.getTotalSpace() : 0L);
            row.put("regionCount", info.getRegionLoads().size());

            long readRequests = 0L;
            long writeRequests = 0L;
            for (ClusterManager.RegionLoad load : info.getRegionLoads().values()) {
                readRequests += load.getReadRequests();
                writeRequests += load.getWriteRequests();
            }
            row.put("readRequests", readRequests);
            row.put("writeRequests", writeRequests);
            row.put("loadScore", displayLoadCalculator.calculateLoadScore(info));
            result.add(row);
        }

        for (ClusterManager.GraveyardEntry entry : clusterManager.getRemovedServers()) {
            Map<String, Object> row = new HashMap<>();
            row.put("serverId", entry.getServerId() != null ? entry.getServerId().getServerName() : null);
            row.put("lastHeartbeat", entry.getLastHeartbeat());
            row.put("online", false);
            row.put("status", "offline");
            row.put("removedAt", entry.getRemovedAt());
            ClusterManager.ServerMetrics metrics = entry.getLastMetrics();
            row.put("cpuUsage", metrics != null ? metrics.getCpuUsage() : 0.0);
            row.put("memoryUsage", metrics != null ? metrics.getMemoryUsage() : 0.0);
            row.put("availableSpace", metrics != null ? metrics.getAvailableSpace() : 0L);
            row.put("totalSpace", metrics != null ? metrics.getTotalSpace() : 0L);
            row.put("regionCount", entry.getRegionCount());
            row.put("readRequests", 0L);
            row.put("writeRequests", 0L);
            row.put("loadScore", 0.0);
            result.add(row);
        }

        result.sort(Comparator
            .comparingInt((Map<String, Object> row) -> statusOrder(String.valueOf(row.getOrDefault("status", "online"))))
            .thenComparing(row -> String.valueOf(row.get("serverId"))));
        return result;
    }

    private int statusOrder(String status) {
        if ("online".equals(status)) return 0;
        if ("warning".equals(status)) return 1;
        if ("offline".equals(status)) return 2;
        return 3;
    }

    public List<Map<String, Object>> regions() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (ClusterManager.ServerInfo info : clusterManager.getActiveServers()) {
            String serverName = info.getServerId() != null ? info.getServerId().getServerName() : null;
            for (Map.Entry<String, ClusterManager.RegionLoad> entry : info.getRegionLoads().entrySet()) {
                String regionId = entry.getKey();
                Region region = metadataManager.getRegion(regionId);
                if (region == null) {
                    continue;
                }
                ClusterManager.RegionLoad load = entry.getValue();
                ServerId primary = clusterManager.getPrimaryServerForRegion(regionId);
                boolean isPrimary = primary != null && primary.equals(info.getServerId());

                Map<String, Object> row = new HashMap<>();
                row.put("id", regionId + "@" + serverName);
                row.put("regionId", regionId);
                row.put("tableName", region.getTableName());
                row.put("serverId", serverName);
                row.put("primaryServer", primary != null ? primary.getServerName() : null);
                row.put("role", isPrimary ? "Primary" : "Replica");
                row.put("state", String.valueOf(clusterManager.getRegionState(regionId)));
                row.put("readRequests", load != null ? load.getReadRequests() : 0L);
                row.put("writeRequests", load != null ? load.getWriteRequests() : 0L);
                row.put("storeFileSize", load != null ? load.getStoreFileSize() : 0L);
                row.put("replicationLag", replicaLagForServer(regionId, info.getServerId()));
                result.add(row);
            }
        }
        result.sort(Comparator
            .comparing((Map<String, Object> row) -> String.valueOf(row.get("regionId")))
            .thenComparing(row -> String.valueOf(row.get("role")))
            .thenComparing(row -> String.valueOf(row.get("serverId"))));
        return result;
    }

    private List<Map<String, Object>> logicalRegions() {
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, HotSpotCoordinator.HotSpotInfo> currentHotSpots = hotSpotCoordinator != null
            ? hotSpotCoordinator.getCurrentHotSpots()
            : Collections.emptyMap();
        for (Region region : metadataManager.getAllRegions()) {
            if (region == null) {
                continue;
            }
            Map<String, Object> row = new HashMap<>();
            row.put("regionId", region.getRegionId());
            row.put("tableName", region.getTableName());
            ServerId primary = clusterManager.getPrimaryServerForRegion(region.getRegionId());
            row.put("primaryServer", primary != null ? primary.getServerName() : null);
            List<ServerId> replicas = clusterManager.getSecondaryServers(region.getRegionId());
            row.put("replicas", replicas == null
                ? Collections.emptyList()
                : replicas.stream().map(ServerId::getServerName).collect(Collectors.toList()));
            row.put("state", String.valueOf(clusterManager.getRegionState(region.getRegionId())));
            ClusterManager.RegionLoad load = resolveLoad(region.getRegionId());
            long readRequests = load != null ? load.getReadRequests() : 0L;
            long writeRequests = load != null ? load.getWriteRequests() : 0L;
            long storeFileSize = load != null ? load.getStoreFileSize() : 0L;
            long replicationLag = maxReplicationLag(region.getRegionId());
            HotSpotCoordinator.HotSpotInfo hotSpotInfo = currentHotSpots.get(region.getRegionId());
            row.put("readRequests", readRequests);
            row.put("writeRequests", writeRequests);
            row.put("storeFileSize", storeFileSize);
            row.put("replicationLag", replicationLag);
            row.put("hotspotDetected", hotSpotInfo != null);
            row.put("hotspotType", hotSpotInfo != null ? hotSpotInfo.getType().name() : null);
            row.put("hotspotScore", hotspotScore(region.getRegionId(), replicationLag));
            row.put("lifecycle", lifecycleStatuses(region.getRegionId()));
            result.add(row);
        }
        result.sort((left, right) -> Double.compare(
            ((Number) right.get("hotspotScore")).doubleValue(),
            ((Number) left.get("hotspotScore")).doubleValue()));
        return result;
    }

    public List<Map<String, Object>> tables() {
        Map<String, Long> tableErrors = sqlMetricsRegistry.tableErrorTotals(DAY_MS);
        Map<String, List<Map<String, Object>>> grouped = logicalRegions().stream()
            .collect(Collectors.groupingBy(region -> String.valueOf(region.get("tableName"))));
        List<Map<String, Object>> result = new ArrayList<>();
        for (Table table : metadataManager.getAllTables()) {
            List<Map<String, Object>> tableRegions = grouped.getOrDefault(table.getTableName(), Collections.emptyList());
            Map<String, Object> row = new HashMap<>();
            row.put("tableName", table.getTableName());
            row.put("regionCount", tableRegions.size());
            row.put("totalReadRequests", sum(tableRegions, "readRequests"));
            row.put("totalWriteRequests", sum(tableRegions, "writeRequests"));
            row.put("totalErrors", tableErrors.getOrDefault(table.getTableName(), 0L));
            row.put("hotspotScore", tableRegions.stream()
                .mapToDouble(region -> ((Number) region.get("hotspotScore")).doubleValue()).sum());
            List<Map<String, Object>> topHotRegions = new ArrayList<>(tableRegions);
            topHotRegions.sort((left, right) -> Double.compare(
                ((Number) right.get("hotspotScore")).doubleValue(),
                ((Number) left.get("hotspotScore")).doubleValue()));
            if (topHotRegions.size() > 3) {
                topHotRegions = new ArrayList<>(topHotRegions.subList(0, 3));
            }
            row.put("topHotRegions", topHotRegions);
            result.add(row);
        }
        result.sort((left, right) -> Double.compare(
            ((Number) right.get("hotspotScore")).doubleValue(),
            ((Number) left.get("hotspotScore")).doubleValue()));
        return result;
    }

    public Map<String, Object> sqlSummary(String window) {
        long windowMs = parseWindow(window);
        SqlMetricsRegistry.SqlMetricSummary summary = sqlMetricsRegistry.summarize(windowMs);
        Map<String, Object> result = new HashMap<>();
        result.put("window", window);
        result.put("requestCount", summary.getRequestCount());
        result.put("successCount", summary.getSuccessCount());
        result.put("errorCount", summary.getErrorCount());
        result.put("qps", summary.getQps());
        result.put("avgLatencyMs", summary.getAvgLatencyMs());
        result.put("p95LatencyMs", summary.getP95LatencyMs());
        result.put("readCount", summary.getReadCount());
        result.put("writeCount", summary.getWriteCount());
        result.put("points", summary.getPoints());
        return result;
    }

    public List<Map<String, Object>> hotspots(String scope, String window) {
        List<Map<String, Object>> values = "table".equalsIgnoreCase(scope) ? tables() : regions();
        return values.size() > 10 ? new ArrayList<>(values.subList(0, 10)) : values;
    }

    public List<Map<String, Object>> regionReplicas() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (ClusterManager.ServerInfo info : clusterManager.getActiveServers()) {
            String serverName = info.getServerId() != null ? info.getServerId().getServerName() : null;
            double serverLoadScore = displayLoadCalculator.calculateLoadScore(info);
            for (Map.Entry<String, ClusterManager.RegionLoad> entry : info.getRegionLoads().entrySet()) {
                String regionId = entry.getKey();
                Region region = metadataManager.getRegion(regionId);
                if (region == null) {
                    continue;
                }
                ClusterManager.RegionLoad load = entry.getValue();
                
                Map<String, Object> row = new HashMap<>();
                row.put("regionId", regionId);
                row.put("tableName", region.getTableName());
                row.put("serverId", serverName);
                
                ServerId primary = clusterManager.getPrimaryServerForRegion(regionId);
                boolean isPrimary = primary != null && primary.equals(info.getServerId());
                row.put("role", isPrimary ? "Primary" : "Replica");
                
                row.put("storeFileSize", load.getStoreFileSize());
                row.put("readRequests", load.getReadRequests());
                row.put("writeRequests", load.getWriteRequests());
                
                // 计算负载分数
                double sizeWeight = load.getStoreFileSize() / (100 * 1024 * 1024.0);
                double requestWeight = (load.getReadRequests() + load.getWriteRequests()) / 10000.0;
                double regionLoadScore = sizeWeight + requestWeight;
                
                row.put("serverLoadScore", serverLoadScore);
                row.put("regionLoadScore", regionLoadScore);
                
                result.add(row);
            }
        }
        result.sort((left, right) -> {
            int roleCmp = String.valueOf(left.get("role")).compareTo(String.valueOf(right.get("role")));
            if (roleCmp != 0) return roleCmp;
            int regionCmp = String.valueOf(left.get("regionId")).compareTo(String.valueOf(right.get("regionId")));
            if (regionCmp != 0) return regionCmp;
            return String.valueOf(left.get("serverId")).compareTo(String.valueOf(right.get("serverId")));
        });
        return result;
    }

    public List<Map<String, Object>> events(Set<String> types, int limit) {
        return eventTimeline.query(types, limit).stream().map(event -> {
            Map<String, Object> row = new HashMap<>();
            row.put("timestamp", event.getTimestamp());
            row.put("type", event.getType());
            row.put("severity", event.getSeverity());
            row.put("regionId", event.getRegionId());
            row.put("tableName", event.getTableName());
            row.put("sourceServer", event.getSourceServer());
            row.put("targetServer", event.getTargetServer());
            row.put("message", event.getMessage());
            row.put("details", event.getDetails());
            return row;
        }).collect(Collectors.toList());
    }

    public Map<String, Object> health() {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "UP");
        result.put("timestamp", System.currentTimeMillis());
        result.put("eventCount", eventTimeline.latest(Integer.MAX_VALUE).size());
        return result;
    }

    public ReplicaMonitor.FailoverCallback replicaEventCallback() {
        return new ReplicaMonitor.FailoverCallback() {
            @Override
            public void onReplicaFailed(String regionId, ServerId failedReplica) {
                recordEvent("SERVER_OFFLINE", "WARN", regionId, tableName(regionId),
                    failedReplica != null ? failedReplica.getServerName() : null,
                    null, "Replica reported offline", null);
            }

            @Override
            public void onReplicaLagging(String regionId, ServerId laggingReplica, long lagMs) {
                recordEvent("REPLICA_LAGGING", "WARN", regionId, tableName(regionId),
                    laggingReplica != null ? laggingReplica.getServerName() : null,
                    null, "Replica lagging", "lag=" + lagMs);
            }

            @Override
            public void onReplicaRecovered(String regionId, ServerId recoveredReplica) {
                recordEvent("REPLICA_RECOVERED", "INFO", regionId, tableName(regionId),
                    recoveredReplica != null ? recoveredReplica.getServerName() : null,
                    null, "Replica recovered", null);
            }
        };
    }

    private ClusterManager.RegionLoad resolveLoad(String regionId) {
        for (ClusterManager.ServerInfo info : clusterManager.getActiveServers()) {
            ClusterManager.RegionLoad load = info.getRegionLoads().get(regionId);
            if (load != null) {
                return load;
            }
        }
        return null;
    }

    private long maxReplicationLag(String regionId) {
        long maxLag = 0L;
        for (ReplicaInfo replica : replicaMonitor.getReplicas(regionId)) {
            if (replica.isPrimary()) {
                continue;
            }
            maxLag = Math.max(maxLag, replica.getReplicationLag());
        }
        return maxLag;
    }

    private long replicaLagForServer(String regionId, ServerId serverId) {
        for (ReplicaInfo replica : replicaMonitor.getReplicas(regionId)) {
            if (replica.getServerId() != null && replica.getServerId().equals(serverId)) {
                return replica.getReplicationLag();
            }
        }
        return 0L;
    }

    private List<Map<String, Object>> lifecycleStatuses(String regionId) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (ReplicaLifecycleManager.ReplicaLifecycleStatus status : lifecycleManager.getAllStatuses().values()) {
            if (!regionId.equals(status.getRegionId())) {
                continue;
            }
            Map<String, Object> row = new HashMap<>();
            row.put("serverId", status.getServerId() != null ? status.getServerId().getServerName() : null);
            row.put("state", status.getState().name());
            row.put("detail", status.getDetail());
            row.put("updatedAt", status.getUpdatedAt());
            result.add(row);
        }
        result.sort(Comparator.comparing(row -> String.valueOf(row.get("serverId"))));
        return result;
    }

    private long parseWindow(String window) {
        if (window == null) {
            return FIVE_MINUTES_MS;
        }
        switch (window.toLowerCase(Locale.ROOT)) {
            case "1m":
                return 60_000L;
            case "5m":
                return FIVE_MINUTES_MS;
            case "1h":
                return 60L * 60L * 1000L;
            case "24h":
                return DAY_MS;
            default:
                return FIVE_MINUTES_MS;
        }
    }

    private double hotspotScore(String regionId, long replicationLag) {
        if (hotSpotCoordinator == null) {
            return 0.0;
        }
        return hotSpotCoordinator.calculateDisplayScore(regionId, replicationLag);
    }

    private long sum(List<Map<String, Object>> rows, String key) {
        return rows.stream().mapToLong(row -> ((Number) row.getOrDefault(key, 0L)).longValue()).sum();
    }

    private String tableName(String regionId) {
        Region region = metadataManager.getRegion(regionId);
        return region != null ? region.getTableName() : null;
    }

    /** 合并快照，用于 SSE 推送 */
    public Map<String, Object> snapshot() {
        Map<String, Object> result = new HashMap<>();
        result.put("overview", overview());
        result.put("servers", servers());
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }

    /** 桥接事件订阅到 ClusterEventTimeline */
    public void setEventSubscriber(Consumer<ClusterEventTimeline.ClusterEvent> subscriber) {
        eventTimeline.subscribe(subscriber);
    }

    /** 移除事件订阅 */
    public void removeEventSubscriber(Consumer<ClusterEventTimeline.ClusterEvent> subscriber) {
        eventTimeline.unsubscribe(subscriber);
    }
}
