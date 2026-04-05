package com.minisql.master.rpc;

import com.minisql.common.model.*;
import com.minisql.common.proto.*;
import com.minisql.master.detect.*;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.rebalance.*;
import com.minisql.master.recover.*;
import com.minisql.master.state.*;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.zookeeper.DistributedLock;
import com.minisql.zookeeper.ZkClient;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Master gRPC 服务实现
 * 提供集群管理、元数据管理、负载均衡等服务
 */
public class MasterServiceImpl extends MasterServiceGrpc.MasterServiceImplBase {

    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final LoadBalancer loadBalancer;
    private final RegionSplitCoordinator splitCoordinator;
    private final RegionMergeCoordinator mergeCoordinator;
    private final HotSpotCoordinator hotSpotCoordinator;
    private final ClusterDetector regionSplitDetector;
    private final ClusterDetector hotSpotDetector;
    private final ReplicationCoordinator replicationCoordinator;
    private final ReplicaMonitor replicaMonitor;
    private final FailoverCoordinator failoverCoordinator;
    private final RecoveryCoordinator recoveryCoordinator;
    private final ReplicaLifecycleManager lifecycleManager;
    private final RegionMigrationCoordinator migrationCoordinator;
    private final RegionServerCommandClient commandClient;
    private final java.util.concurrent.ExecutorService serverFailureRecoveryExecutor;
    private final java.util.Set<String> recoveringRegions = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private MonitoringService monitoringService;
    private ScheduledExecutorService balanceScheduler;
    private volatile boolean leader = true;
    private volatile ZkClient zkClient;

    // Master 状态
    private final String clusterId;
    private final AtomicLong serverSequenceId = new AtomicLong(0);

    public MasterServiceImpl(ClusterManager clusterManager,
                             MetadataManager metadataManager,
                             LoadBalancer loadBalancer,
                             ReplicationCoordinator replicationCoordinator,
                             ReplicaMonitor replicaMonitor,
                             FailoverCoordinator failoverCoordinator,
                             RecoveryCoordinator recoveryCoordinator,
                             ReplicaLifecycleManager lifecycleManager) {
        this(clusterManager, metadataManager, loadBalancer, replicationCoordinator, replicaMonitor,
            failoverCoordinator, recoveryCoordinator, lifecycleManager, new GrpcRegionServerCommandClient(clusterManager));
    }

    public MasterServiceImpl(ClusterManager clusterManager,
                             MetadataManager metadataManager,
                             LoadBalancer loadBalancer,
                             ReplicationCoordinator replicationCoordinator,
                             ReplicaMonitor replicaMonitor,
                             FailoverCoordinator failoverCoordinator,
                             RecoveryCoordinator recoveryCoordinator,
                             ReplicaLifecycleManager lifecycleManager,
                             RegionServerCommandClient commandClient) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.loadBalancer = loadBalancer;
        this.replicationCoordinator = replicationCoordinator;
        this.replicaMonitor = replicaMonitor;
        this.failoverCoordinator = failoverCoordinator;
        this.recoveryCoordinator = recoveryCoordinator;
        this.lifecycleManager = lifecycleManager;
        this.commandClient = commandClient;
        this.migrationCoordinator = new RegionMigrationCoordinator(
            clusterManager, metadataManager, commandClient, lifecycleManager);
        this.splitCoordinator = new RegionSplitCoordinator(clusterManager, metadataManager, loadBalancer, commandClient);
        this.mergeCoordinator = new RegionMergeCoordinator(clusterManager, metadataManager);
        this.hotSpotCoordinator = new HotSpotCoordinator(clusterManager, metadataManager, splitCoordinator, recoveryCoordinator);
        this.regionSplitDetector = new RegionSplitDetector(clusterManager, metadataManager, splitCoordinator);
        this.hotSpotDetector = new HotSpotDetector(hotSpotCoordinator);
        this.serverFailureRecoveryExecutor = java.util.concurrent.Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "ServerFailure-Recovery");
            t.setDaemon(true);
            return t;
        });
        this.clusterId = UUID.randomUUID().toString();

        // 启动自动合并检查
        mergeCoordinator.start();

        // 启动定期负载均衡检查（每 5 分钟一次）
        startLoadBalanceScheduler();
    }

    public void setMonitoringService(MonitoringService monitoringService) {
        this.monitoringService = monitoringService;
        this.migrationCoordinator.setMonitoringService(monitoringService);
        this.splitCoordinator.setMonitoringService(monitoringService);
        this.mergeCoordinator.setMonitoringService(monitoringService);
    }

    public void setLeader(boolean leader) {
        this.leader = leader;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
        this.migrationCoordinator.setZkClient(zkClient);
        this.splitCoordinator.setZkClient(zkClient);
        this.mergeCoordinator.setZkClient(zkClient);
    }

    /**
     * 启动定期负载均衡调度器
     */
    private void startLoadBalanceScheduler() {
        balanceScheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "LoadBalance-Scheduler");
            t.setDaemon(true);
            return t;
        });

        // 每 5 分钟检查一次负载平衡
        balanceScheduler.scheduleAtFixedRate(
            this::runLoadBalance,
            5, 5, TimeUnit.MINUTES
        );

        System.out.println("LoadBalance scheduler started, interval: 5 minutes");
    }

    public ClusterDetector getRegionSplitDetector() {
        return regionSplitDetector;
    }

    public ClusterDetector getHotSpotDetector() {
        return hotSpotDetector;
    }

    public void shutdown() {
        if (balanceScheduler != null) {
            balanceScheduler.shutdownNow();
        }
        serverFailureRecoveryExecutor.shutdownNow();
        mergeCoordinator.stop();
    }

    /**
     * 执行负载均衡检查和迁移
     */
    private void runLoadBalance() {
        try {
            List<ClusterManager.ServerInfo> servers = new ArrayList<>(clusterManager.getActiveServers());
            if (servers.isEmpty()) {
                return;
            }

            // 计算需要的平衡动作
            List<LoadBalancer.BalanceAction> actions = loadBalancer.computeBalanceActions(servers);

            if (!actions.isEmpty()) {
                System.out.println("Load balance triggered: " + actions.size() + " actions");
                executeBalanceActions(actions);
            }
        } catch (Exception e) {
            System.err.println("Error running load balance: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 执行负载均衡动作
     */
    private void executeBalanceActions(List<LoadBalancer.BalanceAction> actions) {
        for (LoadBalancer.BalanceAction action : actions) {
            try {
                if (action == null) {
                    System.err.println("Skipping null balance action");
                    continue;
                }
                migrationCoordinator.execute(action);
            } catch (Exception e) {
                System.err.println("Error executing balance action for " + action.getRegionId() + ": " + e.getMessage());
            }
        }
    }

    private void executeHotSpotActions(List<HotSpotCoordinator.HotSpotAction> actions) {
        for (HotSpotCoordinator.HotSpotAction action : actions) {
            try {
                if (action == null) {
                    System.err.println("Skipping null hot spot action");
                    continue;
                }
                recordEvent("HOTSPOT_DETECTED", "INFO", action.getRegionId(), null,
                    action.getSourceServer(), action.getTargetServer(),
                    "Hot spot action queued: " + action.getType(), null);
                hotSpotCoordinator.executeAction(action);
            } catch (Exception e) {
                System.err.println("Error executing hot spot action for " + action.getRegionId() +
                    ": " + e.getMessage());
            }
        }
    }

    public void handleHotSpotAction(HotSpotCoordinator.HotSpotAction action) {
        executeHotSpotActions(java.util.Collections.singletonList(action));
    }

    public void handleServerFailureEvent(ServerFailedEvent event) {
        if (event == null) {
            return;
        }
        for (String regionId : event.getAffectedRegionIds()) {
            boolean primaryFailed = removeFailedReplicaFromMetadata(regionId, event.getFailedServer());
            lifecycleManager.transition(regionId, event.getFailedServer(),
                ReplicaLifecycleManager.ReplicaLifecycleState.OFFLINE,
                "Server failure event received");
            serverFailureRecoveryExecutor.submit(() ->
                recoverRegionAfterServerFailure(regionId, event.getFailedServer(), primaryFailed));
        }
    }

    public boolean handleRegionSplitSuggestion(RegionSplitSuggestedEvent event) {
        return splitCoordinator.scheduleSplit(
            event.getRegionId(),
            event.getTableName(),
            event.getServerId(),
            event.getRegionLoad()
        );
    }

    private void recoverRegionAfterServerFailure(String regionId, ServerId failedServer, boolean primaryFailed) {
        if (!recoveringRegions.add(regionId)) {
            return;
        }

        try {
            System.out.println("Starting recovery for region via event: " + regionId);
            recordEvent("RECOVERY_STARTED", "INFO", regionId, null, failedServer, null,
                "Server failure event started region recovery", null);
            Region region = metadataManager.getRegion(regionId);
            if (region == null) {
                System.err.println("Region not found in metadata: " + regionId);
                return;
            }

            clusterManager.updateRegionState(regionId, Region.State.OFFLINE);
            if (primaryFailed) {
                lifecycleManager.transition(regionId, failedServer,
                    ReplicaLifecycleManager.ReplicaLifecycleState.OFFLINE,
                    "Primary failure detected by ServerFailureEvent");
                clusterManager.unassignRegion(regionId);
                if (failoverCoordinator == null) {
                    throw new IllegalStateException("FailoverCoordinator is required for primary recovery");
                }
                failoverCoordinator.triggerEmergencyFailover(regionId);
            }

            ServerId newServer = selectNewServerForReplica(regionId, failedServer);
            if (newServer != null) {
                lifecycleManager.transition(regionId, newServer,
                    ReplicaLifecycleManager.ReplicaLifecycleState.BOOTSTRAPPING,
                    "ServerFailureEvent scheduling replacement replica");
                if (recoveryCoordinator == null) {
                    throw new IllegalStateException("RecoveryCoordinator is required for replica bootstrap");
                }
                recoveryCoordinator.bootstrapReplica(regionId, newServer);
            }

            clusterManager.updateRegionState(regionId, Region.State.OPEN);
            recordEvent("RECOVERY_COMPLETED", "INFO", regionId, null, failedServer, null,
                "Server failure event completed region recovery", null);
        } catch (Exception e) {
            lifecycleManager.transition(regionId, failedServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.FAILED,
                "ServerFailureEvent recovery failed: " + e.getMessage());
            recordEvent("RECOVERY_FAILED", "ERROR", regionId, null, failedServer, null,
                "Server failure event recovery failed", e.getMessage());
            System.err.println("Failed to recover region " + regionId + ": " + e.getMessage());
        } finally {
            recoveringRegions.remove(regionId);
        }
    }

    private boolean removeFailedReplicaFromMetadata(String regionId, ServerId failedServer) {
        if (regionId == null || failedServer == null) {
            return false;
        }

        clusterManager.removeReplica(regionId, failedServer);
        replicaMonitor.removeReplica(regionId, failedServer);

        Region region = metadataManager.getRegion(regionId);
        if (region == null) {
            return false;
        }

        boolean changed = false;
        if (region.getReplicas() != null && region.getReplicas().contains(failedServer)) {
            region.removeReplica(failedServer);
            changed = true;
        }

        ServerId currentPrimary = clusterManager.getPrimaryServerForRegion(regionId);
        boolean primaryFailed = failedServer.equals(currentPrimary);

        if (changed && !primaryFailed) {
            metadataManager.registerRegionForTable(region, region.getPrimary());
        }
        return primaryFailed;
    }

    private ServerId selectNewServerForReplica(String regionId, ServerId excludeServer) {
        List<ServerId> currentReplicas = clusterManager.getReplicaServers(regionId);
        List<ClusterManager.ServerInfo> activeServers = new ArrayList<>(clusterManager.getActiveServers());
        List<ClusterManager.ServerInfo> candidates = new ArrayList<>();

        for (ClusterManager.ServerInfo server : activeServers) {
            boolean alreadyReplica = false;
            for (ServerId replica : currentReplicas) {
                if (replica.equals(server.getServerId())) {
                    alreadyReplica = true;
                    break;
                }
            }
            if (!alreadyReplica && !server.getServerId().equals(excludeServer)) {
                candidates.add(server);
            }
        }

        if (candidates.isEmpty()) {
            System.out.println("No available server for new replica of region: " + regionId);
            return null;
        }

        candidates.sort((a, b) -> Long.compare(calculateServerLoad(a), calculateServerLoad(b)));
        return candidates.get(0).getServerId();
    }

    private long calculateServerLoad(ClusterManager.ServerInfo server) {
        long load = 0;
        for (ClusterManager.RegionLoad regionLoad : server.getRegionLoads().values()) {
            load += regionLoad.getMemStoreSize() + regionLoad.getStoreFileSize();
        }
        return load;
    }

    // ==================== RegionServer 管理 ====================

    @Override
    public void registerRegionServer(MasterProto.RegisterRequest request,
                                      StreamObserver<MasterProto.RegisterResponse> responseObserver) {
        try {
            if (!ensureLeader(responseObserver, MasterProto.RegisterResponse::newBuilder)) {
                return;
            }
            ServerId serverId = convertServerId(request.getServerId());
            long seqId = serverSequenceId.incrementAndGet();

            System.out.println("Received register request from: " + serverId);

            clusterManager.registerServer(serverId, request.getTimestamp());
            System.out.println("[REGISTER] clusterId=" + clusterId +
                ", activeServers=" + clusterManager.getActiveServersList().stream()
                    .map(info -> info.getServerId().toString())
                    .collect(Collectors.toList()));

            // 注册 MySQL 配置
            if (request.hasMysqlConfig()) {
                CommonProto.MySQLConfig mysqlConfigProto = request.getMysqlConfig();
                com.minisql.storage.MySQLConfig mysqlConfig = com.minisql.storage.MySQLConfig.builder(
                        mysqlConfigProto.getUrl(),
                        mysqlConfigProto.getUser(),
                        mysqlConfigProto.getPassword()
                ).maxPoolSize(mysqlConfigProto.getMaxPoolSize()).build();
                clusterManager.registerMySQLConfig(serverId, mysqlConfig);
                System.out.println("MySQL config registered for server: " + serverId);
            }

            MasterProto.RegisterResponse response = MasterProto.RegisterResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .setClusterId(clusterId)
                .setServerSequenceId(seqId)
                .build();

            System.out.println("Sending register response to: " + serverId);
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            if (recoveryCoordinator != null) {
                recoveryCoordinator.reconcileRecoveredServer(serverId);
            }

            System.out.println("RegionServer registered: " + serverId);
            recordEvent("SERVER_REGISTERED", "INFO", null, null, serverId, null,
                "RegionServer registered", null);
        } catch (Exception e) {
            System.err.println("Error registering RegionServer: " + e.getMessage());
            e.printStackTrace();
            responseObserver.onNext(MasterProto.RegisterResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void reportSqlMetrics(MasterProto.ReportSqlMetricsRequest request,
                                 StreamObserver<MasterProto.ReportSqlMetricsResponse> responseObserver) {
        try {
            if (!ensureLeader(responseObserver, MasterProto.ReportSqlMetricsResponse::newBuilder)) {
                return;
            }
            if (monitoringService != null) {
                monitoringService.recordSqlMetric(
                    request.getSqlType(),
                    request.getTableName(),
                    request.getSuccess(),
                    request.getLatencyMs(),
                    request.getRegionIdsList(),
                    request.getErrorMessage(),
                    request.getSource()
                );
            }
            responseObserver.onNext(MasterProto.ReportSqlMetricsResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.ReportSqlMetricsResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void heartbeat(MasterProto.HeartbeatRequest request,
                          StreamObserver<MasterProto.HeartbeatResponse> responseObserver) {
        try {
            if (!ensureLeader(responseObserver, MasterProto.HeartbeatResponse::newBuilder)) {
                return;
            }
            ServerId serverId = convertServerId(request.getServerId());

            clusterManager.handleHeartbeat(serverId, request.getTimestamp());

            // Heartbeat only carries runtime metrics. ZooKeeper membership is authoritative
            // for server liveness and failover triggers.
            if (request.getRegionLoadsCount() > 0) {
                for (MasterProto.RegionLoad load : request.getRegionLoadsList()) {
                    clusterManager.updateRegionLoad(serverId, load.getRegionId(),
                        convertRegionLoad(load));
                    hotSpotCoordinator.recordRegionLoad(load.getRegionId(), convertRegionLoad(load));

                    // 更新副本监控心跳
                    if (replicaMonitor != null) {
                        ReplicationLagSnapshot lagSnapshot = fetchReplicationLag(serverId, load.getRegionId());
                        replicaMonitor.updateHeartbeat(load.getRegionId(), serverId, lagSnapshot.lagInEntries);
                        clusterManager.updateReplicaSequenceId(
                            load.getRegionId(),
                            serverId,
                            lagSnapshot.lastAppliedSequenceId
                        );
                    }
                }
            }

            // 处理服务器指标
            if (request.hasMetrics()) {
                clusterManager.updateServerMetrics(serverId, convertMetrics(request.getMetrics()));
            }

            // 心跳仅用于确认存活，不下发命令
            // 所有管理操作通过独立 gRPC 调用完成（openRegion, closeRegion, splitRegion, mergeRegion 等）
            MasterProto.HeartbeatResponse response = MasterProto.HeartbeatResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            System.err.println("[HEARTBEAT] Error: " + e.getMessage());
            e.printStackTrace();
            responseObserver.onNext(MasterProto.HeartbeatResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    // ==================== Region 管理 ====================

    @Override
    public void reportRegionStatus(MasterProto.RegionStatusRequest request,
                                    StreamObserver<MasterProto.RegionStatusResponse> responseObserver) {
        try {
            if (!ensureLeader(responseObserver, MasterProto.RegionStatusResponse::newBuilder)) {
                return;
            }
            String regionId = request.getRegionId();
            CommonProto.RegionState state = request.getState();

            clusterManager.updateRegionState(regionId, convertRegionState(state));

            if (state == CommonProto.RegionState.CLOSED && !request.getErrorMessage().isEmpty()) {
                System.err.println("Region " + regionId + " failed: " + request.getErrorMessage());
                // 触发故障恢复
                handleRegionFailure(regionId);
            }

            responseObserver.onNext(MasterProto.RegionStatusResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.RegionStatusResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getRegionLocation(MasterProto.GetLocationRequest request,
                                   StreamObserver<MasterProto.GetLocationResponse> responseObserver) {
        try {
            String tableName = request.getTableName();
            byte[] rowKey = request.getRowKey().toByteArray();

            Region region = metadataManager.findRegion(tableName, rowKey);

            if (region == null) {
                responseObserver.onNext(MasterProto.GetLocationResponse.newBuilder()
                    .setStatus(createErrorStatus("Region not found for rowKey"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            ServerId primaryServer = clusterManager.getPrimaryServerForRegion(region.getRegionId());

            // 获取 MySQL 配置
            CommonProto.MySQLConfig mysqlConfigProto = null;
            if (primaryServer != null) {
                com.minisql.storage.MySQLConfig mysqlConfig = clusterManager.getMySQLConfig(primaryServer);
                if (mysqlConfig != null) {
                    mysqlConfigProto = CommonProto.MySQLConfig.newBuilder()
                        .setUrl(mysqlConfig.getJdbcUrl())
                        .setUser(mysqlConfig.getUsername())
                        .setPassword(mysqlConfig.getPassword())
                        .setMaxPoolSize(mysqlConfig.getMaxPoolSize())
                        .build();
                }
            }

            MasterProto.GetLocationResponse.Builder responseBuilder = MasterProto.GetLocationResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .setRegion(convertRegionInfo(region, mysqlConfigProto))
                .setServerId(convertToProtoServerId(primaryServer));

            if (mysqlConfigProto != null) {
                responseBuilder.setMysqlConfig(mysqlConfigProto);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.GetLocationResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    // ==================== 表管理 ====================

    @Override
    public void createTable(MasterProto.CreateTableRequest request,
                            StreamObserver<MasterProto.CreateTableResponse> responseObserver) {
        boolean tablePersisted = false;
        List<Region> createdRegions = new ArrayList<>();
        DistributedLock lock = null;
        try {
            if (!ensureLeader(responseObserver, MasterProto.CreateTableResponse::newBuilder)) {
                return;
            }
            CommonProto.TableSchema protoSchema = request.getSchema();
            String tableName = protoSchema.getTableName();
            lock = acquireTableLock(tableName);

            System.out.println("[CREATE TABLE] Start creating table: " + tableName);
            System.out.println("[CREATE TABLE] Columns count: " + protoSchema.getColumnsCount());

            if (metadataManager.tableExists(tableName)) {
                responseObserver.onNext(MasterProto.CreateTableResponse.newBuilder()
                    .setStatus(createErrorStatus("Table already exists: " + tableName))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            // 创建表元数据
            Table table = new Table();
            table.setTableName(tableName);

            // 添加列定义
            if (protoSchema.getColumnsCount() > 0) {
                for (CommonProto.ColumnSchema columnSchema : protoSchema.getColumnsList()) {
                    com.minisql.common.model.Column column = new com.minisql.common.model.Column();
                    column.setName(columnSchema.getName());
                    String columnTypeStr = columnSchema.getType();
                    com.minisql.common.model.Column.ColumnType columnType = convertColumnType(columnTypeStr);
                    System.out.println("[CREATE TABLE] Column: " + columnSchema.getName() +
                        ", Type: " + columnTypeStr + " -> " + columnType);
                    column.setType(columnType);
                    column.setLength(columnSchema.getMaxLength());
                    column.setNullable(columnSchema.getNullable());
                    table.addColumn(column);
                }
            } else {
                System.out.println("[CREATE TABLE] WARNING: No columns defined!");
            }

            // 设置主键（如果有）- 支持复合主键
            // 优先使用 partitionKeys + clusteringKeys
            if (protoSchema.getPartitionKeysCount() > 0) {
                table.setPartitionKeys(protoSchema.getPartitionKeysList());
                System.out.println("[CREATE TABLE] Partition keys: " + protoSchema.getPartitionKeysList());
            }
            if (protoSchema.getClusteringKeysCount() > 0) {
                table.setClusteringKeys(protoSchema.getClusteringKeysList());
                System.out.println("[CREATE TABLE] Clustering keys: " + protoSchema.getClusteringKeysList());
            }
            // 向后兼容：如果只有 primaryKey，使用它
            String primaryKey = protoSchema.getPrimaryKey();
            if (primaryKey != null && !primaryKey.isEmpty()) {
                table.setPrimaryKey(primaryKey);
                System.out.println("[CREATE TABLE] Primary key (legacy): " + primaryKey);
            } else if (table.getColumns() != null && !table.getColumns().isEmpty()) {
                // 如果没有显式指定主键，默认使用第一个列作为主键
                String firstColumn = table.getColumns().get(0).getName();
                table.setPrimaryKey(firstColumn);
                System.out.println("[CREATE TABLE] No primary key specified, using first column: " + firstColumn);
            }


            // 预分区
            int numRegions = request.getNumRegions() > 0 ? request.getNumRegions() : 1;

            // 处理 startKey 和 endKey：proto 默认返回空数组而不是 null
            // proto3 bytes 类型没有 hasXxx() 方法，需要通过 isEmpty() 判断
            byte[] startKey = !request.getStartKey().isEmpty()
                ? request.getStartKey().toByteArray()
                : null;
            byte[] endKey = !request.getEndKey().isEmpty()
                ? request.getEndKey().toByteArray()
                : null;

            List<Region> regions = splitIntoRegions(tableName, numRegions, startKey, endKey);
            System.out.println("[CREATE TABLE] Split into " + regions.size() + " regions");

            // 检查可用的 RegionServer
            Collection<ClusterManager.ServerInfo> activeServers = clusterManager.getActiveServers();
            System.out.println("[CREATE TABLE] Active RegionServers count: " + activeServers.size());
            System.out.println("[CREATE TABLE] clusterId=" + clusterId +
                ", activeServers=" + activeServers.stream()
                    .map(server -> server.getServerId().toString())
                    .collect(Collectors.toList()));
            for (ClusterManager.ServerInfo server : activeServers) {
                System.out.println("[CREATE TABLE]   - " + server.getServerId());
            }
            if (activeServers.isEmpty()) {
                throw new IllegalStateException("No active RegionServer available for table " + tableName);
            }
            metadataManager.createTable(table);
            tablePersisted = true;
            System.out.println("[CREATE TABLE] Table metadata created successfully");

            // 分配 Region 到 RegionServer（带副本）
            List<CommonProto.RegionInfo> regionInfos = new ArrayList<>();
            for (Region region : regions) {
                // 根据 replicationFactor 选择多个 RegionServer
                // replicationFactor 默认为 3（1 个 Primary + 2 个 Secondary）
                int replicationFactor = table.getProperties() != null
                    ? table.getProperties().getReplicationFactor()
                    : 3;
                List<ServerId> selectedServers = selectServersForReplication(
                    region, new ArrayList<>(activeServers), replicationFactor);

                if (selectedServers.size() >= 1) {
                    ServerId primaryServer = selectedServers.get(0);
                    region.setPrimary(primaryServer);
                    region.setReplicas(new ArrayList<>(selectedServers));

                    System.out.println("[CREATE TABLE] Region " + region.getRegionId() +
                        " -> Primary: " + primaryServer +
                        ", Replicas: " + (selectedServers.size() > 1 ? selectedServers.subList(1, selectedServers.size()) : "none"));

                    // 分配主副本到主服务器
                    clusterManager.assignRegionToServer(region.getRegionId(), primaryServer);

                    // 使用新的基于表的路径注册 Region，同时写入主副本信息
                    metadataManager.registerRegionForTable(region, primaryServer);
                    createdRegions.add(region);

                    // 同步通知主 RegionServer 打开 Region（等待完成后再继续）
                    if (!notifyServerOpenRegionSync(primaryServer, region)) {
                        throw new RuntimeException("Failed to open region on server: " + primaryServer);
                    }

                    // 创建副本组（如果选择了多个服务器）
                    if (selectedServers.size() > 1 && replicationCoordinator != null) {
                        replicationCoordinator.createReplicaGroup(region, selectedServers);
                        System.out.println("[CREATE TABLE] Replica group created for region " +
                            region.getRegionId() + " with " + selectedServers.size() + " servers");
                        if (selectedServers.size() < replicationFactor) {
                            System.out.println("[CREATE TABLE] WARNING: Region " + region.getRegionId() +
                                " initialized with " + selectedServers.size() +
                                " replicas, below target replication factor " + replicationFactor);
                        }
                        if (recoveryCoordinator != null) {
                            for (int i = 1; i < selectedServers.size(); i++) {
                                ServerId replicaServer = selectedServers.get(i);
                                System.out.println("[CREATE TABLE] Bootstrapping replica " + replicaServer +
                                    " for region " + region.getRegionId());
                                recoveryCoordinator.bootstrapReplicaSync(region.getRegionId(), replicaServer);
                            }
                        }
                    } else if (selectedServers.size() == 1) {
                        System.out.println("[CREATE TABLE] WARNING: Region " + region.getRegionId() +
                            " initialized with primary only; no secondary replica available at create time");
                    }

                    // 获取 MySQL 配置
                    com.minisql.storage.MySQLConfig mysqlConfig = clusterManager.getMySQLConfig(primaryServer);
                    CommonProto.MySQLConfig mysqlConfigProto = null;
                    if (mysqlConfig != null) {
                        mysqlConfigProto = CommonProto.MySQLConfig.newBuilder()
                            .setUrl(mysqlConfig.getJdbcUrl())
                            .setUser(mysqlConfig.getUsername())
                            .setPassword(mysqlConfig.getPassword())
                            .setMaxPoolSize(mysqlConfig.getMaxPoolSize())
                            .build();
                    }
                    regionInfos.add(convertRegionInfo(region, mysqlConfigProto));
                } else {
                    // 如果没有可用服务器，仍然注册 Region 但不写主副本信息
                    throw new IllegalStateException("No server available for region " + region.getRegionId());
                }
            }

            MasterProto.CreateTableResponse response = MasterProto.CreateTableResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .addAllRegions(regionInfos)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            System.out.println("[CREATE TABLE] Table created successfully: " + tableName);
        } catch (Exception e) {
            for (Region region : createdRegions) {
                try {
                    java.util.LinkedHashSet<ServerId> targetServers = new java.util.LinkedHashSet<>();
                    if (region.getPrimary() != null) {
                        targetServers.add(region.getPrimary());
                    }
                    if (region.getReplicas() != null) {
                        targetServers.addAll(region.getReplicas());
                    }
                    for (ServerId serverId : targetServers) {
                        if (!notifyServerCloseRegionSync(serverId, region.getRegionId(), false)) {
                            System.err.println("[CREATE TABLE] Failed to rollback region close for " +
                                region.getRegionId() + " on " + serverId);
                        }
                    }
                    clusterManager.removeRegionMetadata(region.getTableName(), region.getRegionId());
                    if (replicaMonitor != null) {
                        replicaMonitor.removeRegion(region.getRegionId());
                    }
                    if (lifecycleManager != null) {
                        lifecycleManager.removeRegion(region.getRegionId());
                    }
                    if (replicationCoordinator != null) {
                        replicationCoordinator.removeReplicaGroup(region.getRegionId());
                    }
                    metadataManager.removeRegion(region.getRegionId());
                } catch (Exception cleanupError) {
                    System.err.println("[CREATE TABLE] Failed to rollback region " +
                        region.getRegionId() + ": " + cleanupError.getMessage());
                }
            }
            if (tablePersisted) {
                try {
                    metadataManager.deleteTable(request.getSchema().getTableName());
                } catch (Exception cleanupError) {
                    System.err.println("[CREATE TABLE] Failed to rollback table " +
                        request.getSchema().getTableName() + ": " + cleanupError.getMessage());
                }
            }
            System.err.println("[CREATE TABLE] ERROR: " + e.getMessage());
            e.printStackTrace();
            responseObserver.onNext(MasterProto.CreateTableResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        } finally {
            releaseLock(lock);
        }
    }

    @Override
    public void deleteTable(MasterProto.DeleteTableRequest request,
                            StreamObserver<MasterProto.DeleteTableResponse> responseObserver) {
        DistributedLock lock = null;
        try {
            if (!ensureLeader(responseObserver, MasterProto.DeleteTableResponse::newBuilder)) {
                return;
            }
            String tableName = request.getTableName();
            lock = acquireTableLock(tableName);

            if (!metadataManager.tableExists(tableName)) {
                responseObserver.onNext(MasterProto.DeleteTableResponse.newBuilder()
                    .setStatus(createErrorStatus("Table not found: " + tableName))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            java.util.Collection<Region> regions = metadataManager.getRegionsForTable(tableName);
            java.util.List<String> closeFailures = new java.util.ArrayList<>();

            for (Region region : regions) {
                java.util.LinkedHashSet<ServerId> targetServers = new java.util.LinkedHashSet<>();
                if (region.getPrimary() != null) {
                    targetServers.add(region.getPrimary());
                }
                if (region.getReplicas() != null) {
                    targetServers.addAll(region.getReplicas());
                }
                for (ServerId serverId : targetServers) {
                    if (!notifyServerCloseRegionSync(serverId, region.getRegionId(), true)) {
                        closeFailures.add(region.getRegionId() + "@" + serverId.getServerName());
                    }
                }
            }

            if (!closeFailures.isEmpty()) {
                throw new IllegalStateException("Failed to close regions before deleting table " + tableName +
                    ": " + closeFailures);
            }

            for (Region region : regions) {
                clusterManager.removeRegionMetadata(region.getTableName(), region.getRegionId());
                if (replicaMonitor != null) {
                    replicaMonitor.removeRegion(region.getRegionId());
                }
                if (lifecycleManager != null) {
                    lifecycleManager.removeRegion(region.getRegionId());
                }
                if (replicationCoordinator != null) {
                    replicationCoordinator.removeReplicaGroup(region.getRegionId());
                }
                metadataManager.removeRegion(region.getRegionId());
            }

            metadataManager.deleteTable(tableName);

            responseObserver.onNext(MasterProto.DeleteTableResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build());
            responseObserver.onCompleted();

            System.out.println("Table deleted: " + tableName);
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.DeleteTableResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        } finally {
            releaseLock(lock);
        }
    }

    @Override
    public void listTables(MasterProto.ListTablesRequest request,
                           StreamObserver<MasterProto.ListTablesResponse> responseObserver) {
        try {
            // 获取所有表
            java.util.Collection<Table> tables = metadataManager.getAllTables();
            List<CommonProto.TableSchema> protoTables = new ArrayList<>();

            // 遍历所有表并转换为 protobuf 格式
            for (Table table : tables) {
                CommonProto.TableSchema.Builder schemaBuilder = CommonProto.TableSchema.newBuilder()
                        .setTableName(table.getTableName());

                // 添加列信息（如果有）
                if (table.getColumns() != null) {
                    for (com.minisql.common.model.Column column : table.getColumns()) {
                        CommonProto.ColumnSchema colSchema = CommonProto.ColumnSchema.newBuilder()
                                .setName(column.getName())
                                .setType(column.getType().toString())
                                .setNullable(column.isNullable())
                                .build();
                        schemaBuilder.addColumns(colSchema);
                    }
                }

                protoTables.add(schemaBuilder.build());
            }

            MasterProto.ListTablesResponse response = MasterProto.ListTablesResponse.newBuilder()
                    .setStatus(createSuccessStatus())
                    .addAllTables(protoTables)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.ListTablesResponse.newBuilder()
                    .setStatus(createErrorStatus(e.getMessage()))
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getTableSchema(MasterProto.GetTableSchemaRequest request,
                               StreamObserver<MasterProto.GetTableSchemaResponse> responseObserver) {
        try {
            String tableName = request.getTableName();

            // 获取表结构
            Table table = metadataManager.getTable(tableName);

            if (table == null) {
                MasterProto.GetTableSchemaResponse response = MasterProto.GetTableSchemaResponse.newBuilder()
                        .setStatus(createErrorStatus("Table not found: " + tableName))
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // 转换为 protobuf 格式
            CommonProto.TableSchema.Builder schemaBuilder = CommonProto.TableSchema.newBuilder()
                    .setTableName(table.getTableName());

            // 添加列信息
            if (table.getColumns() != null) {
                for (com.minisql.common.model.Column column : table.getColumns()) {
                    CommonProto.ColumnSchema colSchema = CommonProto.ColumnSchema.newBuilder()
                            .setName(column.getName())
                            .setType(column.getType().toString())
                            .setNullable(column.isNullable())
                            .build();
                    schemaBuilder.addColumns(colSchema);
                }
            }

            // 设置主键
            if (table.getPrimaryKey() != null) {
                schemaBuilder.setPrimaryKey(table.getPrimaryKey());
            }

            MasterProto.GetTableSchemaResponse response = MasterProto.GetTableSchemaResponse.newBuilder()
                    .setStatus(createSuccessStatus())
                    .setSchema(schemaBuilder.build())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.GetTableSchemaResponse.newBuilder()
                    .setStatus(createErrorStatus(e.getMessage()))
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getTableRegions(MasterProto.GetTableRegionsRequest request,
                                 StreamObserver<MasterProto.GetTableRegionsResponse> responseObserver) {
        try {
            String tableName = request.getTableName();

            // 获取表的所有 Region
            java.util.Collection<Region> regions = metadataManager.getRegionsForTable(tableName);

            List<CommonProto.RegionInfo> protoRegions = new ArrayList<>();

            for (Region region : regions) {
                CommonProto.RegionInfo.Builder regionBuilder = CommonProto.RegionInfo.newBuilder()
                        .setRegionId(region.getRegionId())
                        .setTableName(region.getTableName())
                        .setStartKey(com.google.protobuf.ByteString.copyFrom(region.getStartKey()))
                        .setEndKey(com.google.protobuf.ByteString.copyFrom(region.getEndKey()));

                // 添加副本信息
                if (region.getReplicas() != null) {
                    for (com.minisql.common.model.ServerId replica : region.getReplicas()) {
                        regionBuilder.addReplicas(CommonProto.ServerId.newBuilder()
                                .setHost(replica.getHost())
                                .setPort(replica.getPort())
                                .setServerType("REGIONSERVER")
                                .build());
                    }
                }

                // 设置主副本
                if (region.getPrimary() != null) {
                    CommonProto.ServerId.Builder primaryBuilder = CommonProto.ServerId.newBuilder()
                            .setHost(region.getPrimary().getHost())
                            .setPort(region.getPrimary().getPort())
                            .setServerType("REGIONSERVER");
                    regionBuilder.setPrimary(primaryBuilder.build());

                    // 获取 MySQL 配置
                    com.minisql.storage.MySQLConfig mysqlConfig = clusterManager.getMySQLConfig(region.getPrimary());
                    if (mysqlConfig != null) {
                        CommonProto.MySQLConfig mysqlConfigProto = CommonProto.MySQLConfig.newBuilder()
                                .setUrl(mysqlConfig.getJdbcUrl())
                                .setUser(mysqlConfig.getUsername())
                                .setPassword(mysqlConfig.getPassword())
                                .setMaxPoolSize(mysqlConfig.getMaxPoolSize())
                                .build();
                        regionBuilder.setMysqlConfig(mysqlConfigProto);
                    }
                }

                protoRegions.add(regionBuilder.build());
            }

            MasterProto.GetTableRegionsResponse response = MasterProto.GetTableRegionsResponse.newBuilder()
                    .setStatus(createSuccessStatus())
                    .addAllRegions(protoRegions)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.GetTableRegionsResponse.newBuilder()
                    .setStatus(createErrorStatus(e.getMessage()))
                    .build());
            responseObserver.onCompleted();
        }
    }

    // ==================== 负载均衡 ====================

    @Override
    public void triggerBalance(MasterProto.BalanceRequest request,
                                StreamObserver<MasterProto.BalanceResponse> responseObserver) {
        try {
            if (!ensureLeader(responseObserver, MasterProto.BalanceResponse::newBuilder)) {
                return;
            }
            List<ClusterManager.ServerInfo> servers = new ArrayList<>(clusterManager.getActiveServers());

            if (servers.size() < 2) {
                responseObserver.onNext(MasterProto.BalanceResponse.newBuilder()
                    .setStatus(createSuccessStatus())
                    .build());
                responseObserver.onCompleted();
                return;
            }

            // 计算均衡动作
            List<LoadBalancer.BalanceAction> actions = loadBalancer.computeBalanceActions(servers);

            List<MasterProto.BalanceAction> protoActions = new ArrayList<>();
            for (LoadBalancer.BalanceAction action : actions) {
                protoActions.add(MasterProto.BalanceAction.newBuilder()
                    .setRegionId(action.getRegionId())
                    .setSource(convertToProtoServerId(action.getSource()))
                    .setTarget(convertToProtoServerId(action.getTarget()))
                    .setType(MasterProto.ActionType.MOVE)
                    .build());

                // 执行迁移动作
                migrationCoordinator.execute(action);
            }

            MasterProto.BalanceResponse response = MasterProto.BalanceResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .addAllActions(protoActions)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.BalanceResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    // ==================== 私有方法 ====================

    @Override
    public void reportPrimaryChange(MasterProto.PrimaryChangeRequest request,
                                    StreamObserver<MasterProto.PrimaryChangeResponse> responseObserver) {
        DistributedLock lock = null;
        try {
            if (!ensureLeader(responseObserver, MasterProto.PrimaryChangeResponse::newBuilder)) {
                return;
            }
            String regionId = request.getRegionId();
            lock = acquireRegionLock(regionId);
            ServerId newPrimary = convertServerId(request.getNewPrimary());
            ServerId oldPrimary = request.hasOldPrimary() ? convertServerId(request.getOldPrimary()) : null;

            Region region = metadataManager.getRegion(regionId);
            if (region == null) {
                throw new IllegalArgumentException("Region not found: " + regionId);
            }

            clusterManager.promoteReplicaToPrimary(regionId, newPrimary);
            clusterManager.addReplica(regionId, newPrimary);
            region.setPrimary(newPrimary);
            if (!region.getReplicas().contains(newPrimary)) {
                region.addReplica(newPrimary);
            }
            metadataManager.registerRegionForTable(region, newPrimary);

            if (replicaMonitor != null) {
                replicaMonitor.promoteToPrimary(regionId, newPrimary);
            }
            if (lifecycleManager != null) {
                lifecycleManager.transition(regionId, newPrimary,
                    ReplicaLifecycleManager.ReplicaLifecycleState.PRIMARY_READY,
                    "Primary change reported to Master");
                if (oldPrimary != null && !oldPrimary.equals(newPrimary)) {
                    lifecycleManager.transition(regionId, oldPrimary,
                        ReplicaLifecycleManager.ReplicaLifecycleState.OFFLINE,
                        "Primary replaced by " + newPrimary.getServerName());
                }
            }

            responseObserver.onNext(MasterProto.PrimaryChangeResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.PrimaryChangeResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        } finally {
            releaseLock(lock);
        }
    }

    @Override
    public void getReplicaLifecycleStatus(MasterProto.GetReplicaLifecycleStatusRequest request,
                                          StreamObserver<MasterProto.GetReplicaLifecycleStatusResponse> responseObserver) {
        try {
            String regionIdFilter = request.getRegionId();
            ServerId serverIdFilter = request.hasServerId() ? convertServerId(request.getServerId()) : null;

            List<MasterProto.ReplicaLifecycleStatus> statuses = new ArrayList<>();
            for (ReplicaLifecycleManager.ReplicaLifecycleStatus status : lifecycleManager.getAllStatuses().values()) {
                if (regionIdFilter != null && !regionIdFilter.isEmpty()
                    && !regionIdFilter.equals(status.getRegionId())) {
                    continue;
                }
                if (serverIdFilter != null && !serverIdFilter.equals(status.getServerId())) {
                    continue;
                }

                statuses.add(MasterProto.ReplicaLifecycleStatus.newBuilder()
                    .setRegionId(status.getRegionId())
                    .setServerId(convertToProtoServerId(status.getServerId()))
                    .setState(status.getState().name())
                    .setDetail(status.getDetail() == null ? "" : status.getDetail())
                    .setUpdatedAt(status.getUpdatedAt())
                    .build());
            }

            responseObserver.onNext(MasterProto.GetReplicaLifecycleStatusResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .addAllStatuses(statuses)
                .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(MasterProto.GetReplicaLifecycleStatusResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    private CommonProto.Status createSuccessStatus() {
        return CommonProto.Status.newBuilder()
            .setCode(0)
            .setSuccess(true)
            .setMessage("OK")
            .build();
    }

    private CommonProto.Status createErrorStatus(String message) {
        return CommonProto.Status.newBuilder()
            .setCode(-1)
            .setSuccess(false)
            .setMessage(message)
            .build();
    }

    private <T extends com.google.protobuf.GeneratedMessageV3.Builder<T>> boolean ensureLeader(
        StreamObserver<?> responseObserver,
        Supplier<T> builderSupplier
    ) {
        if (leader) {
            return true;
        }
        T builder = builderSupplier.get();
        try {
            builder.getClass().getMethod("setStatus", CommonProto.Status.class)
                .invoke(builder, createErrorStatus("Current master is standby; retry the elected leader"));
            @SuppressWarnings("unchecked")
            StreamObserver<com.google.protobuf.Message> typedObserver =
                (StreamObserver<com.google.protobuf.Message>) responseObserver;
            typedObserver.onNext((com.google.protobuf.Message) builder.build());
            typedObserver.onCompleted();
        } catch (Exception reflectionError) {
            throw new IllegalStateException("Failed to build standby rejection response", reflectionError);
        }
        return false;
    }

    private DistributedLock acquireTableLock(String tableName) throws Exception {
        return acquireLock("/minisql/locks/tables/" + tableName);
    }

    private DistributedLock acquireRegionLock(String regionId) throws Exception {
        return acquireLock("/minisql/locks/regions/" + regionId);
    }

    private DistributedLock acquireLock(String path) throws Exception {
        if (zkClient == null) {
            return null;
        }
        DistributedLock lock = new DistributedLock(zkClient.getClient(), path);
        lock.acquire();
        return lock;
    }

    private void releaseLock(DistributedLock lock) {
        if (lock == null) {
            return;
        }
        try {
            if (lock.isAcquiredInThisProcess()) {
                lock.release();
            }
        } catch (Exception e) {
            System.err.println("Failed to release distributed lock: " + e.getMessage());
        }
    }

    private ServerId convertServerId(CommonProto.ServerId proto) {
        return new ServerId(proto.getHost(), proto.getPort());
    }

    private CommonProto.ServerId convertToProtoServerId(ServerId serverId) {
        return CommonProto.ServerId.newBuilder()
            .setHost(serverId.getHost())
            .setPort(serverId.getPort())
            .build();
    }

    private ClusterManager.RegionLoad convertRegionLoad(MasterProto.RegionLoad load) {
        ClusterManager.RegionLoad result = new ClusterManager.RegionLoad();
        result.setRegionId(load.getRegionId());
        result.setReadRequests(load.getReadRequests());
        result.setWriteRequests(load.getWriteRequests());
        result.setStoreFileSize(load.getStoreFileSize());
        result.setMemStoreSize(load.getMemStoreSize());
        return result;
    }

    private ClusterManager.ServerMetrics convertMetrics(MasterProto.ServerMetrics metrics) {
        ClusterManager.ServerMetrics result = new ClusterManager.ServerMetrics();
        result.setCpuUsage(metrics.getCpuUsage());
        result.setMemoryUsage(metrics.getMemoryUsage());
        result.setAvailableSpace(metrics.getAvailableSpace());
        result.setTotalSpace(metrics.getTotalSpace());
        return result;
    }

    private Region.State convertRegionState(CommonProto.RegionState state) {
        switch (state) {
            case OPEN: return Region.State.OPEN;
            case OPENING: return Region.State.OPENING;
            case CLOSING: return Region.State.CLOSING;
            case CLOSED: return Region.State.CLOSED;
            case SPLITTING: return Region.State.SPLITTING;
            case SPLIT: return Region.State.SPLIT;
            default: return Region.State.CLOSED;
        }
    }

    private CommonProto.RegionInfo convertRegionInfo(Region region) {
        return CommonProto.RegionInfo.newBuilder()
            .setRegionId(region.getRegionId())
            .setTableName(region.getTableName())
            .setStartKey(com.google.protobuf.ByteString.copyFrom(region.getStartKey()))
            .setEndKey(com.google.protobuf.ByteString.copyFrom(region.getEndKey()))
            .setState(CommonProto.RegionState.OPEN)
            .build();
    }

    private CommonProto.RegionInfo convertRegionInfo(Region region, CommonProto.MySQLConfig mysqlConfig) {
        CommonProto.RegionInfo.Builder builder = CommonProto.RegionInfo.newBuilder()
            .setRegionId(region.getRegionId())
            .setTableName(region.getTableName())
            .setStartKey(com.google.protobuf.ByteString.copyFrom(region.getStartKey()))
            .setEndKey(com.google.protobuf.ByteString.copyFrom(region.getEndKey()))
            .setState(CommonProto.RegionState.OPEN);

        if (mysqlConfig != null) {
            builder.setMysqlConfig(mysqlConfig);
        }

        return builder.build();
    }

    private List<Region> splitIntoRegions(String tableName, int numRegions,
                                           byte[] startKey, byte[] endKey) {
        List<Region> regions = new ArrayList<>();

        if (numRegions <= 1) {
            Region region = new Region();
            region.setRegionId(tableName + "_" + UUID.randomUUID().toString().substring(0, 8));
            region.setTableName(tableName);
            region.setStartKey(startKey != null ? startKey : new byte[0]);
            region.setEndKey(endKey != null ? endKey : new byte[]{(byte) 0xFF});
            regions.add(region);
            return regions;
        }

        // 计算分区边界
        byte[] start = startKey != null ? startKey : new byte[0];
        byte[] end = endKey != null ? endKey : new byte[]{(byte) 0xFF};

        // 简化处理：均匀分区
        for (int i = 0; i < numRegions; i++) {
            Region region = new Region();
            region.setRegionId(tableName + "_" + i + "_" + UUID.randomUUID().toString().substring(0, 8));
            region.setTableName(tableName);

            if (i == 0) {
                region.setStartKey(start);
            } else {
                region.setStartKey(calculateSplitKey(start, end, numRegions, i));
            }

            if (i == numRegions - 1) {
                region.setEndKey(end);
            } else {
                region.setEndKey(calculateSplitKey(start, end, numRegions, i + 1));
            }

            regions.add(region);
        }

        return regions;
    }

    /**
     * 为副本复制选择多个 RegionServer
     * @param region Region 对象
     * @param availableServers 可用的服务器列表
     * @param replicationFactor 副本因子
     * @return 选中的服务器列表（第一个为主副本，其余为从副本）
     */
    private List<ServerId> selectServersForReplication(Region region,
                                                        List<ClusterManager.ServerInfo> availableServers,
                                                        int replicationFactor) {
        List<ServerId> selected = new ArrayList<>();

        if (availableServers.isEmpty()) {
            return selected;
        }

        // 使用负载均衡器按顺序选择服务器
        List<ClusterManager.ServerInfo> serversCopy = new ArrayList<>(availableServers);
        while (selected.size() < replicationFactor && !serversCopy.isEmpty()) {
            ServerId serverId = loadBalancer.selectServerForRegion(region, serversCopy);
            if (serverId != null) {
                selected.add(serverId);
                // 从候选列表中移除已选中的服务器，避免重复
                serversCopy.removeIf(s -> s.getServerId().equals(serverId));
            }
        }

        return selected;
    }

    private byte[] calculateSplitKey(byte[] start, byte[] end, int numRegions, int index) {
        // 简化实现：线性插值
        // 实际应该根据数据分布计算
        byte[] result = new byte[start.length];
        for (int i = 0; i < start.length; i++) {
            int startVal = start[i] & 0xFF;
            int endVal = end[i] & 0xFF;
            result[i] = (byte) (startVal + (endVal - startVal) * index / numRegions);
        }
        return result;
    }

    private void handleRegionFailure(String regionId) {
        if (failoverCoordinator == null) {
            throw new IllegalStateException("FailoverCoordinator is required for region failure handling");
        }

        System.out.println("Handling failure for region via FailoverCoordinator: " + regionId);
        failoverCoordinator.triggerEmergencyFailover(regionId);
    }

    private void recordEvent(String type, String severity, String regionId, String tableName,
                             ServerId sourceServer, ServerId targetServer, String message, String details) {
        if (monitoringService != null) {
            monitoringService.recordEvent(type, severity, regionId, tableName,
                sourceServer == null ? null : sourceServer.getHost() + ":" + sourceServer.getPort(),
                targetServer == null ? null : targetServer.getHost() + ":" + targetServer.getPort(),
                message, details);
        }
    }

    private ReplicationLagSnapshot fetchReplicationLag(ServerId serverId, String regionId) {
        try {
            RegionServerProto.GetReplicationLagResponse response =
                commandClient.getReplicationLag(serverId, regionId, TimeUnit.SECONDS.toMillis(5));
            if (!response.getStatus().getSuccess()) {
                return ReplicationLagSnapshot.empty();
            }
            return new ReplicationLagSnapshot(
                response.getLagInEntries(),
                response.getLastAppliedSequenceId()
            );
        } catch (Exception e) {
            System.err.println("Failed to fetch replication lag for region " + regionId +
                " on " + serverId + ": " + e.getMessage());
            return ReplicationLagSnapshot.empty();
        }
    }

    private static final class ReplicationLagSnapshot {
        private final long lagInEntries;
        private final long lastAppliedSequenceId;

        private ReplicationLagSnapshot(long lagInEntries, long lastAppliedSequenceId) {
            this.lagInEntries = lagInEntries;
            this.lastAppliedSequenceId = lastAppliedSequenceId;
        }

        private static ReplicationLagSnapshot empty() {
            return new ReplicationLagSnapshot(0L, 0L);
        }
    }

    /**
     * 通知服务器提升为主副本
     */
    private boolean notifyServerPromoteToPrimary(ServerId serverId, String regionId) {
        try {
            long fencingToken = clusterManager.getFencingToken(regionId) + 1;
            RegionServerProto.PromoteResponse response =
                commandClient.promoteToPrimary(serverId, regionId, fencingToken);
            if (response.getStatus().getSuccess()) {
                clusterManager.updateFencingToken(regionId, fencingToken);
                return true;
            }
            return false;
        } catch (Exception e) {
            System.err.println("Failed to notify server promote to primary: " + e.getMessage());
            return false;
        }
    }

    /**
     * 同步通知 RegionServer 打开 Region（等待完成后再返回）
     * @return 是否成功打开
     */
    private boolean notifyServerOpenRegionSync(ServerId serverId, Region region) {
        System.out.println("Synchronously notifying " + serverId + " to open region " + region.getRegionId());

        try {
            RegionServerProto.OpenRegionResponse response = commandClient.openRegion(serverId, region, false);
            if (response.getStatus().getSuccess()) {
                System.out.println("Region " + region.getRegionId() + " opened successfully on " + serverId);
                return true;
            }
            System.err.println("Failed to open region " + region.getRegionId() + ": " + response.getStatus().getMessage());
            return false;
        } catch (Exception e) {
            System.err.println("Failed to notify server open region: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private boolean notifyServerCloseRegionSync(ServerId serverId, String regionId, boolean dropTable) {
        System.out.println("Synchronously notifying " + serverId + " to close region " + regionId +
            (dropTable ? " and drop table" : ""));

        try {
            RegionServerProto.CloseRegionResponse response =
                commandClient.closeRegion(serverId, regionId, false, dropTable);
            if (response.getStatus().getSuccess()) {
                System.out.println("Region " + regionId + " closed successfully on " + serverId +
                    (dropTable ? " and table dropped" : ""));
                return true;
            }
            System.err.println("Failed to close region " + regionId + ": " + response.getStatus().getMessage());
            return false;
        } catch (Exception e) {
            System.err.println("Failed to synchronously close region " + regionId + " on " + serverId + ": " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 转换 protobuf ColumnType 到 Column.ColumnType
     */
    private com.minisql.common.model.Column.ColumnType convertColumnType(String typeStr) {
        try {
            return com.minisql.common.model.Column.ColumnType.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            // 对于不支持的类型，返回 VARCHAR 作为默认值
            return com.minisql.common.model.Column.ColumnType.VARCHAR;
        }
    }
}
