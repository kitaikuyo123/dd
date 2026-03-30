package com.minisql.regionserver;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.common.model.Column;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.MasterProto;
import com.minisql.common.proto.MasterServiceGrpc;
import com.minisql.replication.ReplicationConfig;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.replication.ReplicationWAL;
import com.minisql.storage.MySQLConfig;
import com.minisql.storage.StorageScanFilter;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * RegionServer 主类
 * 基于 MySQL 存储引擎
 */
public class RegionServer {

    private static final Logger logger = LoggerFactory.getLogger(RegionServer.class);

    private final ServerId serverId;
    private final RegionManager regionManager;
    private final RegionSplitService splitService;
    private final RegionMergeService mergeService;
    private final MySQLConfig primaryMysqlConfig;  // 主 MySQL 配置（默认继承源）
    private volatile HikariDataSource sharedDataSource;   // 共享连接池，同一数据库的所有 Region 复用
    private final String masterAddress;  // Master 地址

    // 副本管理和 WAL 组件
    private final ReplicationCoordinator replicationCoordinator;

    private volatile boolean running = false;
    private ManagedChannel masterChannel;
    private MasterServiceGrpc.MasterServiceBlockingStub masterStub;

    // gRPC 服务器
    private Server grpcServer;

    public RegionServer(String host, int port, MySQLConfig mysqlConfig, String masterAddress) {
        this.serverId = new ServerId(host, port);
        this.primaryMysqlConfig = mysqlConfig;
        this.masterAddress = masterAddress;

        // 传入 this，让 RegionManager 可以调用 getMysqlConfigForRegion()
        this.regionManager = new RegionManager(this);
        this.splitService = new RegionSplitService(regionManager);
        this.mergeService = new RegionMergeService(regionManager);

        // 初始化副本管理器（副本因子=3）
        this.replicationCoordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(3).build(),
            primaryMysqlConfig
        );
    }

    public void start() throws IOException {
        running = true;

        // 初始化 WAL
        replicationCoordinator.start();
        logger.info("ReplicationCoordinator started");

        // 连接到 Master
        connectToMaster();

        // 启动 gRPC 服务
        startGrpcServer();

        logger.info("RegionServer started on {}:{}", serverId.getHost(), serverId.getPort());
        logger.info("Using MySQL storage engine");

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("*** Shutting down gRPC server since JVM is shutting down");
            try {
                stop();
            } catch (Exception e) {
                logger.error("Error stopping server", e);
            }
            logger.info("*** Server shut down");
        }));
    }

    /**
     * 连接到 Master
     */
    private void connectToMaster() {
        if (masterAddress == null || masterAddress.isEmpty()) {
            logger.warn("Master address not configured, skipping Master connection");
            return;
        }

        logger.info("Connecting to Master at {}", masterAddress);
        String[] parts = masterAddress.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 16000;

        masterChannel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();
        masterStub = MasterServiceGrpc.newBlockingStub(masterChannel);
        logger.info("Connected to Master successfully");
    }

    /**
     * 获取表结构
     */
    public Table getTableSchema(String tableName) {
        if (masterStub == null) {
            logger.warn("Master connection not available");
            return null;
        }

        try {
            MasterProto.GetTableSchemaRequest request = MasterProto.GetTableSchemaRequest.newBuilder()
                .setTableName(tableName)
                .build();
            MasterProto.GetTableSchemaResponse response = masterStub.getTableSchema(request);

            if (response.getStatus().getSuccess()) {
                return convertProtoToTable(response.getSchema());
            } else {
                logger.warn("Failed to get table schema: {}", response.getStatus().getMessage());
                return null;
            }
        } catch (Exception e) {
            logger.error("Error getting table schema for " + tableName, e);
            return null;
        }
    }

    /**
     * 将 Proto TableSchema 转换为 Table 对象
     */
    private Table convertProtoToTable(CommonProto.TableSchema proto) {
        Table table = new Table();
        table.setTableName(proto.getTableName());

        // 设置主键（如果有）
        String primaryKey = proto.getPrimaryKey();
        if (primaryKey != null && !primaryKey.isEmpty()) {
            table.setPrimaryKey(primaryKey);
        }

        for (CommonProto.ColumnSchema colProto : proto.getColumnsList()) {
            Column col = new Column();
            col.setName(colProto.getName());
            col.setType(Column.ColumnType.valueOf(colProto.getType()));
            col.setNullable(colProto.getNullable());
            table.addColumn(col);
        }

        return table;
    }

    /**
     * 启动 gRPC 服务器
     */
    private void startGrpcServer() throws IOException {
        RegionServerServiceImpl service = new RegionServerServiceImpl(this);
        logger.info("Created RegionServerServiceImpl, binding to port {}", serverId.getPort());

        grpcServer = ServerBuilder.forPort(serverId.getPort())
            .addService(service)
            .build()
            .start();

        logger.info("gRPC server started on port {}", serverId.getPort());
    }

    public void stop() throws Exception {
        running = false;

        // 停止副本管理器
        if (replicationCoordinator != null) {
            replicationCoordinator.stop();
        }

        // 关闭 Master 连接
        if (masterChannel != null && !masterChannel.isShutdown()) {
            masterChannel.shutdown();
            try {
                if (!masterChannel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    masterChannel.shutdownNow();
                }
            } catch (InterruptedException e) {
                masterChannel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 关闭 gRPC 服务器
        if (grpcServer != null) {
            grpcServer.shutdown();
        }

        // 关闭所有 Region
        for (com.minisql.common.model.Region region : regionManager.getAllRegions()) {
            regionManager.closeRegion(region.getRegionId(), false);
        }

        // 关闭共享连接池
        if (sharedDataSource != null && !sharedDataSource.isClosed()) {
            sharedDataSource.close();
            logger.info("Shared MySQL DataSource closed");
        }

        logger.info("RegionServer stopped");
    }

    /**
     * 写入数据
     */
    public void put(String regionId, List<KeyValue> keyValues) throws Exception {
        put(regionId, keyValues, false);
    }

    public void put(String regionId, List<KeyValue> keyValues, boolean bypassWriteBlock) throws Exception {
        logger.debug("Starting put for region: {}", regionId);

        checkRegionOpen(regionId);
        logger.debug("Region is open: {}", regionId);

        if (!bypassWriteBlock && regionManager.isWriteBlocked(regionId)) {
            throw new IllegalStateException("Region is temporarily read-only during migration: " + regionId);
        }

        MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(regionId);
        if (storage == null) {
            logger.error("MySQL Region storage not found: {}", regionId);
            throw new IllegalStateException("MySQL Region storage not found: " + regionId);
        }

        logger.debug("Storage found, writing data...");
        storage.put(keyValues);
        logger.debug("Put completed successfully for region: {}", regionId);
    }

    /**
     * 读取数据
     */
    public KeyValue get(String regionId, byte[] rowKey) {
        checkRegionOpen(regionId);

        MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(regionId);
        if (storage == null) {
            throw new IllegalStateException("MySQL Region storage not found: " + regionId);
        }
        return storage.get(rowKey);
    }

    /**
     * 扫描数据
     */
    public Iterator<KeyValue> scan(String regionId, byte[] startKey, byte[] endKey) {
        checkRegionOpen(regionId);

        MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(regionId);
        if (storage == null) {
            throw new IllegalStateException("MySQL Region storage not found: " + regionId);
        }
        return storage.scan(startKey, endKey);
    }

    public Iterator<KeyValue> scan(String regionId, StorageScanFilter filter) {
        checkRegionOpen(regionId);

        MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(regionId);
        if (storage == null) {
            throw new IllegalStateException("MySQL Region storage not found: " + regionId);
        }
        return storage.scan(filter);
    }

    /**
     * 删除数据
     */
    public void delete(String regionId, byte[] rowKey) throws Exception {
        delete(regionId, rowKey, false);
    }

    public void delete(String regionId, byte[] rowKey, boolean bypassWriteBlock) throws Exception {
        checkRegionOpen(regionId);

        if (!bypassWriteBlock && regionManager.isWriteBlocked(regionId)) {
            throw new IllegalStateException("Region is temporarily read-only during migration: " + regionId);
        }

        MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(regionId);
        if (storage == null) {
            throw new IllegalStateException("MySQL Region storage not found: " + regionId);
        }
        storage.delete(rowKey);
    }

    /**
     * 刷新 Region 数据
     */
    public void flushRegion(String regionId) throws IOException {
        checkRegionOpen(regionId);
        regionManager.flushRegion(regionId);
    }

    /**
     * 压缩 Region
     */
    public void compactRegion(String regionId, boolean major) throws IOException {
        checkRegionOpen(regionId);
        regionManager.compactRegion(regionId, major);
    }

    /**
     * 检查 Region 是否打开
     */
    private void checkRegionOpen(String regionId) {
        boolean isOpen = regionManager.isRegionOpen(regionId);
        logger.debug("Region {} is open: {}", regionId, isOpen);
        if (!isOpen) {
            RegionManager.RegionState state = regionManager.getRegionState(regionId);
            logger.error("Region {} state: {}", regionId, state);
            throw new IllegalStateException("Region is not open: " + regionId + ", state: " + state);
        }
    }

    public ServerId getServerId() {
        return serverId;
    }

    public RegionManager getRegionManager() {
        return regionManager;
    }

    public RegionSplitService getSplitService() {
        return splitService;
    }

    public RegionMergeService getMergeService() {
        return mergeService;
    }

    public boolean isRunning() {
        return running;
    }

    public MySQLConfig getMysqlConfigForRegion(Region region) {
        logger.info("Region {} using primary MySQL config: {}", 
            region != null ? region.getRegionId() : "unknown",
            primaryMysqlConfig.getJdbcUrl());
        return primaryMysqlConfig;
    }


    /**
     * 获取或创建共享连接池（延迟初始化，线程安全）
     * 同一 RegionServer 上所有共享主数据库的 Region 将复用这个单一池
     */
    public synchronized HikariDataSource getOrCreateSharedDataSource() {
        if (sharedDataSource == null || sharedDataSource.isClosed()) {
            sharedDataSource = primaryMysqlConfig.createDataSource();
            logger.info("Shared MySQL DataSource created: {}", primaryMysqlConfig.getJdbcUrl());
        }
        return sharedDataSource;
    }

    /**
     * 获取主 MySQL 配置（向后兼容）
     */
    public MySQLConfig getMySQLConfig() {
        return primaryMysqlConfig;
    }

    /**
     * 获取副本管理器
     */
    public ReplicationCoordinator getReplicationCoordinator() {
        return replicationCoordinator;
    }

    /**
     * 获取 WAL
     */
    public ReplicationWAL getWal() {
        return replicationCoordinator.getWal();
    }
}
