package com.minisql.master.rpc;

import com.minisql.master.detect.*;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.monitoring.MonitorHttpServer;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.monitoring.SqlConsoleService;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.recover.DataRepairCoordinator;
import com.minisql.master.recover.FailoverCoordinator;
import com.minisql.master.recover.RecoveryCoordinator;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.replication.ReplicationConfig;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.zookeeper.ZkClient;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

/**
 * Master 启动主类
 * 负责启动 Master 服务和 gRPC 服务器
 */
public class MasterMain {

    private static final Logger logger = LoggerFactory.getLogger(MasterMain.class);

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 16000;
    private static final String DEFAULT_ZK_CONNECT = "localhost:2181";

    private Server grpcServer;
    private ZkClient zkClient;
    private TestingServer embeddedZkServer; // 嵌入式 ZooKeeper
    private ClusterManager clusterManager;
    private MetadataManager metadataManager;
    private LoadBalancer loadBalancer;
    private ReplicationCoordinator replicationCoordinator;
    private ServerFailureDetector serverFailureDetector;
    private ClusterEventCoordinator clusterEventCoordinator;
    private ReplicaMonitor replicaMonitor;
    private FailoverCoordinator failoverCoordinator;
    private RecoveryCoordinator recoveryCoordinator;
    private ReplicaLifecycleManager replicaLifecycleManager;
    private DataRepairCoordinator dataRepairCoordinator;
    private MonitoringService monitoringService;
    private MonitorHttpServer monitorHttpServer;
    private MasterServiceImpl serviceImpl;

    public static void main(String[] args) {
        MasterMain master = new MasterMain();
        try {
            master.start(args);
        } catch (Exception e) {
            logger.error("Failed to start Master: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    /**
     * 启动 Master 服务
     */
    public void start(String[] args) throws Exception {
        // 加载配置
        Properties config = loadConfig(args);

        String host = config.getProperty("master.host", DEFAULT_HOST);
        int port = Integer.parseInt(config.getProperty("master.port", String.valueOf(DEFAULT_PORT)));
        String zkConnectString = config.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);

        logger.info("========================================");
        logger.info("  MiniSQL Master Starting...");
        logger.info("========================================");
        logger.info("Host: {}", host);
        logger.info("Port: {}", port);
        logger.info("ZooKeeper: {}", zkConnectString);
        logger.info("----------------------------------------");

        // 初始化 ZooKeeper 客户端
        initZookeeper(zkConnectString);

        // 初始化核心组件
        initComponents(config);

        // 启动 gRPC 服务器
        startGrpcServer(host, port);

        logger.info("----------------------------------------");
        logger.info("Master started successfully!");
        logger.info("Listening on {}:{}", host, port);
        logger.info("========================================");

        // 添加关闭钩子
        addShutdownHook();

        // 阻塞等待服务器终止
        awaitTermination();
    }

    /**
     * 加载配置文件
     */
    private Properties loadConfig(String[] args) {
        Properties config = new Properties();

        // 先加载默认配置
        String configFile = "master.properties";

        // 如果命令行参数指定了配置文件，使用指定的
        if (args.length > 0) {
            configFile = args[0];
        }

        // 尝试加载配置文件
        try (InputStream is = getConfigInputStream(configFile)) {
            if (is != null) {
                config.load(is);
                logger.info("Loaded config from: {}", configFile);
            } else {
                logger.info("Config file not found: {}, using defaults", configFile);
            }
        } catch (IOException e) {
            logger.warn("Failed to load config: {}, using defaults", e.getMessage());
        }

        // 环境变量覆盖配置
        String zkConnect = System.getenv("MINISQL_ZK_CONNECT");
        if (zkConnect != null) {
            config.setProperty("zookeeper.connect", zkConnect);
        }

        // 嵌入式 ZooKeeper 配置（用于开发/测试）
        // 设置 -Dminisql.zk.embedded=true 启用
        String embeddedZk = System.getProperty("minisql.zk.embedded");
        if ("true".equalsIgnoreCase(embeddedZk)) {
            config.setProperty("zookeeper.embedded", "true");
        }

        String masterHost = System.getenv("MINISQL_MASTER_HOST");
        if (masterHost != null) {
            config.setProperty("master.host", masterHost);
        }

        String masterPort = System.getenv("MINISQL_MASTER_PORT");
        if (masterPort != null) {
            config.setProperty("master.port", masterPort);
        }

        return config;
    }

    /**
     * 获取配置文件输入流
     */
    private InputStream getConfigInputStream(String configFile) throws IOException {
        // 先尝试从文件系统加载
        try {
            return new FileInputStream(configFile);
        } catch (IOException e) {
            // 再尝试从 classpath 加载
            InputStream is = getClass().getClassLoader().getResourceAsStream(configFile);
            if (is == null) {
                // 尝试从 resources 目录加载
                is = getClass().getClassLoader().getResourceAsStream("master.properties");
            }
            return is;
        }
    }

    /**
     * 初始化 ZooKeeper 连接
     * 如果启用了嵌入式模式，则启动嵌入式 ZooKeeper
     */
    private void initZookeeper(String connectString) throws Exception {
        // 检查是否启用嵌入式 ZooKeeper
        boolean useEmbeddedZk = Boolean.getBoolean("minisql.zk.embedded");

        if (useEmbeddedZk) {
            logger.info("Starting embedded ZooKeeper...");
            // 启动嵌入式 ZooKeeper（端口 2181）
            embeddedZkServer = new TestingServer(2181, true);
            connectString = embeddedZkServer.getConnectString();
            logger.info("Embedded ZooKeeper started at: {}", connectString);
        }

        logger.info("Connecting to ZooKeeper...");
        zkClient = new ZkClient(connectString);
        zkClient.start();

        // 等待连接建立
        int retries = 0;
        while (!zkClient.isConnected() && retries < 30) {
            Thread.sleep(1000);
            retries++;
        }
        logger.info("");

        if (!zkClient.isConnected()) {
            throw new RuntimeException("Failed to connect to ZooKeeper after 30 seconds");
        }

        logger.info("Connected to ZooKeeper: {}", connectString);

        // 初始化 ZooKeeper 路径
        initializeZkPaths();
    }

    /**
     * 初始化 ZooKeeper 路径
     */
    private void initializeZkPaths() throws Exception {
        logger.info("Initializing ZooKeeper paths...");

        String[] paths = {
            com.minisql.common.Constants.ZK_ROOT_PATH,
            com.minisql.common.Constants.ZK_MASTER_PATH,
            com.minisql.common.Constants.ZK_REGIONSERVERS_PATH,
            com.minisql.common.Constants.ZK_TABLES_PATH,
            com.minisql.common.Constants.ZK_REGIONS_PATH,
            com.minisql.common.Constants.ZK_ASSIGNMENT_PATH
        };

        for (String path : paths) {
            if (!zkClient.exists(path)) {
                zkClient.createPersistent(path, new byte[0]);
                logger.info("  Created path: {}", path);
            }
        }

        logger.info("ZooKeeper paths initialized");
    }

    /**
     * 初始化核心组件
     */
    private void initComponents(Properties config) {
        logger.info("Initializing components...");

        // 创建负载均衡器
        loadBalancer = new LoadBalancer();
        LoadBalancer.Strategy strategy = LoadBalancer.Strategy.fromString(
            config.getProperty("load.balance.strategy", "load_based"));
        loadBalancer.setStrategy(strategy);
        logger.info("Load balance strategy: {}", strategy);

        // 创建集群管理器
        clusterManager = new ClusterManager(loadBalancer);
        // 设置 ZooKeeper 客户端以支持 RegionServer 注册
        clusterManager.setZkClient(zkClient);

        // 创建元数据管理器（连接 ZooKeeper）
        metadataManager = new MetadataManager(zkClient);

        // 创建副本管理器（默认副本因子为 3）
        int replicationFactor = 3; // 可配置化
        replicationCoordinator = new ReplicationCoordinator(ReplicationConfig.builder(replicationFactor).build());
        replicationCoordinator.setZkClient(zkClient);
        replicationCoordinator.start();
        rebuildReplicationGroups();

        // === 新增：初始化容错组件 ===

        // 1. 创建副本监控器
        replicaMonitor = new ReplicaMonitor(clusterManager);
        replicaMonitor.start();
        replicaLifecycleManager = new ReplicaLifecycleManager();
        rebuildReplicaRuntimeState();

        // 2. 创建故障转移管理器 (依赖 replicaMonitor)
        failoverCoordinator = new FailoverCoordinator(clusterManager, metadataManager, replicaMonitor, replicaLifecycleManager);
        failoverCoordinator.setZkClient(zkClient);

        recoveryCoordinator = new RecoveryCoordinator(clusterManager, metadataManager, replicaMonitor, replicationCoordinator, replicaLifecycleManager);
        recoveryCoordinator.start();
        monitoringService = new MonitoringService(clusterManager, metadataManager, replicaMonitor, replicaLifecycleManager);
        replicaMonitor.registerCallback(monitoringService.replicaEventCallback());
        failoverCoordinator.setMonitoringService(monitoringService);
        recoveryCoordinator.setMonitoringService(monitoringService);

        // 3. 创建故障检测器
        serverFailureDetector = new ServerFailureDetector(clusterManager, metadataManager, loadBalancer);
        serverFailureDetector.setLifecycleManager(replicaLifecycleManager);
        serverFailureDetector.setMonitoringService(monitoringService);

        // 4. 创建数据修复管理器
        dataRepairCoordinator = new DataRepairCoordinator(clusterManager, metadataManager);

        logger.info("Components initialized (replication factor: {})", replicationFactor);
    }

    private void rebuildReplicationGroups() {
        for (Region region : metadataManager.getAllRegions()) {
            if (region == null || region.getRegionId() == null) {
                continue;
            }
            if (replicationCoordinator.getReplicaGroup(region.getRegionId()) != null) {
                continue;
            }

            LinkedHashSet<ServerId> replicaServers = new LinkedHashSet<>();
            if (region.getPrimary() != null) {
                replicaServers.add(region.getPrimary());
            }
            replicaServers.addAll(region.getReplicas());

            if (replicaServers.isEmpty()) {
                logger.warn("Skipping replication group rebuild for region {} because no primary/replicas were restored from metadata",
                    region.getRegionId());
                continue;
            }

            List<ServerId> orderedServers = new ArrayList<>(replicaServers);
            replicationCoordinator.createReplicaGroup(region, orderedServers);
            logger.info("Rebuilt replication group for region {} with primary {} and {} known replicas",
                region.getRegionId(), orderedServers.get(0), orderedServers.size());
        }
    }

    private void rebuildReplicaRuntimeState() {
        for (Region region : metadataManager.getAllRegions()) {
            if (region == null || region.getRegionId() == null) {
                continue;
            }

            if (region.getPrimary() != null) {
                clusterManager.assignRegionToServer(region.getRegionId(), region.getPrimary());
                clusterManager.addReplica(region.getRegionId(), region.getPrimary());
                replicaMonitor.registerReplica(region.getRegionId(),
                    createReplicaInfo(region.getRegionId(), region.getPrimary(), ReplicaInfo.ReplicaState.PRIMARY));
            }

            for (ServerId replica : region.getReplicas()) {
                if (replica == null) {
                    continue;
                }
                clusterManager.addReplica(region.getRegionId(), replica);
                ReplicaInfo.ReplicaState state = replica.equals(region.getPrimary())
                    ? ReplicaInfo.ReplicaState.PRIMARY
                    : ReplicaInfo.ReplicaState.SECONDARY;
                replicaMonitor.registerReplica(region.getRegionId(), createReplicaInfo(region.getRegionId(), replica, state));
            }
        }
    }

    private ReplicaInfo createReplicaInfo(String regionId, ServerId serverId, ReplicaInfo.ReplicaState state) {
        ReplicaInfo replicaInfo = new ReplicaInfo();
        replicaInfo.setRegionId(regionId);
        replicaInfo.setServerId(serverId);
        replicaInfo.setState(state);
        replicaInfo.setLastHeartbeat(System.currentTimeMillis());
        replicaInfo.setReplicationLag(0L);
        return replicaInfo;
    }

    /**
     * 启动 gRPC 服务器
     */
    private void startGrpcServer(String host, int port) throws IOException {
        logger.info("Starting gRPC server...");

        // 创建服务实现
        serviceImpl = new MasterServiceImpl(
            clusterManager,
            metadataManager,
            loadBalancer,
            replicationCoordinator,
            replicaMonitor,
            failoverCoordinator,
            recoveryCoordinator,
            replicaLifecycleManager
        );
        serviceImpl.setMonitoringService(monitoringService);
        clusterEventCoordinator = new ClusterEventCoordinator();
        clusterEventCoordinator.registerHandler(HotSpotActionEvent.class, event -> serviceImpl.handleHotSpotAction(event.getAction()));
        clusterEventCoordinator.registerHandler(RegionSplitSuggestedEvent.class, serviceImpl::handleRegionSplitSuggestion);
        clusterEventCoordinator.registerHandler(ServerFailedEvent.class, serviceImpl::handleServerFailureEvent);
        clusterEventCoordinator.registerDetector(serverFailureDetector);
        clusterEventCoordinator.registerDetector(serviceImpl.getRegionSplitDetector());
        clusterEventCoordinator.registerDetector(serviceImpl.getHotSpotDetector());
        clusterEventCoordinator.start();

        // 创建并启动 gRPC 服务器
        grpcServer = ServerBuilder.forPort(port)
            .addService(serviceImpl)
            .build()
            .start();

        logger.info("gRPC server started on port {}", port);
        startMonitorServer(host);

        // 向 ZooKeeper 注册 Master 地址
        registerMasterToZooKeeper(host, port);
    }

    /**
     * 向 ZooKeeper 注册 Master 地址
     */
    private void startMonitorServer(String host) throws IOException {
        String monitorHost = System.getProperty("minisql.monitor.host", host);
        int monitorPort = Integer.parseInt(System.getProperty("minisql.monitor.port", "16010"));
        SqlConsoleService sqlConsoleService = zkClient == null ? null : new SqlConsoleService(zkClient.getConnectString());
        monitorHttpServer = new MonitorHttpServer(monitoringService, sqlConsoleService);
        monitorHttpServer.start(monitorHost, monitorPort);
        logger.info("Monitor HTTP server started on {}:{}", monitorHost, monitorPort);
    }

    private void registerMasterToZooKeeper(String host, int port) {
        try {
            String masterAddress = host + ":" + port;
            String masterPath = com.minisql.common.Constants.ZK_MASTER_PATH;

            // 如果节点已存在，先删除
            if (zkClient.exists(masterPath)) {
                zkClient.delete(masterPath);
            }

            // 创建持久节点，写入 Master 地址
            zkClient.createPersistent(masterPath, masterAddress.getBytes("UTF-8"));
            logger.info("Master registered to ZooKeeper: {} at {}", masterAddress, masterPath);

        } catch (Exception e) {
            logger.error("Failed to register Master to ZooKeeper: {}", e.getMessage(), e);
        }
    }

    /**
     * 添加关闭钩子
     */
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("\n========================================");
            logger.info("  Shutting down Master...");
            logger.info("========================================");
            try {
                stop();
            } catch (Exception e) {
                logger.error("Error during shutdown: {}", e.getMessage());
            }
            logger.info("Master shutdown complete");
        }));
    }

    /**
     * 等待服务器终止
     */
    private void awaitTermination() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    /**
     * 停止 Master 服务
     */
    public void stop() throws Exception {
        // 停止数据修复管理器
        if (dataRepairCoordinator != null) {
            dataRepairCoordinator.stop();
        }

        // 停止故障转移管理器
        if (failoverCoordinator != null) {
            failoverCoordinator.shutdown();
        }

        // 停止故障检测器
        if (clusterEventCoordinator != null) {
            clusterEventCoordinator.stop();
        }

        // 停止副本监控器
        if (replicaMonitor != null) {
            replicaMonitor.stop();
        }
        if (recoveryCoordinator != null) {
            recoveryCoordinator.stop();
        }

        // 停止副本管理器
        if (replicationCoordinator != null) {
            replicationCoordinator.stop();
        }

        // 停止 gRPC 服务器
        if (grpcServer != null) {
            logger.info("Shutting down gRPC server...");
            grpcServer.shutdown();
            grpcServer.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);
            logger.info("gRPC server stopped");
        }

        if (serviceImpl != null) {
            serviceImpl.shutdown();
        }

        // 关闭 ZooKeeper 连接
        if (monitorHttpServer != null) {
            logger.info("Stopping monitor HTTP server...");
            monitorHttpServer.stop();
        }

        if (zkClient != null) {
            logger.info("Closing ZooKeeper connection...");
            zkClient.close();
            logger.info("ZooKeeper connection closed");
        }

        // 停止嵌入式 ZooKeeper
        if (embeddedZkServer != null) {
            logger.info("Stopping embedded ZooKeeper...");
            embeddedZkServer.stop();
            logger.info("Embedded ZooKeeper stopped");
        }
    }
}
