package com.minisql.master.rpc;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
import com.minisql.common.model.ServerId;
import com.minisql.master.monitoring.MonitorHttpServer;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.monitoring.SqlConsoleService;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.rebalance.HotSpotCoordinator;
import com.minisql.master.recover.DataRepairCoordinator;
import com.minisql.master.recover.FailoverCoordinator;
import com.minisql.master.recover.RecoveryCoordinator;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.state.ReplicaLifecycleManager;
import com.minisql.master.state.ReplicaMonitor;
import com.minisql.replication.ReplicationConfig;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.zookeeper.ZkClient;
import com.minisql.zookeeper.ZkManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Master process entrypoint.
 */
public class MasterMain {

    private static final Logger logger = LoggerFactory.getLogger(MasterMain.class);

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 16000;
    private static final String DEFAULT_ZK_CONNECT = "localhost:2181";

    private Server grpcServer;
    private ZkClient zkClient;
    private ZkManager zkManager;
    private TestingServer embeddedZkServer;
    private ClusterManager clusterManager;
    private MetadataManager metadataManager;
    private LoadBalancer loadBalancer;
    private ReplicationCoordinator replicationCoordinator;
    private ReplicaMonitor replicaMonitor;
    private FailoverCoordinator failoverCoordinator;
    private RecoveryCoordinator recoveryCoordinator;
    private ReplicaLifecycleManager replicaLifecycleManager;
    private DataRepairCoordinator dataRepairCoordinator;
    private MonitoringService monitoringService;
    private MonitorHttpServer monitorHttpServer;
    private MasterServiceImpl serviceImpl;
    private ServerId masterServerId;
    private Properties config;
    private long hotSpotDetectorIntervalMs = 10_000L;
    private HotSpotCoordinator.HotSpotSettings hotSpotSettings;
    private boolean loadBalanceEnabled = true;
    private long loadBalanceIntervalMs = TimeUnit.MINUTES.toMillis(5);
    private String configFilePath;
    private long configLastModified;
    private volatile boolean configReloadRunning;

    public static void main(String[] args) {
        MasterMain master = new MasterMain();
        try {
            master.start(args);
        } catch (Exception e) {
            logger.error("Failed to start Master: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    public void start(String[] args) throws Exception {
        Properties config = loadConfig(args);

        String host = config.getProperty("master.host", DEFAULT_HOST);
        int port = Integer.parseInt(config.getProperty("master.port", String.valueOf(DEFAULT_PORT)));
        String zkConnectString = config.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
        masterServerId = new ServerId(host, port);

        logger.info("========================================");
        logger.info("  MiniSQL Master Starting...");
        logger.info("========================================");
        logger.info("Host: {}", host);
        logger.info("Port: {}", port);
        logger.info("ZooKeeper: {}", zkConnectString);
        logger.info("----------------------------------------");

        initZookeeper(zkConnectString);
        this.config = config;
        initComponents(config);
        startGrpcServer(host, port);
        startLeaderElection();

        logger.info("----------------------------------------");
        logger.info("Master started successfully");
        logger.info("Listening on {}:{}", host, port);
        logger.info("========================================");

        addShutdownHook();
        startConfigReloader();
        awaitTermination();
    }

    private Properties loadConfig(String[] args) {
        Properties config = new Properties();
        this.configFilePath = args.length > 0 ? args[0] : "master.properties";

        try (InputStream is = getConfigInputStream(configFilePath)) {
            if (is != null) {
                config.load(is);
                logger.info("Loaded config from: {}", configFilePath);
            } else {
                logger.info("Config file not found: {}, using defaults", configFilePath);
            }
        } catch (IOException e) {
            logger.warn("Failed to load config: {}, using defaults", e.getMessage());
        }

        // Track file modification time for hot-reload detection
        File f = new File(configFilePath);
        if (f.isFile()) {
            configLastModified = f.lastModified();
        }

        String zkConnect = System.getenv("MINISQL_ZK_CONNECT");
        if (zkConnect != null) {
            config.setProperty("zookeeper.connect", zkConnect);
        }
        String masterHost = System.getenv("MINISQL_MASTER_HOST");
        if (masterHost != null) {
            config.setProperty("master.host", masterHost);
        }
        String masterPort = System.getenv("MINISQL_MASTER_PORT");
        if (masterPort != null) {
            config.setProperty("master.port", masterPort);
        }
        if ("true".equalsIgnoreCase(System.getProperty("minisql.zk.embedded"))) {
            config.setProperty("zookeeper.embedded", "true");
        }

        return config;
    }

    private InputStream getConfigInputStream(String configFile) throws IOException {
        try {
            return new FileInputStream(configFile);
        } catch (IOException e) {
            InputStream is = getClass().getClassLoader().getResourceAsStream(configFile);
            if (is == null) {
                is = getClass().getClassLoader().getResourceAsStream("master.properties");
            }
            return is;
        }
    }

    private void initZookeeper(String connectString) throws Exception {
        if (Boolean.getBoolean("minisql.zk.embedded")) {
            logger.info("Starting embedded ZooKeeper...");
            embeddedZkServer = new TestingServer(2181, true);
            connectString = embeddedZkServer.getConnectString();
            logger.info("Embedded ZooKeeper started at: {}", connectString);
        }

        logger.info("Connecting to ZooKeeper...");
        zkManager = new ZkManager(connectString, masterServerId);
        zkManager.start();
        zkClient = zkManager.getClient();

        int retries = 0;
        while (!zkClient.isConnected() && retries < 30) {
            Thread.sleep(1000);
            retries++;
        }
        if (!zkClient.isConnected()) {
            throw new IllegalStateException("Failed to connect to ZooKeeper after 30 seconds");
        }
        logger.info("Connected to ZooKeeper: {}", connectString);
    }

    private void initComponents(Properties config) {
        logger.info("Initializing components...");

        loadBalancer = new LoadBalancer();
        LoadBalancer.Strategy strategy = LoadBalancer.Strategy.fromString(
            config.getProperty("load.balance.strategy", "load_based"));
        double loadBalanceThreshold = parseDoubleProperty(config, "load.balance.threshold", 20.0d);
        long minMigrationIntervalMs = Math.max(0L,
            parseLongProperty(config, "load.balance.min.migration.interval.ms", TimeUnit.MINUTES.toMillis(5)));
        loadBalancer.setStrategy(strategy);
        loadBalancer.setBalanceThreshold(loadBalanceThreshold);
        loadBalancer.setMinMigrationIntervalMs(minMigrationIntervalMs);
        int maxMigrationsPerRound = parseIntProperty(config, "load.balance.max.migrations.per.round", 3);
        loadBalancer.setMaxMigrationsPerRound(maxMigrationsPerRound);
        double hotSpotPenalty = parseDoubleProperty(config, "load.balance.hotspot.penalty.weight", 15.0);
        loadBalancer.setHotSpotPenaltyWeight(hotSpotPenalty);
        loadBalanceEnabled = parseBooleanProperty(config, "load.balance.enabled", true);
        loadBalanceIntervalMs = Math.max(1_000L,
            parseLongProperty(config, "load.balance.interval.ms", TimeUnit.MINUTES.toMillis(5)));
        logger.info(
            "Configured load balance: enabled={} interval={}ms strategy={} threshold={} minMigration={}ms maxMigrations={}",
            loadBalanceEnabled, loadBalanceIntervalMs, strategy,
            loadBalancer.getBalanceThreshold(), loadBalancer.getMinMigrationIntervalMs(), maxMigrationsPerRound);
        hotSpotDetectorIntervalMs = parseLongProperty(config, "hotspot.detector.interval.ms", 10_000L);
        long hotSpotReadThreshold = parseLongProperty(config, "hotspot.read.threshold.per.interval", 200L);
        long hotSpotWriteThreshold = parseLongProperty(config, "hotspot.write.threshold.per.interval", 100L);
        double hotSpotGrowthThreshold = parseDoubleProperty(config, "hotspot.growth.threshold", 1.2d);
        int hotSpotTargetReadReplicaCount = parseIntProperty(config, "hotspot.target.read.replica.count", 3);
        long hotSpotCooldownMs = parseLongProperty(config, "hotspot.cooldown.ms", TimeUnit.MINUTES.toMillis(5));
        hotSpotSettings = new HotSpotCoordinator.HotSpotSettings(
            hotSpotReadThreshold,
            hotSpotWriteThreshold,
            hotSpotGrowthThreshold,
            hotSpotTargetReadReplicaCount,
            hotSpotCooldownMs);
        logger.info(
            "Configured hotspot properties: interval={}ms readThreshold={} writeThreshold={} targetReadReplicaCount={} cooldown={}ms",
            hotSpotDetectorIntervalMs,
            hotSpotReadThreshold,
            hotSpotWriteThreshold,
            hotSpotTargetReadReplicaCount,
            hotSpotCooldownMs);

        clusterManager = new ClusterManager(loadBalancer);
        clusterManager.setZkClient(zkClient);

        metadataManager = new MetadataManager(zkClient);

        int replicationFactor = Integer.parseInt(config.getProperty("replication.factor", "3"));
        int replicationThreadPool = parseIntProperty(config, "replication.thread.pool.size", 0);
        ReplicationConfig replicationCfg = ReplicationConfig.builder(replicationFactor)
            .replicationThreadPoolSize(replicationThreadPool)
            .build();
        replicationCoordinator = new ReplicationCoordinator(replicationCfg);
        replicationCoordinator.setZkClient(zkClient);
        replicationCoordinator.start();
        rebuildReplicationGroups();

        replicaMonitor = new ReplicaMonitor(clusterManager);
        replicaLifecycleManager = new ReplicaLifecycleManager();
        rebuildReplicaRuntimeState();

        int failoverThreadPool = parseIntProperty(config, "failover.thread.pool.size", 3);
        int recoveryThreadPool = parseIntProperty(config, "recovery.thread.pool.size", 2);
        failoverCoordinator = new FailoverCoordinator(clusterManager, metadataManager, replicaMonitor, replicaLifecycleManager,
            new GrpcRegionServerCommandClient(clusterManager), 3, 30000, 300000, 10000, 60000, failoverThreadPool);
        failoverCoordinator.setZkClient(zkClient);
        failoverCoordinator.setReplicationCoordinator(replicationCoordinator);

        recoveryCoordinator = new RecoveryCoordinator(
            clusterManager, metadataManager, replicaMonitor, replicationCoordinator, replicaLifecycleManager,
            new GrpcRegionServerCommandClient(clusterManager), recoveryThreadPool);
        recoveryCoordinator.start();

        monitoringService = new MonitoringService(clusterManager, metadataManager, replicaMonitor, replicaLifecycleManager);
        replicaMonitor.registerCallback(monitoringService.replicaEventCallback());
        failoverCoordinator.setMonitoringService(monitoringService);
        recoveryCoordinator.setMonitoringService(monitoringService);

        int repairThreadPool = parseIntProperty(config, "repair.thread.pool.size", 4);
        dataRepairCoordinator = new DataRepairCoordinator(clusterManager, metadataManager, repairThreadPool);
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
                continue;
            }

            List<ServerId> orderedServers = new ArrayList<>(replicaServers);
            replicationCoordinator.createReplicaGroup(region, orderedServers);
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

    private void startGrpcServer(String host, int port) throws IOException {
        logger.info("Starting gRPC server...");

        serviceImpl = new MasterServiceImpl(
            clusterManager,
            metadataManager,
            loadBalancer,
            replicationCoordinator,
            replicaMonitor,
            failoverCoordinator,
            recoveryCoordinator,
            replicaLifecycleManager,
            new GrpcRegionServerCommandClient(clusterManager),
            hotSpotDetectorIntervalMs,
            hotSpotSettings,
            loadBalanceEnabled,
            loadBalanceIntervalMs,
            config
        );
        serviceImpl.setMonitoringService(monitoringService);
        serviceImpl.setZkClient(zkClient);
        serviceImpl.setLeader(false);

        grpcServer = ServerBuilder.forPort(port)
            .addService(serviceImpl)
            .maxInboundMessageSize(32 * 1024 * 1024)
            .build()
            .start();

        logger.info("gRPC server started on port {}", port);
        startMonitorServer(host);
    }

    private void startMonitorServer(String host) throws IOException {
        String monitorHost = System.getProperty("minisql.monitor.host", host);
        int monitorPort = Integer.parseInt(System.getProperty("minisql.monitor.port", "16010"));
        SqlConsoleService sqlConsoleService = zkClient == null ? null : new SqlConsoleService(zkClient.getConnectString());
        monitorHttpServer = new MonitorHttpServer(monitoringService, sqlConsoleService);
        monitorHttpServer.setDemoService(new com.minisql.master.monitoring.DemoService(
            monitoringService, clusterManager, metadataManager, loadBalancer,
            serviceImpl != null ? serviceImpl.getMigrationCoordinator() : null,
            sqlConsoleService));
        monitorHttpServer.start(monitorHost, monitorPort);
        logger.info("Monitor HTTP server started on {}:{}", monitorHost, monitorPort);
    }

    private void startLeaderElection() throws Exception {
        zkManager.addListener(new ZkManager.ServerListener() {
            @Override
            public void onLeadershipChange(boolean isLeader) {
                if (serviceImpl != null) {
                    serviceImpl.setLeader(isLeader);
                }
                if (isLeader) {
                    try {
                        zkManager.publishLeader();
                    } catch (Exception e) {
                        logger.error("Failed to publish leader node", e);
                    }
                }
                logger.info("Master leadership changed: {} -> {}", masterServerId.getInstanceName(), isLeader);
            }

            @Override
            public void onServerAdded(String path) {
                ServerId serverId = parseServerIdFromPath(path);
                if (serverId == null) {
                    return;
                }
                clusterManager.registerServer(serverId, System.currentTimeMillis());
                logger.info("RegionServer discovered via ZooKeeper: {}", serverId);
            }

            @Override
            public void onServerRemoved(String path) {
                if (!zkManager.isLeader()) {
                    return;
                }
                ServerId serverId = parseServerIdFromPath(path);
                if (serverId == null) {
                    return;
                }
                // 防竞态：RS 快速重启后旧 ZK session 过期触发 remove，
                // 但此时 ZK 里已有新的 ephemeral node，检查 ZK 确认是否仍在线
                try {
                    String serverKey = serverId.getHost() + ":" + serverId.getPort();
                    for (String active : zkManager.getActiveRegionServers()) {
                        if (active.startsWith(serverKey + "@") || active.equals(serverKey)) {
                            logger.info("Skipping stale ZK removal for re-registered RS: {} (new node exists)", serverId);
                            return;
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to verify ZK state for {}, proceeding with removal", serverId, e);
                }
                LinkedHashSet<String> affected = new LinkedHashSet<>(clusterManager.getRegionsAssignedToServer(serverId));
                affected.addAll(clusterManager.getRegionsReplicatedOnServer(serverId));
                List<String> affectedRegions = new ArrayList<>(affected);
                clusterManager.removeServer(serverId);
                logger.warn("RegionServer removed from ZooKeeper: {} affectedRegions={}", serverId, affectedRegions);
                if (serviceImpl != null && !affectedRegions.isEmpty()) {
                    // 处理每个受影响的 Region
                    for (String regionId : affectedRegions) {
                        ServerId primaryServer = clusterManager.getPrimaryServerForRegion(regionId);
                        boolean primaryFailed = serverId.equals(primaryServer);
                        serviceImpl.recoverRegionAfterServerFailure(regionId, serverId, primaryFailed);
                    }
                }
            }
        });
        zkManager.watchRegionServers();
        zkManager.participateMasterElection();
    }

    private ServerId parseServerIdFromPath(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        int separator = path.lastIndexOf('/');
        String serverPart = separator >= 0 ? path.substring(separator + 1) : path;
        if (serverPart.isBlank()) {
            return null;
        }
        int at = serverPart.indexOf('@');
        if (at > 0) {
            serverPart = serverPart.substring(0, at);
        }
        int colon = serverPart.lastIndexOf(':');
        if (colon <= 0 || colon >= serverPart.length() - 1) {
            // CuratorCache fires events for the parent path (e.g. /minisql/regionservers)
            // on startup — that's expected, not a warning-worthy condition.
            if (separator >= 0 && !serverPart.contains(":")) {
                logger.debug("Skipping non-server ZK path: {}", path);
            } else {
                logger.warn("Ignoring invalid RegionServer path from ZooKeeper: {}", path);
            }
            return null;
        }
        try {
            String host = serverPart.substring(0, colon);
            int port = Integer.parseInt(serverPart.substring(colon + 1));
            return new ServerId(host, port);
        } catch (NumberFormatException e) {
            logger.warn("Ignoring invalid RegionServer port from ZooKeeper path: {}", path);
            return null;
        }
    }
    private long parseLongProperty(Properties config, String key, long defaultValue) {
        String value = config.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid long property {}={}, fallback to {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    private int parseIntProperty(Properties config, String key, int defaultValue) {
        String value = config.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid int property {}={}, fallback to {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    private double parseDoubleProperty(Properties config, String key, double defaultValue) {
        String value = config.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid double property {}={}, fallback to {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    private boolean parseBooleanProperty(Properties config, String key, boolean defaultValue) {
        String value = config.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT);
        if ("true".equals(normalized) || "false".equals(normalized)) {
            return Boolean.parseBoolean(normalized);
        }
        logger.warn("Invalid boolean property {}={}, fallback to {}", key, value, defaultValue);
        return defaultValue;
    }
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Master...");
            try {
                stop();
            } catch (Exception e) {
                logger.error("Error during shutdown: {}", e.getMessage(), e);
            }
        }));
    }

    private void awaitTermination() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    /**
     * Start a background thread that periodically checks the config file
     * for changes and hot-reloads tunable parameters without a restart.
     */
    private void startConfigReloader() {
        configReloadRunning = true;
        Thread reloader = new Thread(() -> {
            logger.info("Config reloader started (poll interval: 30s, file: {})", configFilePath);
            while (configReloadRunning) {
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                try {
                    checkAndReloadConfig();
                } catch (Exception e) {
                    logger.warn("Config reload check failed: {}", e.getMessage());
                }
            }
            logger.info("Config reloader stopped");
        }, "ConfigReloader");
        reloader.setDaemon(true);
        reloader.start();
    }

    private void checkAndReloadConfig() {
        File f = new File(configFilePath);
        if (!f.isFile() || f.lastModified() <= configLastModified) {
            return;
        }
        logger.info("Config file {} changed (lastModified {} -> {}), reloading...",
            configFilePath, configLastModified, f.lastModified());

        Properties newConfig = new Properties();
        try (InputStream is = new FileInputStream(f)) {
            newConfig.load(is);
        } catch (IOException e) {
            logger.warn("Failed to read updated config file {}: {}", configFilePath, e.getMessage());
            return;
        }

        // Hot-reload load balancer settings
        if (loadBalancer != null) {
            LoadBalancer.Strategy strategy = LoadBalancer.Strategy.fromString(
                newConfig.getProperty("load.balance.strategy", "load_based"));
            double threshold = parseDoubleProperty(newConfig, "load.balance.threshold", loadBalancer.getBalanceThreshold());
            loadBalancer.setStrategy(strategy);
            loadBalancer.setBalanceThreshold(threshold);
            int maxMigrations = parseIntProperty(newConfig, "load.balance.max.migrations.per.round", 3);
            loadBalancer.setMaxMigrationsPerRound(maxMigrations);
            double hsPenalty = parseDoubleProperty(newConfig, "load.balance.hotspot.penalty.weight", 15.0);
            loadBalancer.setHotSpotPenaltyWeight(hsPenalty);
            loadBalanceEnabled = parseBooleanProperty(newConfig, "load.balance.enabled", loadBalanceEnabled);
            loadBalanceIntervalMs = Math.max(1_000L,
                parseLongProperty(newConfig, "load.balance.interval.ms", loadBalanceIntervalMs));
            logger.info("Reloaded load-balancer: strategy={} threshold={} enabled={} interval={}ms",
                strategy, threshold, loadBalanceEnabled, loadBalanceIntervalMs);
            if (serviceImpl != null) {
                serviceImpl.setLoadBalanceConfig(loadBalanceEnabled, loadBalanceIntervalMs);
            }
        }

        // Hot-reload hotspot settings
        long readThreshold = parseLongProperty(newConfig, "hotspot.read.threshold.per.interval",
            hotSpotSettings != null ? hotSpotSettings.getReadThresholdPerInterval() : 200L);
        long writeThreshold = parseLongProperty(newConfig, "hotspot.write.threshold.per.interval",
            hotSpotSettings != null ? hotSpotSettings.getWriteThresholdPerInterval() : 100L);
        long hotSpotCooldown = parseLongProperty(newConfig, "hotspot.cooldown.ms",
            hotSpotSettings != null ? hotSpotSettings.getCooldownMs() : TimeUnit.MINUTES.toMillis(5));
        hotSpotDetectorIntervalMs = parseLongProperty(newConfig, "hotspot.detector.interval.ms", hotSpotDetectorIntervalMs);
        if (hotSpotSettings != null) {
            hotSpotSettings = new HotSpotCoordinator.HotSpotSettings(
                readThreshold, writeThreshold,
                hotSpotSettings.getTargetReadReplicaCount(),
                hotSpotCooldown);
            if (serviceImpl != null) {
                serviceImpl.setHotSpotSettings(hotSpotSettings);
                serviceImpl.setHotSpotDetectorIntervalMs(hotSpotDetectorIntervalMs);
            }
            logger.info("Reloaded hotspot: readThresh={} writeThresh={} cooldown={}ms interval={}ms",
                readThreshold, writeThreshold, hotSpotCooldown, hotSpotDetectorIntervalMs);
        }

        this.config = newConfig;
        configLastModified = f.lastModified();
        logger.info("Config reloaded successfully from {}", configFilePath);
    }

    public void stop() throws Exception {
        configReloadRunning = false;
        if (dataRepairCoordinator != null) {
            dataRepairCoordinator.stop();
        }
        if (failoverCoordinator != null) {
            failoverCoordinator.shutdown();
        }
        if (recoveryCoordinator != null) {
            recoveryCoordinator.stop();
        }
        if (replicationCoordinator != null) {
            replicationCoordinator.stop();
        }
        if (grpcServer != null) {
            grpcServer.shutdown();
            grpcServer.awaitTermination(30, TimeUnit.SECONDS);
        }
        if (serviceImpl != null) {
            serviceImpl.shutdown();
        }
        if (monitorHttpServer != null) {
            monitorHttpServer.stop();
        }
        if (zkManager != null) {
            zkManager.close();
        } else if (zkClient != null) {
            zkClient.close();
        }
        if (embeddedZkServer != null) {
            embeddedZkServer.stop();
        }
    }
}
