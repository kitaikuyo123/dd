package com.minisql.master.rpc;

import com.minisql.common.model.Region;
import com.minisql.common.model.ReplicaInfo;
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
import com.minisql.replication.ReplicationConfig;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.zookeeper.ZkClient;
import com.minisql.zookeeper.ZkManager;
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
        initComponents(config);
        startGrpcServer(host, port);
        startLeaderElection();

        logger.info("----------------------------------------");
        logger.info("Master started successfully");
        logger.info("Listening on {}:{}", host, port);
        logger.info("========================================");

        addShutdownHook();
        awaitTermination();
    }

    private Properties loadConfig(String[] args) {
        Properties config = new Properties();
        String configFile = args.length > 0 ? args[0] : "master.properties";

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
        loadBalancer.setStrategy(strategy);

        clusterManager = new ClusterManager(loadBalancer);
        clusterManager.setZkClient(zkClient);

        metadataManager = new MetadataManager(zkClient);

        int replicationFactor = 3;
        replicationCoordinator = new ReplicationCoordinator(ReplicationConfig.builder(replicationFactor).build());
        replicationCoordinator.setZkClient(zkClient);
        replicationCoordinator.start();
        rebuildReplicationGroups();

        replicaMonitor = new ReplicaMonitor(clusterManager);
        replicaMonitor.start();
        replicaLifecycleManager = new ReplicaLifecycleManager();
        rebuildReplicaRuntimeState();

        failoverCoordinator = new FailoverCoordinator(clusterManager, metadataManager, replicaMonitor, replicaLifecycleManager);
        failoverCoordinator.setZkClient(zkClient);

        recoveryCoordinator = new RecoveryCoordinator(
            clusterManager, metadataManager, replicaMonitor, replicationCoordinator, replicaLifecycleManager);
        recoveryCoordinator.start();

        monitoringService = new MonitoringService(clusterManager, metadataManager, replicaMonitor, replicaLifecycleManager);
        replicaMonitor.registerCallback(monitoringService.replicaEventCallback());
        failoverCoordinator.setMonitoringService(monitoringService);
        recoveryCoordinator.setMonitoringService(monitoringService);

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
            replicaLifecycleManager
        );
        serviceImpl.setMonitoringService(monitoringService);
        serviceImpl.setZkClient(zkClient);
        serviceImpl.setLeader(false);

        grpcServer = ServerBuilder.forPort(port)
            .addService(serviceImpl)
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
            logger.warn("Ignoring invalid RegionServer path from ZooKeeper: {}", path);
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

    public void stop() throws Exception {
        if (dataRepairCoordinator != null) {
            dataRepairCoordinator.stop();
        }
        if (failoverCoordinator != null) {
            failoverCoordinator.shutdown();
        }
        if (replicaMonitor != null) {
            replicaMonitor.stop();
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
