package com.minisql.regionserver;

import com.minisql.storage.MySQLConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * RegionServer entrypoint backed by MySQL storage.
 */
public class RegionServerMain {

    private static final Logger logger = LoggerFactory.getLogger(RegionServerMain.class);

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 16020;
    private static final String DEFAULT_MASTER_ADDRESS = "localhost:16000";
    private static final String DEFAULT_ZK_CONNECT = "localhost:2181";
    private static final long DEFAULT_HEARTBEAT_INTERVAL = 5000L;

    private RegionServer regionServer;
    private HeartbeatSender heartbeatSender;

    public static void main(String[] args) {
        RegionServerMain server = new RegionServerMain();
        try {
            server.start(args);
        } catch (Exception e) {
            logger.error("Failed to start RegionServer", e);
            System.exit(1);
        }
    }

    public void start(String[] args) throws Exception {
        Properties config = loadConfig(args);

        String host = config.getProperty("regionserver.host", DEFAULT_HOST);
        int port = Integer.parseInt(config.getProperty("regionserver.port", String.valueOf(DEFAULT_PORT)));
        String masterAddress = config.getProperty("master.address", DEFAULT_MASTER_ADDRESS);
        String zkConnect = config.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
        long heartbeatInterval = Long.parseLong(
            config.getProperty("heartbeat.interval.ms", String.valueOf(DEFAULT_HEARTBEAT_INTERVAL)));

        String mysqlHost = config.getProperty("mysql.host", "localhost");
        int mysqlPort = Integer.parseInt(config.getProperty("mysql.port", "3306"));
        String mysqlDatabase = config.getProperty("mysql.database", "minisql");
        String mysqlUsername = config.getProperty("mysql.username", "root");
        String mysqlPassword = config.getProperty("mysql.password", "");
        int mysqlMaxConnections = Integer.parseInt(config.getProperty("mysql.max.connections", "10"));

        long diskCapacityMb = Long.parseLong(config.getProperty("regionserver.disk.capacity.mb", "1024"));
        long splitThresholdMb = Long.parseLong(config.getProperty("regionserver.region.split.threshold.mb", "256"));
        long splitMinMb = Long.parseLong(config.getProperty("regionserver.region.split.min.mb", "64"));

        logger.info("========================================");
        logger.info("  MiniSQL RegionServer Starting...");
        logger.info("========================================");
        logger.info("Host: {}", host);
        logger.info("Port: {}", port);
        logger.info("ZooKeeper: {}", zkConnect);
        logger.info("Master fallback: {}", masterAddress);
        logger.info("MySQL: {}:{}/{}", mysqlHost, mysqlPort, mysqlDatabase);
        logger.info("Heartbeat interval: {}ms", heartbeatInterval);
        logger.info("----------------------------------------");

        MySQLConfig mysqlConfig = MySQLConfig.builder(
            "jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/" + mysqlDatabase,
            mysqlUsername,
            mysqlPassword
        ).maxPoolSize(mysqlMaxConnections).build();

        initRegionServer(host, port, mysqlConfig, masterAddress, splitThresholdMb, splitMinMb);
        initHeartbeatSender(zkConnect, masterAddress, heartbeatInterval, mysqlConfig, diskCapacityMb);

        logger.info("----------------------------------------");
        logger.info("RegionServer started successfully");
        logger.info("Listening on {}:{}", host, port);
        logger.info("========================================");

        addShutdownHook();
        awaitTermination();
    }

    private Properties loadConfig(String[] args) {
        Properties config = new Properties();
        String configFile = args.length > 0 ? args[0] : "regionserver.properties";

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

        String masterAddr = System.getenv("MINISQL_MASTER_ADDRESS");
        if (masterAddr != null) {
            config.setProperty("master.address", masterAddr);
        }
        String zkConnect = System.getenv("MINISQL_ZK_CONNECT");
        if (zkConnect != null) {
            config.setProperty("zookeeper.connect", zkConnect);
        }
        String rsHost = System.getenv("MINISQL_RS_HOST");
        if (rsHost != null) {
            config.setProperty("regionserver.host", rsHost);
        }
        String rsPort = System.getenv("MINISQL_RS_PORT");
        if (rsPort != null) {
            config.setProperty("regionserver.port", rsPort);
        }
        String mysqlHost = System.getenv("MINISQL_MYSQL_HOST");
        if (mysqlHost != null) {
            config.setProperty("mysql.host", mysqlHost);
        }
        String mysqlPassword = System.getenv("MINISQL_MYSQL_PASSWORD");
        if (mysqlPassword != null) {
            config.setProperty("mysql.password", mysqlPassword);
        }

        return config;
    }

    private InputStream getConfigInputStream(String configFile) throws IOException {
        try {
            return new FileInputStream(configFile);
        } catch (IOException e) {
            InputStream is = getClass().getClassLoader().getResourceAsStream(configFile);
            if (is == null) {
                is = getClass().getClassLoader().getResourceAsStream("regionserver.properties");
            }
            return is;
        }
    }

    private void initRegionServer(String host,
                                  int port,
                                  MySQLConfig mysqlConfig,
                                  String masterAddress,
                                  long splitThresholdMb,
                                  long splitMinMb) throws IOException {
        regionServer = new RegionServer(host, port, mysqlConfig, masterAddress);
        regionServer.getSplitService().setConfig(splitThresholdMb, splitMinMb);
        regionServer.start();
    }

    private void initHeartbeatSender(String zkConnect,
                                     String masterAddress,
                                     long heartbeatInterval,
                                     MySQLConfig mysqlConfig,
                                     long diskCapacityMb) {
        heartbeatSender = new HeartbeatSender(
            regionServer.getServerId(),
            regionServer.getRegionManager(),
            heartbeatInterval
        );
        heartbeatSender.setZkConnectString(zkConnect);
        heartbeatSender.setMasterAddress(masterAddress);
        heartbeatSender.setMySQLConfig(mysqlConfig);
        heartbeatSender.setDiskCapacityMb(diskCapacityMb);
        heartbeatSender.start();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down RegionServer...");
            try {
                stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));
    }

    private void awaitTermination() throws InterruptedException {
        final Object lock = new Object();
        synchronized (lock) {
            while (regionServer.isRunning()) {
                lock.wait(1000);
            }
        }
    }

    public void stop() throws Exception {
        if (heartbeatSender != null) {
            heartbeatSender.stop();
        }
        if (regionServer != null) {
            regionServer.stop();
        }
    }
}
