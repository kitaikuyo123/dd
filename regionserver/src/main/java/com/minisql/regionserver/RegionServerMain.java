package com.minisql.regionserver;

import com.minisql.storage.RocksDBConfig;
import com.minisql.storage.RocksDBEngineFactory;
import com.minisql.storage.StorageEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * RegionServer entrypoint.
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

        long diskCapacityMb = Long.parseLong(config.getProperty("regionserver.disk.capacity.mb", "1024"));
        long splitThresholdMb = Long.parseLong(config.getProperty("regionserver.region.split.threshold.mb", "256"));
        long splitMinMb = Long.parseLong(config.getProperty("regionserver.region.split.min.mb", "64"));
        int replicationFactor = Integer.parseInt(config.getProperty("replication.factor", "3"));

        logger.info("========================================");
        logger.info("  MiniSQL RegionServer Starting...");
        logger.info("========================================");
        logger.info("Host: {}", host);
        logger.info("Port: {}", port);
        logger.info("ZooKeeper: {}", zkConnect);
        logger.info("Master fallback: {}", masterAddress);
        logger.info("Heartbeat interval: {}ms", heartbeatInterval);
        logger.info("----------------------------------------");

        // Storage engine (currently RocksDB only)
        String rocksdbDir = config.getProperty("rocksdb.data.dir", "./data/rocksdb");
        long writeBufferMb = Long.parseLong(config.getProperty("rocksdb.write.buffer.size.mb", "64"));
        String compression = config.getProperty("rocksdb.compression", "snappy");
        RocksDBConfig rocksDBConfig = RocksDBConfig.builder(rocksdbDir)
            .writeBufferSizeBytes(writeBufferMb * 1024 * 1024)
            .compressionType(compression)
            .build();
        StorageEngineFactory engineFactory = new RocksDBEngineFactory(rocksDBConfig);
        logger.info("Using RocksDB storage engine at {}", rocksdbDir);

        initRegionServer(host, port, engineFactory, masterAddress, splitThresholdMb, splitMinMb, replicationFactor);
        initHeartbeatSender(zkConnect, masterAddress, heartbeatInterval, diskCapacityMb);

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
                                  StorageEngineFactory engineFactory,
                                  String masterAddress,
                                  long splitThresholdMb,
                                  long splitMinMb,
                                  int replicationFactor) throws IOException {
        regionServer = new RegionServer(host, port, engineFactory, masterAddress, replicationFactor);
        regionServer.getSplitService().setConfig(splitThresholdMb, splitMinMb);
        regionServer.start();
    }

    private void initHeartbeatSender(String zkConnect,
                                     String masterAddress,
                                     long heartbeatInterval,
                                     long diskCapacityMb) {
        heartbeatSender = new HeartbeatSender(
            regionServer.getServerId(),
            regionServer.getRegionManager(),
            heartbeatInterval
        );
        heartbeatSender.setZkConnectString(zkConnect);
        heartbeatSender.setMasterAddress(masterAddress);
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
