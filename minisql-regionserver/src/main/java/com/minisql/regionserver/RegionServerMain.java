package com.minisql.regionserver;

import com.minisql.storage.MySQLConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * RegionServer 启动主类
 * 基于 MySQL 存储引擎
 */
public class RegionServerMain {

    private static final Logger logger = LoggerFactory.getLogger(RegionServerMain.class);

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 16020;
    private static final String DEFAULT_MASTER_ADDRESS = "localhost:16000";
    private static final long DEFAULT_HEARTBEAT_INTERVAL = 5000; // 5 秒

    private RegionServer regionServer;
    private HeartbeatSender heartbeatSender;

    public static void main(String[] args) {
        RegionServerMain server = new RegionServerMain();
        try {
            server.start(args);
        } catch (Exception e) {
            logger.error("Failed to start RegionServer: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    /**
     * 启动 RegionServer 服务
     */
    public void start(String[] args) throws Exception {
        // 加载配置
        Properties config = loadConfig(args);

        String host = config.getProperty("regionserver.host", DEFAULT_HOST);
        int port = Integer.parseInt(config.getProperty("regionserver.port", String.valueOf(DEFAULT_PORT)));
        String masterAddress = config.getProperty("master.address", DEFAULT_MASTER_ADDRESS);
        long heartbeatInterval = Long.parseLong(
            config.getProperty("heartbeat.interval.ms", String.valueOf(DEFAULT_HEARTBEAT_INTERVAL)));

        // MySQL 配置
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
        logger.info("Master: {}", masterAddress);
        logger.info("MySQL: {}:{}/{}", mysqlHost, mysqlPort, mysqlDatabase);
        logger.info("Heartbeat Interval: {}ms", heartbeatInterval);
        logger.info("----------------------------------------");

        // 初始化 MySQL 配置
        MySQLConfig mysqlConfig = MySQLConfig.builder(
            "jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/" + mysqlDatabase,
            mysqlUsername,
            mysqlPassword
        ).maxPoolSize(mysqlMaxConnections).build();

        // 初始化并启动 RegionServer
        initRegionServer(host, port, mysqlConfig, masterAddress, splitThresholdMb, splitMinMb);

        // 初始化并启动心跳发送器
        initHeartbeatSender(masterAddress, heartbeatInterval, mysqlConfig, diskCapacityMb);

        logger.info("----------------------------------------");
        logger.info("RegionServer started successfully!");
        logger.info("Listening on {}:{}", host, port);
        logger.info("========================================");

        // 添加关闭钩子
        addShutdownHook();

        // 阻塞等待
        awaitTermination();
    }

    /**
     * 加载配置文件
     */
    private Properties loadConfig(String[] args) {
        Properties config = new Properties();

        // 先加载默认配置
        String configFile = "regionserver.properties";

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
        String masterAddr = System.getenv("MINISQL_MASTER_ADDRESS");
        if (masterAddr != null) {
            config.setProperty("master.address", masterAddr);
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
                is = getClass().getClassLoader().getResourceAsStream("regionserver.properties");
            }
            return is;
        }
    }

    /**
     * 初始化 RegionServer
     */
    private void initRegionServer(String host, int port, MySQLConfig mysqlConfig, String masterAddress, long splitThresholdMb, long splitMinMb) throws IOException {
        logger.info("Initializing RegionServer with MySQL storage...");

        regionServer = new RegionServer(host, port, mysqlConfig, masterAddress);
        
        // 设置区域分离策略的配置
        regionServer.getSplitService().setConfig(splitThresholdMb, splitMinMb);
        
        regionServer.start();

        logger.info("RegionServer initialized");
    }

    /**
     * 初始化心跳发送器
     */
    private void initHeartbeatSender(String masterAddress, long heartbeatInterval, MySQLConfig mysqlConfig, long diskCapacityMb) {
        logger.info("Initializing heartbeat sender...");

        heartbeatSender = new HeartbeatSender(
            regionServer.getServerId(),
            regionServer.getRegionManager(),
            heartbeatInterval
        );

        // 设置 Master 地址
        heartbeatSender.setMasterAddress(masterAddress);

        // 设置 MySQL 配置
        heartbeatSender.setMySQLConfig(mysqlConfig);
        
        // 设置磁盘容量限制
        heartbeatSender.setDiskCapacityMb(diskCapacityMb);

        // 启动心跳发送
        heartbeatSender.start();

        logger.info("Heartbeat sender started");
    }

    /**
     * 添加关闭钩子
     */
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("\n========================================");
            logger.info("  Shutting down RegionServer...");
            logger.info("========================================");
            try {
                stop();
            } catch (Exception e) {
                logger.error("Error during shutdown: {}", e.getMessage());
            }
            logger.info("RegionServer shutdown complete");
        }));
    }

    /**
     * 等待终止信号
     */
    private void awaitTermination() throws InterruptedException {
        // 使用一个计数器来保持主线程运行
        final Object lock = new Object();
        synchronized (lock) {
            while (regionServer.isRunning()) {
                lock.wait(1000);
            }
        }
    }

    /**
     * 停止 RegionServer 服务
     */
    public void stop() throws Exception {
        // 停止心跳发送器
        if (heartbeatSender != null) {
            logger.info("Stopping heartbeat sender...");
            heartbeatSender.stop();
            logger.info("Heartbeat sender stopped");
        }

        // 停止 RegionServer
        if (regionServer != null) {
            logger.info("Stopping RegionServer...");
            regionServer.stop();
            logger.info("RegionServer stopped");
        }
    }
}
