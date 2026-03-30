package com.minisql.regionserver;

import com.minisql.common.Constants;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 心跳发送器
 * 负责定期向 Master 发送心跳
 */
public class HeartbeatSender {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatSender.class);

    private final ServerId serverId;
    private final RegionManager regionManager;
    private final ScheduledExecutorService scheduler;

    private volatile String masterAddress;
    private volatile ManagedChannel masterChannel;
    private volatile MasterServiceGrpc.MasterServiceBlockingStub masterStub;

    private final long heartbeatIntervalMs;
    private com.minisql.storage.MySQLConfig mysqlConfig;
    private long diskCapacityMb = 1024L; // Default 1GB

    // 是否正在运行
    private volatile boolean running = false;

    public HeartbeatSender(ServerId serverId, RegionManager regionManager) {
        this(serverId, regionManager, Constants.DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    public HeartbeatSender(ServerId serverId, RegionManager regionManager, long heartbeatIntervalMs) {
        this.serverId = serverId;
        this.regionManager = regionManager;
        this.heartbeatIntervalMs = heartbeatIntervalMs;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HeartbeatSender");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 设置 MySQL 配置
     */
    public void setMySQLConfig(com.minisql.storage.MySQLConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    public void setDiskCapacityMb(long diskCapacityMb) {
        this.diskCapacityMb = diskCapacityMb;
    }

    /**
     * 设置 Master 地址
     */
    public synchronized void setMasterAddress(String address) {
        if (address == null || address.equals(this.masterAddress)) {
            return;
        }

        // 关闭旧连接
        closeMasterConnection();

        this.masterAddress = address;

        // 建立新连接
        connectToMaster(address);
    }

    /**
     * 连接到 Master
     */
    private void connectToMaster(String address) {
        try {
            String[] parts = address.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : Constants.DEFAULT_MASTER_PORT;

            masterChannel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

            // 不设置全局 Deadline，改为在每次请求时设置
            masterStub = MasterServiceGrpc.newBlockingStub(masterChannel);

            logger.info("HeartbeatSender connected to Master: {}", address);
        } catch (Exception e) {
            logger.error("Failed to connect to Master: {}", e.getMessage());
            closeMasterConnection();
        }
    }

    /**
     * 关闭 Master 连接
     */
    private void closeMasterConnection() {
        if (masterChannel != null) {
            try {
                masterChannel.shutdown();
                masterChannel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Error closing Master connection: {}", e.getMessage());
            }
            masterChannel = null;
            masterStub = null;
        }
    }

    /**
     * 启动心跳发送
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;

        // 先注册到 Master
        registerWithMaster();

        // 启动定时心跳
        scheduler.scheduleAtFixedRate(
            this::sendHeartbeat,
            heartbeatIntervalMs,
            heartbeatIntervalMs,
            TimeUnit.MILLISECONDS
        );

        logger.info("HeartbeatSender started, interval: {}ms", heartbeatIntervalMs);
    }

    /**
     * 停止心跳发送
     */
    public void stop() {
        running = false;
        scheduler.shutdown();
        closeMasterConnection();

        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("HeartbeatSender stopped");
    }

    /**
     * 向 Master 注册
     */
    private void registerWithMaster() {
        if (masterStub == null) {
            logger.error("Cannot register: Master not connected (masterStub is null)");
            logger.error("Master address: {}", masterAddress);
            return;
        }

        try {
            logger.info("Sending register request to Master...");

            MasterProto.RegisterRequest.Builder requestBuilder = MasterProto.RegisterRequest.newBuilder()
                .setServerId(CommonProto.ServerId.newBuilder()
                    .setHost(serverId.getHost())
                    .setPort(serverId.getPort())
                    .build())
                .setTimestamp(System.currentTimeMillis());

            // 添加 MySQL 配置
            if (mysqlConfig != null) {
                CommonProto.MySQLConfig mysqlConfigProto = CommonProto.MySQLConfig.newBuilder()
                    .setUrl(mysqlConfig.getJdbcUrl())
                    .setUser(mysqlConfig.getUsername())
                    .setPassword(mysqlConfig.getPassword())
                    .setMaxPoolSize(mysqlConfig.getMaxPoolSize())
                    .build();
                requestBuilder.setMysqlConfig(mysqlConfigProto);
                logger.info("MySQL config included in register request: {}", mysqlConfig.getJdbcUrl());
            }

            MasterProto.RegisterRequest request = requestBuilder.build();
            MasterProto.RegisterResponse response = masterStub.registerRegionServer(request);

            if (response.getStatus().getSuccess()) {
                logger.info("Registered with Master successfully, clusterId: {}", response.getClusterId());
            } else {
                logger.error("Failed to register with Master: {}", response.getStatus().getMessage());
            }
        } catch (Exception e) {
            logger.error("Error registering with Master: {}", e.getMessage());
            logger.debug("Register exception details:", e);
        }
    }

    /**
     * 发送心跳
     */
    private void sendHeartbeat() {
        if (masterStub == null) {
            // 尝试重新连接
            if (masterAddress != null) {
                connectToMaster(masterAddress);
            }
            return;
        }

        try {
            // 构建心跳请求
            MasterProto.HeartbeatRequest.Builder requestBuilder = MasterProto.HeartbeatRequest.newBuilder()
                .setServerId(CommonProto.ServerId.newBuilder()
                    .setHost(serverId.getHost())
                    .setPort(serverId.getPort())
                    .build())
                .setTimestamp(System.currentTimeMillis());

            // 添加 Region 负载信息
            List<MasterProto.RegionLoad> regionLoads = collectRegionLoads();
            requestBuilder.addAllRegionLoads(regionLoads);

            // 添加服务器指标
            MasterProto.ServerMetrics metrics = collectServerMetrics();
            requestBuilder.setMetrics(metrics);

            // 发送心跳，设置 10 秒超时
            MasterProto.HeartbeatResponse response = masterStub
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .heartbeat(requestBuilder.build());

            if (response.getStatus().getSuccess()) {
                // 心跳仅用于确认存活，不再处理命令
                // 所有管理操作通过独立 gRPC 调用完成（openRegion, closeRegion, splitRegion, mergeRegion 等）
            } else {
                logger.error("Heartbeat failed: {}", response.getStatus().getMessage());
            }

        } catch (Exception e) {
            logger.error("Error sending heartbeat: {}", e.getMessage());
            // 连接可能已断开，尝试重新连接
            closeMasterConnection();
            if (masterAddress != null) {
                connectToMaster(masterAddress);
            }
        }
    }

    /**
     * 收集 Region 负载信息
     */
    private List<MasterProto.RegionLoad> collectRegionLoads() {
        List<MasterProto.RegionLoad> loads = new ArrayList<>();

        try {
            for (com.minisql.common.model.Region region : regionManager.getAllRegions()) {
                MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(region.getRegionId());
                if (storage != null) {
                    // 使用实际大小（不再返回 0）
                    long reportSize = storage.getStoreFileSize();

                    MasterProto.RegionLoad load = MasterProto.RegionLoad.newBuilder()
                        .setRegionId(region.getRegionId())
                        .setReadRequests(storage.getReadRequestCount())
                        .setWriteRequests(storage.getWriteRequestCount())
                        .setStoreFileSize(reportSize)  // 不再返回 0
                        .setMemStoreSize(0)
                        .build();
                    loads.add(load);
                }
            }
        } catch (Exception e) {
            logger.error("Error collecting region loads: {}", e.getMessage());
        }

        return loads;
    }

    /**
     * 收集服务器指标
     */
    private MasterProto.ServerMetrics collectServerMetrics() {
        try {
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

            // CPU 强制硬编码为 0.0（测试环境获取不到有意义的值）
            double cpuUsage = 0.0;

            long totalMemory = memoryBean.getHeapMemoryUsage().getMax();
            long usedMemory = memoryBean.getHeapMemoryUsage().getUsed();
            double memoryUsage = totalMemory > 0 ? (double) usedMemory / totalMemory : 0;

            // 获取磁盘空间（总空间使用配置文件设定的配置上限）
            long totalSpace = diskCapacityMb * 1024L * 1024L;
            long usedSpace = 0;
            
            // 使用所有 Region 的物理大小总计作为 usedSpace
            for (com.minisql.common.model.Region region : regionManager.getAllRegions()) {
                MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(region.getRegionId());
                if (storage != null) {
                    usedSpace += storage.getStoreFileSize();
                }
            }
            
            long availableSpace = totalSpace - usedSpace;
            if (availableSpace < 0) {
                availableSpace = 0;
            }

            return MasterProto.ServerMetrics.newBuilder()
                .setCpuUsage(cpuUsage)
                .setMemoryUsage(memoryUsage)
                .setAvailableSpace(availableSpace)
                .setTotalSpace(totalSpace)
                .build();

        } catch (Exception e) {
            logger.error("Error collecting server metrics: {}", e.getMessage());
            return MasterProto.ServerMetrics.getDefaultInstance();
        }
    }
}
