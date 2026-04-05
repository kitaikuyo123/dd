package com.minisql.regionserver;

import com.minisql.common.Constants;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.MasterProto;
import com.minisql.common.proto.MasterServiceGrpc;
import com.minisql.zookeeper.ZkManager;
import com.minisql.zookeeper.ZkPayloads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sends registration and heartbeats to the current leader master.
 */
public class HeartbeatSender {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatSender.class);

    private final ServerId serverId;
    private final RegionManager regionManager;
    private final ScheduledExecutorService scheduler;
    private final long heartbeatIntervalMs;

    private volatile String masterAddress;
    private volatile ManagedChannel masterChannel;
    private volatile MasterServiceGrpc.MasterServiceBlockingStub masterStub;
    private volatile boolean registeredWithCurrentMaster;

    private volatile boolean running;
    private volatile String zkConnectString;
    private volatile ZkManager zkManager;

    private com.minisql.storage.MySQLConfig mysqlConfig;
    private long diskCapacityMb = 1024L;

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

    public void setMySQLConfig(com.minisql.storage.MySQLConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    public void setDiskCapacityMb(long diskCapacityMb) {
        this.diskCapacityMb = diskCapacityMb;
    }

    public void setZkConnectString(String zkConnectString) {
        this.zkConnectString = zkConnectString;
    }

    public synchronized void setMasterAddress(String address) {
        if (address == null || address.isBlank()) {
            return;
        }
        if (address.equals(this.masterAddress) && masterStub != null) {
            return;
        }

        closeMasterConnection();
        this.masterAddress = address;
        connectToMaster(address);
        registeredWithCurrentMaster = false;
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;

        startZkCoordinationIfConfigured();
        registerWithMaster();

        scheduler.scheduleAtFixedRate(
            this::sendHeartbeat,
            heartbeatIntervalMs,
            heartbeatIntervalMs,
            TimeUnit.MILLISECONDS
        );

        logger.info("HeartbeatSender started, interval={}ms", heartbeatIntervalMs);
    }

    public void stop() {
        running = false;
        scheduler.shutdown();
        closeMasterConnection();

        if (zkManager != null) {
            try {
                zkManager.close();
            } catch (Exception e) {
                logger.warn("Failed to close ZooKeeper coordination", e);
            } finally {
                zkManager = null;
            }
        }

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

    private void startZkCoordinationIfConfigured() {
        if (zkConnectString == null || zkConnectString.isBlank()) {
            return;
        }
        try {
            zkManager = new ZkManager(zkConnectString, serverId);
            zkManager.start();
            zkManager.addListener(new ZkManager.ServerListener() {
                @Override
                public void onLeadershipChange(boolean isLeader) {
                }

                @Override
                public void onLeaderAddressChanged(String leaderAddress) {
                    if (leaderAddress != null && !leaderAddress.isBlank()) {
                        setMasterAddress(leaderAddress);
                    }
                }

                @Override
                public void onServerAdded(String path) {
                }

                @Override
                public void onServerRemoved(String path) {
                }
            });
            zkManager.watchLeader();
            zkManager.registerRegionServer(ZkPayloads.encodeRegionServerNode(
                serverId,
                serverId.getServerName(),
                mysqlConfig == null ? "" : mysqlConfig.getJdbcUrl(),
                mysqlConfig == null ? "" : mysqlConfig.getUsername(),
                serverId.getStartTime(),
                0L
            ));
            logger.info("RegionServer registered in ZooKeeper: {}", serverId.getInstanceName());
        } catch (Exception e) {
            logger.error("Failed to initialize ZooKeeper-based master discovery", e);
        }
    }

    private void connectToMaster(String address) {
        try {
            String[] parts = address.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : Constants.DEFAULT_MASTER_PORT;

            masterChannel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
            masterStub = MasterServiceGrpc.newBlockingStub(masterChannel);
            logger.info("HeartbeatSender connected to Master: {}", address);
        } catch (Exception e) {
            logger.error("Failed to connect to Master: {}", address, e);
            closeMasterConnection();
        }
    }

    private void closeMasterConnection() {
        if (masterChannel != null) {
            try {
                masterChannel.shutdown();
                masterChannel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.debug("Error closing Master connection", e);
            }
        }
        masterChannel = null;
        masterStub = null;
        registeredWithCurrentMaster = false;
    }

    private void registerWithMaster() {
        if (masterStub == null || registeredWithCurrentMaster) {
            return;
        }

        try {
            MasterProto.RegisterRequest.Builder requestBuilder = MasterProto.RegisterRequest.newBuilder()
                .setServerId(CommonProto.ServerId.newBuilder()
                    .setHost(serverId.getHost())
                    .setPort(serverId.getPort())
                    .build())
                .setTimestamp(System.currentTimeMillis());

            if (mysqlConfig != null) {
                requestBuilder.setMysqlConfig(CommonProto.MySQLConfig.newBuilder()
                    .setUrl(mysqlConfig.getJdbcUrl())
                    .setUser(mysqlConfig.getUsername())
                    .setPassword(mysqlConfig.getPassword())
                    .setMaxPoolSize(mysqlConfig.getMaxPoolSize())
                    .build());
            }

            MasterProto.RegisterResponse response = masterStub.registerRegionServer(requestBuilder.build());
            if (response.getStatus().getSuccess()) {
                registeredWithCurrentMaster = true;
                logger.info("Registered with Master successfully, clusterId={}", response.getClusterId());
            } else {
                logger.warn("Master registration rejected: {}", response.getStatus().getMessage());
            }
        } catch (Exception e) {
            logger.error("Error registering with Master", e);
            registeredWithCurrentMaster = false;
        }
    }

    private void sendHeartbeat() {
        if (masterStub == null) {
            if (masterAddress != null) {
                connectToMaster(masterAddress);
                registerWithMaster();
            }
            return;
        }

        registerWithMaster();

        try {
            MasterProto.HeartbeatRequest.Builder requestBuilder = MasterProto.HeartbeatRequest.newBuilder()
                .setServerId(CommonProto.ServerId.newBuilder()
                    .setHost(serverId.getHost())
                    .setPort(serverId.getPort())
                    .build())
                .setTimestamp(System.currentTimeMillis())
                .addAllRegionLoads(collectRegionLoads())
                .setMetrics(collectServerMetrics());

            MasterProto.HeartbeatResponse response = masterStub
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .heartbeat(requestBuilder.build());

            if (!response.getStatus().getSuccess()) {
                logger.warn("Heartbeat rejected: {}", response.getStatus().getMessage());
                registeredWithCurrentMaster = false;
            }
        } catch (Exception e) {
            logger.error("Error sending heartbeat", e);
            closeMasterConnection();
            if (masterAddress != null) {
                connectToMaster(masterAddress);
            }
        }
    }

    private List<MasterProto.RegionLoad> collectRegionLoads() {
        List<MasterProto.RegionLoad> loads = new ArrayList<>();
        try {
            for (com.minisql.common.model.Region region : regionManager.getAllRegions()) {
                MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(region.getRegionId());
                if (storage == null) {
                    continue;
                }
                loads.add(MasterProto.RegionLoad.newBuilder()
                    .setRegionId(region.getRegionId())
                    .setReadRequests(storage.getReadRequestCount())
                    .setWriteRequests(storage.getWriteRequestCount())
                    .setStoreFileSize(storage.getStoreFileSize())
                    .setMemStoreSize(0)
                    .build());
            }
        } catch (Exception e) {
            logger.error("Error collecting region loads", e);
        }
        return loads;
    }

    private MasterProto.ServerMetrics collectServerMetrics() {
        try {
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            double cpuUsage = 0.0;

            long totalMemory = memoryBean.getHeapMemoryUsage().getMax();
            long usedMemory = memoryBean.getHeapMemoryUsage().getUsed();
            double memoryUsage = totalMemory > 0 ? (double) usedMemory / totalMemory : 0.0;

            long totalSpace = diskCapacityMb * 1024L * 1024L;
            long usedSpace = 0L;
            for (com.minisql.common.model.Region region : regionManager.getAllRegions()) {
                MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(region.getRegionId());
                if (storage != null) {
                    usedSpace += storage.getStoreFileSize();
                }
            }

            long availableSpace = Math.max(0L, totalSpace - usedSpace);
            return MasterProto.ServerMetrics.newBuilder()
                .setCpuUsage(cpuUsage)
                .setMemoryUsage(memoryUsage)
                .setAvailableSpace(availableSpace)
                .setTotalSpace(totalSpace)
                .build();
        } catch (Exception e) {
            logger.error("Error collecting server metrics", e);
            return MasterProto.ServerMetrics.getDefaultInstance();
        }
    }
}
