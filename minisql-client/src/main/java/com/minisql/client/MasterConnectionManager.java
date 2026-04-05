package com.minisql.client;

import com.minisql.common.Constants;
import com.minisql.common.proto.MasterServiceGrpc;
import com.minisql.zookeeper.ZkClient;
import com.minisql.zookeeper.ZkPayloads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Master 连接管理器
 * 负责管理与 Master 的连接，支持自动重连
 */
public class MasterConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(MasterConnectionManager.class);

    private final ZkClient zkClient;
    private final AtomicReference<ManagedChannel> channelRef = new AtomicReference<>();
    private final AtomicReference<MasterServiceGrpc.MasterServiceBlockingStub> stubRef = new AtomicReference<>();

    // 当前连接的 Master 地址
    private volatile String currentMasterAddress;

    // 重试配置
    private final int maxRetries;
    private final long retryIntervalMs;
    private final long connectionTimeoutMs;

    public MasterConnectionManager(ZkClient zkClient) {
        this(zkClient, 3, 1000, 5000);
    }

    public MasterConnectionManager(ZkClient zkClient, int maxRetries, long retryIntervalMs, long connectionTimeoutMs) {
        this.zkClient = zkClient;
        this.maxRetries = maxRetries;
        this.retryIntervalMs = retryIntervalMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    /**
     * 初始化连接
     */
    public void initialize() throws SQLException {
        refreshConnection();
    }

    /**
     * 获取 Master Stub（自动处理连接问题）
     */
    public MasterServiceGrpc.MasterServiceBlockingStub getMasterStub() throws SQLException {
        MasterServiceGrpc.MasterServiceBlockingStub stub = stubRef.get();

        if (stub == null || !isConnectionHealthy()) {
            refreshConnection();
            stub = stubRef.get();
        }

        if (stub == null) {
            throw new SQLException("Failed to connect to Master");
        }

        return stub;
    }

    /**
     * 检查连接是否健康
     */
    private boolean isConnectionHealthy() {
        ManagedChannel channel = channelRef.get();
        if (channel == null) {
            return false;
        }

        // 检查通道状态
        if (channel.isShutdown() || channel.isTerminated()) {
            return false;
        }

        // 尝试简单的健康检查（通过调用 listTables）
        try {
            MasterServiceGrpc.MasterServiceBlockingStub stub = stubRef.get();
            if (stub == null) {
                return false;
            }

            // 使用短超时进行健康检查
            MasterServiceGrpc.MasterServiceBlockingStub healthCheckStub = stub.withDeadlineAfter(2000, TimeUnit.MILLISECONDS);
            com.minisql.common.proto.MasterProto.ListTablesRequest request =
                com.minisql.common.proto.MasterProto.ListTablesRequest.newBuilder().build();
            healthCheckStub.listTables(request);
            return true;

        } catch (StatusRuntimeException e) {
            // 如果是 UNAVAILABLE，说明连接有问题
            if (e.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE) {
                logger.warn("Master connection is unhealthy: {}", e.getMessage());
                return false;
            }
            // 其他错误（如 NOT_FOUND）说明连接正常，只是请求有问题
            return true;
        } catch (Exception e) {
            logger.warn("Health check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 刷新 Master 连接
     */
    public synchronized void refreshConnection() throws SQLException {
        // 关闭旧连接
        closeCurrentConnection();

        // 从 ZooKeeper 获取新的 Master 地址
        String newMasterAddress = getMasterAddressFromZk();

        if (newMasterAddress == null) {
            throw new SQLException("No Master available in ZooKeeper");
        }

        // 建立新连接（带重试）
        for (int i = 0; i < maxRetries; i++) {
            try {
                connectToMaster(newMasterAddress);
                currentMasterAddress = newMasterAddress;
                logger.info("Connected to Master: {}", newMasterAddress);
                return;
            } catch (Exception e) {
                logger.warn("Failed to connect to Master (attempt {}/{}): {}",
                    (i + 1), maxRetries, e.getMessage());

                if (i < maxRetries - 1) {
                    try {
                        Thread.sleep(retryIntervalMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted while connecting to Master", ie);
                    }

                    // 重试前再次从 ZK 获取地址（可能 Master 已变更）
                    newMasterAddress = getMasterAddressFromZk();
                    if (newMasterAddress == null) {
                        throw new SQLException("No Master available in ZooKeeper");
                    }
                }
            }
        }

        throw new SQLException("Failed to connect to Master after " + maxRetries + " attempts");
    }

    /**
     * 连接到指定 Master
     */
    private void connectToMaster(String address) {
        String[] parts = address.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : Constants.DEFAULT_MASTER_PORT;

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();

        MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel)
            .withDeadlineAfter(connectionTimeoutMs, TimeUnit.MILLISECONDS);

        channelRef.set(channel);
        stubRef.set(stub);
    }

    /**
     * 关闭当前连接
     */
    private void closeCurrentConnection() {
        ManagedChannel channel = channelRef.getAndSet(null);
        stubRef.set(null);

        if (channel != null) {
            try {
                channel.shutdown();
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * 从 ZooKeeper 获取 Master 地址
     */
    private String getMasterAddressFromZk() {
        try {
            String masterPath = Constants.ZK_MASTER_LEADER_PATH;
            if (zkClient.exists(masterPath)) {
                byte[] data = zkClient.getData(masterPath);
                return ZkPayloads.decodeLeaderAddress(data);
            }
        } catch (Exception e) {
            logger.warn("Failed to get Master address from ZooKeeper: {}", e.getMessage());
        }
        return null;
    }

    /**
     * 获取当前 Master 地址
     */
    public String getCurrentMasterAddress() {
        return currentMasterAddress;
    }

    /**
     * 关闭连接管理器
     */
    public void close() {
        closeCurrentConnection();
        logger.info("MasterConnectionManager closed");
    }
}
