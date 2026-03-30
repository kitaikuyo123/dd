package com.minisql.master.recover;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.*;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;
import java.util.concurrent.*;

/**
 * 数据修复管理器
 * 协调数据损坏的修复过程，从健康副本复制数据
 */
public class DataRepairCoordinator {

    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final ExecutorService repairExecutor;

    // 正在进行的修复任务
    private final Map<String, RepairTask> activeRepairs = new ConcurrentHashMap<>();

    // 修复历史
    private final List<RepairRecord> repairHistory = new CopyOnWriteArrayList<>();

    public DataRepairCoordinator(ClusterManager clusterManager, MetadataManager metadataManager) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;

        this.repairExecutor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "DataRepair-Worker");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 停止修复管理器
     */
    public void stop() {
        repairExecutor.shutdown();
        try {
            if (!repairExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                repairExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            repairExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 调度数据修复
     *
     * @param regionId 损坏的 Region ID
     * @param corruptedServer 数据损坏的服务器
     * @return 修复任务 ID
     */
    public String scheduleRepair(String regionId, ServerId corruptedServer) {
        String taskId = UUID.randomUUID().toString();

        // 检查是否已在修复中
        if (activeRepairs.containsKey(regionId)) {
            System.out.println("Repair already in progress for region: " + regionId);
            return activeRepairs.get(regionId).getTaskId();
        }

        RepairTask task = new RepairTask(taskId, regionId, corruptedServer);
        activeRepairs.put(regionId, task);

        System.out.println("Scheduled repair task " + taskId + " for region " + regionId);

        // 异步执行修复
        repairExecutor.submit(() -> executeRepair(task));

        return taskId;
    }

    /**
     * 执行数据修复
     */
    private void executeRepair(RepairTask task) {
        String regionId = task.getRegionId();
        ServerId corruptedServer = task.getCorruptedServer();

        try {
            task.setStatus(RepairStatus.IN_PROGRESS);
            task.setStartTime(System.currentTimeMillis());

            System.out.println("Starting repair for region " + regionId);

            // 1. 获取 Region 信息
            Region region = metadataManager.getRegion(regionId);
            if (region == null) {
                throw new IllegalStateException("Region not found: " + regionId);
            }

            // 2. 获取健康副本列表
            List<ServerId> healthyReplicas = findHealthyReplicas(regionId, corruptedServer);

            if (healthyReplicas.isEmpty()) {
                throw new IllegalStateException("No healthy replicas available for repair: " + regionId);
            }

            // 3. 选择最佳副本（复制进度最快的）
            ServerId sourceReplica = selectBestSourceReplica(regionId, healthyReplicas);
            System.out.println("Selected source replica for repair: " + sourceReplica);

            // 4. 如果损坏的是主副本，先进行故障转移
            ServerId currentPrimary = clusterManager.getPrimaryServerForRegion(regionId);
            if (corruptedServer.equals(currentPrimary)) {
                System.out.println("Corrupted server is primary, performing failover");
                failoverPrimary(regionId, sourceReplica);
            }

            // 5. 触发全量同步
            boolean syncSuccess = triggerFullSync(regionId, sourceReplica, corruptedServer);

            if (syncSuccess) {
                task.setStatus(RepairStatus.COMPLETED);
                System.out.println("Repair completed successfully for region " + regionId);
            } else {
                task.setStatus(RepairStatus.FAILED);
                System.err.println("Repair failed for region " + regionId);
            }

            // 6. 记录修复历史
            RepairRecord record = new RepairRecord(
                task.getTaskId(),
                regionId,
                corruptedServer,
                sourceReplica,
                task.getStartTime(),
                System.currentTimeMillis(),
                task.getStatus()
            );
            repairHistory.add(record);

        } catch (Exception e) {
            System.err.println("Error repairing region " + regionId + ": " + e.getMessage());
            task.setStatus(RepairStatus.FAILED);
            task.setErrorMessage(e.getMessage());
        } finally {
            activeRepairs.remove(regionId);
            task.setEndTime(System.currentTimeMillis());
        }
    }

    /**
     * 查找健康副本
     */
    private List<ServerId> findHealthyReplicas(String regionId, ServerId excludeServer) {
        List<ServerId> healthyReplicas = new ArrayList<>();
        List<ServerId> allReplicas = clusterManager.getReplicaServers(regionId);

        for (ServerId replica : allReplicas) {
            if (replica.equals(excludeServer)) {
                continue;
            }

            // 检查副本是否健康（在活跃服务器列表中）
            for (ClusterManager.ServerInfo info : clusterManager.getActiveServers()) {
                if (info.getServerId().equals(replica)) {
                    healthyReplicas.add(replica);
                    break;
                }
            }
        }

        return healthyReplicas;
    }

    /**
     * 选择最佳源副本（复制进度最快的）
     */
    private ServerId selectBestSourceReplica(String regionId, List<ServerId> candidates) {
        ServerId best = null;
        long maxSequenceId = -1;

        for (ServerId candidate : candidates) {
            long seqId = clusterManager.getReplicaSequenceId(regionId, candidate);
            if (seqId > maxSequenceId) {
                maxSequenceId = seqId;
                best = candidate;
            }
        }

        return best != null ? best : candidates.get(0);
    }

    /**
     * 故障转移主副本
     */
    private void failoverPrimary(String regionId, ServerId newPrimary) {
        clusterManager.promoteReplicaToPrimary(regionId, newPrimary);

        // 通知新主副本
        try {
            ManagedChannel channel = ManagedChannelBuilder
                .forAddress(newPrimary.getHost(), newPrimary.getPort())
                .usePlaintext()
                .build();

            try {
                RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                    RegionServerServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(10000, TimeUnit.MILLISECONDS);

                RegionServerProto.PromoteRequest request = RegionServerProto.PromoteRequest.newBuilder()
                    .setRegionId(regionId)
                    .build();

                RegionServerProto.PromoteResponse response = stub.promoteToPrimary(request);

                if (response.getStatus().getSuccess()) {
                    System.out.println("Failover successful, " + newPrimary + " is now primary for " + regionId);
                } else {
                    System.err.println("Failover failed: " + response.getStatus().getMessage());
                }

            } finally {
                channel.shutdown();
            }
        } catch (Exception e) {
            System.err.println("Error during failover: " + e.getMessage());
        }
    }

    /**
     * 触发全量同步
     */
    private boolean triggerFullSync(String regionId, ServerId sourceServer, ServerId targetServer) {
        try {
            ManagedChannel channel = ManagedChannelBuilder
                .forAddress(sourceServer.getHost(), sourceServer.getPort())
                .usePlaintext()
                .build();

            try {
                RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                    RegionServerServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(300000, TimeUnit.MILLISECONDS); // 5分钟超时

                RegionServerProto.MigrateRequest request = RegionServerProto.MigrateRequest.newBuilder()
                    .setRegionId(regionId)
                    .setTargetServer(CommonProto.ServerId.newBuilder()
                        .setHost(targetServer.getHost())
                        .setPort(targetServer.getPort())
                        .build())
                    .build();

                RegionServerProto.MigrateResponse response = stub.startMigration(request);

                if (response.getStatus().getSuccess()) {
                    System.out.println("Full sync triggered successfully from " + sourceServer + " to " + targetServer);

                    // 等待同步完成（简化实现）
                    return waitForSyncCompletion(regionId, targetServer);
                } else {
                    System.err.println("Failed to trigger full sync: " + response.getStatus().getMessage());
                    return false;
                }

            } finally {
                channel.shutdown();
            }
        } catch (Exception e) {
            System.err.println("Error triggering full sync: " + e.getMessage());
            return false;
        }
    }

    /**
     * 等待同步完成
     */
    private boolean waitForSyncCompletion(String regionId, ServerId targetServer) {
        // 简化实现：等待固定时间
        // 实际应该轮询同步进度
        try {
            Thread.sleep(10000); // 等待 10 秒
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 获取活跃的修复任务
     */
    public Collection<RepairTask> getActiveRepairs() {
        return activeRepairs.values();
    }

    /**
     * 获取修复历史
     */
    public List<RepairRecord> getRepairHistory() {
        return new ArrayList<>(repairHistory);
    }

    /**
     * 修复状态
     */
    public enum RepairStatus {
        PENDING,
        IN_PROGRESS,
        COMPLETED,
        FAILED
    }

    /**
     * 修复任务
     */
    public static class RepairTask {
        private final String taskId;
        private final String regionId;
        private final ServerId corruptedServer;
        private volatile RepairStatus status = RepairStatus.PENDING;
        private volatile long startTime;
        private volatile long endTime;
        private volatile String errorMessage;

        public RepairTask(String taskId, String regionId, ServerId corruptedServer) {
            this.taskId = taskId;
            this.regionId = regionId;
            this.corruptedServer = corruptedServer;
        }

        public String getTaskId() { return taskId; }
        public String getRegionId() { return regionId; }
        public ServerId getCorruptedServer() { return corruptedServer; }
        public RepairStatus getStatus() { return status; }
        public void setStatus(RepairStatus status) { this.status = status; }
        public long getStartTime() { return startTime; }
        public void setStartTime(long startTime) { this.startTime = startTime; }
        public long getEndTime() { return endTime; }
        public void setEndTime(long endTime) { this.endTime = endTime; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    }

    /**
     * 修复记录
     */
    public static class RepairRecord {
        private final String taskId;
        private final String regionId;
        private final ServerId corruptedServer;
        private final ServerId sourceServer;
        private final long startTime;
        private final long endTime;
        private final RepairStatus status;

        public RepairRecord(String taskId, String regionId, ServerId corruptedServer,
                           ServerId sourceServer, long startTime, long endTime, RepairStatus status) {
            this.taskId = taskId;
            this.regionId = regionId;
            this.corruptedServer = corruptedServer;
            this.sourceServer = sourceServer;
            this.startTime = startTime;
            this.endTime = endTime;
            this.status = status;
        }

        public String getTaskId() { return taskId; }
        public String getRegionId() { return regionId; }
        public ServerId getCorruptedServer() { return corruptedServer; }
        public ServerId getSourceServer() { return sourceServer; }
        public long getStartTime() { return startTime; }
        public long getEndTime() { return endTime; }
        public RepairStatus getStatus() { return status; }
    }
}
