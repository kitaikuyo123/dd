package com.minisql.master.rebalance;

import com.minisql.common.proto.*;
import com.minisql.common.model.ServerId;
import com.minisql.master.recover.DataExporter;
import com.minisql.master.recover.DataImporter;
import com.minisql.master.recover.DataVerifier;
import com.minisql.master.state.ClusterManager;
import com.minisql.storage.MySQLConfig;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.minisql.common.proto.MigrationProto.*;

/**
 * 迁移协调器
 * 负责调度和管理 Region 迁移任务
 */
public class MigrationCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(MigrationCoordinator.class);

    private final ClusterManager clusterManager;
    private final ExecutorService executor;

    // 进行中的迁移任务
    private final Map<String, MigrationTaskInfo> runningMigrations = new ConcurrentHashMap<>();

    // 已完成的迁移任务历史
    private final Map<String, MigrationTaskInfo> completedMigrations = new ConcurrentHashMap<>();

    public MigrationCoordinator(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.executor = Executors.newFixedThreadPool(5);
    }

    /**
     * 开始迁移 Region
     */
    public StartMigrationResponse startMigration(StartMigrationRequest request,
                                                  StreamObserver<StartMigrationResponse> responseObserver) {
        String regionId = request.getRegionId();
        ServerId source = toInternalServerId(request.getSource());
        ServerId target = toInternalServerId(request.getTarget());

        // 检查 Region 是否存在
        ClusterManager.RegionAssignment assignment = clusterManager.getRegionAssignment(regionId);
        if (assignment == null) {
            responseObserver.onNext(createErrorResponse("Region not found: " + regionId));
            responseObserver.onCompleted();
            return null;
        }

        // 检查目标服务器是否可用
        if (!clusterManager.isServerActive(target)) {
            responseObserver.onNext(createErrorResponse("Target server is not active: " + target));
            responseObserver.onCompleted();
            return null;
        }

        // 创建迁移任务
        String migrationId = generateMigrationId(regionId);
        MigrationTaskInfo task = createMigrationTask(migrationId, regionId, source, target);

        // 保存迁移任务
        runningMigrations.put(migrationId, task);

        // 异步执行迁移
        executor.submit(() -> executeMigration(migrationId, regionId, source, target, request.getTargetMySQLConfig(), request.getVerifyData()));

        StartMigrationResponse response = StartMigrationResponse.newBuilder()
                .setStatus(CommonProto.Status.newBuilder().setSuccess(true).setMessage("Migration started"))
                .setMigrationId(migrationId)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        return response;
    }

    /**
     * 执行迁移流程
     */
    private void executeMigration(String migrationId, String regionId,
                                   ServerId source,
                                   ServerId target,
                                   CommonProto.MySQLConfig targetMySQLConfig,
                                   boolean verifyData) {
        MigrationTaskInfo task = runningMigrations.get(migrationId);
        if (task == null) {
            return;
        }

        try {
            // 1. 锁定 Region（暂停写入）
            updateTaskState(task, MigrationState.PENDING, "Locking region");

            // 2. 导出源数据
            updateTaskState(task, MigrationState.EXPORTING, "Exporting data from source");
            DataExporter.ExportResult exportResult = exportData(regionId, source);
            task.setTotalRows(exportResult.getRowCount());

            // 3. 导入目标
            updateTaskState(task, MigrationState.IMPORTING, "Importing data to target");
            importData(regionId, exportResult, target, targetMySQLConfig, task);

            // 4. 验证数据一致性（可选）
            if (verifyData) {
                updateTaskState(task, MigrationState.VERIFYING, "Verifying data consistency");
                verifyDataConsistency(regionId, source, target, targetMySQLConfig);
            }

            // 5. 更新路由表
            updateTaskState(task, MigrationState.COMPLETED, "Updating routing table");
            clusterManager.updateRegionAssignment(regionId, target);

            // 完成任务
            completeTask(task);

        } catch (Exception e) {
            failTask(task, e.getMessage());
        }
    }

    /**
     * 导出 Region 数据
     */
    private DataExporter.ExportResult exportData(String regionId, ServerId source) throws Exception {
        // 获取源服务器的 MySQL 配置
        MySQLConfig mysqlConfig = clusterManager.getMySQLConfig(source);
        if (mysqlConfig == null) {
            throw new RuntimeException("MySQL config not found for source server: " + source);
        }

        // 使用 DataExporter 导出数据
        DataExporter exporter = new DataExporter(mysqlConfig);
        return exporter.exportRegion(regionId);
    }

    /**
     * 导入数据到目标
     */
    private void importData(String regionId, DataExporter.ExportResult exportResult,
                            ServerId target,
                            CommonProto.MySQLConfig targetMySQLConfig,
                            MigrationTaskInfo task) throws Exception {
        // 构建目标 MySQL 配置
        MySQLConfig mysqlConfig = MySQLConfig.builder(
                targetMySQLConfig.getUrl(),
                targetMySQLConfig.getUser(),
                targetMySQLConfig.getPassword()
        ).maxPoolSize(targetMySQLConfig.getMaxPoolSize()).build();

        // 使用 DataImporter 导入数据
        DataImporter importer = new DataImporter(mysqlConfig);
        importer.importData(regionId, exportResult, migratedRows -> {
            task.setMigratedRows(migratedRows);
        });
    }

    /**
     * 验证数据一致性
     */
    private void verifyDataConsistency(String regionId, ServerId source, ServerId target,
                                        CommonProto.MySQLConfig targetMySQLConfig) throws Exception {
        // 获取源服务器的 MySQL 配置
        MySQLConfig sourceConfig = clusterManager.getMySQLConfig(source);
        if (sourceConfig == null) {
            throw new RuntimeException("MySQL config not found for source server: " + source);
        }

        // 构建目标 MySQL 配置
        MySQLConfig targetConfig = MySQLConfig.builder(
                targetMySQLConfig.getUrl(),
                targetMySQLConfig.getUser(),
                targetMySQLConfig.getPassword()
        ).maxPoolSize(targetMySQLConfig.getMaxPoolSize()).build();

        // 使用 DataVerifier 验证
        DataVerifier verifier = new DataVerifier(sourceConfig, targetConfig);

        // 首先进行行数验证
        DataVerifier.VerificationResult rowResult = verifier.verifyRowCount(regionId);
        if (!rowResult.isConsistent()) {
            throw new RuntimeException("Data verification failed: " + rowResult.getMessage());
        }

        // 然后进行 checksum 验证
        DataVerifier.VerificationResult checksumResult = verifier.verifyChecksum(regionId);
        if (!checksumResult.isConsistent()) {
            throw new RuntimeException("Data verification failed: " + checksumResult.getMessage());
        }

        logger.info("Data verification completed successfully for region: {}", regionId);
    }

    /**
     * 查询迁移进度
     */
    public GetProgressResponse getMigrationProgress(String migrationId) {
        MigrationTaskInfo task = runningMigrations.get(migrationId);
        if (task == null) {
            task = completedMigrations.get(migrationId);
        }

        if (task == null) {
            return GetProgressResponse.newBuilder()
                    .setStatus(CommonProto.Status.newBuilder().setSuccess(false).setMessage("Migration not found"))
                    .build();
        }

        double progress = calculateProgress(task);

        return GetProgressResponse.newBuilder()
                .setStatus(CommonProto.Status.newBuilder().setSuccess(true))
                .setMigrationId(migrationId)
                .setState(task.getState())
                .setProgress(progress)
                .setTotalRows(task.getTotalRows())
                .setMigratedRows(task.getMigratedRows())
                .setCurrentStage(task.getCurrentStage())
                .setErrorMessage(task.getErrorMessage())
                .build();
    }

    /**
     * 取消迁移
     */
    public CancelResponse cancelMigration(String migrationId, String reason) {
        MigrationTaskInfo task = runningMigrations.get(migrationId);
        if (task == null) {
            return CancelResponse.newBuilder()
                    .setStatus(CommonProto.Status.newBuilder().setSuccess(false).setMessage("Migration not found"))
                    .build();
        }

        // 取消迁移
        task.setState(MigrationState.CANCELLED);
        task.setErrorMessage("Cancelled: " + reason);

        return CancelResponse.newBuilder()
                .setStatus(CommonProto.Status.newBuilder().setSuccess(true).setMessage("Migration cancelled"))
                .build();
    }

    // ========== 辅助方法 ==========

    private StartMigrationResponse createErrorResponse(String message) {
        return StartMigrationResponse.newBuilder()
                .setStatus(CommonProto.Status.newBuilder().setSuccess(false).setMessage(message))
                .build();
    }

    private String generateMigrationId(String regionId) {
        return "mig-" + regionId + "-" + System.currentTimeMillis();
    }

    private MigrationTaskInfo createMigrationTask(String migrationId, String regionId,
                                                   ServerId source,
                                                   ServerId target) {
        return new MigrationTaskInfo(migrationId, regionId, source, target);
    }

    private void updateTaskState(MigrationTaskInfo task, MigrationState state, String stage) {
        task.setState(state);
        task.setCurrentStage(stage);
        if (state == MigrationState.COMPLETED || state == MigrationState.FAILED || state == MigrationState.CANCELLED) {
            runningMigrations.remove(task.getMigrationId());
            completedMigrations.put(task.getMigrationId(), task);
        }
    }

    private void completeTask(MigrationTaskInfo task) {
        updateTaskState(task, MigrationState.COMPLETED, "Completed");
    }

    private void failTask(MigrationTaskInfo task, String errorMessage) {
        task.setState(MigrationState.FAILED);
        task.setErrorMessage(errorMessage);
        runningMigrations.remove(task.getMigrationId());
        completedMigrations.put(task.getMigrationId(), task);
    }

    private double calculateProgress(MigrationTaskInfo task) {
        if (task.getState() == MigrationState.PENDING) return 0.0;
        if (task.getState() == MigrationState.EXPORTING) return 0.25;
        if (task.getState() == MigrationState.IMPORTING) {
            if (task.getTotalRows() > 0) {
                return 0.25 + 0.5 * ((double) task.getMigratedRows() / task.getTotalRows());
            }
            return 0.5;
        }
        if (task.getState() == MigrationState.VERIFYING) return 0.75;
        if (task.getState() == MigrationState.COMPLETED) return 1.0;
        return 0.0;
    }

    private ServerId toInternalServerId(CommonProto.ServerId protoId) {
        return new ServerId(protoId.getHost(), protoId.getPort());
    }

    private CommonProto.ServerId toProtoServerId(ServerId internalId) {
        return CommonProto.ServerId.newBuilder()
                .setHost(internalId.getHost())
                .setPort(internalId.getPort())
                .build();
    }

    /**
     * 关闭执行器
     */
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 内部任务信息类（可变）
     */
    private static class MigrationTaskInfo {
        private final String migrationId;
        private final String regionId;
        private final ServerId source;
        private final ServerId target;
        private volatile MigrationState state;
        private final long createTime;
        private long startTime;
        private long endTime;
        private String errorMessage;
        private long totalRows;
        private volatile long migratedRows;
        private String currentStage;

        public MigrationTaskInfo(String migrationId, String regionId, ServerId source, ServerId target) {
            this.migrationId = migrationId;
            this.regionId = regionId;
            this.source = source;
            this.target = target;
            this.state = MigrationState.PENDING;
            this.createTime = System.currentTimeMillis();
        }

        public String getMigrationId() { return migrationId; }
        public String getRegionId() { return regionId; }
        public ServerId getSource() { return source; }
        public ServerId getTarget() { return target; }
        public MigrationState getState() { return state; }
        public void setState(MigrationState state) { this.state = state; }
        public long getCreateTime() { return createTime; }
        public long getStartTime() { return startTime; }
        public void setStartTime(long startTime) { this.startTime = startTime; }
        public long getEndTime() { return endTime; }
        public void setEndTime(long endTime) { this.endTime = endTime; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        public long getTotalRows() { return totalRows; }
        public void setTotalRows(long totalRows) { this.totalRows = totalRows; }
        public long getMigratedRows() { return migratedRows; }
        public void setMigratedRows(long migratedRows) { this.migratedRows = migratedRows; }
        public String getCurrentStage() { return currentStage; }
        public void setCurrentStage(String currentStage) { this.currentStage = currentStage; }

        public com.minisql.common.proto.MigrationProto.MigrationTask toProto() {
            return com.minisql.common.proto.MigrationProto.MigrationTask.newBuilder()
                    .setMigrationId(migrationId)
                    .setRegionId(regionId)
                    .setSource(CommonProto.ServerId.newBuilder().setHost(source.getHost()).setPort(source.getPort()).build())
                    .setTarget(CommonProto.ServerId.newBuilder().setHost(target.getHost()).setPort(target.getPort()).build())
                    .setState(state)
                    .setCreateTime(createTime)
                    .setStartTime(startTime)
                    .setEndTime(endTime)
                    .setErrorMessage(errorMessage != null ? errorMessage : "")
                    .setTotalRows(totalRows)
                    .setMigratedRows(migratedRows)
                    .setCurrentStage(currentStage != null ? currentStage : "")
                    .build();
        }
    }
}
