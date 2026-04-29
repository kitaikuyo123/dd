package com.minisql.replication;

import com.minisql.common.model.ServerId;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * 副本同步协调器
 *
 * 负责新副本的全量同步（Full Sync）和已有副本的重新同步（Resync）。
 * 同步流程:
 *   1. 将副本角色标记为 CANDIDATE
 *   2. 通过传输客户端执行快照流式传输（避免内存中物化完整快照）
 *   3. 更新副本进度和角色（CANDIDATE -> SECONDARY）
 *
 * 支持异步（CompletableFuture）和同步（带超时）两种调用模式。
 */
public class ReplicaSyncCoordinator {

    private final ReplicaGroupRegistry registry;
    private final ReplicationTransportClient transportClient;
    private final ReplicationConfig config;
    private final ExecutorService syncExecutor;

    public ReplicaSyncCoordinator(ReplicaGroupRegistry registry,
                                  ReplicationTransportClient transportClient,
                                  ReplicationConfig config) {
        this.registry = registry;
        this.transportClient = transportClient;
        this.config = config;
        this.syncExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "Replication-FullSync");
            t.setDaemon(true);
            return t;
        });
    }

    public void shutdown() {
        syncExecutor.shutdownNow();
    }

    public CompletableFuture<Boolean> synchronizeReplica(String regionId,
                                                         ServerId replica,
                                                         boolean newReplica,
                                                         LongSupplier currentSequenceSupplier) {
        return CompletableFuture.supplyAsync(() ->
            doSynchronize(regionId, replica, newReplica, currentSequenceSupplier), syncExecutor);
    }

    public boolean synchronizeReplicaSync(String regionId,
                                          ServerId replica,
                                          boolean newReplica,
                                          LongSupplier currentSequenceSupplier,
                                          long timeoutMs) {
        try {
            return synchronizeReplica(regionId, replica, newReplica, currentSequenceSupplier)
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Timed out waiting for full sync for region " + regionId + " on " + replica, e);
        } catch (Exception e) {
            throw new IllegalStateException("Full sync failed for region " + regionId + " on " + replica + ": " + e.getMessage(), e);
        }
    }

    private boolean doSynchronize(String regionId,
                                  ServerId replica,
                                  boolean newReplica,
                                  LongSupplier currentSequenceSupplier) {
        ReplicaGroup group = registry.getReplicaGroup(regionId);
        if (group == null) {
            throw new IllegalArgumentException("Replica group not found: " + regionId);
        }

        ServerId primary = group.getPrimary();
        if (primary == null) {
            throw new IllegalStateException("No primary replica for region: " + regionId);
        }
        if (primary.equals(replica)) {
            return true;
        }

        if (newReplica) {
            registry.addReplica(regionId, replica, ReplicaRole.CANDIDATE);
        } else {
            registry.updateReplicaRole(regionId, replica, ReplicaRole.CANDIDATE);
        }

        long currentSequence = currentSequenceSupplier == null ? 0L : currentSequenceSupplier.getAsLong();
        // Use direct streaming to avoid materializing the full snapshot in memory
        boolean success = transportClient.streamSnapshotDirect(
            primary,
            replica,
            regionId,
            1000,
            Math.max(config.getReplicationTimeoutMs(), config.getAckTimeoutMs() * 2),
            currentSequence
        );
        if (!success) {
            return false;
        }

        registry.updateReplicaProgress(regionId, replica, currentSequence, 0L);
        registry.updateReplicaRole(regionId, replica, ReplicaRole.SECONDARY);
        return true;
    }
}
