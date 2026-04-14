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
 * Handles full sync and resync flows for replicas.
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

        List<com.minisql.common.model.KeyValue> snapshot =
            transportClient.fetchSnapshot(primary, regionId, config.getReplicationTimeoutMs());
        long currentSequence = currentSequenceSupplier == null ? 0L : currentSequenceSupplier.getAsLong();
        boolean success = transportClient.sendSnapshot(
            replica,
            regionId,
            snapshot,
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
