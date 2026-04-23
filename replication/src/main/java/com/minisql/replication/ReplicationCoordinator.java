package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinates replication lifecycle while delegating storage, transport and failover concerns.
 */
public class ReplicationCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationCoordinator.class);

    private final ReplicationConfig config;
    private final ReplicationWAL wal;
    private final ReplicaGroupRegistry registry;
    private final ReplicationTransportClient transportClient;
    private final ReplicaSyncCoordinator syncCoordinator;
    private final PrimaryChangeNotifier primaryChangeNotifier;
    private final PrimaryFailoverCoordinator failoverCoordinator;
    private final Map<String, BlockingQueue<ReplicationTask>> replicationQueues = new ConcurrentHashMap<>();
    private final ExecutorService replicationExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Set<String> catchUpInProgress = ConcurrentHashMap.newKeySet();
    private ScheduledExecutorService healthCheckScheduler;

    public ReplicationCoordinator(ReplicationConfig config) {
        this(config, null, new GrpcReplicationTransportClient());
    }

    public ReplicationCoordinator(ReplicationConfig config,
                                  ReplicationWAL wal,
                                  ReplicationTransportClient transportClient) {
        this.config = config;
        this.wal = wal;
        this.registry = new ReplicaGroupRegistry();
        this.transportClient = transportClient;
        this.primaryChangeNotifier = new PrimaryChangeNotifier();
        this.syncCoordinator = new ReplicaSyncCoordinator(registry, transportClient, config);
        this.failoverCoordinator = new PrimaryFailoverCoordinator(registry, config, primaryChangeNotifier);
        this.replicationExecutor = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            r -> {
                Thread t = new Thread(r, "Replication-Worker");
                t.setDaemon(true);
                return t;
            }
        );
    }

    public void setZkClient(ZkClient zkClient) {
        primaryChangeNotifier.setZkClient(zkClient);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        if (wal != null) {
            try {
                wal.initialize();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to initialize replication WAL", e);
            }
        }

        healthCheckScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Replication-HealthChecker");
            t.setDaemon(true);
            return t;
        });
        healthCheckScheduler.scheduleAtFixedRate(
            this::performHealthCheck,
            config.getHealthCheckIntervalMs(),
            config.getHealthCheckIntervalMs(),
            TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        if (healthCheckScheduler != null) {
            healthCheckScheduler.shutdownNow();
        }
        syncCoordinator.shutdown();
        replicationExecutor.shutdown();
        try {
            replicationExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        transportClient.close();
        if (wal != null) {
            wal.close();
        }
    }

    public void createReplicaGroup(Region region, List<ServerId> replicaServers) {
        ReplicaGroup group = registry.createReplicaGroup(region, replicaServers, config.getReplicationFactor());
        replicationQueues.put(region.getRegionId(), new LinkedBlockingQueue<>());
        startReplicationWorker(region.getRegionId());
        registry.recordPrimaryProgress(region.getRegionId(), currentSequenceId(region.getRegionId()));

        // Restore persisted replication progress for each replica
        if (wal != null) {
            for (ServerId replica : group.getReplicas()) {
                String addr = replica.getHost() + ":" + replica.getPort();
                long progress = wal.getAppliedProgress(region.getRegionId(), addr);
                if (progress > 0) {
                    registry.updateReplicaProgress(region.getRegionId(), replica, progress, 0L);
                    logger.info("Restored replication progress for region={} replica={}: seqId={}",
                        region.getRegionId(), addr, progress);
                }
            }
        }

        logger.info("Replica group created for region: {} with {} replicas",
            region.getRegionId(), group.getReplicas().size());
    }

    public ReplicationLogEntry logMutations(String regionId, List<KeyValue> mutations) {
        if (wal == null) {
            throw new IllegalStateException("ReplicationWAL is required for mutation logging");
        }
        try {
            ReplicationLogEntry entry = wal.append(regionId, mutations);
            registry.recordPrimaryProgress(regionId, entry.getSequenceId());
            return entry;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to append replication WAL for region " + regionId, e);
        }
    }

    public CompletableFuture<Boolean> replicate(String regionId, List<KeyValue> mutations) {
        return replicate(regionId, logMutations(regionId, mutations));
    }

    public CompletableFuture<Boolean> replicate(String regionId, ReplicationLogEntry entry) {
        ReplicaGroup group = registry.getReplicaGroup(regionId);
        if (group == null) {
            return CompletableFuture.completedFuture(false);
        }

        registry.recordPrimaryProgress(regionId, entry.getSequenceId());
        if (group.getReplicas().size() <= 1) {
            return CompletableFuture.completedFuture(true);
        }

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        BlockingQueue<ReplicationTask> queue = replicationQueues.computeIfAbsent(regionId, ignored -> {
            startReplicationWorker(regionId);
            return new LinkedBlockingQueue<>();
        });
        queue.offer(new ReplicationTask(entry, future));
        return future;
    }

    public boolean replicateSync(String regionId, List<KeyValue> mutations) {
        try {
            return replicate(regionId, mutations).get(config.getReplicationTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.warn("Synchronous replication failed for region {}: {}", regionId, e.getMessage());
            return false;
        }
    }

    public boolean replicateSync(String regionId, ReplicationLogEntry entry) {
        try {
            return replicate(regionId, entry).get(config.getReplicationTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.warn("Synchronous replication failed for region {}: {}", regionId, e.getMessage());
            return false;
        }
    }

    public void promoteToPrimary(String regionId, ServerId newPrimary) {
        failoverCoordinator.promoteToPrimary(regionId, newPrimary);
    }

    public void failover(String regionId) {
        failoverCoordinator.failover(regionId);
    }

    public void addReplica(String regionId, ServerId newReplica) {
        ReplicaGroup group = requireGroup(regionId);
        if (group.getReplicas().contains(newReplica)) {
            resyncReplica(regionId, newReplica);
            return;
        }
        registry.addReplica(regionId, newReplica, ReplicaRole.CANDIDATE);
        syncCoordinator.synchronizeReplica(regionId, newReplica, true, () -> currentSequenceId(regionId));
    }

    public boolean addReplicaSync(String regionId, ServerId newReplica, long timeoutMs) {
        ReplicaGroup group = requireGroup(regionId);
        if (!group.getReplicas().contains(newReplica)) {
            registry.addReplica(regionId, newReplica, ReplicaRole.CANDIDATE);
            return syncCoordinator.synchronizeReplicaSync(regionId, newReplica, true, () -> currentSequenceId(regionId), timeoutMs);
        }
        return resyncReplicaSync(regionId, newReplica, timeoutMs);
    }

    public void resyncReplica(String regionId, ServerId replica) {
        ReplicaGroup group = requireGroup(regionId);
        if (replica.equals(group.getPrimary())) {
            return;
        }
        syncCoordinator.synchronizeReplica(regionId, replica, false, () -> currentSequenceId(regionId));
    }

    public boolean resyncReplicaSync(String regionId, ServerId replica, long timeoutMs) {
        ReplicaGroup group = requireGroup(regionId);
        if (replica.equals(group.getPrimary())) {
            return true;
        }
        return syncCoordinator.synchronizeReplicaSync(regionId, replica, false, () -> currentSequenceId(regionId), timeoutMs);
    }

    public void removeReplica(String regionId, ServerId replica) {
        ReplicaGroup group = registry.getReplicaGroup(regionId);
        if (group == null) {
            return;
        }

        if (replica.equals(group.getPrimary()) && group.getReplicas().size() > 1) {
            failover(regionId);
        }
        registry.removeReplica(regionId, replica);
    }

    public void removeReplicaGroup(String regionId) {
        registry.removeReplicaGroup(regionId);
        replicationQueues.remove(regionId);
        if (wal != null) {
            try {
                wal.deleteRegion(regionId);
            } catch (Exception e) {
                logger.warn("Failed to delete WAL for region {}: {}", regionId, e.getMessage());
            }
        }
    }

    public ReplicaGroup getReplicaGroup(String regionId) {
        return registry.getReplicaGroup(regionId);
    }

    public Map<String, ReplicaGroup> getAllReplicaGroups() {
        return registry.getAllReplicaGroups();
    }

    public int getReplicationFactor() {
        return config.getReplicationFactor();
    }

    public ReplicationConfig getConfig() {
        return config;
    }

    public ReplicationWAL getWal() {
        return wal;
    }

    public boolean isConsistent(String regionId) {
        ReplicaGroup group = registry.getReplicaGroup(regionId);
        if (group == null) {
            return false;
        }

        long primarySequenceId = currentSequenceId(regionId);
        for (ServerId replica : group.getReplicas()) {
            ReplicaGroup.ReplicaState state = group.getReplicaState(replica);
            if (state != null && primarySequenceId - state.getLastAppliedSequenceId() > 10) {
                return false;
            }
        }
        return true;
    }

    private void startReplicationWorker(String regionId) {
        replicationExecutor.submit(() -> {
            BlockingQueue<ReplicationTask> queue = replicationQueues.get(regionId);
            if (queue == null) {
                return;
            }

            while (running.get()) {
                try {
                    ReplicationTask firstTask = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (firstTask == null) {
                        continue;
                    }

                    // Drain up to maxBatchSize tasks that are immediately available
                    int maxBatch = config.getMaxReplicationBatchSize();
                    List<ReplicationTask> batch = new ArrayList<>(maxBatch);
                    batch.add(firstTask);
                    queue.drainTo(batch, maxBatch - 1);

                    processBatchReplicationTask(regionId, batch);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    private void processBatchReplicationTask(String regionId, List<ReplicationTask> batch) {
        ReplicaGroup group = requireGroup(regionId);
        ServerId primary = group.getPrimary();
        if (primary == null) {
            for (ReplicationTask task : batch) {
                task.future.complete(false);
            }
            return;
        }

        // Collect all entries from the batch
        List<ReplicationLogEntry> entries = new ArrayList<>(batch.size());
        for (ReplicationTask task : batch) {
            entries.add(task.entry);
        }

        List<CompletableFuture<AckResult>> futures = new ArrayList<>();
        for (ServerId replica : group.getReplicas()) {
            if (replica.equals(primary)) {
                continue;
            }

            futures.add(CompletableFuture.supplyAsync(() -> {
                boolean success = false;
                for (int attempt = 0; attempt < config.getMaxRetryCount(); attempt++) {
                    success = transportClient.replicateBatch(replica, regionId, entries, config.getReplicationTimeoutMs());
                    if (success) {
                        break;
                    }
                    try {
                        Thread.sleep((long) Math.pow(2, attempt) * 100L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return new AckResult(replica, false);
                    }
                }
                return new AckResult(replica, success);
            }, replicationExecutor));
        }

        int totalReplicas = group.getReplicas().size();
        int secondaryCount = totalReplicas - 1;
        int requiredAcks;
        if (secondaryCount == 0) {
            requiredAcks = 0;
        } else if (config.isQuorumAckEnabled()) {
            int majority = totalReplicas / 2 + 1;
            requiredAcks = majority - 1;
        } else {
            requiredAcks = secondaryCount;
        }

        int successCount = 0;
        long lastSequenceId = entries.get(entries.size() - 1).getSequenceId();
        for (CompletableFuture<AckResult> future : futures) {
            try {
                AckResult result = future.get(config.getAckTimeoutMs(), TimeUnit.MILLISECONDS);
                if (result.success) {
                    successCount++;
                    registry.updateReplicaProgress(regionId, result.replica, lastSequenceId, 0L);
                    if (wal != null) {
                        for (ReplicationLogEntry entry : entries) {
                            try {
                                wal.markAsApplied(regionId, entry.getSequenceId(),
                                    result.replica.getHost() + ":" + result.replica.getPort());
                            } catch (Exception e) {
                                logger.warn("Failed to mark WAL as applied: {}", e.getMessage());
                            }
                        }
                    }
                }
            } catch (Exception ignored) {
                // timeout/transport failure counts as no ack
            }
        }

        boolean acked = successCount >= requiredAcks;
        if (!acked) {
            if (successCount == 0 && secondaryCount > 0) {
                logger.warn("All secondaries unreachable for region {}, degrading to primary-only write (total replicas={})",
                    regionId, totalReplicas);
                acked = true;
            } else if (secondaryCount > 0) {
                logger.warn("Replication ack shortfall for region {}: got {}/{} required secondary acks (total replicas={})",
                    regionId, successCount, requiredAcks, totalReplicas);
            }
        }

        for (ReplicationTask task : batch) {
            task.future.complete(acked);
        }

        if (successCount >= requiredAcks && wal != null) {
            try {
                long minConfirmed = computeMinConfirmedSequence(group, primary);
                wal.cleanup(regionId, config.getWalRetentionCount(), minConfirmed);
            } catch (Exception e) {
                logger.warn("WAL cleanup failed for region {}: {}", regionId, e.getMessage());
            }
        }
    }

    private void performHealthCheck() {
        for (Map.Entry<String, ReplicaGroup> entry : registry.getAllReplicaGroups().entrySet()) {
            String regionId = entry.getKey();
            ReplicaGroup group = entry.getValue();
            ServerId primary = group.getPrimary();
            if (primary == null) {
                continue;
            }

            // Refresh the primary's lastUpdateTime as a heartbeat.
            ReplicaGroup.ReplicaState primaryState = group.getReplicaState(primary);
            if (primaryState != null) {
                primaryState.setLastUpdateTime(System.currentTimeMillis());
            }

            // Auto-recovery: detect stale secondaries and attempt catch-up
            if (wal == null) continue;
            long primarySeqId = currentSequenceId(regionId);
            for (ServerId replica : group.getReplicas()) {
                if (replica.equals(primary)) continue;

                ReplicaGroup.ReplicaState state = group.getReplicaState(replica);
                if (state == null) continue;

                long lag = primarySeqId - state.getLastAppliedSequenceId();
                if (lag < config.getCatchUpLagThreshold()) continue;

                String catchUpKey = regionId + ":" + replica.getHost() + ":" + replica.getPort();
                if (!catchUpInProgress.add(catchUpKey)) continue;

                final long fromSeqId = state.getLastAppliedSequenceId();
                replicationExecutor.submit(() -> {
                    try {
                        attemptCatchUp(regionId, replica, fromSeqId);
                    } finally {
                        catchUpInProgress.remove(catchUpKey);
                    }
                });
            }
        }
    }

    private void attemptCatchUp(String regionId, ServerId replica, long fromSeqId) {
        List<ReplicationLogEntry> entries = wal.getEntries(regionId, fromSeqId + 1);
        if (entries.isEmpty()) return;

        // Check for WAL gap: if the first available entry is not the expected one
        if (entries.get(0).getSequenceId() > fromSeqId + 1) {
            logger.warn("WAL gap detected for region={} replica={}: expected seqId {} but found {}. " +
                "Falling back to full snapshot sync.",
                regionId, replica, fromSeqId + 1, entries.get(0).getSequenceId());
            syncCoordinator.synchronizeReplica(regionId, replica, false, () -> currentSequenceId(regionId));
            return;
        }

        boolean success = transportClient.replicateBatch(
            replica, regionId, entries, config.getReplicationTimeoutMs());
        if (success) {
            long lastSeqId = entries.get(entries.size() - 1).getSequenceId();
            registry.updateReplicaProgress(regionId, replica, lastSeqId, 0L);
            wal.markAsApplied(regionId, lastSeqId, replica.getHost() + ":" + replica.getPort());
            logger.info("Auto catch-up succeeded for region={} replica={}: advanced from {} to {}",
                regionId, replica, fromSeqId, lastSeqId);
        } else {
            logger.debug("Auto catch-up failed for region={} replica={}: replica still unreachable",
                regionId, replica);
        }
    }

    private long computeMinConfirmedSequence(ReplicaGroup group, ServerId primary) {
        long min = Long.MAX_VALUE;
        for (ServerId replica : group.getReplicas()) {
            ReplicaGroup.ReplicaState state = group.getReplicaState(replica);
            long seqId = (state != null) ? state.getLastAppliedSequenceId() : 0;
            if (seqId < min) min = seqId;
        }
        return min == Long.MAX_VALUE ? 0 : min;
    }

    private long currentSequenceId(String regionId) {
        if (wal != null) {
            try {
                return wal.getCurrentSequenceId(regionId);
            } catch (Exception e) {
                logger.debug("Falling back to in-memory replication progress for region {} " +
                    "because WAL is unavailable", regionId, e);
            }
        }
        ReplicaGroup group = registry.getReplicaGroup(regionId);
        if (group == null || group.getPrimary() == null) {
            return 0L;
        }
        ReplicaGroup.ReplicaState state = group.getReplicaState(group.getPrimary());
        return state == null ? 0L : state.getLastAppliedSequenceId();
    }

    private ReplicaGroup requireGroup(String regionId) {
        ReplicaGroup group = registry.getReplicaGroup(regionId);
        if (group == null) {
            throw new IllegalArgumentException("Replica group not found: " + regionId);
        }
        return group;
    }

    private static final class ReplicationTask {
        private final ReplicationLogEntry entry;
        private final CompletableFuture<Boolean> future;

        private ReplicationTask(ReplicationLogEntry entry, CompletableFuture<Boolean> future) {
            this.entry = entry;
            this.future = future;
        }
    }

    private static final class AckResult {
        private final ServerId replica;
        private final boolean success;

        private AckResult(ServerId replica, boolean success) {
            this.replica = replica;
            this.success = success;
        }
    }
}
