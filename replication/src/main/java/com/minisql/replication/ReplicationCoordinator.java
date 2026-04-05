package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.storage.MySQLConfig;
import com.minisql.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private ScheduledExecutorService healthCheckScheduler;

    public ReplicationCoordinator(ReplicationConfig config) {
        this(config, null, new GrpcReplicationTransportClient());
    }

    public ReplicationCoordinator(ReplicationConfig config, MySQLConfig walConfig) {
        this(config, walConfig == null ? null : new ReplicationWAL(walConfig), new GrpcReplicationTransportClient());
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

    public static int requiredReplicaAcks(int replicaCount) {
        return Math.max(0, replicaCount / 2);
    }

    private void startReplicationWorker(String regionId) {
        replicationExecutor.submit(() -> {
            BlockingQueue<ReplicationTask> queue = replicationQueues.get(regionId);
            if (queue == null) {
                return;
            }

            while (running.get()) {
                try {
                    ReplicationTask task = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        processReplicationTask(regionId, task);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    private void processReplicationTask(String regionId, ReplicationTask task) {
        ReplicaGroup group = requireGroup(regionId);
        ServerId primary = group.getPrimary();
        if (primary == null) {
            task.future.complete(false);
            return;
        }

        List<CompletableFuture<AckResult>> futures = new ArrayList<>();
        for (ServerId replica : group.getReplicas()) {
            if (replica.equals(primary)) {
                continue;
            }

            futures.add(CompletableFuture.supplyAsync(() -> {
                boolean success = false;
                for (int attempt = 0; attempt < config.getMaxRetryCount(); attempt++) {
                    success = transportClient.replicate(replica, regionId, task.entry, config.getReplicationTimeoutMs());
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

        int requiredAcks = Math.max(0, config.getRequiredAcks(group.getReplicas().size()) - 1);
        int successCount = 0;
        for (CompletableFuture<AckResult> future : futures) {
            try {
                AckResult result = future.get(config.getAckTimeoutMs(), TimeUnit.MILLISECONDS);
                if (result.success) {
                    successCount++;
                    registry.updateReplicaProgress(regionId, result.replica, task.entry.getSequenceId(), 0L);
                    if (wal != null) {
                        try {
                            wal.markAsApplied(regionId, task.entry.getSequenceId(),
                                result.replica.getHost() + ":" + result.replica.getPort());
                        } catch (Exception e) {
                            logger.warn("Failed to mark WAL as applied: {}", e.getMessage());
                        }
                    }
                }
            } catch (Exception ignored) {
                // timeout/transport failure counts as no ack
            }
        }

        task.future.complete(successCount >= requiredAcks);
    }

    private void performHealthCheck() {
        for (Map.Entry<String, ReplicaGroup> entry : registry.getAllReplicaGroups().entrySet()) {
            String regionId = entry.getKey();
            ReplicaGroup group = entry.getValue();
            ServerId primary = group.getPrimary();
            if (primary == null) {
                continue;
            }

            ReplicaGroup.ReplicaState state = group.getReplicaState(primary);
            if (state == null || state.getLastUpdateTime() == 0L) {
                continue;
            }
            long timeSinceLastUpdate = System.currentTimeMillis() - state.getLastUpdateTime();
            if (timeSinceLastUpdate > config.getHealthCheckIntervalMs() * 3L && group.getReplicas().size() > 1) {
                logger.debug("Replication health check observed stale primary progress for region {}, " +
                    "but automatic failover is owned by the master control plane", regionId);
            }
        }
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
