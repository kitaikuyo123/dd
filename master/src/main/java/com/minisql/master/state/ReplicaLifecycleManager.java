package com.minisql.master.state;

import com.minisql.common.model.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Tracks replica lifecycle transitions across bootstrap, recovery, balancing
 * and failover paths so all orchestration flows report into a single state map.
 */
public class ReplicaLifecycleManager {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaLifecycleManager.class);

    public enum ReplicaLifecycleState {
        BOOTSTRAPPING,
        CATCHING_UP,
        SECONDARY_READY,
        FINALIZING,
        PROMOTING,
        PRIMARY_READY,
        LAGGING,
        OFFLINE,
        REBUILDING,
        REMOVED,
        FAILED
    }

    public static final class ReplicaLifecycleStatus {
        private final String regionId;
        private final ServerId serverId;
        private volatile ReplicaLifecycleState state;
        private volatile String detail;
        private volatile long updatedAt;

        private ReplicaLifecycleStatus(String regionId, ServerId serverId,
                                       ReplicaLifecycleState state, String detail) {
            this.regionId = regionId;
            this.serverId = serverId;
            this.state = state;
            this.detail = detail;
            this.updatedAt = System.currentTimeMillis();
        }

        public String getRegionId() {
            return regionId;
        }

        public ServerId getServerId() {
            return serverId;
        }

        public ReplicaLifecycleState getState() {
            return state;
        }

        public String getDetail() {
            return detail;
        }

        public long getUpdatedAt() {
            return updatedAt;
        }
    }

    private final Map<String, ReplicaLifecycleStatus> statuses = new ConcurrentHashMap<>();

    private final Map<String, CompletableFuture<ReplicaLifecycleState>> readyFutures = new ConcurrentHashMap<>();

    public void transition(String regionId, ServerId serverId, ReplicaLifecycleState state, String detail) {
        if (regionId == null || serverId == null || state == null) {
            return;
        }

        String key = buildKey(regionId, serverId);
        statuses.compute(key, (ignored, current) -> {
            if (current == null) {
                return new ReplicaLifecycleStatus(regionId, serverId, state, detail);
            }
            current.state = state;
            current.detail = detail;
            current.updatedAt = System.currentTimeMillis();
            return current;
        });

        logger.info("[REPLICA-LIFECYCLE] region={} server={} state={}{}",
            regionId, serverId, state,
            (detail != null && !detail.isEmpty() ? " detail=" + detail : ""));

        if (state == ReplicaLifecycleState.SECONDARY_READY) {
            CompletableFuture<ReplicaLifecycleState> f = readyFutures.remove(key);
            if (f != null) {
                f.complete(state);
            }
        }
    }

    public ReplicaLifecycleStatus getStatus(String regionId, ServerId serverId) {
        return statuses.get(buildKey(regionId, serverId));
    }

    public Map<String, ReplicaLifecycleStatus> getAllStatuses() {
        return new ConcurrentHashMap<>(statuses);
    }

    public void removeRegion(String regionId) {
        if (regionId == null) {
            return;
        }
        String prefix = regionId + "|";
        statuses.keySet().removeIf(key -> key.startsWith(prefix));
        readyFutures.keySet().removeIf(key -> key.startsWith(prefix));
    }

    /**
     * Returns a future that completes when the given replica reaches SECONDARY_READY.
     * If already ready, returns an already-completed future.
     */
    public CompletableFuture<ReplicaLifecycleState> whenReady(String regionId, ServerId serverId) {
        ReplicaLifecycleStatus current = getStatus(regionId, serverId);
        if (current != null && current.getState() == ReplicaLifecycleState.SECONDARY_READY) {
            return CompletableFuture.completedFuture(current.getState());
        }
        return readyFutures.computeIfAbsent(buildKey(regionId, serverId),
            k -> new CompletableFuture<>());
    }

    /**
     * Waits for all specified replicas of a region to reach SECONDARY_READY,
     * returning true if all reached in time, false on timeout.
     */
    public boolean awaitReplicasReady(String regionId, List<ServerId> replicaServers,
                                      long timeoutMs) {
        if (replicaServers == null || replicaServers.isEmpty()) {
            return true;
        }

        List<CompletableFuture<ReplicaLifecycleState>> futures = replicaServers.stream()
            .map(server -> whenReady(regionId, server))
            .collect(Collectors.toList());

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            return false;
        }
    }

    private String buildKey(String regionId, ServerId serverId) {
        return regionId + "|" + serverId.getHost() + ":" + serverId.getPort();
    }
}
