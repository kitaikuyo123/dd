package com.minisql.master.state;

import com.minisql.common.model.ServerId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks replica lifecycle transitions across bootstrap, recovery, balancing
 * and failover paths so all orchestration flows report into a single state map.
 */
public class ReplicaLifecycleManager {

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

        System.out.println("[REPLICA-LIFECYCLE] region=" + regionId +
            " server=" + serverId +
            " state=" + state +
            (detail != null && !detail.isEmpty() ? " detail=" + detail : ""));
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
    }

    private String buildKey(String regionId, ServerId serverId) {
        return regionId + "|" + serverId.getHost() + ":" + serverId.getPort();
    }
}
