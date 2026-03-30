package com.minisql.master.monitoring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ClusterEventTimeline {

    private static final long DEFAULT_RETENTION_MS = 24L * 60L * 60L * 1000L;

    public static final class ClusterEvent {
        private final long timestamp;
        private final String type;
        private final String severity;
        private final String regionId;
        private final String tableName;
        private final String sourceServer;
        private final String targetServer;
        private final String message;
        private final String details;

        public ClusterEvent(long timestamp, String type, String severity, String regionId, String tableName,
                            String sourceServer, String targetServer, String message, String details) {
            this.timestamp = timestamp;
            this.type = type;
            this.severity = severity;
            this.regionId = regionId;
            this.tableName = tableName;
            this.sourceServer = sourceServer;
            this.targetServer = targetServer;
            this.message = message;
            this.details = details;
        }

        public long getTimestamp() { return timestamp; }
        public String getType() { return type; }
        public String getSeverity() { return severity; }
        public String getRegionId() { return regionId; }
        public String getTableName() { return tableName; }
        public String getSourceServer() { return sourceServer; }
        public String getTargetServer() { return targetServer; }
        public String getMessage() { return message; }
        public String getDetails() { return details; }
    }

    private final long retentionMs;
    private final ConcurrentLinkedDeque<ClusterEvent> events = new ConcurrentLinkedDeque<>();

    public ClusterEventTimeline() {
        this(DEFAULT_RETENTION_MS);
    }

    public ClusterEventTimeline(long retentionMs) {
        this.retentionMs = retentionMs;
    }

    public void record(String type, String severity, String regionId, String tableName,
                       String sourceServer, String targetServer, String message, String details) {
        purgeExpired();
        events.addLast(new ClusterEvent(System.currentTimeMillis(), type, severity, regionId, tableName,
            sourceServer, targetServer, message, details));
    }

    public List<ClusterEvent> query(Set<String> types, int limit) {
        purgeExpired();
        List<ClusterEvent> result = new ArrayList<>();
        for (ClusterEvent event : events) {
            if (types == null || types.isEmpty() || types.contains(event.getType())) {
                result.add(event);
            }
        }
        result.sort(Comparator.comparingLong(ClusterEvent::getTimestamp).reversed());
        if (limit > 0 && result.size() > limit) {
            return new ArrayList<>(result.subList(0, limit));
        }
        return result;
    }

    public List<ClusterEvent> latest(int limit) {
        return query(Collections.emptySet(), limit);
    }

    private void purgeExpired() {
        long cutoff = System.currentTimeMillis() - retentionMs;
        while (true) {
            ClusterEvent first = events.peekFirst();
            if (first == null || first.getTimestamp() >= cutoff) {
                break;
            }
            events.pollFirst();
        }
    }
}
