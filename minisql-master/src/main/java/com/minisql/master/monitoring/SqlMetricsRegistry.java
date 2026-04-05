package com.minisql.master.monitoring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class SqlMetricsRegistry {

    private static final long MINUTE_MS = 60_000L;
    private static final long DEFAULT_RETENTION_MS = 24L * 60L * 60L * 1000L;

    public static final class SqlMetricSummary {
        private final long requestCount;
        private final long successCount;
        private final long errorCount;
        private final double qps;
        private final double avgLatencyMs;
        private final double p95LatencyMs;
        private final long readCount;
        private final long writeCount;
        private final List<Map<String, Object>> points;

        public SqlMetricSummary(long requestCount, long successCount, long errorCount, double qps,
                                double avgLatencyMs, double p95LatencyMs, long readCount, long writeCount,
                                List<Map<String, Object>> points) {
            this.requestCount = requestCount;
            this.successCount = successCount;
            this.errorCount = errorCount;
            this.qps = qps;
            this.avgLatencyMs = avgLatencyMs;
            this.p95LatencyMs = p95LatencyMs;
            this.readCount = readCount;
            this.writeCount = writeCount;
            this.points = points;
        }

        public long getRequestCount() { return requestCount; }
        public long getSuccessCount() { return successCount; }
        public long getErrorCount() { return errorCount; }
        public double getQps() { return qps; }
        public double getAvgLatencyMs() { return avgLatencyMs; }
        public double getP95LatencyMs() { return p95LatencyMs; }
        public long getReadCount() { return readCount; }
        public long getWriteCount() { return writeCount; }
        public List<Map<String, Object>> getPoints() { return points; }
    }

    private static final class MinuteBucket {
        private final AtomicLong requestCount = new AtomicLong();
        private final AtomicLong successCount = new AtomicLong();
        private final AtomicLong errorCount = new AtomicLong();
        private final AtomicLong readCount = new AtomicLong();
        private final AtomicLong writeCount = new AtomicLong();
        private final AtomicLong latencySumMs = new AtomicLong();
        private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        private final ConcurrentMap<String, AtomicLong> tableRequestCounts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> tableErrorCounts = new ConcurrentHashMap<>();
    }

    private final long retentionMs;
    private final ConcurrentMap<Long, MinuteBucket> buckets = new ConcurrentHashMap<>();

    public SqlMetricsRegistry() {
        this(DEFAULT_RETENTION_MS);
    }

    public SqlMetricsRegistry(long retentionMs) {
        this.retentionMs = retentionMs;
    }

    public void record(String sqlType, String tableName, boolean success, long latencyMs) {
        long bucketKey = currentMinuteBucket(System.currentTimeMillis());
        MinuteBucket bucket = buckets.computeIfAbsent(bucketKey, ignored -> new MinuteBucket());
        bucket.requestCount.incrementAndGet();
        if (success) {
            bucket.successCount.incrementAndGet();
        } else {
            bucket.errorCount.incrementAndGet();
        }
        if (isRead(sqlType)) {
            bucket.readCount.incrementAndGet();
        } else {
            bucket.writeCount.incrementAndGet();
        }
        bucket.latencySumMs.addAndGet(Math.max(0L, latencyMs));
        bucket.latencies.add(Math.max(0L, latencyMs));
        String normalizedTable = tableName == null ? "" : tableName;
        bucket.tableRequestCounts.computeIfAbsent(normalizedTable, ignored -> new AtomicLong()).incrementAndGet();
        if (!success) {
            bucket.tableErrorCounts.computeIfAbsent(normalizedTable, ignored -> new AtomicLong()).incrementAndGet();
        }
        purgeExpired();
    }

    public SqlMetricSummary summarize(long windowMs) {
        purgeExpired();
        long cutoff = currentMinuteBucket(System.currentTimeMillis() - windowMs);
        List<Long> allLatencies = new ArrayList<>();
        List<Map<String, Object>> points = new ArrayList<>();
        long requestCount = 0L;
        long successCount = 0L;
        long errorCount = 0L;
        long readCount = 0L;
        long writeCount = 0L;
        long latencySum = 0L;

        List<Long> keys = new ArrayList<>(buckets.keySet());
        Collections.sort(keys);
        for (Long key : keys) {
            if (key < cutoff) {
                continue;
            }
            MinuteBucket bucket = buckets.get(key);
            if (bucket == null) {
                continue;
            }
            long bucketRequests = bucket.requestCount.get();
            long bucketSuccess = bucket.successCount.get();
            long bucketErrors = bucket.errorCount.get();
            long bucketRead = bucket.readCount.get();
            long bucketWrite = bucket.writeCount.get();
            long bucketLatencySum = bucket.latencySumMs.get();
            requestCount += bucketRequests;
            successCount += bucketSuccess;
            errorCount += bucketErrors;
            readCount += bucketRead;
            writeCount += bucketWrite;
            latencySum += bucketLatencySum;
            synchronized (bucket.latencies) {
                allLatencies.addAll(bucket.latencies);
            }
            Map<String, Object> point = new HashMap<>();
            point.put("minuteBucket", key);
            point.put("requestCount", bucketRequests);
            point.put("successCount", bucketSuccess);
            point.put("errorCount", bucketErrors);
            point.put("avgLatencyMs", bucketRequests == 0 ? 0.0 : ((double) bucketLatencySum) / bucketRequests);
            point.put("p95LatencyMs", percentile(copyLatencies(bucket), 0.95));
            point.put("readCount", bucketRead);
            point.put("writeCount", bucketWrite);
            points.add(point);
        }

        double seconds = Math.max(1.0, windowMs / 1000.0);
        return new SqlMetricSummary(
            requestCount,
            successCount,
            errorCount,
            requestCount / seconds,
            requestCount == 0 ? 0.0 : ((double) latencySum) / requestCount,
            percentile(allLatencies, 0.95),
            readCount,
            writeCount,
            points
        );
    }

    public Map<String, Long> tableErrorTotals(long windowMs) {
        return aggregatePerTable(windowMs, true);
    }

    static long currentMinuteBucket(long timestampMs) {
        return (timestampMs / MINUTE_MS) * MINUTE_MS;
    }

    private Map<String, Long> aggregatePerTable(long windowMs, boolean errorsOnly) {
        purgeExpired();
        long cutoff = currentMinuteBucket(System.currentTimeMillis() - windowMs);
        Map<String, Long> totals = new HashMap<>();
        for (Map.Entry<Long, MinuteBucket> entry : buckets.entrySet()) {
            if (entry.getKey() < cutoff) {
                continue;
            }
            ConcurrentMap<String, AtomicLong> tableMap = errorsOnly
                ? entry.getValue().tableErrorCounts
                : entry.getValue().tableRequestCounts;
            for (Map.Entry<String, AtomicLong> tableEntry : tableMap.entrySet()) {
                totals.merge(tableEntry.getKey(), tableEntry.getValue().get(), (a, b) -> a + b);

            }
        }
        return totals;
    }

    private void purgeExpired() {
        long cutoff = currentMinuteBucket(System.currentTimeMillis() - retentionMs);
        buckets.keySet().removeIf(bucket -> bucket < cutoff);
    }

    private boolean isRead(String sqlType) {
        return "SELECT".equalsIgnoreCase(sqlType)
            || "SHOW".equalsIgnoreCase(sqlType)
            || "DESCRIBE".equalsIgnoreCase(sqlType);
    }

    private List<Long> copyLatencies(MinuteBucket bucket) {
        synchronized (bucket.latencies) {
            return new ArrayList<>(bucket.latencies);
        }
    }

    private double percentile(List<Long> values, double percentile) {
        if (values.isEmpty()) {
            return 0.0;
        }
        values.sort(Comparator.naturalOrder());
        int index = Math.max(0, (int) Math.ceil(values.size() * percentile) - 1);
        return values.get(Math.min(index, values.size() - 1));
    }
}
