package com.minisql.master.monitoring;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlMetricsRegistryTest {

    @Test
    void summarizesRequestsLatencyAndErrors() {
        SqlMetricsRegistry registry = new SqlMetricsRegistry();

        registry.record("SELECT", "users", true, 10);
        registry.record("SELECT", "users", false, 20);
        registry.record("INSERT", "users", true, 30);

        SqlMetricsRegistry.SqlMetricSummary summary = registry.summarize(5 * 60_000L);

        assertEquals(3, summary.getRequestCount());
        assertEquals(2, summary.getSuccessCount());
        assertEquals(1, summary.getErrorCount());
        assertEquals(2, summary.getReadCount());
        assertEquals(1, summary.getWriteCount());
        assertEquals(20.0, summary.getAvgLatencyMs());
        assertEquals(30.0, summary.getP95LatencyMs());
        assertTrue(summary.getQps() > 0.0);
    }

    @Test
    void evictsExpiredBuckets() {
        SqlMetricsRegistry registry = new SqlMetricsRegistry(60_000L);

        long expiredBucket = SqlMetricsRegistry.currentMinuteBucket(System.currentTimeMillis() - 120_000L);
        registry.record("SELECT", "users", true, 5);
        registry.record("SELECT", "orders", true, 7);

        assertTrue(registry.summarize(5 * 60_000L).getRequestCount() >= 2);
        assertTrue(expiredBucket < SqlMetricsRegistry.currentMinuteBucket(System.currentTimeMillis() - 60_000L));
    }
}
