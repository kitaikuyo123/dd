package com.minisql.stress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StressTestResult {

    private static final Logger logger = LoggerFactory.getLogger(StressTestResult.class);

    public long totalOps;
    public double throughputOpsPerSec;
    public double p50Micros;
    public double p95Micros;
    public double p99Micros;
    public long minMicros;
    public long maxMicros;
    public long errorCount;
    public long durationMs;

    public void printSummary() {
        logger.info("=== Stress Test Results ===");
        logger.info("Total ops:       {}", totalOps);
        logger.info("Throughput:       {} ops/sec", String.format("%.1f", throughputOpsPerSec));
        logger.info("Latency p50:      {} us", String.format("%.0f", p50Micros));
        logger.info("Latency p95:      {} us", String.format("%.0f", p95Micros));
        logger.info("Latency p99:      {} us", String.format("%.0f", p99Micros));
        logger.info("Latency min/max:  {} / {} us", minMicros, maxMicros);
        logger.info("Errors:           {}", errorCount);
        logger.info("Duration:         {} ms", durationMs);

        System.out.println("=== Stress Test Results ===");
        System.out.printf("Total ops:       %d%n", totalOps);
        System.out.printf("Throughput:       %.1f ops/sec%n", throughputOpsPerSec);
        System.out.printf("Latency p50:      %.0f us%n", p50Micros);
        System.out.printf("Latency p95:      %.0f us%n", p95Micros);
        System.out.printf("Latency p99:      %.0f us%n", p99Micros);
        System.out.printf("Latency min/max:  %d / %d us%n", minMicros, maxMicros);
        System.out.printf("Errors:           %d%n", errorCount);
        System.out.printf("Duration:         %d ms%n", durationMs);
    }
}
