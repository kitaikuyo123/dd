package com.minisql.stress;

public class StressTestConfig {

    public enum OperationType { PUT, GET, SCAN, MIXED }

    private OperationType operationType = OperationType.MIXED;
    private int threadCount = 4;
    private int durationSeconds = 10;
    private int keySpaceSize = 10000;
    private int valueSizeBytes = 64;
    private int warmupSeconds = 2;

    public OperationType getOperationType() { return operationType; }
    public void setOperationType(OperationType operationType) { this.operationType = operationType; }

    public int getThreadCount() { return threadCount; }
    public void setThreadCount(int threadCount) { this.threadCount = threadCount; }

    public int getDurationSeconds() { return durationSeconds; }
    public void setDurationSeconds(int durationSeconds) { this.durationSeconds = durationSeconds; }

    public int getKeySpaceSize() { return keySpaceSize; }
    public void setKeySpaceSize(int keySpaceSize) { this.keySpaceSize = keySpaceSize; }

    public int getValueSizeBytes() { return valueSizeBytes; }
    public void setValueSizeBytes(int valueSizeBytes) { this.valueSizeBytes = valueSizeBytes; }

    public int getWarmupSeconds() { return warmupSeconds; }
    public void setWarmupSeconds(int warmupSeconds) { this.warmupSeconds = warmupSeconds; }

    public static StressTestConfig defaults() { return new StressTestConfig(); }
}
