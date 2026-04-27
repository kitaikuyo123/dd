package com.minisql.replication;

/**
 * 复制管理器配置类
 */
public class ReplicationConfig {

    // 复制因子
    private final int replicationFactor;

    // 复制超时时间（毫秒）
    private final long replicationTimeoutMs;

    // 最大重试次数
    private final int maxRetryCount;

    // 副本健康检查间隔（毫秒）
    private final long healthCheckIntervalMs;

    // 复制确认等待时间（毫秒）
    private final long ackTimeoutMs;

    // WAL 保留条目数
    private final int walRetentionCount;

    // 是否启用同步复制
    private final boolean syncReplicationEnabled;

    // 大多数副本确认即可（quorum）
    private final boolean quorumAckEnabled;

    // 批量复制最大条目数
    private final int maxReplicationBatchSize;

    // 自动追赶滞后阈值（条目数）
    private final int catchUpLagThreshold;

    // 复制线程池大小（0 表示使用 CPU 核数自适应）
    private final int replicationThreadPoolSize;

    private ReplicationConfig(Builder builder) {
        this.replicationFactor = builder.replicationFactor;
        this.replicationTimeoutMs = builder.replicationTimeoutMs;
        this.maxRetryCount = builder.maxRetryCount;
        this.healthCheckIntervalMs = builder.healthCheckIntervalMs;
        this.ackTimeoutMs = builder.ackTimeoutMs;
        this.walRetentionCount = builder.walRetentionCount;
        this.syncReplicationEnabled = builder.syncReplicationEnabled;
        this.quorumAckEnabled = builder.quorumAckEnabled;
        this.maxReplicationBatchSize = builder.maxReplicationBatchSize;
        this.catchUpLagThreshold = builder.catchUpLagThreshold;
        this.replicationThreadPoolSize = builder.replicationThreadPoolSize;
    }

    public int getReplicationFactor() { return replicationFactor; }
    public long getReplicationTimeoutMs() { return replicationTimeoutMs; }
    public int getMaxRetryCount() { return maxRetryCount; }
    public long getHealthCheckIntervalMs() { return healthCheckIntervalMs; }
    public long getAckTimeoutMs() { return ackTimeoutMs; }
    public int getWalRetentionCount() { return walRetentionCount; }
    public boolean isSyncReplicationEnabled() { return syncReplicationEnabled; }
    public boolean isQuorumAckEnabled() { return quorumAckEnabled; }
    public int getMaxReplicationBatchSize() { return maxReplicationBatchSize; }
    public int getCatchUpLagThreshold() { return catchUpLagThreshold; }
    public int getReplicationThreadPoolSize() { return replicationThreadPoolSize; }

    /**
     * 计算需要的确认数
     */
    public int getRequiredAcks(int totalReplicas) {
        if (quorumAckEnabled) {
            // 大多数副本确认
            return (totalReplicas - 1) / 2 + 1;
        } else {
            // 需要所有副本（含本地主副本）确认
            return totalReplicas;
        }
    }

    public static Builder builder(int replicationFactor) {
        return new Builder(replicationFactor);
    }

    public static class Builder {
        private final int replicationFactor;
        private long replicationTimeoutMs = 5000;
        private int maxRetryCount = 3;
        private long healthCheckIntervalMs = 10000;
        private long ackTimeoutMs = 3000;
        private int walRetentionCount = 10000;
        private boolean syncReplicationEnabled = false;
        private boolean quorumAckEnabled = true;
        private int maxReplicationBatchSize = 64;
        private int catchUpLagThreshold = 100;
        private int replicationThreadPoolSize = 0;

        public Builder(int replicationFactor) {
            this.replicationFactor = replicationFactor;
        }

        public Builder replicationTimeoutMs(long timeoutMs) {
            this.replicationTimeoutMs = timeoutMs;
            return this;
        }

        public Builder maxRetryCount(int count) {
            this.maxRetryCount = count;
            return this;
        }

        public Builder healthCheckIntervalMs(long intervalMs) {
            this.healthCheckIntervalMs = intervalMs;
            return this;
        }

        public Builder ackTimeoutMs(long timeoutMs) {
            this.ackTimeoutMs = timeoutMs;
            return this;
        }

        public Builder walRetentionCount(int count) {
            this.walRetentionCount = count;
            return this;
        }

        public Builder syncReplicationEnabled(boolean enabled) {
            this.syncReplicationEnabled = enabled;
            return this;
        }

        public Builder quorumAckEnabled(boolean enabled) {
            this.quorumAckEnabled = enabled;
            return this;
        }

        public Builder maxReplicationBatchSize(int size) {
            this.maxReplicationBatchSize = size;
            return this;
        }

        public Builder catchUpLagThreshold(int threshold) {
            this.catchUpLagThreshold = threshold;
            return this;
        }

        public Builder replicationThreadPoolSize(int size) {
            this.replicationThreadPoolSize = size;
            return this;
        }

        public ReplicationConfig build() {
            return new ReplicationConfig(this);
        }
    }
}
