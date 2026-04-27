package com.minisql.storage;

/**
 * Configuration for RocksDB storage engine.
 */
public class RocksDBConfig {

    private final String dataDir;
    private final boolean enableWal;
    private final long writeBufferSizeBytes;
    private final int maxWriteBufferNumber;
    private final String compressionType;
    private final long blockCacheSizeBytes;
    private final int bloomFilterBitsPerKey;
    private final String compactionStyle;
    private final boolean enableStatistics;
    private final long rateLimiterBytesPerSec;
    private final int maxBackgroundJobs;

    private RocksDBConfig(Builder builder) {
        this.dataDir = builder.dataDir;
        this.enableWal = builder.enableWal;
        this.writeBufferSizeBytes = builder.writeBufferSizeBytes;
        this.maxWriteBufferNumber = builder.maxWriteBufferNumber;
        this.compressionType = builder.compressionType;
        this.blockCacheSizeBytes = builder.blockCacheSizeBytes;
        this.bloomFilterBitsPerKey = builder.bloomFilterBitsPerKey;
        this.compactionStyle = builder.compactionStyle;
        this.enableStatistics = builder.enableStatistics;
        this.rateLimiterBytesPerSec = builder.rateLimiterBytesPerSec;
        this.maxBackgroundJobs = builder.maxBackgroundJobs;
    }

    public String getDataDir() { return dataDir; }
    public boolean isEnableWal() { return enableWal; }
    public long getWriteBufferSizeBytes() { return writeBufferSizeBytes; }
    public int getMaxWriteBufferNumber() { return maxWriteBufferNumber; }
    public String getCompressionType() { return compressionType; }
    public long getBlockCacheSizeBytes() { return blockCacheSizeBytes; }
    public int getBloomFilterBitsPerKey() { return bloomFilterBitsPerKey; }
    public String getCompactionStyle() { return compactionStyle; }
    public boolean isEnableStatistics() { return enableStatistics; }
    public long getRateLimiterBytesPerSec() { return rateLimiterBytesPerSec; }
    public int getMaxBackgroundJobs() { return maxBackgroundJobs; }

    public static Builder builder(String dataDir) {
        return new Builder(dataDir);
    }

    public static class Builder {
        private final String dataDir;
        private boolean enableWal = true;
        private long writeBufferSizeBytes = 64 * 1024 * 1024; // 64 MB
        private int maxWriteBufferNumber = 2;
        private String compressionType = "snappy";
        private long blockCacheSizeBytes = 128 * 1024 * 1024; // 128 MB
        private int bloomFilterBitsPerKey = 10;
        private String compactionStyle = "LEVEL";
        private boolean enableStatistics = true;
        private long rateLimiterBytesPerSec = 0; // 0 = disabled
        private int maxBackgroundJobs = 2;

        private Builder(String dataDir) {
            this.dataDir = dataDir;
        }

        public Builder enableWal(boolean enableWal) {
            this.enableWal = enableWal;
            return this;
        }

        public Builder writeBufferSizeBytes(long bytes) {
            this.writeBufferSizeBytes = bytes;
            return this;
        }

        public Builder maxWriteBufferNumber(int num) {
            this.maxWriteBufferNumber = num;
            return this;
        }

        public Builder compressionType(String type) {
            this.compressionType = type;
            return this;
        }

        public Builder blockCacheSizeBytes(long bytes) {
            this.blockCacheSizeBytes = bytes;
            return this;
        }

        public Builder bloomFilterBitsPerKey(int bits) {
            this.bloomFilterBitsPerKey = Math.max(0, bits);
            return this;
        }

        public Builder compactionStyle(String style) {
            this.compactionStyle = style;
            return this;
        }

        public Builder enableStatistics(boolean enable) {
            this.enableStatistics = enable;
            return this;
        }

        public Builder rateLimiterBytesPerSec(long bytesPerSec) {
            this.rateLimiterBytesPerSec = bytesPerSec;
            return this;
        }

        public Builder maxBackgroundJobs(int jobs) {
            this.maxBackgroundJobs = Math.max(1, jobs);
            return this;
        }

        public RocksDBConfig build() {
            return new RocksDBConfig(this);
        }
    }
}
