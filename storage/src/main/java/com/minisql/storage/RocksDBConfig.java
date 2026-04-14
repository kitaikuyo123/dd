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

    private RocksDBConfig(Builder builder) {
        this.dataDir = builder.dataDir;
        this.enableWal = builder.enableWal;
        this.writeBufferSizeBytes = builder.writeBufferSizeBytes;
        this.maxWriteBufferNumber = builder.maxWriteBufferNumber;
        this.compressionType = builder.compressionType;
    }

    public String getDataDir() { return dataDir; }
    public boolean isEnableWal() { return enableWal; }
    public long getWriteBufferSizeBytes() { return writeBufferSizeBytes; }
    public int getMaxWriteBufferNumber() { return maxWriteBufferNumber; }
    public String getCompressionType() { return compressionType; }

    public static Builder builder(String dataDir) {
        return new Builder(dataDir);
    }

    public static class Builder {
        private final String dataDir;
        private boolean enableWal = true;
        private long writeBufferSizeBytes = 64 * 1024 * 1024; // 64 MB
        private int maxWriteBufferNumber = 2;
        private String compressionType = "snappy";

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

        public RocksDBConfig build() {
            return new RocksDBConfig(this);
        }
    }
}
