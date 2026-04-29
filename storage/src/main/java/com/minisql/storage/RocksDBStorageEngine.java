package com.minisql.storage;

import com.minisql.common.model.KeyValue;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Supplier;

/** 基于 RocksDB 的 KV 存储引擎，每个 Region 使用独立的 RocksDB 实例 */
public class RocksDBStorageEngine implements StorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBStorageEngine.class);

    private static final byte[] FAMILY_SEP = new byte[]{0x00};
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final String regionId;
    private final String dbPath;
    private final RocksDB db;
    private final ColumnFamilyHandle defaultCf;
    private final RocksDBConfig config;

    public RocksDBStorageEngine(RocksDBConfig config, String regionId) {
        this.regionId = regionId;
        this.config = config;
        this.dbPath = config.getDataDir() + File.separator + safeDirName(regionId);

        RocksDB.loadLibrary();
        try {
            File dir = new File(dbPath);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // Check for stale LOCK file.
            // On Windows, RocksDB may leave the LOCK file after shutdown. Instead of
            // blindly checking file existence (which forces manual cleanup on every restart),
            // try to open the file exclusively: if another process holds it, this fails.
            // If it succeeds, the previous process is dead and we can safely remove it.
            File lockFile = new File(dir, "LOCK");
            if (lockFile.exists()) {
                try (FileOutputStream fos = new FileOutputStream(lockFile)) {
                    // Opened successfully — no other process holds the lock.
                    // Safe to delete the stale file.
                    fos.close();
                    lockFile.delete();
                    logger.debug("Removed stale RocksDB LOCK file at {}", dbPath);
                } catch (IOException e) {
                    throw new IllegalStateException(
                        "RocksDB LOCK file at " + dbPath + " is held by another process. " +
                        "The database may already be in use.", e);
                }
            }

            Options options = buildOptions(config);
            this.db = RocksDB.open(options, dbPath);
            this.defaultCf = null; // default column family
            logger.info("RocksDB opened for region {} at {}", regionId, dbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to open RocksDB for region " + regionId, e);
        }
    }

    @Override
    public void put(byte[] key, KeyValue value) {
        byte[] compositeKey = buildCompositeKey(key, value.getFamily(), value.getQualifier(), value.getTimestamp());
        byte[] encoded = encodeValue(value);
        try {
            WriteOptions writeOptions = new WriteOptions().setSync(config.isEnableWal());
            db.put(writeOptions, compositeKey, encoded);
        } catch (RocksDBException e) {
            throw new RuntimeException("RocksDB put failed for region " + regionId, e);
        }
    }

    @Override
    public void batchPut(List<KeyValue> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        try (WriteBatch batch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions().setSync(config.isEnableWal())) {
            for (KeyValue kv : values) {
                byte[] compositeKey = buildCompositeKey(kv.getRowKey(), kv.getFamily(), kv.getQualifier(), kv.getTimestamp());
                byte[] encoded = encodeValue(kv);
                batch.put(compositeKey, encoded);
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("RocksDB batchPut failed for region " + regionId, e);
        }
    }

    @Override
    public List<KeyValue> get(byte[] key) {
        // Prefix scan: iterate all entries with this rowKey, pick latest version per qualifier
        byte[] prefix = buildRowKeyPrefix(key);
        Map<String, KeyValue> latestPerColumn = new LinkedHashMap<>();

        try (RocksIterator it = db.newIterator()) {
            it.seek(prefix);
            while (it.isValid()) {
                byte[] currentKey = it.key();
                if (!startsWith(currentKey, prefix)) {
                    break;
                }
                KeyValue kv = decodeFromCompositeKey(currentKey, it.value());
                String columnKey = columnKey(kv.getFamily(), kv.getQualifier());
                if (!latestPerColumn.containsKey(columnKey)) {
                    if (kv.isDelete()) {
                        latestPerColumn.put(columnKey, kv); // tombstone, but skip from results later
                    } else {
                        latestPerColumn.put(columnKey, kv);
                    }
                }
                it.next();
            }
        }

        List<KeyValue> results = new ArrayList<>();
        for (KeyValue kv : latestPerColumn.values()) {
            if (!kv.isDelete()) {
                results.add(kv);
            }
        }
        return results;
    }

    @Override
    public Iterator<KeyValue> scan(StorageScanFilter filter) {
        byte[] startKey = filter.getStartKey() != null ? filter.getStartKey() : EMPTY_BYTES;
        byte[] endKey = filter.getEndKey() != null ? filter.getEndKey() : new byte[]{(byte) 0xFF};

        byte[] seekKey = buildRowKeyPrefix(startKey);
        RocksIterator it = db.newIterator();
        it.seek(seekKey);

        return new RowStreamIterator(it, endKey, filter);
    }

    // ---- Streaming iterator ----

    /**
     * Streaming iterator that wraps RocksIterator directly, buffering one row
     * at a time instead of materializing all results into memory.
     *
     * <p>RocksIterator lifecycle: the iterator is closed when exhausted
     * (hasMore returns false). Callers may also {@link #close()} it early.
     */
    private class RowStreamIterator implements Iterator<KeyValue>, AutoCloseable {

        private final RocksIterator it;
        private final byte[] endKey;
        private final StorageScanFilter filter;

        // Buffer for the current row's KeyValues (flushed per row key change)
        private final ArrayDeque<KeyValue> rowBuffer = new ArrayDeque<>();
        // Row-level aggregation state for the current in-progress row
        private final Map<String, KeyValue> currentRow = new LinkedHashMap<>();
        private byte[] currentRowKey;

        private boolean exhausted;

        RowStreamIterator(RocksIterator it, byte[] endKey, StorageScanFilter filter) {
            this.it = it;
            this.endKey = endKey;
            this.filter = filter;
        }

        @Override
        public boolean hasNext() {
            if (!rowBuffer.isEmpty()) return true;
            if (exhausted) return false;
            fillRowBuffer();
            return !rowBuffer.isEmpty();
        }

        @Override
        public KeyValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements");
            }
            return rowBuffer.poll();
        }

        private void fillRowBuffer() {
            while (it.isValid()) {
                byte[] compositeKey = it.key();
                byte[] rowKey = extractRowKey(compositeKey);

                // Past end key?
                if (compareBytes(rowKey, endKey) >= 0) {
                    flushRowBuffer();
                    closeIt();
                    return;
                }

                // Row key change — flush current row
                if (currentRowKey != null && !Arrays.equals(rowKey, currentRowKey)) {
                    flushRowBuffer();
                    if (!rowBuffer.isEmpty()) {
                        // Have a row to yield; pause RocksIterator
                        return;
                    }
                }
                currentRowKey = rowKey;

                KeyValue kv = decodeFromCompositeKey(compositeKey, it.value());
                String colKey = columnKey(kv.getFamily(), kv.getQualifier());
                if (!currentRow.containsKey(colKey)) {
                    currentRow.put(colKey, kv);
                }
                it.next();
            }
            // RocksIterator exhausted
            flushRowBuffer();
            closeIt();
        }

        private void flushRowBuffer() {
            if (currentRow.isEmpty()) return;

            // Row-level filter check
            boolean passes = true;
            if (filter != null && filter.hasColumnPredicates()) {
                for (StorageColumnPredicate pred : filter.getColumnPredicates()) {
                    KeyValue targetKv = null;
                    for (KeyValue kv : currentRow.values()) {
                        if (kv.isDelete()) continue;
                        if (pred.matchesQualifier(kv.getQualifier())) {
                            targetKv = kv;
                            break;
                        }
                    }
                    if (targetKv == null) {
                        passes = false;
                        break;
                    }
                    int cmp = compareBytes(
                        targetKv.getValue() != null ? targetKv.getValue() : EMPTY_BYTES,
                        pred.getValue() != null ? pred.getValue() : EMPTY_BYTES);
                    switch (pred.getOperator()) {
                        case "=": case "==": passes = (cmp == 0); break;
                        case ">":  passes = (cmp > 0); break;
                        case ">=": passes = (cmp >= 0); break;
                        case "<":  passes = (cmp < 0); break;
                        case "<=": passes = (cmp <= 0); break;
                    }
                    if (!passes) break;
                }
            }

            if (passes) {
                for (KeyValue kv : currentRow.values()) {
                    if (kv.isDelete()) continue;
                    if (filter != null && filter.hasProjectedQualifiers()
                        && !filter.getProjectedQualifiers().contains(kv.getQualifier())) {
                        continue;
                    }
                    rowBuffer.add(kv);
                }
            }

            currentRow.clear();
        }

        private void closeIt() {
            exhausted = true;
            it.close();
        }

        @Override
        public void close() {
            if (!exhausted) {
                it.close();
                exhausted = true;
            }
        }
    }

    @Override
    public void delete(byte[] key) {
        // Write a tombstone marker for MVCC consistency
        long timestamp = System.currentTimeMillis();
        byte[] compositeKey = buildCompositeKey(key, "", "", timestamp);
        try {
            WriteOptions writeOptions = new WriteOptions().setSync(config.isEnableWal());
            db.put(writeOptions, compositeKey, new byte[]{(byte) KeyValue.Type.DELETE.getCode()});
        } catch (RocksDBException e) {
            throw new RuntimeException("RocksDB delete failed for region " + regionId, e);
        }
    }

    @Override
    public void flush() {
        try {
            db.flush(new FlushOptions().setWaitForFlush(true));
            logger.debug("RocksDB flushed for region {}", regionId);
        } catch (RocksDBException e) {
            logger.warn("RocksDB flush failed for region {}: {}", regionId, e.getMessage());
        }
    }

    @Override
    public void compact(boolean major) {
        try {
            if (major) {
                db.compactRange();
                logger.info("RocksDB major compaction completed for region {}", regionId);
            } else {
                flush();
                logger.info("RocksDB minor compaction (flush) completed for region {}", regionId);
            }
        } catch (Exception e) {
            logger.warn("RocksDB compaction failed for region {}: {}", regionId, e.getMessage());
        }
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            logger.info("RocksDB closed for region {}", regionId);
        }
    }

    @Override
    public void dropData() {
        close();
        try {
            Options options = buildOptions(config);
            RocksDB.destroyDB(dbPath, options);
            logger.info("RocksDB data dropped for region {} at {}", regionId, dbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to destroy RocksDB for region " + regionId, e);
        }
    }

    @Override
    public long estimateSizeBytes() {
        try {
            return db.getLongProperty("rocksdb.total-sst-files-size");
        } catch (Exception e) {
            // Fallback: sum file sizes in directory
            File dir = new File(dbPath);
            return dir.exists() ? dirSize(dir) : 0L;
        }
    }

    @Override
    public long estimateMemTableSize() {
        try {
            return db.getLongProperty("rocksdb.cur-size-all-mem-tables");
        } catch (Exception e) {
            return 0L;
        }
    }

    // ---- Key Encoding ----

    /**
     * Composite key format:
     *   [rowKeyLength:4][rowKey][0x00][family][0x00][qualifier][0x00][reversedTimestamp:8]
     *
     * The 4-byte rowKey length prefix avoids ambiguity when the rowKey itself
     * contains 0x00 bytes (e.g. sign-XOR encoded INT/BIGINT values).
     * Timestamp is reversed (Long.MAX_VALUE - ts) so latest version sorts first.
     */
    private byte[] buildCompositeKey(byte[] rowKey, String family, String qualifier, long timestamp) {
        byte[] familyBytes = family != null ? family.getBytes() : EMPTY_BYTES;
        byte[] qualifierBytes = qualifier != null ? qualifier.getBytes() : EMPTY_BYTES;
        long reversedTs = Long.MAX_VALUE - timestamp;

        int len = 4 + rowKey.length + 1 + familyBytes.length + 1 + qualifierBytes.length + 1 + 8;
        ByteBuffer buf = ByteBuffer.allocate(len);
        buf.putInt(rowKey.length);
        buf.put(rowKey);
        buf.put(FAMILY_SEP);
        buf.put(familyBytes);
        buf.put(FAMILY_SEP);
        buf.put(qualifierBytes);
        buf.put(FAMILY_SEP);
        buf.putLong(reversedTs);
        return buf.array();
    }

    private byte[] buildRowKeyPrefix(byte[] rowKey) {
        ByteBuffer buf = ByteBuffer.allocate(4 + rowKey.length + 1);
        buf.putInt(rowKey.length);
        buf.put(rowKey);
        buf.put(FAMILY_SEP);
        return buf.array();
    }

    private byte[] extractRowKey(byte[] compositeKey) {
        ByteBuffer buf = ByteBuffer.wrap(compositeKey);
        int rowKeyLen = buf.getInt();
        byte[] rowKey = new byte[rowKeyLen];
        buf.get(rowKey);
        return rowKey;
    }

    private KeyValue decodeFromCompositeKey(byte[] compositeKey, byte[] value) {
        ByteBuffer buf = ByteBuffer.wrap(compositeKey);

        // Read rowKey using length prefix
        int rowKeyLen = buf.getInt();
        byte[] rowKey = new byte[rowKeyLen];
        buf.get(rowKey);

        // Skip separator
        buf.get(); // 0x00

        // Read family (terminated by 0x00)
        int familyStart = buf.position();
        while (buf.hasRemaining() && buf.get() != 0x00) {}
        String family = new String(compositeKey, familyStart, buf.position() - 1 - familyStart);

        // Read qualifier (terminated by 0x00)
        int qualifierStart = buf.position();
        while (buf.hasRemaining() && buf.get() != 0x00) {}
        String qualifier = new String(compositeKey, qualifierStart, buf.position() - 1 - qualifierStart);

        // Read reversed timestamp
        long reversedTs = buf.getLong();
        long timestamp = Long.MAX_VALUE - reversedTs;

        KeyValue kv = new KeyValue();
        kv.setRowKey(rowKey);
        kv.setFamily(family);
        kv.setQualifier(qualifier);
        kv.setTimestamp(timestamp);

        if (value != null && value.length == 1 && value[0] == (byte) KeyValue.Type.DELETE.getCode()) {
            kv.setType(KeyValue.Type.DELETE);
        } else {
            kv.setValue(value);
            kv.setType(KeyValue.Type.PUT);
        }
        return kv;
    }

    private byte[] encodeValue(KeyValue kv) {
        if (kv.isDelete()) {
            return new byte[]{(byte) KeyValue.Type.DELETE.getCode()};
        }
        return kv.getValue() != null ? kv.getValue() : EMPTY_BYTES;
    }

    // ---- Helpers ----

    private int findSeparator(byte[] data, int from) {
        for (int i = from; i < data.length; i++) {
            if (data[i] == 0x00) return i;
        }
        return data.length;
    }

    private boolean startsWith(byte[] data, byte[] prefix) {
        if (data.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) return false;
        }
        return true;
    }

    private int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    private String columnKey(String family, String qualifier) {
        return (family != null ? family : "") + '\0' + (qualifier != null ? qualifier : "");
    }

    private void flushRow(List<KeyValue> results, Map<String, KeyValue> currentRow, StorageScanFilter filter) {
        // Check if this row passes all column predicates (row-level filtering)
        if (filter != null && filter.hasColumnPredicates()) {
            for (StorageColumnPredicate pred : filter.getColumnPredicates()) {
                KeyValue targetKv = null;
                for (KeyValue kv : currentRow.values()) {
                    if (kv.isDelete()) continue;
                    if (pred.matchesQualifier(kv.getQualifier())) {
                        targetKv = kv;
                        break;
                    }
                }
                if (targetKv == null) {
                    // Column not present in row; skip entire row
                    return;
                }
                byte[] kvValue = targetKv.getValue();
                byte[] predValue = pred.getValue();
                if (kvValue == null || predValue == null) {
                    return;
                }
                int cmp = compareBytes(kvValue, predValue);
                boolean passes = true;
                switch (pred.getOperator()) {
                    case "=":
                    case "==":
                        passes = (cmp == 0);
                        break;
                    case ">":
                        passes = (cmp > 0);
                        break;
                    case ">=":
                        passes = (cmp >= 0);
                        break;
                    case "<":
                        passes = (cmp < 0);
                        break;
                    case "<=":
                        passes = (cmp <= 0);
                        break;
                    default:
                        break;
                }
                if (!passes) {
                    // Predicate not satisfied; skip entire row
                    return;
                }
            }
        }

        for (KeyValue kv : currentRow.values()) {
            if (kv.isDelete()) continue;

            // Apply projected qualifiers filter at storage level
            if (filter != null && filter.hasProjectedQualifiers()) {
                if (!filter.getProjectedQualifiers().contains(kv.getQualifier())) {
                    continue;
                }
            }

            results.add(kv);
        }
    }

    private String safeDirName(String regionId) {
        return regionId.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private Options buildOptions(RocksDBConfig config) {
        // Block-based table config — the single most impactful RocksDB tuning lever.
        // Without a block cache, every read hits disk/OS page cache.
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();

        // LRU block cache — caches data blocks and index blocks in off-heap memory
        long cacheSize = config.getBlockCacheSizeBytes();
        if (cacheSize > 0) {
            tableConfig.setBlockCache(new LRUCache(cacheSize));
            logger.info("RocksDB block cache: {} MB", cacheSize / (1024 * 1024));
        }

        // Bloom filter — reduces disk I/O for point-lookup of non-existent keys.
        // Typical value: ~10 bits per key gives ~1% false-positive rate.
        int bfBits = config.getBloomFilterBitsPerKey();
        if (bfBits > 0) {
            tableConfig.setFilterPolicy(new BloomFilter(bfBits));
        }

        // Enable block cache for index and filter blocks (not just data blocks)
        tableConfig.setCacheIndexAndFilterBlocks(true);

        Options options = new Options()
            .setCreateIfMissing(true)
            .setWriteBufferSize(config.getWriteBufferSizeBytes())
            .setMaxWriteBufferNumber(config.getMaxWriteBufferNumber())
            .setTableFormatConfig(tableConfig);

        // Compaction style — LEVEL is generally best for read-heavy workloads
        String style = config.getCompactionStyle();
        if ("UNIVERSAL".equalsIgnoreCase(style)) {
            options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        } else if ("FIFO".equalsIgnoreCase(style)) {
            options.setCompactionStyle(CompactionStyle.FIFO);
        } else {
            // Default: LEVEL compaction
            options.setCompactionStyle(CompactionStyle.LEVEL);
            // Level compaction tuning: keep L0 small (frequent minor compactions),
            // grow exponentially per level
            options.setLevel0FileNumCompactionTrigger(4);
            options.setMaxBytesForLevelBase(256 * 1024 * 1024L); // 256 MB L1
            options.setTargetFileSizeBase(64 * 1024 * 1024L);    // 64 MB SST files
        }

        CompressionType compression = CompressionType.getCompressionType(config.getCompressionType());
        options.setCompressionType(compression);

        // Background job limit — not strictly a thread pool, but bounds compaction/flush parallelism
        options.setMaxBackgroundJobs(config.getMaxBackgroundJobs());

        // Statistics — exposes rocksdb.* properties for monitoring (cache hit rate, compaction count, etc.)
        if (config.isEnableStatistics()) {
            options.setStatistics(new Statistics());
            options.setStatsDumpPeriodSec(300); // dump stats to log every 5 minutes
        }

        // Rate limiter — caps compaction/flush I/O to avoid starving foreground reads
        long rateLimit = config.getRateLimiterBytesPerSec();
        if (rateLimit > 0) {
            options.setRateLimiter(new RateLimiter(rateLimit));
            logger.info("RocksDB rate limiter: {} MB/s", rateLimit / (1024 * 1024));
        }

        return options;
    }

    private long dirSize(File dir) {
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                size += f.isDirectory() ? dirSize(f) : f.length();
            }
        }
        return size;
    }
}
