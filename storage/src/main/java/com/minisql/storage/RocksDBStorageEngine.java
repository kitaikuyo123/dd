package com.minisql.storage;

import com.minisql.common.model.KeyValue;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * RocksDB-backed KV storage engine.
 * Each region gets its own RocksDB instance under {dataDir}/{regionId}/.
 */
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

            // Clean up stale LOCK file from previous runs
            File lockFile = new File(dir, "LOCK");
            if (lockFile.exists()) {
                logger.warn("Found stale LOCK file at {}, deleting...", dbPath);
                if (!lockFile.delete()) {
                    logger.warn("Failed to delete stale LOCK file");
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

        // Use rowKey-level prefix for iterating
        byte[] seekKey = buildRowKeyPrefix(startKey);

        List<KeyValue> results = new ArrayList<>();
        byte[] endPrefix = buildRowKeyPrefix(endKey);

        try (RocksIterator it = db.newIterator()) {
            it.seek(seekKey);
            Map<String, KeyValue> currentRow = new LinkedHashMap<>();
            byte[] currentRowKey = null;

            while (it.isValid()) {
                byte[] compositeKey = it.key();
                byte[] rowKey = extractRowKey(compositeKey);

                // Check if past end key
                if (compareBytes(rowKey, endKey) >= 0) {
                    break;
                }

                // If new rowKey, flush previous row
                if (currentRowKey != null && !Arrays.equals(rowKey, currentRowKey)) {
                    flushRow(results, currentRow, filter);
                    currentRow.clear();
                }
                currentRowKey = rowKey;

                KeyValue kv = decodeFromCompositeKey(compositeKey, it.value());
                String colKey = columnKey(kv.getFamily(), kv.getQualifier());
                if (!currentRow.containsKey(colKey)) {
                    currentRow.put(colKey, kv);
                }
                it.next();
            }
            // Flush last row
            if (!currentRow.isEmpty()) {
                flushRow(results, currentRow, filter);
            }
        }

        return results.iterator();
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
     * Composite key: [rowKey][0x00][family][0x00][qualifier][0x00][reversedTimestamp]
     * Timestamp reversed (Long.MAX_VALUE - ts) so latest version sorts first.
     */
    private byte[] buildCompositeKey(byte[] rowKey, String family, String qualifier, long timestamp) {
        byte[] familyBytes = family != null ? family.getBytes() : EMPTY_BYTES;
        byte[] qualifierBytes = qualifier != null ? qualifier.getBytes() : EMPTY_BYTES;
        long reversedTs = Long.MAX_VALUE - timestamp;

        int len = rowKey.length + 1 + familyBytes.length + 1 + qualifierBytes.length + 1 + 8;
        ByteBuffer buf = ByteBuffer.allocate(len);
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
        ByteBuffer buf = ByteBuffer.allocate(rowKey.length + 1);
        buf.put(rowKey);
        buf.put(FAMILY_SEP);
        return buf.array();
    }

    private byte[] extractRowKey(byte[] compositeKey) {
        int sep = findSeparator(compositeKey, 0);
        byte[] rowKey = new byte[sep];
        System.arraycopy(compositeKey, 0, rowKey, 0, sep);
        return rowKey;
    }

    private KeyValue decodeFromCompositeKey(byte[] compositeKey, byte[] value) {
        int sep1 = findSeparator(compositeKey, 0);
        int sep2 = findSeparator(compositeKey, sep1 + 1);
        int sep3 = findSeparator(compositeKey, sep2 + 1);

        byte[] rowKey = new byte[sep1];
        System.arraycopy(compositeKey, 0, rowKey, 0, sep1);

        String family = new String(compositeKey, sep1 + 1, sep2 - sep1 - 1);
        String qualifier = new String(compositeKey, sep2 + 1, sep3 - sep2 - 1);

        ByteBuffer tsBuf = ByteBuffer.wrap(compositeKey, sep3 + 1, 8);
        long reversedTs = tsBuf.getLong();
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
        for (KeyValue kv : currentRow.values()) {
            if (!kv.isDelete()) {
                results.add(kv);
            }
        }
    }

    private String safeDirName(String regionId) {
        return regionId.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private Options buildOptions(RocksDBConfig config) {
        Options options = new Options()
            .setCreateIfMissing(true)
            .setWriteBufferSize(config.getWriteBufferSizeBytes())
            .setMaxWriteBufferNumber(config.getMaxWriteBufferNumber());

        CompressionType compression = CompressionType.getCompressionType(config.getCompressionType());
        options.setCompressionType(compression);

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
