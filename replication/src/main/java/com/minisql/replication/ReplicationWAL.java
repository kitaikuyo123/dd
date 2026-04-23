package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persistent write-ahead log for replication, backed by a dedicated RocksDB instance.
 *
 * <p>WAL entry key layout: {@code <regionId-UTF8><0x00><sequenceId-big-endian-8bytes>}
 * <p>WAL entry value: binary-encoded ReplicationLogEntry (sequenceId + timestamp + mutations).
 *
 * <p>Applied progress key layout: {@code <regionId-UTF8><0x01><replicaAddress-UTF8>}
 * <p>Applied progress value: 8-byte big-endian lastAppliedSequenceId.
 */
public class ReplicationWAL implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationWAL.class);
    private static final byte[] SEP = new byte[]{0x00};
    private static final byte[] PROGRESS_SEP = new byte[]{0x01};

    private RocksDB db;
    private final String dbPath;
    private final Map<String, AtomicLong> sequenceIdCache = new ConcurrentHashMap<>();

    public ReplicationWAL() {
        this("./data/wal");
    }

    public ReplicationWAL(String dbPath) {
        this.dbPath = dbPath;
    }

    public void initialize() {
        if (db != null) {
            return;
        }
        RocksDB.loadLibrary();
        try {
            File dir = new File(dbPath);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // Clean up stale LOCK file
            File lockFile = new File(dir, "LOCK");
            if (lockFile.exists()) {
                logger.warn("Found stale WAL LOCK file at {}, deleting...", dbPath);
                if (!lockFile.delete()) {
                    logger.warn("Failed to delete stale WAL LOCK file");
                }
            }

            Options options = new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(4 * 1024 * 1024)  // 4 MB
                .setMaxWriteBufferNumber(2);
            db = RocksDB.open(options, dbPath);
            logger.info("ReplicationWAL opened at {}", dbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to open WAL at " + dbPath, e);
        }
    }

    public long getCurrentSequenceId(String regionId) {
        AtomicLong cached = sequenceIdCache.get(regionId);
        if (cached != null) {
            return cached.get();
        }

        // Seek to last key with this region prefix
        long maxSeq = 0;
        try (RocksIterator it = db.newIterator()) {
            byte[] prefix = regionPrefix(regionId);
            byte[] upperBound = regionUpperBound(regionId);
            it.seek(prefix);
            while (it.isValid()) {
                byte[] key = it.key();
                if (upperBound != null && compareBytes(key, upperBound) >= 0) {
                    break;
                }
                if (!startsWith(key, prefix)) {
                    break;
                }
                long seqId = extractSequenceId(key, prefix.length);
                if (seqId > maxSeq) {
                    maxSeq = seqId;
                }
                it.next();
            }
        }

        sequenceIdCache.put(regionId, new AtomicLong(maxSeq));
        return maxSeq;
    }

    public ReplicationLogEntry append(String regionId, List<KeyValue> mutations) {
        long seqId = getNextSequenceId(regionId);
        long timestamp = System.currentTimeMillis();
        byte[] key = buildKey(regionId, seqId);
        byte[] value = encodeEntry(seqId, timestamp, mutations);
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException("WAL append failed for region " + regionId, e);
        }
        return new ReplicationLogEntry(seqId, timestamp, mutations);
    }

    public List<ReplicationLogEntry> appendBatch(String regionId, List<List<KeyValue>> mutationBatches) {
        List<ReplicationLogEntry> entries = new ArrayList<>();
        try (WriteBatch batch = new WriteBatch()) {
            long timestamp = System.currentTimeMillis();
            for (List<KeyValue> mutations : mutationBatches) {
                long seqId = getNextSequenceId(regionId);
                byte[] key = buildKey(regionId, seqId);
                byte[] value = encodeEntry(seqId, timestamp, mutations);
                batch.put(key, value);
                entries.add(new ReplicationLogEntry(seqId, timestamp, mutations));
            }
            db.write(new WriteOptions(), batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("WAL batch append failed for region " + regionId, e);
        }
        return entries;
    }

    public List<ReplicationLogEntry> getEntries(String regionId, long fromSequenceId) {
        List<ReplicationLogEntry> entries = new ArrayList<>();
        byte[] seekKey = buildKey(regionId, fromSequenceId);
        byte[] upperBound = regionUpperBound(regionId);

        try (RocksIterator it = db.newIterator()) {
            it.seek(seekKey);
            while (it.isValid()) {
                byte[] key = it.key();
                if (upperBound != null && compareBytes(key, upperBound) >= 0) {
                    break;
                }
                byte[] prefix = regionPrefix(regionId);
                if (!startsWith(key, prefix)) {
                    break;
                }
                entries.add(decodeEntry(it.value()));
                it.next();
            }
        }
        return entries;
    }

    public void markAsApplied(String regionId, long sequenceId, String replicaAddress) {
        if (db == null) return;
        try {
            byte[] key = buildProgressKey(regionId, replicaAddress);
            byte[] value = ByteBuffer.allocate(8).putLong(sequenceId).array();
            db.put(key, value);
        } catch (RocksDBException e) {
            logger.warn("Failed to persist applied progress for region={} replica={}: {}",
                regionId, replicaAddress, e.getMessage());
        }
    }

    public long getAppliedProgress(String regionId, String replicaAddress) {
        if (db == null) return 0;
        try {
            byte[] key = buildProgressKey(regionId, replicaAddress);
            byte[] value = db.get(key);
            if (value == null) return 0;
            return ByteBuffer.wrap(value).getLong();
        } catch (RocksDBException e) {
            logger.warn("Failed to read applied progress for region={} replica={}: {}",
                regionId, replicaAddress, e.getMessage());
            return 0;
        }
    }

    public void cleanup(String regionId, int maxRetention) {
        doCleanup(regionId, maxRetention, 0);
    }

    public void cleanup(String regionId, int maxRetention, long minConfirmedSeqId) {
        doCleanup(regionId, maxRetention, minConfirmedSeqId);
    }

    private void doCleanup(String regionId, int maxRetention, long minConfirmedSeqId) {
        long currentMax = getCurrentSequenceId(regionId);
        long cutoffSeq = currentMax - maxRetention;
        // Don't delete entries that haven't been confirmed by all replicas
        if (minConfirmedSeqId > 0) {
            cutoffSeq = Math.min(cutoffSeq, minConfirmedSeqId);
        }
        if (cutoffSeq <= 0) {
            return;
        }

        byte[] startKey = regionPrefix(regionId);
        byte[] endKey = buildKey(regionId, cutoffSeq);

        try {
            // Delete entries from the beginning of the region up to cutoffSeq
            byte[] upperBound = buildKey(regionId, cutoffSeq + 1);
            try (RocksIterator it = db.newIterator()) {
                it.seek(startKey);
                List<byte[]> toDelete = new ArrayList<>();
                while (it.isValid()) {
                    byte[] key = it.key();
                    if (compareBytes(key, upperBound) >= 0) {
                        break;
                    }
                    if (!startsWith(key, regionPrefix(regionId))) {
                        break;
                    }
                    toDelete.add(key);
                    it.next();
                }
                for (byte[] key : toDelete) {
                    db.delete(key);
                }
            }
        } catch (RocksDBException e) {
            logger.warn("WAL cleanup failed for region {}: {}", regionId, e.getMessage());
        }
    }

    public void deleteRegion(String regionId) {
        byte[] prefix = regionPrefix(regionId);
        byte[] upperBound = regionUpperBound(regionId);

        try (RocksIterator it = db.newIterator()) {
            it.seek(prefix);
            List<byte[]> toDelete = new ArrayList<>();
            while (it.isValid()) {
                byte[] key = it.key();
                if (upperBound != null && compareBytes(key, upperBound) >= 0) {
                    break;
                }
                if (!startsWith(key, prefix)) {
                    break;
                }
                toDelete.add(key);
                it.next();
            }
            for (byte[] key : toDelete) {
                db.delete(key);
            }
        } catch (RocksDBException e) {
            logger.warn("WAL deleteRegion failed for region {}: {}", regionId, e.getMessage());
        }

        // Also delete progress keys for this region
        byte[] progressPrefix = progressRegionPrefix(regionId);
        try (RocksIterator it = db.newIterator()) {
            it.seek(progressPrefix);
            List<byte[]> toDelete = new ArrayList<>();
            while (it.isValid()) {
                byte[] key = it.key();
                if (!startsWith(key, progressPrefix)) {
                    break;
                }
                toDelete.add(key);
                it.next();
            }
            for (byte[] key : toDelete) {
                db.delete(key);
            }
        } catch (RocksDBException e) {
            logger.warn("WAL deleteRegion progress cleanup failed for {}: {}", regionId, e.getMessage());
        }

        sequenceIdCache.remove(regionId);
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            db = null;
            logger.info("ReplicationWAL closed");
        }
    }

    // --- Key Encoding ---

    private byte[] regionPrefix(String regionId) {
        byte[] idBytes = regionId.getBytes();
        byte[] prefix = new byte[idBytes.length + 1];
        System.arraycopy(idBytes, 0, prefix, 0, idBytes.length);
        prefix[idBytes.length] = SEP[0];
        return prefix;
    }

    private byte[] progressRegionPrefix(String regionId) {
        byte[] idBytes = regionId.getBytes();
        byte[] prefix = new byte[idBytes.length + 1];
        System.arraycopy(idBytes, 0, prefix, 0, idBytes.length);
        prefix[idBytes.length] = PROGRESS_SEP[0];
        return prefix;
    }

    private byte[] buildProgressKey(String regionId, String replicaAddress) {
        byte[] idBytes = regionId.getBytes();
        byte[] addrBytes = replicaAddress.getBytes();
        byte[] key = new byte[idBytes.length + 1 + addrBytes.length];
        System.arraycopy(idBytes, 0, key, 0, idBytes.length);
        key[idBytes.length] = PROGRESS_SEP[0];
        System.arraycopy(addrBytes, 0, key, idBytes.length + 1, addrBytes.length);
        return key;
    }

    private byte[] regionUpperBound(String regionId) {
        // Upper bound: increment the last byte of regionId to get exclusive end
        byte[] idBytes = regionId.getBytes();
        // Increment last byte; if overflow, append 0xFF
        byte[] incremented = new byte[idBytes.length];
        System.arraycopy(idBytes, 0, incremented, 0, idBytes.length);
        int i = incremented.length - 1;
        while (i >= 0) {
            if (incremented[i] != (byte) 0xFF) {
                incremented[i]++;
                break;
            }
            i--;
        }
        if (i < 0) {
            // All 0xFF, append 0x00
            incremented = new byte[idBytes.length + 1];
            System.arraycopy(idBytes, 0, incremented, 0, idBytes.length);
            incremented[idBytes.length] = 0x00;
        }
        return incremented;
    }

    private byte[] buildKey(String regionId, long sequenceId) {
        byte[] prefix = regionPrefix(regionId);
        byte[] key = new byte[prefix.length + 8];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        ByteBuffer.wrap(key, prefix.length, 8).putLong(sequenceId);
        return key;
    }

    private long extractSequenceId(byte[] key, int offset) {
        return ByteBuffer.wrap(key, offset, 8).getLong();
    }

    // --- Value Encoding ---
    // Format: [sequenceId:8][timestamp:8][mutationCount:4]
    //   per mutation: [type:1][rowKeyLen:4][rowKey][familyLen:4][family][qualifierLen:4][qualifier][timestamp:8][valueLen:4][value]

    private byte[] encodeEntry(long sequenceId, long timestamp, List<KeyValue> mutations) {
        int size = 8 + 8 + 4; // seqId + timestamp + count
        List<byte[]> encoded = new ArrayList<>(mutations.size());
        for (KeyValue kv : mutations) {
            byte[] part = encodeMutation(kv);
            encoded.add(part);
            size += part.length;
        }
        byte[] buf = new byte[size];
        ByteBuffer bb = ByteBuffer.wrap(buf);
        bb.putLong(sequenceId);
        bb.putLong(timestamp);
        bb.putInt(mutations.size());
        for (byte[] part : encoded) {
            bb.put(part);
        }
        return buf;
    }

    private byte[] encodeMutation(KeyValue kv) {
        byte[] rowKey = kv.getRowKey() != null ? kv.getRowKey() : new byte[0];
        byte[] family = kv.getFamily() != null ? kv.getFamily().getBytes() : new byte[0];
        byte[] qualifier = kv.getQualifier() != null ? kv.getQualifier().getBytes() : new byte[0];
        byte[] value = kv.getValue() != null ? kv.getValue() : new byte[0];
        byte type = kv.getType() != null ? (byte) kv.getType().getCode() : (byte) KeyValue.Type.PUT.getCode();

        int size = 1 + 4 + rowKey.length + 4 + family.length + 4 + qualifier.length + 8 + 4 + value.length;
        byte[] buf = new byte[size];
        ByteBuffer bb = ByteBuffer.wrap(buf);
        bb.put(type);
        bb.putInt(rowKey.length);
        bb.put(rowKey);
        bb.putInt(family.length);
        bb.put(family);
        bb.putInt(qualifier.length);
        bb.put(qualifier);
        bb.putLong(kv.getTimestamp());
        bb.putInt(value.length);
        bb.put(value);
        return buf;
    }

    private ReplicationLogEntry decodeEntry(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        long sequenceId = bb.getLong();
        long timestamp = bb.getLong();
        int count = bb.getInt();
        List<KeyValue> mutations = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            mutations.add(decodeMutation(bb));
        }
        return new ReplicationLogEntry(sequenceId, timestamp, mutations);
    }

    private KeyValue decodeMutation(ByteBuffer bb) {
        byte typeCode = bb.get();
        byte[] rowKey = new byte[bb.getInt()];
        bb.get(rowKey);
        byte[] family = new byte[bb.getInt()];
        bb.get(family);
        byte[] qualifier = new byte[bb.getInt()];
        bb.get(qualifier);
        long ts = bb.getLong();
        byte[] value = new byte[bb.getInt()];
        bb.get(value);

        KeyValue kv = new KeyValue();
        kv.setRowKey(rowKey);
        kv.setFamily(new String(family));
        kv.setQualifier(new String(qualifier));
        kv.setTimestamp(ts);
        kv.setValue(value);
        kv.setType(typeCode == (byte) KeyValue.Type.DELETE.getCode() ? KeyValue.Type.DELETE : KeyValue.Type.PUT);
        return kv;
    }

    // --- Helpers ---

    private synchronized long getNextSequenceId(String regionId) {
        AtomicLong counter = sequenceIdCache.computeIfAbsent(regionId,
            k -> new AtomicLong(getCurrentSequenceId(regionId)));
        return counter.incrementAndGet();
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
}
