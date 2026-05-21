package com.minisql.regionserver;

import com.minisql.common.Constants;
import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.utils.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Region split service.
 */
public class RegionSplitService {

    private static final Logger logger = LoggerFactory.getLogger(RegionSplitService.class);
    private static final int MAX_SPLIT_KEY_SAMPLE_SIZE = 4096;

    private final RegionManager regionManager;

    // 分裂阈值
    private long splitThreshold = Constants.DEFAULT_SPLIT_THRESHOLD;
    // 最小分裂大小（防止过度分裂）
    private long minSplitSize = Constants.DEFAULT_SPLIT_MIN_SIZE;
    private final Map<String, PendingSplit> pendingSplits = new ConcurrentHashMap<>();

    public RegionSplitService(RegionManager regionManager) {
        this.regionManager = regionManager;
    }

    public void setConfig(long splitThresholdMb, long minSplitSizeMb) {
        this.splitThreshold = splitThresholdMb * 1024L * 1024L;
        this.minSplitSize = minSplitSizeMb * 1024L * 1024L;
        logger.info("RegionSplitService configured: threshold={}MB, minSize={}MB", splitThresholdMb, minSplitSizeMb);
    }

    /**
     * 寻找最佳分裂点（中点分裂策略）
     */
    public byte[] findBestSplitPoint(String regionId) throws IOException {
        RegionStorage storage = regionManager.getRegionStorage(regionId);
        if (storage == null) {
            throw new IOException("Region storage not found: " + regionId);
        }

        Region region = regionManager.getRegion(regionId);
        if (region == null) {
            throw new IOException("Region not found: " + regionId);
        }

        int sampleLimit = MAX_SPLIT_KEY_SAMPLE_SIZE;
        if (sampleLimit > 0) {
            List<byte[]> rowKeySamples = sampleDistinctRowKeys(
                storage, region.getStartKey(), region.getEndKey(), sampleLimit);
            if (rowKeySamples.size() >= 2) {
                rowKeySamples.sort(BytesUtil::compareTo);
                byte[] splitKey = selectMedianValidSplitKey(rowKeySamples, region);
                if (splitKey != null) {
                    return Arrays.copyOf(splitKey, splitKey.length);
                }
            }
            logger.info("Sample-based split key unavailable for region {} (distinctRowKeys={}), falling back to midpoint",
                regionId, rowKeySamples.size());
        }

        // 策略：中点分裂（采样不足时的 fallback）
        byte[] start = region.getStartKey();
        byte[] end = region.getEndKey();

        if (start == null || start.length == 0) start = new byte[] { 0x00 };
        if (end == null || end.length == 0) end = new byte[] { (byte) 0xFF };

        // 计算中点
        byte[] splitKey = new byte[Math.max(start.length, end.length)];
        int carry = 0;
        for (int i = splitKey.length - 1; i >= 0; i--) {
            int startVal = i < start.length ? start[i] & 0xFF : 0;
            int endVal = i < end.length ? end[i] & 0xFF : 0;
            int mid = (startVal + endVal + carry) / 2;
            carry = (startVal + endVal + carry) % 2;
            splitKey[i] = (byte) mid;
        }

        return splitKey;
    }

    /**
     * Prepare a split. This copies child-region data and blocks parent writes, but
     * does not close the parent region until Master commits metadata.
     */
    public RegionSplitResult splitRegion(String regionId, byte[] splitKey,
                                         String leftRegionId, String rightRegionId) throws Exception {
        PendingSplit existing = pendingSplits.get(regionId);
        if (existing != null && existing.matches(leftRegionId, rightRegionId)) {
            logger.info("Returning existing prepared split for region: {}", regionId);
            return existing.toResult();
        }

        RegionStorage oldStorage = regionManager.getRegionStorage(regionId);
        if (oldStorage == null) {
            throw new IOException("Region storage not found: " + regionId);
        }

        Region oldRegion = regionManager.getRegion(regionId);
        if (oldRegion == null) {
            throw new IOException("Region not found: " + regionId);
        }
        if (splitKey == null || splitKey.length == 0) {
            splitKey = findBestSplitPoint(regionId);
        }
        if (!isValidSplitKey(oldRegion, splitKey)) {
            throw new IllegalStateException(
                "Invalid split key for region " + regionId + ": " + BytesUtil.bytesToHex(splitKey));
        }

        logger.info("Preparing split for region: {} at key: {}", regionId, BytesUtil.bytesToHex(splitKey));
        regionManager.blockWrites(regionId);
        try {

        // 1. 创建两个新 Region 元数据（优先用 Master 传过来的 ID）
        String leftId = (leftRegionId != null && !leftRegionId.isEmpty())
            ? leftRegionId : oldRegion.getTableName() + "_l_" + UUID.randomUUID().toString().substring(0, 6);
        String rightId = (rightRegionId != null && !rightRegionId.isEmpty())
            ? rightRegionId : oldRegion.getTableName() + "_r_" + UUID.randomUUID().toString().substring(0, 6);

        Region leftRegion = new Region();
        leftRegion.setRegionId(leftId);
        leftRegion.setTableName(oldRegion.getTableName());
        leftRegion.setStartKey(oldRegion.getStartKey());
        leftRegion.setEndKey(splitKey);

        Region rightRegion = new Region();
        rightRegion.setRegionId(rightId);
        rightRegion.setTableName(oldRegion.getTableName());
        rightRegion.setStartKey(splitKey);
        rightRegion.setEndKey(oldRegion.getEndKey());

        // 2. 创建新的存储
        RegionStorage leftStorage = regionManager.createRegionStorage(leftId);
        RegionStorage rightStorage = regionManager.createRegionStorage(rightId);

        // 3. 数据迁移 - 左半部分
        logger.info("Migrating left part for region: {}", leftId);
        Iterator<KeyValue> leftIterator = oldStorage.scan(oldRegion.getStartKey(), splitKey);
        int leftCount = 0;
        while (leftIterator.hasNext()) {
            KeyValue kv = leftIterator.next();
            leftStorage.put(kv);
            leftCount++;
        }
        logger.info("Migrated {} entries to left region", leftCount);

        // 4. 数据迁移 - 右半部分
        logger.info("Migrating right part for region: {}", rightId);
        Iterator<KeyValue> rightIterator = oldStorage.scan(splitKey, oldRegion.getEndKey());
        int rightCount = 0;
        while (rightIterator.hasNext()) {
            KeyValue kv = rightIterator.next();
            rightStorage.put(kv);
            rightCount++;
        }
        logger.info("Migrated {} entries to right region", rightCount);
        if (leftCount == 0 || rightCount == 0) {
            cleanupSplitStorage(leftId, leftStorage);
            cleanupSplitStorage(rightId, rightStorage);
            throw new IllegalStateException(String.format(
                "Split aborted for region %s: unbalanced split key %s (leftEntries=%d, rightEntries=%d). " +
                    "Likely a single-key or extremely skewed hotspot that split cannot mitigate.",
                regionId, BytesUtil.bytesToHex(splitKey), leftCount, rightCount));
        }

        pendingSplits.put(regionId, new PendingSplit(regionId, leftRegion, rightRegion,
            leftStorage, rightStorage, splitKey));
        logger.info("Region split prepared: {} -> {} + {}", regionId, leftId, rightId);

        RegionSplitResult result = new RegionSplitResult();
        result.setParentRegionId(regionId);
        result.setLeftRegion(leftRegion);
        result.setRightRegion(rightRegion);
        result.setSplitKey(splitKey);

        return result;
        } catch (Exception e) {
            regionManager.unblockWrites(regionId);
            throw e;
        }
    }

    public void commitSplit(String parentRegionId, String leftRegionId, String rightRegionId) throws Exception {
        PendingSplit pending = pendingSplits.get(parentRegionId);
        if (pending == null) {
            if (regionManager.getRegion(parentRegionId) == null
                    && regionManager.getRegion(leftRegionId) != null
                    && regionManager.getRegion(rightRegionId) != null) {
                logger.info("Split already committed for parent region: {}", parentRegionId);
                return;
            }
            throw new IOException("Pending split not found: " + parentRegionId);
        }
        if (!pending.matches(leftRegionId, rightRegionId)) {
            throw new IllegalStateException("Pending split child ids do not match commit request: " + parentRegionId);
        }

        logger.info("Committing split for region: {} -> {} + {}", parentRegionId, leftRegionId, rightRegionId);
        regionManager.closeRegion(parentRegionId, true, true);
        regionManager.registerOpenedRegion(pending.leftRegion, pending.leftStorage);
        regionManager.registerOpenedRegion(pending.rightRegion, pending.rightStorage);
        pendingSplits.remove(parentRegionId);
        logger.info("Region split committed: {} -> {} + {}", parentRegionId, leftRegionId, rightRegionId);
    }

    public void abortSplit(String parentRegionId, String leftRegionId, String rightRegionId) {
        PendingSplit pending = pendingSplits.remove(parentRegionId);
        if (pending == null) {
            regionManager.unblockWrites(parentRegionId);
            logger.info("No pending split to abort for region: {}", parentRegionId);
            return;
        }

        if (!pending.matches(leftRegionId, rightRegionId)) {
            pendingSplits.put(parentRegionId, pending);
            throw new IllegalStateException("Pending split child ids do not match abort request: " + parentRegionId);
        }

        cleanupSplitStorage(pending.leftRegion.getRegionId(), pending.leftStorage);
        cleanupSplitStorage(pending.rightRegion.getRegionId(), pending.rightStorage);
        regionManager.unblockWrites(parentRegionId);
        logger.info("Region split aborted: {}", parentRegionId);
    }

    private static class PendingSplit {
        final String parentRegionId;
        final Region leftRegion;
        final Region rightRegion;
        final RegionStorage leftStorage;
        final RegionStorage rightStorage;
        final byte[] splitKey;

        PendingSplit(String parentRegionId, Region leftRegion, Region rightRegion,
                     RegionStorage leftStorage, RegionStorage rightStorage, byte[] splitKey) {
            this.parentRegionId = parentRegionId;
            this.leftRegion = leftRegion;
            this.rightRegion = rightRegion;
            this.leftStorage = leftStorage;
            this.rightStorage = rightStorage;
            this.splitKey = splitKey;
        }

        boolean matches(String leftRegionId, String rightRegionId) {
            return leftRegion.getRegionId().equals(leftRegionId)
                && rightRegion.getRegionId().equals(rightRegionId);
        }

        RegionSplitResult toResult() {
            RegionSplitResult result = new RegionSplitResult();
            result.setParentRegionId(parentRegionId);
            result.setLeftRegion(leftRegion);
            result.setRightRegion(rightRegion);
            result.setSplitKey(splitKey);
            return result;
        }
    }

    /**
     * 分裂结果
     */
    private List<byte[]> sampleDistinctRowKeys(RegionStorage storage,
                                               byte[] startKey,
                                               byte[] endKey,
                                               int sampleLimit) {
        List<byte[]> samples = new ArrayList<>(Math.max(16, sampleLimit));
        long distinctCount = 0L;
        byte[] lastRowKey = null;

        Iterator<KeyValue> iterator = storage.scan(startKey, endKey);
        while (iterator.hasNext()) {
            KeyValue kv = iterator.next();
            byte[] rowKey = kv.getRowKey();
            if (rowKey == null) {
                continue;
            }
            if (lastRowKey != null && BytesUtil.equals(lastRowKey, rowKey)) {
                continue;
            }

            lastRowKey = rowKey;
            byte[] rowKeyCopy = Arrays.copyOf(rowKey, rowKey.length);
            distinctCount++;

            if (samples.size() < sampleLimit) {
                samples.add(rowKeyCopy);
            } else {
                long replacementIndex = ThreadLocalRandom.current().nextLong(distinctCount);
                if (replacementIndex < sampleLimit) {
                    samples.set((int) replacementIndex, rowKeyCopy);
                }
            }
        }

        logger.info("Collected split-key samples: distinctRows={}, sampledRows={}", distinctCount, samples.size());
        return samples;
    }

    private byte[] selectMedianValidSplitKey(List<byte[]> sortedRowKeys, Region region) {
        if (sortedRowKeys == null || sortedRowKeys.size() < 2) {
            return null;
        }

        int mid = sortedRowKeys.size() / 2;
        for (int offset = 0; offset < sortedRowKeys.size(); offset++) {
            int leftIndex = mid - offset;
            if (isCandidateIndexValid(leftIndex, sortedRowKeys.size())) {
                byte[] candidate = sortedRowKeys.get(leftIndex);
                if (isValidSplitKey(region, candidate)) {
                    return candidate;
                }
            }

            int rightIndex = mid + offset;
            if (rightIndex != leftIndex && isCandidateIndexValid(rightIndex, sortedRowKeys.size())) {
                byte[] candidate = sortedRowKeys.get(rightIndex);
                if (isValidSplitKey(region, candidate)) {
                    return candidate;
                }
            }
        }

        return null;
    }

    private boolean isCandidateIndexValid(int index, int size) {
        // Keep at least one row-key on each side.
        return index > 0 && index < size - 1;
    }

    private boolean isValidSplitKey(Region region, byte[] splitKey) {
        if (region == null || splitKey == null || splitKey.length == 0) {
            return false;
        }

        byte[] startKey = region.getStartKey();
        if (startKey != null && startKey.length > 0 && BytesUtil.compareTo(splitKey, startKey) <= 0) {
            return false;
        }

        byte[] endKey = region.getEndKey();
        if (endKey != null && endKey.length > 0 && BytesUtil.compareTo(splitKey, endKey) >= 0) {
            return false;
        }

        return true;
    }

    private void cleanupSplitStorage(String regionId, RegionStorage storage) {
        if (storage == null) {
            return;
        }

        try {
            storage.dropData();
        } catch (Exception e) {
            logger.warn("Failed to drop split temp table for region {}: {}", regionId, e.getMessage());
        }

        try {
            storage.close();
        } catch (Exception e) {
            logger.warn("Failed to close split temp storage for region {}: {}", regionId, e.getMessage());
        }
    }

    public static class RegionSplitResult {
        private String parentRegionId;
        private com.minisql.common.model.Region leftRegion;
        private com.minisql.common.model.Region rightRegion;
        private byte[] splitKey;

        public String getParentRegionId() { return parentRegionId; }
        public void setParentRegionId(String parentRegionId) { this.parentRegionId = parentRegionId; }
        public com.minisql.common.model.Region getLeftRegion() { return leftRegion; }
        public void setLeftRegion(com.minisql.common.model.Region leftRegion) { this.leftRegion = leftRegion; }
        public com.minisql.common.model.Region getRightRegion() { return rightRegion; }
        public void setRightRegion(com.minisql.common.model.Region rightRegion) { this.rightRegion = rightRegion; }
        public byte[] getSplitKey() { return splitKey; }
        public void setSplitKey(byte[] splitKey) { this.splitKey = splitKey; }
    }
}
