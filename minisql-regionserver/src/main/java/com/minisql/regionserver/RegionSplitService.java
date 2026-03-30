package com.minisql.regionserver;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.storage.MySQLConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Region 分裂服务
 * 基于 MySQL 存储引擎
 */
public class RegionSplitService {

    private static final Logger logger = LoggerFactory.getLogger(RegionSplitService.class);

    private final RegionManager regionManager;

    // 分裂阈值
    private long splitThreshold = 10L * 1024 * 1024 * 1024; // Default 10GB
    // 最小分裂大小（防止过度分裂）
    private long minSplitSize = 1L * 1024 * 1024 * 1024; // Default 1GB

    public static final long DEFAULT_SPLIT_THRESHOLD = 10L * 1024 * 1024 * 1024;
    public static final long MIN_SPLIT_SIZE_LIMIT = 1L * 1024 * 1024 * 1024;

    public RegionSplitService(RegionManager regionManager) {
        this.regionManager = regionManager;
    }

    public void setConfig(long splitThresholdMb, long minSplitSizeMb) {
        this.splitThreshold = splitThresholdMb * 1024L * 1024L;
        this.minSplitSize = minSplitSizeMb * 1024L * 1024L;
        logger.info("RegionSplitService configured: threshold={}MB, minSize={}MB", splitThresholdMb, minSplitSizeMb);
    }

    /**
     * 检查 Region 是否需要分裂
     */
    public boolean shouldSplit(String regionId) {
        try {
            MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(regionId);
            if (storage == null) return false;

            // 获取实际大小（定期校准估算值）
            long actualSize = storage.getActualTableSize();

            // 小于最小分裂大小，不分裂（防止过度分裂）
            if (actualSize < minSplitSize) {
                return false;
            }

            // 超过阈值，需要分裂
            return actualSize >= splitThreshold;
        } catch (Exception e) {
            logger.error("Error checking split for region: " + regionId, e);
            return false;
        }
    }

    /**
     * 寻找最佳分裂点（中点分裂策略）
     */
    public byte[] findBestSplitPoint(String regionId) throws IOException {
        MySQLRegionStorage storage = regionManager.getMySQLRegionStorage(regionId);
        if (storage == null) {
            throw new IOException("Region storage not found: " + regionId);
        }

        Region region = regionManager.getRegion(regionId);
        if (region == null) {
            throw new IOException("Region not found: " + regionId);
        }

        // 策略：中点分裂（简单均匀）
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
     * 执行 Region 分裂
     */
    public RegionSplitResult splitRegion(String regionId, byte[] splitKey) throws Exception {
        MySQLRegionStorage oldStorage = regionManager.getMySQLRegionStorage(regionId);
        if (oldStorage == null) {
            throw new IOException("Region storage not found: " + regionId);
        }

        Region oldRegion = regionManager.getRegion(regionId);
        if (oldRegion == null) {
            throw new IOException("Region not found: " + regionId);
        }

        logger.info("Splitting region: {} at key: {}", regionId, bytesToHex(splitKey));

        // 1. 创建两个新 Region 元数据
        String leftRegionId = oldRegion.getTableName() + "_l_" + UUID.randomUUID().toString().substring(0, 6);
        String rightRegionId = oldRegion.getTableName() + "_r_" + UUID.randomUUID().toString().substring(0, 6);

        Region leftRegion = new Region();
        leftRegion.setRegionId(leftRegionId);
        leftRegion.setTableName(oldRegion.getTableName());
        leftRegion.setStartKey(oldRegion.getStartKey());
        leftRegion.setEndKey(splitKey);

        Region rightRegion = new Region();
        rightRegion.setRegionId(rightRegionId);
        rightRegion.setTableName(oldRegion.getTableName());
        rightRegion.setStartKey(splitKey);
        rightRegion.setEndKey(oldRegion.getEndKey());

        // 2. 创建新的 MySQL 存储
        MySQLConfig config = regionManager.getMysqlConfigForRegion(oldRegion);
        MySQLRegionStorage leftStorage = new MySQLRegionStorage(leftRegionId, config);
        MySQLRegionStorage rightStorage = new MySQLRegionStorage(rightRegionId, config);
        leftStorage.start();
        rightStorage.start();

        // 3. 数据迁移 - 左半部分
        logger.info("Migrating left part for region: {}", leftRegionId);
        Iterator<KeyValue> leftIterator = oldStorage.scan(oldRegion.getStartKey(), splitKey);
        int leftCount = 0;
        while (leftIterator.hasNext()) {
            KeyValue kv = leftIterator.next();
            leftStorage.put(kv);
            leftCount++;
        }
        logger.info("Migrated {} entries to left region", leftCount);

        // 4. 数据迁移 - 右半部分
        logger.info("Migrating right part for region: {}", rightRegionId);
        Iterator<KeyValue> rightIterator = oldStorage.scan(splitKey, oldRegion.getEndKey());
        int rightCount = 0;
        while (rightIterator.hasNext()) {
            KeyValue kv = rightIterator.next();
            rightStorage.put(kv);
            rightCount++;
        }
        logger.info("Migrated {} entries to right region", rightCount);

        // 5. 关闭旧 Region（不删除表，用于回滚）
        logger.info("Closing old region: {}", regionId);
        regionManager.closeRegion(regionId, true);

        // 6. 注册并打开新 Region
        regionManager.registerRegionInternal(leftRegion);
        regionManager.setRegionState(leftRegionId, RegionManager.RegionState.OPEN);
        regionManager.registerMySQLRegionStorage(leftRegionId, leftStorage);

        regionManager.registerRegionInternal(rightRegion);
        regionManager.setRegionState(rightRegionId, RegionManager.RegionState.OPEN);
        regionManager.registerMySQLRegionStorage(rightRegionId, rightStorage);

        logger.info("Region split completed: {} -> {} + {}", regionId, leftRegionId, rightRegionId);

        RegionSplitResult result = new RegionSplitResult();
        result.setParentRegionId(regionId);
        result.setLeftRegion(leftRegion);
        result.setRightRegion(rightRegion);
        result.setSplitKey(splitKey);

        return result;
    }

    /**
     * 字节数组转十六进制（用于日志）
     */
    private String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * 分裂结果
     */
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
