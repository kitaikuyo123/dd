package com.minisql.regionserver;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

/**
 * Region 合并服务
 * 基于 MySQL 存储引擎
 */
public class RegionMergeService {

    private static final Logger logger = LoggerFactory.getLogger(RegionMergeService.class);

    private final RegionManager regionManager;

    // 合并阈值：两个 Region 都小于此值时可以合并（100MB）
    public static final long MERGE_THRESHOLD = 100L * 1024 * 1024;

    // 最大合并大小：合并后超过此值则不合并（8GB）
    public static final long MAX_MERGE_SIZE = 8L * 1024 * 1024 * 1024;

    // 最小合并大小：单个 Region 小于此值时强制合并（10MB）
    public static final long MIN_MERGE_SIZE = 10L * 1024 * 1024;

    public RegionMergeService(RegionManager regionManager) {
        this.regionManager = regionManager;
    }

    /**
     * 检查两个 Region 是否可以合并
     */
    public boolean canMerge(String leftRegionId, String rightRegionId) {
        try {
            // 1. 检查是否相邻
            if (!isAdjacent(leftRegionId, rightRegionId)) {
                return false;
            }

            // 2. 获取大小
            RegionStorage leftStorage = regionManager.getRegionStorage(leftRegionId);
            RegionStorage rightStorage = regionManager.getRegionStorage(rightRegionId);
            if (leftStorage == null || rightStorage == null) {
                return false;
            }

            long leftSize = leftStorage.getStoreFileSize();
            long rightSize = rightStorage.getStoreFileSize();
            long totalSize = leftSize + rightSize;

            // 3. 合并后超过最大合并大小，不能合并
            if (totalSize > MAX_MERGE_SIZE) {
                logger.debug("Regions too large to merge: {} + {} = {} > {}",
                    leftRegionId, rightRegionId, totalSize, MAX_MERGE_SIZE);
                return false;
            }

            // 4. 两个都很小，可以合并
            if (leftSize < MERGE_THRESHOLD && rightSize < MERGE_THRESHOLD) {
                logger.debug("Both regions small enough to merge: {}={}, {}={}",
                    leftRegionId, leftSize, rightRegionId, rightSize);
                return true;
            }

            // 5. 其中一个非常小，强制合并
            if (leftSize < MIN_MERGE_SIZE || rightSize < MIN_MERGE_SIZE) {
                logger.debug("One region very small, forcing merge: {}={}, {}={}",
                    leftRegionId, leftSize, rightRegionId, rightSize);
                return true;
            }

            return false;
        } catch (Exception e) {
            logger.error("Error checking merge for regions: " + leftRegionId + ", " + rightRegionId, e);
            return false;
        }
    }

    /**
     * 检查两个 Region 是否相邻
     */
    private boolean isAdjacent(String leftRegionId, String rightRegionId) {
        Region left = regionManager.getRegion(leftRegionId);
        Region right = regionManager.getRegion(rightRegionId);

        if (left == null || right == null) {
            return false;
        }

        byte[] leftEnd = left.getEndKey();
        byte[] rightStart = right.getStartKey();

        if (leftEnd == null && rightStart == null) {
            return true;
        }
        if (leftEnd == null || rightStart == null) {
            return false;
        }
        return Arrays.equals(leftEnd, rightStart);
    }

    /**
     * 执行 Region 合并（带事务回滚机制）
     */
    public RegionMergeResult mergeRegions(String leftRegionId, String rightRegionId) throws Exception {
        RegionStorage leftStorage = regionManager.getRegionStorage(leftRegionId);
        RegionStorage rightStorage = regionManager.getRegionStorage(rightRegionId);

        if (leftStorage == null || rightStorage == null) {
            throw new IOException("Region storage not found: " + leftRegionId + " or " + rightRegionId);
        }

        Region leftRegion = regionManager.getRegion(leftRegionId);
        Region rightRegion = regionManager.getRegion(rightRegionId);

        if (leftRegion == null || rightRegion == null) {
            throw new IOException("Region not found: " + leftRegionId + " or " + rightRegionId);
        }

        logger.info("Merging regions: {} + {}", leftRegionId, rightRegionId);

        // ========== 事务开始：创建检查点 ==========
        MergeCheckpoint checkpoint = new MergeCheckpoint();
        checkpoint.leftRegionId = leftRegionId;
        checkpoint.rightRegionId = rightRegionId;
        checkpoint.leftRegion = leftRegion;
        checkpoint.rightRegion = rightRegion;
        checkpoint.mergedRegionId = null;
        checkpoint.mergedStorage = null;

        try {
            // 1. 创建合并后的新 Region
            String mergedRegionId = leftRegion.getTableName() + "_m_" + UUID.randomUUID().toString().substring(0, 6);
            checkpoint.mergedRegionId = mergedRegionId;

            Region mergedRegion = new Region();
            mergedRegion.setRegionId(mergedRegionId);
            mergedRegion.setTableName(leftRegion.getTableName());
            mergedRegion.setStartKey(leftRegion.getStartKey());
            mergedRegion.setEndKey(rightRegion.getEndKey());

            // 2. 创建新存储（会自动创建 kv_store_{mergedRegionId} 表）
            RegionStorage mergedStorage = regionManager.createRegionStorage(mergedRegionId);
            checkpoint.mergedStorage = mergedStorage;
            mergedStorage.start();

            int leftCount = 0;
            int rightCount = 0;

            try {
                // 3. 迁移数据 - 左半部分
                logger.info("Migrating left part for region: {}", mergedRegionId);
                Iterator<KeyValue> leftIterator = leftStorage.scan(leftRegion.getStartKey(), leftRegion.getEndKey());
                while (leftIterator.hasNext()) {
                    KeyValue kv = leftIterator.next();
                    mergedStorage.put(kv);
                    leftCount++;
                }
                logger.info("Migrated {} entries from left region", leftCount);

                // 4. 迁移数据 - 右半部分
                logger.info("Migrating right part for region: {}", mergedRegionId);
                Iterator<KeyValue> rightIterator = rightStorage.scan(rightRegion.getStartKey(), rightRegion.getEndKey());
                while (rightIterator.hasNext()) {
                    KeyValue kv = rightIterator.next();
                    mergedStorage.put(kv);
                    rightCount++;
                }
                logger.info("Migrated {} entries from right region", rightCount);


            } catch (Exception e) {
                logger.error("Error migrating data during merge, rolling back...", e);
                rollbackMerge(checkpoint);
                throw new IOException("Failed to migrate data during merge: " + e.getMessage(), e);
            }

            // 5. 关闭旧 Region
            logger.info("Closing old regions: {} and {}", leftRegionId, rightRegionId);
            regionManager.closeRegion(leftRegionId, true);
            regionManager.closeRegion(rightRegionId, true);

            // 6. 打开新 Region
            regionManager.registerOpenedRegion(mergedRegion, mergedStorage);

            logger.info("Region merge completed: {} + {} -> {}", leftRegionId, rightRegionId, mergedRegionId);

            // 7. 构建结果
            RegionMergeResult result = new RegionMergeResult();
            result.setLeftRegionId(leftRegionId);
            result.setRightRegionId(rightRegionId);
            result.setMergedRegion(mergedRegion);
            result.setTotalEntries(leftCount + rightCount);

            return result;

        } catch (Exception e) {
            logger.error("Merge failed, attempting rollback...", e);
            try {
                rollbackMerge(checkpoint);
            } catch (Exception rollbackError) {
                logger.error("Rollback also failed! Manual intervention may be required.", rollbackError);
            }
            throw e;
        }
    }

    /**
     * 合并事务回滚
     */
    private void rollbackMerge(MergeCheckpoint checkpoint) throws Exception {
        logger.info("Rolling back merge for regions: {} + {}", checkpoint.leftRegionId, checkpoint.rightRegionId);

        try {
            // 1. 如果新 Region 已创建但未完成，删除它
            if (checkpoint.mergedStorage != null) {
                try {
                    logger.info("Dropping merged table: kv_store_{}", checkpoint.mergedRegionId);
                    checkpoint.mergedStorage.close();
                    // 注意：这里不删除表，保留用于故障恢复
                    logger.info("Merged storage closed (table preserved for recovery)");
                } catch (Exception e) {
                    logger.warn("Error closing merged storage: {}", e.getMessage());
                }
            }

            // 2. 重新打开原始 Region
            if (!regionManager.isRegionOpen(checkpoint.leftRegionId)) {
                logger.info("Re-opening left region: {}", checkpoint.leftRegionId);
                regionManager.openRegion(checkpoint.leftRegion);
            }

            if (!regionManager.isRegionOpen(checkpoint.rightRegionId)) {
                logger.info("Re-opening right region: {}", checkpoint.rightRegionId);
                regionManager.openRegion(checkpoint.rightRegion);
            }

            // 3. 恢复 Region 状态
            regionManager.setRegionState(checkpoint.leftRegionId, RegionManager.RegionState.OPEN);
            regionManager.setRegionState(checkpoint.rightRegionId, RegionManager.RegionState.OPEN);

            logger.info("Rollback completed successfully: {} and {} are back online",
                checkpoint.leftRegionId, checkpoint.rightRegionId);

        } catch (Exception e) {
            logger.error("Rollback failed! Regions {} and {} may need manual recovery",
                checkpoint.leftRegionId, checkpoint.rightRegionId, e);
            throw e;
        }
    }

    /**
     * 合并事务检查点（用于回滚）
     */
    private static class MergeCheckpoint {
        String leftRegionId;
        String rightRegionId;
        Region leftRegion;
        Region rightRegion;
        String mergedRegionId;
        RegionStorage mergedStorage;
    }

    /**
     * 合并结果
     */
    public static class RegionMergeResult {
        private String leftRegionId;
        private String rightRegionId;
        private Region mergedRegion;
        private long totalEntries;

        public String getLeftRegionId() { return leftRegionId; }
        public void setLeftRegionId(String leftRegionId) { this.leftRegionId = leftRegionId; }

        public String getRightRegionId() { return rightRegionId; }
        public void setRightRegionId(String rightRegionId) { this.rightRegionId = rightRegionId; }

        public Region getMergedRegion() { return mergedRegion; }
        public void setMergedRegion(Region mergedRegion) { this.mergedRegion = mergedRegion; }

        public long getTotalEntries() { return totalEntries; }
        public void setTotalEntries(long totalEntries) { this.totalEntries = totalEntries; }
    }
}
