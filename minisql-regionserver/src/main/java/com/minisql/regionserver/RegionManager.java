package com.minisql.regionserver;

import com.minisql.common.model.Region;
import com.minisql.storage.MySQLConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Region 管理器
 * 基于 MySQL 存储引擎
 */
public class RegionManager {

    private static final Logger logger = LoggerFactory.getLogger(RegionManager.class);

    // 管理的 Region: regionId -> Region
    private final ConcurrentMap<String, Region> regions = new ConcurrentHashMap<>();

    // Region 存储引擎：regionId -> MySQLRegionStorage
    private final ConcurrentMap<String, MySQLRegionStorage> mysqlRegionStorages = new ConcurrentHashMap<>();

    // Region 状态：regionId -> RegionState
    private final ConcurrentMap<String, RegionState> regionStates = new ConcurrentHashMap<>();

    // Region 主副本状态：regionId -> 是否为主副本
    private final ConcurrentMap<String, Boolean> regionPrimaryStatus = new ConcurrentHashMap<>();

    // Region 已应用复制序列号：regionId -> lastAppliedSequenceId
    private final ConcurrentMap<String, AtomicLong> lastAppliedReplicationSequenceIds = new ConcurrentHashMap<>();

    // Region 写入冻结状态：regionId -> writesBlocked
    private final ConcurrentMap<String, Boolean> regionWriteBlocked = new ConcurrentHashMap<>();

    // RegionServer 引用（用于获取 MySQL 配置，支持继承机制）
    private final RegionServer regionServer;

    public enum RegionState {
        OPENING, OPEN, CLOSING, CLOSED
    }

    public RegionManager(RegionServer regionServer) {
        this.regionServer = regionServer;
    }

    /**
     * 打开 Region
     */
    public void openRegion(Region region) {
        String regionId = region.getRegionId();
        regionStates.put(regionId, RegionState.OPENING);

        try {
            MySQLRegionStorage storage = new MySQLRegionStorage(regionId, regionServer.getOrCreateSharedDataSource());
            storage.start();
            mysqlRegionStorages.put(regionId, storage);

            regions.put(regionId, region);
            regionStates.put(regionId, RegionState.OPEN);
            boolean primaryOnThisServer = region.getPrimary() == null
                || region.getPrimary().equals(regionServer.getServerId());
            regionPrimaryStatus.put(regionId, primaryOnThisServer);
            lastAppliedReplicationSequenceIds.putIfAbsent(regionId, new AtomicLong(0));
            regionWriteBlocked.put(regionId, false);

            logger.info("Region opened: {} ({})", regionId, primaryOnThisServer ? "primary" : "replica");
        } catch (Exception e) {
            regionStates.put(regionId, RegionState.CLOSED);
            throw new RuntimeException("Failed to open region: " + regionId, e);
        }
    }

    /**
     * 关闭 Region
     * @param regionId Region ID
     * @param abort 是否中止（不 flush 数据）
     * @param dropTable 是否删除 MySQL 表（用于 DROP TABLE 操作）
     */
    public void closeRegion(String regionId, boolean abort, boolean dropTable) {
        RegionState currentState = regionStates.get(regionId);
        if (currentState == null || currentState == RegionState.CLOSED) {
            return;
        }

        regionStates.put(regionId, RegionState.CLOSING);

        try {
            if (!abort) {
                // 刷新数据到磁盘
                MySQLRegionStorage storage = mysqlRegionStorages.get(regionId);
                if (storage != null) {
                    storage.flush();
                }
            }

            // 删除 MySQL 表（如果需要）
            if (dropTable) {
                MySQLRegionStorage storage = mysqlRegionStorages.get(regionId);
                if (storage != null) {
                    storage.dropTable();
                }
            }

            // 关闭存储引擎
            MySQLRegionStorage storage = mysqlRegionStorages.remove(regionId);
            if (storage != null) {
                storage.close();
            }

            regions.remove(regionId);
            regionStates.put(regionId, RegionState.CLOSED);
            regionPrimaryStatus.remove(regionId);
            lastAppliedReplicationSequenceIds.remove(regionId);
            regionWriteBlocked.remove(regionId);
            logger.info("Region closed: {}{}", regionId, dropTable ? " and table dropped" : "");
        } catch (Exception e) {
            regionStates.put(regionId, RegionState.OPEN);
            throw new RuntimeException("Failed to close region: " + regionId, e);
        }
    }

    /**
     * 关闭 Region（向后兼容）
     */
    public void closeRegion(String regionId, boolean abort) {
        closeRegion(regionId, abort, false);
    }

    /**
     * 获取 MySQL Region 存储引擎
     */
    public MySQLRegionStorage getMySQLRegionStorage(String regionId) {
        return mysqlRegionStorages.get(regionId);
    }

    /**
     * 获取 Region
     */
    public Region getRegion(String regionId) {
        return regions.get(regionId);
    }

    /**
     * 获取 Region 状态
     */
    public RegionState getRegionState(String regionId) {
        return regionStates.get(regionId);
    }

    /**
     * 检查 Region 是否处于 OPEN 状态
     */
    public boolean isRegionOpen(String regionId) {
        return regionStates.get(regionId) == RegionState.OPEN;
    }

    /**
     * 获取所有管理的 Region
     */
    public Collection<Region> getAllRegions() {
        return regions.values();
    }

    /**
     * Flush Region 数据
     */
    public void flushRegion(String regionId) throws IOException {
        MySQLRegionStorage storage = mysqlRegionStorages.get(regionId);
        if (storage != null) {
            storage.flush();
        }
    }

    /**
     * Compact Region
     */
    public void compactRegion(String regionId, boolean major) throws IOException {
        MySQLRegionStorage storage = mysqlRegionStorages.get(regionId);
        if (storage != null) {
            storage.compact(major);
        }
    }

    /**
     * 注册 MySQL Region 存储引擎
     */
    public void registerMySQLRegionStorage(String regionId, MySQLRegionStorage storage) {
        mysqlRegionStorages.put(regionId, storage);
    }

    /**
     * 内部注册 Region
     */
    public void registerRegionInternal(Region region) {
        regions.put(region.getRegionId(), region);
    }

    /**
     * 设置 Region 状态
     */
    public void setRegionState(String regionId, RegionState state) {
        regionStates.put(regionId, state);
    }

    /**
     * 获取 MySQL 配置（向后兼容，返回主配置）
     */
    public MySQLConfig getMySQLConfig() {
        return regionServer.getMySQLConfig();
    }

    /**
     * 获取指定 Region 的 MySQL 配置（支持继承/覆盖）
     * @param region Region 对象
     * @return Region 的 MySQL 配置（如果有独立配置则返回独立配置，否则返回主配置）
     */
    public MySQLConfig getMysqlConfigForRegion(Region region) {
        return regionServer.getMysqlConfigForRegion(region);
    }

    // ==================== Fencing Token 管理（防止脑裂）====================

    // Region 的 Fencing Token: regionId -> fencingToken
    private final ConcurrentMap<String, AtomicLong> regionFencingTokens = new ConcurrentHashMap<>();

    /**
     * 将 Region 提升为主副本
     */
    public void promoteToPrimary(String regionId) {
        regionPrimaryStatus.put(regionId, true);
        logger.info("Region {} promoted to primary", regionId);
    }

    /**
     * 将 Region 降级为从副本
     */
    public void demoteToReplica(String regionId) {
        regionPrimaryStatus.put(regionId, false);
        logger.info("Region {} demoted to replica", regionId);
    }

    /**
     * 检查 Region 是否为主副本
     */
    public boolean isPrimary(String regionId) {
        return regionPrimaryStatus.getOrDefault(regionId, false);
    }

    /**
     * 更新 Fencing Token
     */
    public void updateFencingToken(String regionId, long newToken) {
        AtomicLong token = regionFencingTokens.computeIfAbsent(regionId, k -> new AtomicLong(0));
        long oldToken = token.getAndSet(newToken);
        logger.info("Region {} fencing token updated: {} -> {}", regionId, oldToken, newToken);
    }

    /**
     * 获取当前 Fencing Token
     */
    public long getFencingToken(String regionId) {
        AtomicLong token = regionFencingTokens.get(regionId);
        return token != null ? token.get() : 0;
    }

    /**
     * 验证 Fencing Token（防止旧主写入）
     */
    public boolean verifyFencingToken(String regionId, long token) {
        long currentToken = getFencingToken(regionId);
        return token >= currentToken;
    }

    public long getLastAppliedReplicationSequenceId(String regionId) {
        AtomicLong sequenceId = lastAppliedReplicationSequenceIds.get(regionId);
        return sequenceId != null ? sequenceId.get() : 0L;
    }

    public void updateLastAppliedReplicationSequenceId(String regionId, long sequenceId) {
        lastAppliedReplicationSequenceIds
            .computeIfAbsent(regionId, key -> new AtomicLong(0))
            .updateAndGet(current -> Math.max(current, sequenceId));
    }

    public void blockWrites(String regionId) {
        regionWriteBlocked.put(regionId, true);
    }

    public void unblockWrites(String regionId) {
        regionWriteBlocked.put(regionId, false);
    }

    public boolean isWriteBlocked(String regionId) {
        return regionWriteBlocked.getOrDefault(regionId, false);
    }
}
