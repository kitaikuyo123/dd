package com.minisql.regionserver;

import com.minisql.common.model.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Region manager backed by MySQL storage.
 */
public class RegionManager {

    private static final Logger logger = LoggerFactory.getLogger(RegionManager.class);

    // regionId -> Region metadata
    private final ConcurrentMap<String, Region> regions = new ConcurrentHashMap<>();

    // regionId -> Region storage
    private final ConcurrentMap<String, MySQLRegionStorage> mysqlRegionStorages = new ConcurrentHashMap<>();

    // regionId -> lifecycle state
    private final ConcurrentMap<String, RegionState> regionStates = new ConcurrentHashMap<>();

    // regionId -> whether primary on this server
    private final ConcurrentMap<String, Boolean> regionPrimaryStatus = new ConcurrentHashMap<>();

    // regionId -> last applied replication sequence id
    private final ConcurrentMap<String, AtomicLong> lastAppliedReplicationSequenceIds = new ConcurrentHashMap<>();

    // regionId -> writes blocked during migration/fencing window
    private final ConcurrentMap<String, Boolean> regionWriteBlocked = new ConcurrentHashMap<>();

    // regionId -> fencing token
    private final ConcurrentMap<String, AtomicLong> regionFencingTokens = new ConcurrentHashMap<>();

    private final RegionServer regionServer;

    public enum RegionState {
        OPENING, OPEN, CLOSING, CLOSED
    }

    public RegionManager(RegionServer regionServer) {
        this.regionServer = regionServer;
    }

    /**
     * Opens a region and creates its storage using the RegionServer shared pool.
     */
    public void openRegion(Region region) {
        String regionId = region.getRegionId();
        regionStates.put(regionId, RegionState.OPENING);

        try {
            MySQLRegionStorage storage = createRegionStorage(regionId);
            storage.start();
            registerOpenedRegion(region, storage);
        } catch (Exception e) {
            regionStates.put(regionId, RegionState.CLOSED);
            throw new RuntimeException("Failed to open region: " + regionId, e);
        }
    }

    /**
     * Creates region storage backed by the RegionServer-level shared Hikari pool.
     */
    public MySQLRegionStorage createRegionStorage(String regionId) {
        return new MySQLRegionStorage(regionId, regionServer.getOrCreateSharedDataSource());
    }

    /**
     * Registers a region as OPEN using the same state initialization as the standard open path.
     */
    public void registerOpenedRegion(Region region, MySQLRegionStorage storage) {
        String regionId = region.getRegionId();
        mysqlRegionStorages.put(regionId, storage);
        regions.put(regionId, region);
        regionStates.put(regionId, RegionState.OPEN);

        boolean primaryOnThisServer = region.getPrimary() == null
            || region.getPrimary().equals(regionServer.getServerId());
        regionPrimaryStatus.put(regionId, primaryOnThisServer);
        lastAppliedReplicationSequenceIds.putIfAbsent(regionId, new AtomicLong(0));
        regionWriteBlocked.put(regionId, false);

        logger.info("Region opened: {} ({})", regionId, primaryOnThisServer ? "primary" : "replica");
    }

    /**
     * Closes a region.
     */
    public void closeRegion(String regionId, boolean abort, boolean dropTable) {
        RegionState currentState = regionStates.get(regionId);
        if (currentState == null || currentState == RegionState.CLOSED) {
            return;
        }

        regionStates.put(regionId, RegionState.CLOSING);

        try {
            if (!abort) {
                MySQLRegionStorage storage = mysqlRegionStorages.get(regionId);
                if (storage != null) {
                    storage.flush();
                }
            }

            if (dropTable) {
                MySQLRegionStorage storage = mysqlRegionStorages.get(regionId);
                if (storage != null) {
                    storage.dropTable();
                }
            }

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
     * Backward-compatible overload.
     */
    public void closeRegion(String regionId, boolean abort) {
        closeRegion(regionId, abort, false);
    }

    public MySQLRegionStorage getMySQLRegionStorage(String regionId) {
        return mysqlRegionStorages.get(regionId);
    }

    public Region getRegion(String regionId) {
        return regions.get(regionId);
    }

    public RegionState getRegionState(String regionId) {
        return regionStates.get(regionId);
    }

    public boolean isRegionOpen(String regionId) {
        return regionStates.get(regionId) == RegionState.OPEN;
    }

    public Collection<Region> getAllRegions() {
        return regions.values();
    }

    public void flushRegion(String regionId) throws IOException {
        MySQLRegionStorage storage = mysqlRegionStorages.get(regionId);
        if (storage != null) {
            storage.flush();
        }
    }

    public void compactRegion(String regionId, boolean major) throws IOException {
        MySQLRegionStorage storage = mysqlRegionStorages.get(regionId);
        if (storage != null) {
            storage.compact(major);
        }
    }

    public void registerMySQLRegionStorage(String regionId, MySQLRegionStorage storage) {
        mysqlRegionStorages.put(regionId, storage);
    }

    public void registerRegionInternal(Region region) {
        regions.put(region.getRegionId(), region);
    }

    public void setRegionState(String regionId, RegionState state) {
        regionStates.put(regionId, state);
    }

    public void promoteToPrimary(String regionId) {
        regionPrimaryStatus.put(regionId, true);
        logger.info("Region {} promoted to primary", regionId);
    }

    public void demoteToReplica(String regionId) {
        regionPrimaryStatus.put(regionId, false);
        logger.info("Region {} demoted to replica", regionId);
    }

    public boolean isPrimary(String regionId) {
        return regionPrimaryStatus.getOrDefault(regionId, false);
    }

    public void updateFencingToken(String regionId, long newToken) {
        AtomicLong token = regionFencingTokens.computeIfAbsent(regionId, k -> new AtomicLong(0));
        long oldToken = token.getAndSet(newToken);
        logger.info("Region {} fencing token updated: {} -> {}", regionId, oldToken, newToken);
    }

    public long getFencingToken(String regionId) {
        AtomicLong token = regionFencingTokens.get(regionId);
        return token != null ? token.get() : 0;
    }

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
