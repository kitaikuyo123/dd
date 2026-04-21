package com.minisql.regionserver;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.KeyValue;
import com.minisql.storage.StorageEngine;
import com.minisql.storage.StorageEngineFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionManager unit tests")
class RegionManagerTest {

    // ---------- hand-written fakes ----------

    /**
     * Minimal fake StorageEngine that does nothing but record calls.
     */
    static class FakeStorageEngine implements StorageEngine {
        boolean started;
        boolean flushed;
        boolean compacted;
        boolean closed;
        boolean dropped;
        long estimatedSize;
        boolean throwOnStart;

        @Override public void put(byte[] key, KeyValue value) {}
        @Override public void batchPut(List<KeyValue> values) {}
        @Override public List<KeyValue> get(byte[] key) { return Collections.emptyList(); }
        @Override public Iterator<KeyValue> scan(byte[] startKey, byte[] endKey) {
            return Collections.<KeyValue>emptyList().iterator();
        }
        @Override public void delete(byte[] key) {}
        @Override public void flush() { flushed = true; }
        @Override public void compact(boolean major) { compacted = true; }
        @Override public void close() { closed = true; }
        @Override public void dropData() { dropped = true; }
        @Override public long estimateSizeBytes() { return estimatedSize; }
    }

    /**
     * Fake factory that returns {@link FakeStorageEngine} instances.
     * Optionally configured to throw on a specific region to simulate failures.
     */
    static class FakeEngineFactory implements StorageEngineFactory {
        FakeStorageEngine lastCreated;
        boolean throwOnCreate;
        String failRegionId;

        @Override
        public StorageEngine create(String regionId) {
            if (throwOnCreate) {
                throw new RuntimeException("Simulated factory failure for " + regionId);
            }
            FakeStorageEngine engine = new FakeStorageEngine();
            lastCreated = engine;
            return engine;
        }

        @Override
        public void close() {}
    }

    /**
     * Minimal fake RegionServer that only provides the serverId and engineFactory
     * required by {@link RegionManager}.
     */
    static class FakeRegionServer extends RegionServer {
        final FakeEngineFactory factory;
        final ServerId serverId;

        FakeRegionServer(String host, int port, FakeEngineFactory factory) {
            super(host, port, factory, null, 1, "./data/test-wal-mgr");
            this.factory = factory;
            this.serverId = new ServerId(host, port);
        }

        @Override
        public ServerId getServerId() {
            return serverId;
        }

        @Override
        public StorageEngineFactory getEngineFactory() {
            return factory;
        }
    }

    // ---------- helpers ----------

    private FakeEngineFactory factory;
    private FakeRegionServer fakeServer;
    private RegionManager regionManager;

    private Region makeRegion(String regionId) {
        return new Region(regionId, "test_table",
                "a".getBytes(), "z".getBytes());
    }

    private Region makeRegionWithPrimary(String regionId, String primaryHost, int primaryPort) {
        Region r = makeRegion(regionId);
        r.setPrimary(new ServerId(primaryHost, primaryPort));
        return r;
    }

    /**
     * Directly registers a region as OPEN without going through openRegion,
     * for tests that need a region in OPEN state without the full open lifecycle.
     */
    private void registerOpen(String regionId) {
        Region region = makeRegion(regionId);
        FakeStorageEngine engine = new FakeStorageEngine();
        RegionStorage storage = new RegionStorage(regionId, engine);
        regionManager.registerOpenedRegion(region, storage);
    }

    @BeforeEach
    void setUp() {
        factory = new FakeEngineFactory();
        fakeServer = new FakeRegionServer("localhost", 16020, factory);
        regionManager = fakeServer.getRegionManager();
    }

    // ==================== openRegion ====================

    @Nested
    @DisplayName("openRegion")
    class OpenRegionTests {

        @Test
        @DisplayName("opens a region and sets state to OPEN")
        void testOpenRegionBasic() {
            Region region = makeRegion("r1");

            regionManager.openRegion(region);

            assertTrue(regionManager.isRegionOpen("r1"));
            assertEquals(RegionManager.RegionState.OPEN, regionManager.getRegionState("r1"));
            assertNotNull(regionManager.getRegionStorage("r1"));
            assertNotNull(regionManager.getRegion("r1"));
        }

        @Test
        @DisplayName("openRegion is idempotent when region is already OPEN")
        void testOpenRegionIdempotent() {
            Region region = makeRegion("r1");
            regionManager.openRegion(region);

            // open again -- should be a no-op, not throw, not create a second storage
            RegionStorage first = regionManager.getRegionStorage("r1");
            regionManager.openRegion(region);
            RegionStorage second = regionManager.getRegionStorage("r1");

            assertSame(first, second, "Idempotent open should preserve the same storage");
            assertEquals(RegionManager.RegionState.OPEN, regionManager.getRegionState("r1"));
        }

        @Test
        @DisplayName("openRegion sets CLOSED state when storage creation fails")
        void testOpenRegionFailureSetsClosed() {
            factory.throwOnCreate = true;
            Region region = makeRegion("r-bad");

            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> regionManager.openRegion(region));

            assertTrue(ex.getMessage().contains("Failed to open region"));
            assertEquals(RegionManager.RegionState.CLOSED,
                    regionManager.getRegionState("r-bad"));
        }
    }

    // ==================== closeRegion ====================

    @Nested
    @DisplayName("closeRegion")
    class CloseRegionTests {

        @Test
        @DisplayName("closeRegion transitions OPEN region to CLOSED")
        void testCloseRegionFromOpen() {
            registerOpen("r1");

            regionManager.closeRegion("r1", false);

            assertEquals(RegionManager.RegionState.CLOSED, regionManager.getRegionState("r1"));
            assertNull(regionManager.getRegionStorage("r1"));
            assertNull(regionManager.getRegion("r1"));
        }

        @Test
        @DisplayName("closeRegion flushes storage when abort is false")
        void testCloseRegionFlushes() {
            FakeStorageEngine engine = new FakeStorageEngine();
            RegionStorage storage = new RegionStorage("r1", engine);
            regionManager.registerOpenedRegion(makeRegion("r1"), storage);

            regionManager.closeRegion("r1", false);

            assertTrue(engine.flushed, "closeRegion(abort=false) should flush storage");
        }

        @Test
        @DisplayName("closeRegion skips flush when abort is true")
        void testCloseRegionAbortSkipsFlush() {
            FakeStorageEngine engine = new FakeStorageEngine();
            RegionStorage storage = new RegionStorage("r1", engine);
            regionManager.registerOpenedRegion(makeRegion("r1"), storage);

            regionManager.closeRegion("r1", true);

            assertFalse(engine.flushed, "closeRegion(abort=true) should NOT flush storage");
        }

        @Test
        @DisplayName("closeRegion with dropTable=true drops storage data")
        void testCloseRegionDropTable() {
            FakeStorageEngine engine = new FakeStorageEngine();
            RegionStorage storage = new RegionStorage("r1", engine);
            regionManager.registerOpenedRegion(makeRegion("r1"), storage);

            regionManager.closeRegion("r1", false, true);

            assertTrue(engine.dropped, "closeRegion with dropTable=true should drop data");
        }

        @Test
        @DisplayName("closeRegion with dropTable=false does not drop data")
        void testCloseRegionNoDrop() {
            FakeStorageEngine engine = new FakeStorageEngine();
            RegionStorage storage = new RegionStorage("r1", engine);
            regionManager.registerOpenedRegion(makeRegion("r1"), storage);

            regionManager.closeRegion("r1", false, false);

            assertFalse(engine.dropped);
        }

        @Test
        @DisplayName("closeRegion is a no-op for unknown/CLOSED regions")
        void testCloseRegionAlreadyClosedOrUnknown() {
            // Unknown region -- should not throw
            assertDoesNotThrow(() -> regionManager.closeRegion("unknown", false));

            // CLOSED region -- should not throw
            registerOpen("r1");
            regionManager.closeRegion("r1", false);
            assertDoesNotThrow(() -> regionManager.closeRegion("r1", false));
        }

        @Test
        @DisplayName("closeRegion removes primary status and write-block flag")
        void testCloseRegionCleansUpMetadata() {
            registerOpen("r1");
            regionManager.promoteToPrimary("r1");
            regionManager.blockWrites("r1");

            regionManager.closeRegion("r1", false);

            assertFalse(regionManager.isPrimary("r1"));
            assertFalse(regionManager.isWriteBlocked("r1"));
        }
    }

    // ==================== promoteToPrimary / demoteToReplica ====================

    @Nested
    @DisplayName("promoteToPrimary / demoteToReplica")
    class PrimaryReplicaTests {

        @Test
        @DisplayName("promoteToPrimary sets isPrimary to true")
        void testPromoteToPrimary() {
            registerOpen("r1");
            regionManager.demoteToReplica("r1");
            assertFalse(regionManager.isPrimary("r1"));

            regionManager.promoteToPrimary("r1");
            assertTrue(regionManager.isPrimary("r1"));
        }

        @Test
        @DisplayName("demoteToReplica sets isPrimary to false")
        void testDemoteToReplica() {
            registerOpen("r1");
            regionManager.promoteToPrimary("r1");
            assertTrue(regionManager.isPrimary("r1"));

            regionManager.demoteToReplica("r1");
            assertFalse(regionManager.isPrimary("r1"));
        }

        @Test
        @DisplayName("double promote keeps isPrimary true")
        void testDoublePromote() {
            registerOpen("r1");
            regionManager.promoteToPrimary("r1");
            regionManager.promoteToPrimary("r1");
            assertTrue(regionManager.isPrimary("r1"));
        }

        @Test
        @DisplayName("isPrimary returns false for unknown regionId")
        void testIsPrimaryUnknown() {
            assertFalse(regionManager.isPrimary("nonexistent"));
        }

        @Test
        @DisplayName("registerOpenedRegion detects primary when region primary matches serverId")
        void testRegisterOpenedRegionPrimaryOnThisServer() {
            Region region = makeRegion("r1");
            region.setPrimary(fakeServer.getServerId());
            RegionStorage storage = new RegionStorage("r1", new FakeStorageEngine());

            regionManager.registerOpenedRegion(region, storage);

            assertTrue(regionManager.isPrimary("r1"));
        }

        @Test
        @DisplayName("registerOpenedRegion detects replica when region primary is different server")
        void testRegisterOpenedRegionReplicaOnThisServer() {
            Region region = makeRegion("r1");
            region.setPrimary(new ServerId("other-host", 9999));
            RegionStorage storage = new RegionStorage("r1", new FakeStorageEngine());

            regionManager.registerOpenedRegion(region, storage);

            assertFalse(regionManager.isPrimary("r1"));
        }

        @Test
        @DisplayName("registerOpenedRegion treats null primary as primary on this server")
        void testRegisterOpenedRegionNullPrimary() {
            Region region = makeRegion("r1");
            // primary is null by default
            assertNull(region.getPrimary());
            RegionStorage storage = new RegionStorage("r1", new FakeStorageEngine());

            regionManager.registerOpenedRegion(region, storage);

            assertTrue(regionManager.isPrimary("r1"),
                    "Null primary should default to primary on this server");
        }
    }

    // ==================== verifyFencingToken / updateFencingToken ====================

    @Nested
    @DisplayName("verifyFencingToken / updateFencingToken")
    class FencingTokenTests {

        @Test
        @DisplayName("default fencing token is 0")
        void testDefaultFencingToken() {
            assertEquals(0L, regionManager.getFencingToken("unknown"));
        }

        @Test
        @DisplayName("updateFencingToken sets a new token")
        void testUpdateFencingToken() {
            registerOpen("r1");
            regionManager.updateFencingToken("r1", 42L);
            assertEquals(42L, regionManager.getFencingToken("r1"));
        }

        @Test
        @DisplayName("verifyFencingToken returns true when token >= current")
        void testVerifyFencingTokenGreaterOrEqual() {
            registerOpen("r1");
            regionManager.updateFencingToken("r1", 10L);

            assertTrue(regionManager.verifyFencingToken("r1", 10L));
            assertTrue(regionManager.verifyFencingToken("r1", 11L));
            assertFalse(regionManager.verifyFencingToken("r1", 9L));
        }

        @Test
        @DisplayName("verifyFencingToken returns true for token 0 when no token set")
        void testVerifyFencingTokenNoTokenSet() {
            assertTrue(regionManager.verifyFencingToken("unknown", 0L));
            assertTrue(regionManager.verifyFencingToken("unknown", 1L));
        }

        @Test
        @DisplayName("updateFencingToken overwrites previous token")
        void testUpdateFencingTokenOverwrites() {
            registerOpen("r1");
            regionManager.updateFencingToken("r1", 5L);
            regionManager.updateFencingToken("r1", 100L);
            assertEquals(100L, regionManager.getFencingToken("r1"));
        }
    }

    // ==================== blockWrites / unblockWrites / isWriteBlocked ====================

    @Nested
    @DisplayName("blockWrites / unblockWrites / isWriteBlocked")
    class WriteBlockTests {

        @Test
        @DisplayName("writes are not blocked by default")
        void testDefaultNotBlocked() {
            assertFalse(regionManager.isWriteBlocked("r1"));
        }

        @Test
        @DisplayName("blockWrites blocks and unblockWrites unblocks")
        void testBlockAndUnblock() {
            registerOpen("r1");

            regionManager.blockWrites("r1");
            assertTrue(regionManager.isWriteBlocked("r1"));

            regionManager.unblockWrites("r1");
            assertFalse(regionManager.isWriteBlocked("r1"));
        }

        @Test
        @DisplayName("blockWrites is idempotent")
        void testBlockWritesIdempotent() {
            registerOpen("r1");
            regionManager.blockWrites("r1");
            regionManager.blockWrites("r1");
            assertTrue(regionManager.isWriteBlocked("r1"));

            regionManager.unblockWrites("r1");
            assertFalse(regionManager.isWriteBlocked("r1"));
        }

        @Test
        @DisplayName("registerOpenedRegion resets write block to false")
        void testRegisterResetsWriteBlock() {
            registerOpen("r1");
            regionManager.blockWrites("r1");
            assertTrue(regionManager.isWriteBlocked("r1"));

            // re-register as if opened again
            RegionStorage freshStorage = new RegionStorage("r1", new FakeStorageEngine());
            regionManager.registerOpenedRegion(makeRegion("r1"), freshStorage);

            assertFalse(regionManager.isWriteBlocked("r1"),
                    "registerOpenedRegion should reset write-block flag");
        }
    }

    // ==================== updateLastAppliedReplicationSequenceId ====================

    @Nested
    @DisplayName("updateLastAppliedReplicationSequenceId")
    class ReplicationSequenceIdTests {

        @Test
        @DisplayName("default last applied sequence ID is 0")
        void testDefaultSequenceId() {
            assertEquals(0L, regionManager.getLastAppliedReplicationSequenceId("unknown"));
        }

        @Test
        @DisplayName("updateLastAppliedReplicationSequenceId sets the value")
        void testUpdateSequenceId() {
            registerOpen("r1");
            regionManager.updateLastAppliedReplicationSequenceId("r1", 100L);
            assertEquals(100L, regionManager.getLastAppliedReplicationSequenceId("r1"));
        }

        @Test
        @DisplayName("updateLastAppliedReplicationSequenceId only increases, never decreases")
        void testSequenceIdMonotonic() {
            registerOpen("r1");
            regionManager.updateLastAppliedReplicationSequenceId("r1", 50L);
            regionManager.updateLastAppliedReplicationSequenceId("r1", 30L);
            assertEquals(50L, regionManager.getLastAppliedReplicationSequenceId("r1"),
                    "Should keep the higher value");

            regionManager.updateLastAppliedReplicationSequenceId("r1", 100L);
            assertEquals(100L, regionManager.getLastAppliedReplicationSequenceId("r1"));
        }

        @Test
        @DisplayName("updateLastAppliedReplicationSequenceId for unknown region initializes to 0 then updates")
        void testSequenceIdUnknownRegion() {
            regionManager.updateLastAppliedReplicationSequenceId("fresh", 5L);
            assertEquals(5L, regionManager.getLastAppliedReplicationSequenceId("fresh"));
        }
    }

    // ==================== getManagedRegions / getAllRegions ====================

    @Nested
    @DisplayName("getManagedRegions / getAllRegions")
    class ManagedRegionsTests {

        @Test
        @DisplayName("getAllRegions returns empty collection when no regions registered")
        void testGetAllRegionsEmpty() {
            assertTrue(regionManager.getAllRegions().isEmpty());
        }

        @Test
        @DisplayName("getAllRegions returns all registered regions")
        void testGetAllRegions() {
            registerOpen("r1");
            registerOpen("r2");
            registerOpen("r3");

            assertEquals(3, regionManager.getAllRegions().size());
        }

        @Test
        @DisplayName("getRegion returns the correct region metadata")
        void testGetRegion() {
            Region region = makeRegion("r-specific");
            RegionStorage storage = new RegionStorage("r-specific", new FakeStorageEngine());
            regionManager.registerOpenedRegion(region, storage);

            Region fetched = regionManager.getRegion("r-specific");
            assertNotNull(fetched);
            assertEquals("r-specific", fetched.getRegionId());
            assertEquals("test_table", fetched.getTableName());
        }

        @Test
        @DisplayName("getRegion returns null for unknown region")
        void testGetRegionUnknown() {
            assertNull(regionManager.getRegion("nonexistent"));
        }

        @Test
        @DisplayName("getRegionState returns null for unknown region")
        void testGetRegionStateUnknown() {
            assertNull(regionManager.getRegionState("nonexistent"));
        }

        @Test
        @DisplayName("isRegionOpen returns false for unknown region")
        void testIsRegionOpenUnknown() {
            assertFalse(regionManager.isRegionOpen("nonexistent"));
        }

        @Test
        @DisplayName("getRegionStorage returns null for unknown region")
        void testGetRegionStorageUnknown() {
            assertNull(regionManager.getRegionStorage("nonexistent"));
        }
    }

    // ==================== registerRegionInternal / setRegionState ====================

    @Nested
    @DisplayName("registerRegionInternal / setRegionState")
    class InternalRegistrationTests {

        @Test
        @DisplayName("registerRegionInternal adds region metadata without storage")
        void testRegisterRegionInternal() {
            Region region = makeRegion("r-internal");
            regionManager.registerRegionInternal(region);

            assertEquals(region, regionManager.getRegion("r-internal"));
            assertNull(regionManager.getRegionStorage("r-internal"),
                    "registerRegionInternal should not create storage");
        }

        @Test
        @DisplayName("setRegionState updates the state directly")
        void testSetRegionState() {
            Region region = makeRegion("r-state");
            regionManager.registerRegionInternal(region);

            regionManager.setRegionState("r-state", RegionManager.RegionState.OPENING);
            assertEquals(RegionManager.RegionState.OPENING, regionManager.getRegionState("r-state"));

            regionManager.setRegionState("r-state", RegionManager.RegionState.OPEN);
            assertEquals(RegionManager.RegionState.OPEN, regionManager.getRegionState("r-state"));
        }
    }

    // ==================== registerRegionStorage ====================

    @Nested
    @DisplayName("registerRegionStorage")
    class RegisterRegionStorageTests {

        @Test
        @DisplayName("registerRegionStorage associates storage with a regionId")
        void testRegisterRegionStorage() {
            RegionStorage storage = new RegionStorage("r1", new FakeStorageEngine());
            regionManager.registerRegionStorage("r1", storage);

            assertSame(storage, regionManager.getRegionStorage("r1"));
        }

        @Test
        @DisplayName("registerRegionStorage can overwrite existing storage")
        void testRegisterRegionStorageOverwrite() {
            RegionStorage first = new RegionStorage("r1", new FakeStorageEngine());
            RegionStorage second = new RegionStorage("r1", new FakeStorageEngine());
            regionManager.registerRegionStorage("r1", first);
            regionManager.registerRegionStorage("r1", second);

            assertSame(second, regionManager.getRegionStorage("r1"));
        }
    }

    // ==================== flushRegion / compactRegion ====================

    @Nested
    @DisplayName("flushRegion / compactRegion")
    class FlushCompactTests {

        @Test
        @DisplayName("flushRegion delegates to storage flush")
        void testFlushRegion() throws IOException {
            FakeStorageEngine engine = new FakeStorageEngine();
            RegionStorage storage = new RegionStorage("r1", engine);
            regionManager.registerRegionStorage("r1", storage);

            regionManager.flushRegion("r1");

            assertTrue(engine.flushed);
        }

        @Test
        @DisplayName("flushRegion on unknown region does not throw")
        void testFlushRegionUnknown() {
            assertDoesNotThrow(() -> regionManager.flushRegion("unknown"));
        }

        @Test
        @DisplayName("compactRegion delegates to storage compact")
        void testCompactRegion() throws IOException {
            FakeStorageEngine engine = new FakeStorageEngine();
            RegionStorage storage = new RegionStorage("r1", engine);
            regionManager.registerRegionStorage("r1", storage);

            regionManager.compactRegion("r1", true);

            assertTrue(engine.compacted);
        }

        @Test
        @DisplayName("compactRegion on unknown region does not throw")
        void testCompactRegionUnknown() {
            assertDoesNotThrow(() -> regionManager.compactRegion("unknown", true));
        }
    }

    // ==================== createRegionStorage ====================

    @Nested
    @DisplayName("createRegionStorage")
    class CreateRegionStorageTests {

        @Test
        @DisplayName("createRegionStorage uses the factory and returns a RegionStorage")
        void testCreateRegionStorage() {
            RegionStorage storage = regionManager.createRegionStorage("r-new");

            assertNotNull(storage);
            assertEquals("r-new", storage.getRegionId());
            assertNotNull(factory.lastCreated);
        }
    }
}
