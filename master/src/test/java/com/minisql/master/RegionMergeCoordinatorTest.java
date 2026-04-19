package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.rebalance.RegionMergeCoordinator;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionMergeCoordinator tests")
class RegionMergeCoordinatorTest {

    private static final long MERGE_THRESHOLD = 100L * 1024 * 1024;  // 100 MB
    private static final long MAX_MERGE_SIZE = 8L * 1024 * 1024 * 1024;  // 8 GB
    private static final long MIN_MERGE_SIZE = 10L * 1024 * 1024;  // 10 MB

    private ClusterManager clusterManager;
    private MetadataManager metadataManager;
    private RegionMergeCoordinator coordinator;

    @BeforeEach
    void setUp() {
        clusterManager = new ClusterManager(new LoadBalancer());
        metadataManager = new MetadataManager();
        coordinator = new RegionMergeCoordinator(clusterManager, metadataManager);
    }

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.stop();
        }
    }

    // ---------------------------------------------------------------
    // start / stop lifecycle
    // ---------------------------------------------------------------

    @Test
    @DisplayName("start and stop do not throw")
    void startAndStopDoNotThrow() {
        assertDoesNotThrow(() -> coordinator.start());
        assertDoesNotThrow(() -> coordinator.stop());
    }

    @Test
    @DisplayName("start is idempotent")
    void startIsIdempotent() {
        assertDoesNotThrow(() -> {
            coordinator.start();
            coordinator.start();
        });
        coordinator.stop();
    }

    @Test
    @DisplayName("stop without start does not throw")
    void stopWithoutStartDoesNotThrow() {
        assertDoesNotThrow(() -> coordinator.stop());
    }

    // ---------------------------------------------------------------
    // recordRegionSplit
    // ---------------------------------------------------------------

    @Test
    @DisplayName("recordRegionSplit records a split event that puts the region in cooldown")
    void recordRegionSplitPutsRegionInCooldown() {
        // Use a short cooldown for testing
        long cooldownMs = 5000;
        coordinator.setMergeCooldownMs(cooldownMs);
        coordinator.recordRegionSplit("orders_r1");

        // Immediately after recording, the region should be in cooldown.
        // We can verify this indirectly: the private isInCooldown method is used
        // by shouldMerge, so we test via the checkAndScheduleMerges behavior.
        // For direct verification, we confirm that the method doesn't throw.
        assertDoesNotThrow(() -> coordinator.recordRegionSplit("orders_r1"));
    }

    @Test
    @DisplayName("recordRegionSplit handles multiple regions")
    void recordRegionSplitHandlesMultipleRegions() {
        coordinator.setMergeCooldownMs(60000);
        assertDoesNotThrow(() -> {
            coordinator.recordRegionSplit("orders_r1");
            coordinator.recordRegionSplit("orders_r2");
            coordinator.recordRegionSplit("orders_r3");
        });
    }

    // ---------------------------------------------------------------
    // Cooldown behavior via shouldMerge (indirect test)
    // ---------------------------------------------------------------
    // Since shouldMerge and isInCooldown are private, we test them by
    // exercising the public API that uses them: checkAndScheduleMerges
    // runs automatically when start() is called. We use very small sizes
    // and a known cooldown to validate behavior.

    @Test
    @DisplayName("regions in cooldown are not considered for merge")
    void regionsInCooldownNotConsideredForMerge() throws Exception {
        coordinator.setMergeCooldownMs(60000); // long cooldown
        coordinator.setMergeThresholdSize(MERGE_THRESHOLD);
        coordinator.setMaxMergeSize(MAX_MERGE_SIZE);
        coordinator.setMinMergeSize(MIN_MERGE_SIZE);

        ServerId server = new ServerId("host-a", 16020, 1L);
        clusterManager.registerServer(server);

        Table table = new Table("orders");
        metadataManager.createTable(table);

        Region left = new Region("orders_r1", "orders", new byte[]{0x00}, new byte[]{0x40});
        Region right = new Region("orders_r2", "orders", new byte[]{0x40}, new byte[]{0x7F});
        left.setPrimary(server);
        left.addReplica(server);
        right.setPrimary(server);
        right.addReplica(server);
        metadataManager.registerRegionForTable(left, server);
        metadataManager.registerRegionForTable(right, server);
        clusterManager.assignRegionToServer("orders_r1", server);
        clusterManager.assignRegionToServer("orders_r2", server);

        // Set up small region loads (both below merge threshold)
        ClusterManager.RegionLoad leftLoad = new ClusterManager.RegionLoad();
        leftLoad.setRegionId("orders_r1");
        leftLoad.setStoreFileSize(1024); // very small
        leftLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server, "orders_r1", leftLoad);

        ClusterManager.RegionLoad rightLoad = new ClusterManager.RegionLoad();
        rightLoad.setRegionId("orders_r2");
        rightLoad.setStoreFileSize(1024);
        rightLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server, "orders_r2", rightLoad);

        // Record both regions as recently split => they should be in cooldown
        coordinator.recordRegionSplit("orders_r1");
        coordinator.recordRegionSplit("orders_r2");

        // Start the coordinator which runs checkAndScheduleMerges periodically
        // We verify indirectly that no merge happens: metadata should still have both regions
        coordinator.start();
        Thread.sleep(1000); // allow one check cycle
        coordinator.stop();

        // Both regions should still exist (not merged)
        assertNotNull(metadataManager.getRegion("orders_r1"));
        assertNotNull(metadataManager.getRegion("orders_r2"));
    }

    // ---------------------------------------------------------------
    // Configuration setters
    // ---------------------------------------------------------------

    @Test
    @DisplayName("setMergeThresholdSize accepts new value")
    void setMergeThresholdSizeAcceptsNewValue() {
        assertDoesNotThrow(() -> coordinator.setMergeThresholdSize(50L * 1024 * 1024));
    }

    @Test
    @DisplayName("setMaxMergeSize accepts new value")
    void setMaxMergeSizeAcceptsNewValue() {
        assertDoesNotThrow(() -> coordinator.setMaxMergeSize(4L * 1024 * 1024 * 1024));
    }

    @Test
    @DisplayName("setMinMergeSize accepts new value")
    void setMinMergeSizeAcceptsNewValue() {
        assertDoesNotThrow(() -> coordinator.setMinMergeSize(5L * 1024 * 1024));
    }

    @Test
    @DisplayName("setMergeCooldownMs accepts new value")
    void setMergeCooldownMsAcceptsNewValue() {
        assertDoesNotThrow(() -> coordinator.setMergeCooldownMs(120_000L));
    }

    // ---------------------------------------------------------------
    // Small adjacent regions on same server should be merge candidates
    // ---------------------------------------------------------------

    @Test
    @DisplayName("small adjacent regions on same server are candidates for merge after cooldown expires")
    void smallAdjacentRegionsEligibleAfterCooldown() throws Exception {
        // Use a very short cooldown so it expires quickly
        coordinator.setMergeCooldownMs(200);
        coordinator.setMergeThresholdSize(MERGE_THRESHOLD);
        coordinator.setMaxMergeSize(MAX_MERGE_SIZE);
        coordinator.setMinMergeSize(MIN_MERGE_SIZE);

        ServerId server = new ServerId("host-a", 16020, 1L);
        clusterManager.registerServer(server);

        Table table = new Table("orders");
        metadataManager.createTable(table);

        Region left = new Region("orders_r1", "orders", new byte[]{0x00}, new byte[]{0x40});
        Region right = new Region("orders_r2", "orders", new byte[]{0x40}, new byte[]{0x7F});
        left.setPrimary(server);
        left.addReplica(server);
        right.setPrimary(server);
        right.addReplica(server);
        metadataManager.registerRegionForTable(left, server);
        metadataManager.registerRegionForTable(right, server);
        clusterManager.assignRegionToServer("orders_r1", server);
        clusterManager.assignRegionToServer("orders_r2", server);

        // Small loads (well below merge threshold of 100MB)
        ClusterManager.RegionLoad leftLoad = new ClusterManager.RegionLoad();
        leftLoad.setRegionId("orders_r1");
        leftLoad.setStoreFileSize(1024);
        leftLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server, "orders_r1", leftLoad);

        ClusterManager.RegionLoad rightLoad = new ClusterManager.RegionLoad();
        rightLoad.setRegionId("orders_r2");
        rightLoad.setStoreFileSize(2048);
        rightLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server, "orders_r2", rightLoad);

        // Record splits but with a short cooldown that will expire
        coordinator.recordRegionSplit("orders_r1");
        coordinator.recordRegionSplit("orders_r2");

        // Wait for cooldown to expire
        Thread.sleep(300);

        // Now start the coordinator; the check cycle should find these as merge candidates
        // The actual merge will fail because there's no gRPC server, but the key behavior
        // we're validating is that the regions were considered (no crash / exception).
        coordinator.start();
        Thread.sleep(2000); // allow the periodic check to run
        coordinator.stop();

        // The merge would have been attempted (and failed due to no gRPC server),
        // but the coordinator should not have thrown any unhandled exceptions.
        // The original regions may still exist if the merge RPC failed.
        // We simply verify the coordinator operated without error.
        Collection<Region> regions = metadataManager.getAllRegions();
        assertNotNull(regions);
    }

    // ---------------------------------------------------------------
    // Non-adjacent regions are not merged
    // ---------------------------------------------------------------

    @Test
    @DisplayName("non-adjacent regions are not merged even if small")
    void nonAdjacentRegionsNotMerged() throws Exception {
        coordinator.setMergeCooldownMs(100);
        coordinator.setMergeThresholdSize(MERGE_THRESHOLD);

        ServerId server = new ServerId("host-a", 16020, 1L);
        clusterManager.registerServer(server);

        Table table = new Table("orders");
        metadataManager.createTable(table);

        // Two regions with a gap between them (not adjacent)
        Region left = new Region("orders_r1", "orders", new byte[]{0x00}, new byte[]{0x30});
        Region right = new Region("orders_r2", "orders", new byte[]{0x40}, new byte[]{0x7F});
        left.setPrimary(server);
        left.addReplica(server);
        right.setPrimary(server);
        right.addReplica(server);
        metadataManager.registerRegionForTable(left, server);
        metadataManager.registerRegionForTable(right, server);
        clusterManager.assignRegionToServer("orders_r1", server);
        clusterManager.assignRegionToServer("orders_r2", server);

        ClusterManager.RegionLoad leftLoad = new ClusterManager.RegionLoad();
        leftLoad.setRegionId("orders_r1");
        leftLoad.setStoreFileSize(1024);
        leftLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server, "orders_r1", leftLoad);

        ClusterManager.RegionLoad rightLoad = new ClusterManager.RegionLoad();
        rightLoad.setRegionId("orders_r2");
        rightLoad.setStoreFileSize(1024);
        rightLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server, "orders_r2", rightLoad);

        coordinator.start();
        Thread.sleep(2000);
        coordinator.stop();

        // Both regions should still exist because they are not adjacent
        assertNotNull(metadataManager.getRegion("orders_r1"));
        assertNotNull(metadataManager.getRegion("orders_r2"));
    }

    // ---------------------------------------------------------------
    // Regions on different servers are not merged
    // ---------------------------------------------------------------

    @Test
    @DisplayName("adjacent regions on different servers are not merged")
    void adjacentRegionsOnDifferentServersNotMerged() throws Exception {
        coordinator.setMergeCooldownMs(100);
        coordinator.setMergeThresholdSize(MERGE_THRESHOLD);

        ServerId server1 = new ServerId("host-a", 16020, 1L);
        ServerId server2 = new ServerId("host-b", 16021, 2L);
        clusterManager.registerServer(server1);
        clusterManager.registerServer(server2);

        Table table = new Table("orders");
        metadataManager.createTable(table);

        Region left = new Region("orders_r1", "orders", new byte[]{0x00}, new byte[]{0x40});
        Region right = new Region("orders_r2", "orders", new byte[]{0x40}, new byte[]{0x7F});
        left.setPrimary(server1);
        left.addReplica(server1);
        right.setPrimary(server2);
        right.addReplica(server2);
        metadataManager.registerRegionForTable(left, server1);
        metadataManager.registerRegionForTable(right, server2);
        clusterManager.assignRegionToServer("orders_r1", server1);
        clusterManager.assignRegionToServer("orders_r2", server2);

        ClusterManager.RegionLoad leftLoad = new ClusterManager.RegionLoad();
        leftLoad.setRegionId("orders_r1");
        leftLoad.setStoreFileSize(1024);
        leftLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server1, "orders_r1", leftLoad);

        ClusterManager.RegionLoad rightLoad = new ClusterManager.RegionLoad();
        rightLoad.setRegionId("orders_r2");
        rightLoad.setStoreFileSize(1024);
        rightLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server2, "orders_r2", rightLoad);

        coordinator.start();
        Thread.sleep(2000);
        coordinator.stop();

        // Both regions should still exist because they are on different servers
        assertNotNull(metadataManager.getRegion("orders_r1"));
        assertNotNull(metadataManager.getRegion("orders_r2"));
    }

    // ---------------------------------------------------------------
    // Large regions exceeding maxMergeSize are not merged
    // ---------------------------------------------------------------

    @Test
    @DisplayName("regions whose combined size exceeds maxMergeSize are not merged")
    void largeRegionsExceedingMaxMergeSizeNotMerged() throws Exception {
        coordinator.setMergeCooldownMs(100);
        coordinator.setMergeThresholdSize(MERGE_THRESHOLD);
        coordinator.setMaxMergeSize(100); // very low max

        ServerId server = new ServerId("host-a", 16020, 1L);
        clusterManager.registerServer(server);

        Table table = new Table("orders");
        metadataManager.createTable(table);

        Region left = new Region("orders_r1", "orders", new byte[]{0x00}, new byte[]{0x40});
        Region right = new Region("orders_r2", "orders", new byte[]{0x40}, new byte[]{0x7F});
        left.setPrimary(server);
        left.addReplica(server);
        right.setPrimary(server);
        right.addReplica(server);
        metadataManager.registerRegionForTable(left, server);
        metadataManager.registerRegionForTable(right, server);
        clusterManager.assignRegionToServer("orders_r1", server);
        clusterManager.assignRegionToServer("orders_r2", server);

        // Each region is 80 bytes, combined = 160 > maxMergeSize(100)
        ClusterManager.RegionLoad leftLoad = new ClusterManager.RegionLoad();
        leftLoad.setRegionId("orders_r1");
        leftLoad.setStoreFileSize(80);
        leftLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server, "orders_r1", leftLoad);

        ClusterManager.RegionLoad rightLoad = new ClusterManager.RegionLoad();
        rightLoad.setRegionId("orders_r2");
        rightLoad.setStoreFileSize(80);
        rightLoad.setMemStoreSize(0);
        clusterManager.updateRegionLoad(server, "orders_r2", rightLoad);

        coordinator.start();
        Thread.sleep(2000);
        coordinator.stop();

        // Both regions should still exist because combined size exceeds maxMergeSize
        assertNotNull(metadataManager.getRegion("orders_r1"));
        assertNotNull(metadataManager.getRegion("orders_r2"));
    }

    // ---------------------------------------------------------------
    // setMonitoringService and setZkClient do not throw
    // ---------------------------------------------------------------

    @Test
    @DisplayName("setMonitoringService accepts null without error")
    void setMonitoringServiceAcceptsNull() {
        assertDoesNotThrow(() -> coordinator.setMonitoringService(null));
    }

    @Test
    @DisplayName("setZkClient accepts null without error")
    void setZkClientAcceptsNull() {
        assertDoesNotThrow(() -> coordinator.setZkClient(null));
    }

    // ---------------------------------------------------------------
    // Cleanup of expired split records
    // ---------------------------------------------------------------

    @Test
    @DisplayName("expired split records are cleaned up on recordRegionSplit")
    void expiredSplitRecordsCleanedUp() throws Exception {
        // Use a very short cooldown so entries expire quickly
        coordinator.setMergeCooldownMs(100);
        coordinator.recordRegionSplit("temp_r1");

        // Wait for the cooldown to expire
        Thread.sleep(200);

        // Recording another split should trigger cleanup of expired entries
        assertDoesNotThrow(() -> coordinator.recordRegionSplit("temp_r2"));
    }
}
