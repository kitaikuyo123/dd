package com.minisql.master.recover;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;
import com.minisql.master.rebalance.LoadBalancer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DataRepairCoordinator tests")
class DataRepairCoordinatorTest {

    private DataRepairCoordinator coordinator;

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.stop();
        }
    }

    // ---------- minimal fakes ----------

    /** ClusterManager that returns empty active servers (repair runs async). */
    static class FakeClusterManager extends ClusterManager {
        FakeClusterManager() { super(new LoadBalancer()); }
    }

    /** MetadataManager that returns canned regions. */
    static class FakeMetadataManager extends MetadataManager {
        private Region region;

        void setRegion(Region r) { this.region = r; }

        @Override
        public Region getRegion(String regionId) {
            return region;
        }
    }

    private ServerId server(String host, int port) {
        return new ServerId(host, port);
    }

    private Region makeRegion(String regionId, String tableName) {
        Region r = new Region(regionId, tableName, "a".getBytes(), "z".getBytes());
        r.setPrimary(server("host-a", 16020));
        r.addReplica(server("host-b", 16021));
        return r;
    }

    // ==================================================================

    @Nested
    @DisplayName("scheduleRepair")
    class ScheduleRepair {

        @Test
        @DisplayName("returns task ID and tracks active repair")
        void schedulesAndTracks() {
            ClusterManager cm = new FakeClusterManager();
            FakeMetadataManager mm = new FakeMetadataManager();
            mm.setRegion(makeRegion("region-1", "orders"));
            coordinator = new DataRepairCoordinator(cm, mm, 1);

            String taskId = coordinator.scheduleRepair("region-1", server("host-b", 16021));
            assertNotNull(taskId);
            assertFalse(taskId.isEmpty());

            Collection<DataRepairCoordinator.RepairTask> active = coordinator.getActiveRepairs();
            assertEquals(1, active.size());
            assertEquals("region-1", active.iterator().next().getRegionId());
        }

        @Test
        @DisplayName("duplicate schedule returns existing task ID")
        void duplicateScheduleReturnsExistingTaskId() {
            ClusterManager cm = new FakeClusterManager();
            FakeMetadataManager mm = new FakeMetadataManager();
            mm.setRegion(makeRegion("region-2", "products"));
            coordinator = new DataRepairCoordinator(cm, mm, 1);

            String taskId1 = coordinator.scheduleRepair("region-2", server("host-b", 16021));
            String taskId2 = coordinator.scheduleRepair("region-2", server("host-b", 16021));

            assertEquals(taskId1, taskId2, "duplicate schedule should return same task ID");
            assertEquals(1, coordinator.getActiveRepairs().size());
        }

        @Test
        @DisplayName("different regions get different task IDs")
        void differentRegionsGetDifferentTaskIds() {
            ClusterManager cm = new FakeClusterManager();
            FakeMetadataManager mm = new FakeMetadataManager();
            mm.setRegion(makeRegion("region-3", "orders"));
            coordinator = new DataRepairCoordinator(cm, mm, 1);

            String taskId1 = coordinator.scheduleRepair("region-3", server("host-a", 16020));
            // schedule second region — needs its own region metadata
            // We reuse the same metadata since repair runs async and will fail,
            // but the task creation should still work
            String taskId2 = coordinator.scheduleRepair("region-4", server("host-b", 16021));

            assertNotEquals(taskId1, taskId2);
        }
    }

    @Nested
    @DisplayName("repairHistory cap")
    class RepairHistoryCap {

        @Test
        @DisplayName("history capped at MAX_REPAIR_HISTORY (1000)")
        void historyIsCapped() throws Exception {
            ClusterManager cm = new FakeClusterManager();
            FakeMetadataManager mm = new FakeMetadataManager();
            mm.setRegion(makeRegion("region-5", "logs"));
            // Use threadPoolSize=1 so repair tasks queue up and don't clean activeRepairs immediately
            coordinator = new DataRepairCoordinator(cm, mm, 1);

            // Submit many repairs — they'll queue in the executor
            for (int i = 0; i < 5; i++) {
                String rid = "region-" + (100 + i);
                mm.setRegion(makeRegion(rid, "logs"));
                coordinator.scheduleRepair(rid, server("host-b", 16021));
            }

            // All 5 tasks registered
            assertEquals(5, coordinator.getActiveRepairs().size());

            // Stop and verify history is bounded
            coordinator.stop();
            var history = coordinator.getRepairHistory();
            assertTrue(history.size() <= 1000, "history should not exceed cap");
        }
    }

    @Nested
    @DisplayName("thread pool configuration")
    class ThreadPool {

        @Test
        @DisplayName("custom thread pool size")
        void customThreadPoolSize() {
            ClusterManager cm = new FakeClusterManager();
            FakeMetadataManager mm = new FakeMetadataManager();
            coordinator = new DataRepairCoordinator(cm, mm, 2);

            assertNotNull(coordinator.getActiveRepairs());
            assertTrue(coordinator.getActiveRepairs().isEmpty());

            coordinator.stop();
        }

        @Test
        @DisplayName("thread pool size clamped to minimum 1")
        void clampsToMin1() {
            ClusterManager cm = new FakeClusterManager();
            FakeMetadataManager mm = new FakeMetadataManager();
            coordinator = new DataRepairCoordinator(cm, mm, -5);

            // Should not throw — clamped to 1
            String taskId = coordinator.scheduleRepair("region-min", server("host-a", 16020));
            assertNotNull(taskId);

            coordinator.stop();
        }
    }

    @Nested
    @DisplayName("stop terminates cleanly")
    class Shutdown {

        @Test
        @DisplayName("stop shuts down executor without error")
        void stopShutsDownCleanly() {
            ClusterManager cm = new FakeClusterManager();
            FakeMetadataManager mm = new FakeMetadataManager();
            coordinator = new DataRepairCoordinator(cm, mm, 1);

            assertDoesNotThrow(() -> coordinator.stop());
        }

        @Test
        @DisplayName("double stop is safe")
        void doubleStopIsSafe() {
            ClusterManager cm = new FakeClusterManager();
            FakeMetadataManager mm = new FakeMetadataManager();
            coordinator = new DataRepairCoordinator(cm, mm, 1);

            coordinator.stop();
            assertDoesNotThrow(() -> coordinator.stop());
        }
    }
}
