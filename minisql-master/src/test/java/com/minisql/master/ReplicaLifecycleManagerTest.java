package com.minisql.master;

import com.minisql.common.model.ServerId;
import com.minisql.master.state.ReplicaLifecycleManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicaLifecycleManager tests")
class ReplicaLifecycleManagerTest {

    @Test
    @DisplayName("transition creates and updates lifecycle status")
    void testTransitionUpdatesStatus() {
        ReplicaLifecycleManager manager = new ReplicaLifecycleManager();
        ServerId replica = new ServerId("replica-host", 16020);

        manager.transition("region-1", replica,
            ReplicaLifecycleManager.ReplicaLifecycleState.BOOTSTRAPPING,
            "bootstrap started");
        ReplicaLifecycleManager.ReplicaLifecycleStatus initial = manager.getStatus("region-1", replica);

        assertNotNull(initial);
        assertEquals(ReplicaLifecycleManager.ReplicaLifecycleState.BOOTSTRAPPING, initial.getState());
        assertEquals("bootstrap started", initial.getDetail());

        manager.transition("region-1", replica,
            ReplicaLifecycleManager.ReplicaLifecycleState.SECONDARY_READY,
            "replica caught up");
        ReplicaLifecycleManager.ReplicaLifecycleStatus updated = manager.getStatus("region-1", replica);

        assertEquals(ReplicaLifecycleManager.ReplicaLifecycleState.SECONDARY_READY, updated.getState());
        assertEquals("replica caught up", updated.getDetail());
        assertTrue(updated.getUpdatedAt() >= initial.getUpdatedAt());
    }

    @Test
    @DisplayName("removeRegion clears all lifecycle entries for region")
    void testRemoveRegionClearsStatuses() {
        ReplicaLifecycleManager manager = new ReplicaLifecycleManager();
        ServerId replica1 = new ServerId("replica-host-1", 16020);
        ServerId replica2 = new ServerId("replica-host-2", 16021);

        manager.transition("region-1", replica1,
            ReplicaLifecycleManager.ReplicaLifecycleState.BOOTSTRAPPING, "boot");
        manager.transition("region-1", replica2,
            ReplicaLifecycleManager.ReplicaLifecycleState.SECONDARY_READY, "ready");

        manager.removeRegion("region-1");

        assertNull(manager.getStatus("region-1", replica1));
        assertNull(manager.getStatus("region-1", replica2));
        assertTrue(manager.getAllStatuses().isEmpty());
    }
}
