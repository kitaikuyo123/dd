package com.minisql.replication;

import com.minisql.common.model.ServerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicaGroup tests")
class ReplicaGroupTest {

    private static ServerId server(String host, int port) {
        return new ServerId(host, port);
    }

    private ReplicaGroup group;

    @BeforeEach
    void setUp() {
        group = new ReplicaGroup("region-test");
    }

    @Nested
    @DisplayName("Construction and basic getters")
    class Construction {

        @Test
        @DisplayName("regionId is stored and returned")
        void testRegionId() {
            assertEquals("region-test", group.getRegionId());
        }

        @Test
        @DisplayName("new group has no primary")
        void testNoPrimaryInitially() {
            assertNull(group.getPrimary());
        }

        @Test
        @DisplayName("new group has empty replica list")
        void testEmptyReplicas() {
            assertTrue(group.getReplicas().isEmpty());
        }

        @Test
        @DisplayName("new group has empty role map")
        void testEmptyRoles() {
            assertTrue(group.getReplicaRoles().isEmpty());
        }
    }

    @Nested
    @DisplayName("setPrimary")
    class SetPrimary {

        @Test
        @DisplayName("setting primary stores it and adds it as a replica with PRIMARY role")
        void testSetPrimary() {
            ServerId s1 = server("host1", 16020);
            group.setPrimary(s1);

            assertEquals(s1, group.getPrimary());
            assertTrue(group.getReplicas().contains(s1));
            assertEquals(ReplicaRole.PRIMARY, group.getReplicaRole(s1));
        }

        @Test
        @DisplayName("changing primary demotes old primary to SECONDARY")
        void testPrimaryChangeDemotesOld() {
            ServerId oldPrimary = server("old", 16020);
            ServerId newPrimary = server("new", 16021);
            group.setPrimary(oldPrimary);
            group.setPrimary(newPrimary);

            assertEquals(newPrimary, group.getPrimary());
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(oldPrimary));
            assertEquals(ReplicaRole.PRIMARY, group.getReplicaRole(newPrimary));
        }

        @Test
        @DisplayName("setting primary to null clears primary but keeps replicas")
        void testSetPrimaryNull() {
            ServerId s1 = server("host1", 16020);
            group.setPrimary(s1);
            group.setPrimary(null);

            assertNull(group.getPrimary());
            assertTrue(group.getReplicas().contains(s1));
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(s1));
        }

        @Test
        @DisplayName("setting the same primary again does not demote it")
        void testSetSamePrimaryTwice() {
            ServerId s1 = server("host1", 16020);
            group.setPrimary(s1);
            group.setPrimary(s1);

            assertEquals(s1, group.getPrimary());
            assertEquals(ReplicaRole.PRIMARY, group.getReplicaRole(s1));
        }

        @Test
        @DisplayName("setting first primary when previous is null does not throw")
        void testFirstPrimaryNoPrevious() {
            ServerId s1 = server("host1", 16020);
            assertDoesNotThrow(() -> group.setPrimary(s1));
            assertEquals(s1, group.getPrimary());
        }
    }

    @Nested
    @DisplayName("addReplica")
    class AddReplica {

        @Test
        @DisplayName("addReplica with default role adds as SECONDARY")
        void testAddDefaultRole() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1);

            assertEquals(1, group.getReplicas().size());
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(s1));
        }

        @Test
        @DisplayName("addReplica with explicit role stores the given role")
        void testAddExplicitRole() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1, ReplicaRole.OBSERVER);

            assertEquals(ReplicaRole.OBSERVER, group.getReplicaRole(s1));
        }

        @Test
        @DisplayName("adding the same replica twice does not duplicate it")
        void testAddDuplicateIgnored() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1);
            group.addReplica(s1);

            assertEquals(1, group.getReplicas().size());
        }

        @Test
        @DisplayName("re-adding a replica updates its role")
        void testReAddUpdatesRole() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1, ReplicaRole.SECONDARY);
            group.addReplica(s1, ReplicaRole.PRIMARY);

            assertEquals(ReplicaRole.PRIMARY, group.getReplicaRole(s1));
        }

        @Test
        @DisplayName("addReplica creates a ReplicaState with non-zero lastUpdateTime")
        void testAddReplicaCreatesState() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1);

            ReplicaGroup.ReplicaState state = group.getReplicaState(s1);
            assertNotNull(state);
            assertTrue(state.getLastUpdateTime() > 0);
            assertEquals(ReplicaRole.SECONDARY, state.getRole());
        }

        @Test
        @DisplayName("re-adding a replica does not reset lastUpdateTime if already set")
        void testReAddDoesNotResetLastUpdateTime() throws InterruptedException {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1);
            long firstTime = group.getReplicaState(s1).getLastUpdateTime();

            Thread.sleep(10);
            group.addReplica(s1);

            assertEquals(firstTime, group.getReplicaState(s1).getLastUpdateTime());
        }

        @Test
        @DisplayName("multiple replicas are all tracked")
        void testMultipleReplicas() {
            ServerId s1 = server("host1", 16020);
            ServerId s2 = server("host2", 16021);
            ServerId s3 = server("host3", 16022);

            group.addReplica(s1);
            group.addReplica(s2);
            group.addReplica(s3);

            List<ServerId> replicas = group.getReplicas();
            assertEquals(3, replicas.size());
            assertTrue(replicas.contains(s1));
            assertTrue(replicas.contains(s2));
            assertTrue(replicas.contains(s3));
        }
    }

    @Nested
    @DisplayName("removeReplica")
    class RemoveReplica {

        @Test
        @DisplayName("removing a replica removes it from the list, states, and roles")
        void testRemoveReplica() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1);
            group.removeReplica(s1);

            assertFalse(group.getReplicas().contains(s1));
            assertNull(group.getReplicaState(s1));
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(s1)); // default
        }

        @Test
        @DisplayName("removing the current primary also clears the primary field")
        void testRemovePrimary() {
            ServerId s1 = server("host1", 16020);
            group.setPrimary(s1);
            group.removeReplica(s1);

            assertNull(group.getPrimary());
        }

        @Test
        @DisplayName("removing a non-existent replica does not throw")
        void testRemoveNonExistent() {
            ServerId s1 = server("host1", 16020);
            assertDoesNotThrow(() -> group.removeReplica(s1));
        }

        @Test
        @DisplayName("removing a secondary does not affect the primary")
        void testRemoveSecondaryKeepsPrimary() {
            ServerId primary = server("primary", 16020);
            ServerId secondary = server("secondary", 16021);
            group.setPrimary(primary);
            group.addReplica(secondary);

            group.removeReplica(secondary);

            assertEquals(primary, group.getPrimary());
            assertEquals(1, group.getReplicas().size());
        }
    }

    @Nested
    @DisplayName("updateReplicaState")
    class UpdateReplicaState {

        @Test
        @DisplayName("updates applied sequence ID and replication lag")
        void testUpdateState() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1);

            group.updateReplicaState(s1, 100L, 2048L);

            ReplicaGroup.ReplicaState state = group.getReplicaState(s1);
            assertNotNull(state);
            assertEquals(100L, state.getLastAppliedSequenceId());
            assertEquals(2048L, state.getReplicationLag());
        }

        @Test
        @DisplayName("sets replicationActive to true")
        void testSetsReplicationActive() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1);
            group.updateReplicaState(s1, 1L, 0L);

            assertTrue(group.getReplicaState(s1).isReplicationActive());
        }

        @Test
        @DisplayName("refreshes lastUpdateTime")
        void testRefreshesLastUpdateTime() throws InterruptedException {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1);
            long before = group.getReplicaState(s1).getLastUpdateTime();

            Thread.sleep(10);
            group.updateReplicaState(s1, 5L, 0L);

            assertTrue(group.getReplicaState(s1).getLastUpdateTime() >= before);
        }

        @Test
        @DisplayName("creates state on the fly for unknown replicas")
        void testUpdateStateCreatesForUnknown() {
            ServerId unknown = server("unknown", 9999);
            group.updateReplicaState(unknown, 10L, 50L);

            ReplicaGroup.ReplicaState state = group.getReplicaState(unknown);
            assertNotNull(state);
            assertEquals(10L, state.getLastAppliedSequenceId());
            assertEquals(50L, state.getReplicationLag());
        }

        @Test
        @DisplayName("preserves the replica's existing role after update")
        void testUpdateStatePreservesRole() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1, ReplicaRole.PRIMARY);
            group.updateReplicaState(s1, 10L, 0L);

            assertEquals(ReplicaRole.PRIMARY, group.getReplicaState(s1).getRole());
        }
    }

    @Nested
    @DisplayName("getReplicaRole")
    class GetReplicaRole {

        @Test
        @DisplayName("returns SECONDARY for unknown replica")
        void testUnknownReturnsSecondary() {
            ServerId unknown = server("unknown", 9999);
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(unknown));
        }

        @Test
        @DisplayName("returns the stored role for a known replica")
        void testKnownRole() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1, ReplicaRole.OBSERVER);
            assertEquals(ReplicaRole.OBSERVER, group.getReplicaRole(s1));
        }
    }

    @Nested
    @DisplayName("setReplicaRole")
    class SetReplicaRole {

        @Test
        @DisplayName("updates role on an existing replica")
        void testUpdateRole() {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1, ReplicaRole.SECONDARY);
            group.setReplicaRole(s1, ReplicaRole.OBSERVER);

            assertEquals(ReplicaRole.OBSERVER, group.getReplicaRole(s1));
            assertEquals(ReplicaRole.OBSERVER, group.getReplicaState(s1).getRole());
        }

        @Test
        @DisplayName("setting role to PRIMARY updates the group primary")
        void testSetRoleToPrimaryUpdatesPrimary() {
            ServerId s1 = server("host1", 16020);
            ServerId s2 = server("host2", 16021);
            group.setPrimary(s1);
            group.addReplica(s2);

            group.setReplicaRole(s2, ReplicaRole.PRIMARY);

            assertEquals(s2, group.getPrimary());
        }

        @Test
        @DisplayName("setting role on unknown replica auto-adds it")
        void testSetRoleOnUnknownAutoAdds() {
            ServerId unknown = server("new", 16030);
            group.setReplicaRole(unknown, ReplicaRole.CANDIDATE);

            assertTrue(group.getReplicas().contains(unknown));
            assertEquals(ReplicaRole.CANDIDATE, group.getReplicaRole(unknown));
        }

        @Test
        @DisplayName("setting role refreshes lastUpdateTime on state")
        void testSetRoleRefreshesLastUpdateTime() throws InterruptedException {
            ServerId s1 = server("host1", 16020);
            group.addReplica(s1);
            long before = group.getReplicaState(s1).getLastUpdateTime();

            Thread.sleep(10);
            group.setReplicaRole(s1, ReplicaRole.CANDIDATE);

            assertTrue(group.getReplicaState(s1).getLastUpdateTime() > before);
        }
    }

    @Nested
    @DisplayName("Immutable collection returns")
    class ImmutableReturns {

        @Test
        @DisplayName("getReplicas returns unmodifiable list")
        void testReplicasUnmodifiable() {
            group.addReplica(server("h1", 16020));
            assertThrows(UnsupportedOperationException.class, () ->
                group.getReplicas().add(server("h2", 16021))
            );
        }

        @Test
        @DisplayName("getReplicaRoles returns unmodifiable map")
        void testRolesUnmodifiable() {
            group.addReplica(server("h1", 16020));
            assertThrows(UnsupportedOperationException.class, () ->
                group.getReplicaRoles().put(server("h2", 16021), ReplicaRole.PRIMARY)
            );
        }
    }

    @Nested
    @DisplayName("ReplicaState inner class")
    class ReplicaStateTests {

        @Test
        @DisplayName("new ReplicaState has sensible defaults")
        void testDefaults() {
            ReplicaGroup.ReplicaState state = new ReplicaGroup.ReplicaState();

            assertEquals(0L, state.getLastAppliedSequenceId());
            assertEquals(0L, state.getReplicationLag());
            assertEquals(0L, state.getLastUpdateTime());
            assertFalse(state.isReplicationActive());
            assertEquals(ReplicaRole.SECONDARY, state.getRole());
        }

        @Test
        @DisplayName("all setters and getters round-trip")
        void testSettersAndGetters() {
            ReplicaGroup.ReplicaState state = new ReplicaGroup.ReplicaState();

            state.setLastAppliedSequenceId(42L);
            assertEquals(42L, state.getLastAppliedSequenceId());

            state.setReplicationLag(1024L);
            assertEquals(1024L, state.getReplicationLag());

            state.setLastUpdateTime(9999L);
            assertEquals(9999L, state.getLastUpdateTime());

            state.setReplicationActive(true);
            assertTrue(state.isReplicationActive());

            state.setRole(ReplicaRole.PRIMARY);
            assertEquals(ReplicaRole.PRIMARY, state.getRole());
        }
    }
}
