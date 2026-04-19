package com.minisql.replication;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicaGroupRegistry tests")
class ReplicaGroupRegistryTest {

    private static ServerId server(String host, int port) {
        return new ServerId(host, port);
    }

    private static Region region(String id) {
        return new Region(id, "test-table", new byte[]{0x00}, new byte[]{0x7F});
    }

    private ReplicaGroupRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ReplicaGroupRegistry();
    }

    @Nested
    @DisplayName("createReplicaGroup")
    class CreateReplicaGroup {

        @Test
        @DisplayName("creates a group with first server as PRIMARY and others as SECONDARY")
        void testCreateGroup() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            ServerId s3 = server("h3", 16022);
            Region r = region("r1");

            ReplicaGroup group = registry.createReplicaGroup(r, List.of(s1, s2, s3), 3);

            assertNotNull(group);
            assertEquals("r1", group.getRegionId());
            assertEquals(s1, group.getPrimary());
            assertEquals(ReplicaRole.PRIMARY, group.getReplicaRole(s1));
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(s2));
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(s3));
            assertEquals(3, group.getReplicas().size());
        }

        @Test
        @DisplayName("respects replicationFactor smaller than server list")
        void testReplicationFactorSmallerThanServers() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            ServerId s3 = server("h3", 16022);
            Region r = region("r2");

            ReplicaGroup group = registry.createReplicaGroup(r, List.of(s1, s2, s3), 2);

            assertEquals(2, group.getReplicas().size());
            assertTrue(group.getReplicas().contains(s1));
            assertTrue(group.getReplicas().contains(s2));
            assertFalse(group.getReplicas().contains(s3));
        }

        @Test
        @DisplayName("replicationFactor larger than server list uses all available servers")
        void testReplicationFactorLargerThanServers() {
            ServerId s1 = server("h1", 16020);
            Region r = region("r3");

            ReplicaGroup group = registry.createReplicaGroup(r, List.of(s1), 5);

            assertEquals(1, group.getReplicas().size());
            assertEquals(s1, group.getPrimary());
        }

        @Test
        @DisplayName("null server list throws IllegalArgumentException")
        void testNullServerList() {
            assertThrows(IllegalArgumentException.class, () ->
                registry.createReplicaGroup(region("r4"), null, 3)
            );
        }

        @Test
        @DisplayName("empty server list throws IllegalArgumentException")
        void testEmptyServerList() {
            assertThrows(IllegalArgumentException.class, () ->
                registry.createReplicaGroup(region("r5"), List.of(), 3)
            );
        }

        @Test
        @DisplayName("initializes replica states with sequence 0 and lag 0")
        void testInitialReplicaStates() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            ReplicaGroup group = registry.createReplicaGroup(region("r6"), List.of(s1, s2), 2);

            ReplicaGroup.ReplicaState state1 = group.getReplicaState(s1);
            ReplicaGroup.ReplicaState state2 = group.getReplicaState(s2);

            assertNotNull(state1);
            assertNotNull(state2);
            assertEquals(0L, state1.getLastAppliedSequenceId());
            assertEquals(0L, state1.getReplicationLag());
            assertEquals(0L, state2.getLastAppliedSequenceId());
            assertEquals(0L, state2.getReplicationLag());
        }

        @Test
        @DisplayName("overwrites an existing group with same regionId")
        void testOverwriteExistingGroup() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            Region r = region("r-dup");

            registry.createReplicaGroup(r, List.of(s1), 1);
            registry.createReplicaGroup(r, List.of(s2), 1);

            assertEquals(s2, registry.getReplicaGroup("r-dup").getPrimary());
            assertEquals(1, registry.getReplicaGroup("r-dup").getReplicas().size());
        }
    }

    @Nested
    @DisplayName("getReplicaGroup")
    class GetReplicaGroup {

        @Test
        @DisplayName("returns null for unknown regionId")
        void testUnknownReturnsNull() {
            assertNull(registry.getReplicaGroup("no-such-region"));
        }

        @Test
        @DisplayName("returns the created group for known regionId")
        void testKnownReturnsGroup() {
            ServerId s1 = server("h1", 16020);
            registry.createReplicaGroup(region("r-known"), List.of(s1), 1);

            assertNotNull(registry.getReplicaGroup("r-known"));
            assertEquals("r-known", registry.getReplicaGroup("r-known").getRegionId());
        }
    }

    @Nested
    @DisplayName("getAllReplicaGroups")
    class GetAllReplicaGroups {

        @Test
        @DisplayName("returns empty map when no groups created")
        void testEmpty() {
            assertTrue(registry.getAllReplicaGroups().isEmpty());
        }

        @Test
        @DisplayName("returns all created groups")
        void testAllGroups() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            registry.createReplicaGroup(region("ra"), List.of(s1), 1);
            registry.createReplicaGroup(region("rb"), List.of(s2), 1);

            Map<String, ReplicaGroup> all = registry.getAllReplicaGroups();
            assertEquals(2, all.size());
            assertTrue(all.containsKey("ra"));
            assertTrue(all.containsKey("rb"));
        }

        @Test
        @DisplayName("returns unmodifiable map")
        void testUnmodifiable() {
            ServerId s1 = server("h1", 16020);
            registry.createReplicaGroup(region("ra"), List.of(s1), 1);

            assertThrows(UnsupportedOperationException.class, () ->
                registry.getAllReplicaGroups().put("new", new ReplicaGroup("new"))
            );
        }
    }

    @Nested
    @DisplayName("removeReplicaGroup")
    class RemoveReplicaGroup {

        @Test
        @DisplayName("removes an existing group")
        void testRemoveExisting() {
            ServerId s1 = server("h1", 16020);
            registry.createReplicaGroup(region("to-remove"), List.of(s1), 1);

            registry.removeReplicaGroup("to-remove");
            assertNull(registry.getReplicaGroup("to-remove"));
        }

        @Test
        @DisplayName("removing a non-existent group does not throw")
        void testRemoveNonExistent() {
            assertDoesNotThrow(() -> registry.removeReplicaGroup("no-such-region"));
        }
    }

    @Nested
    @DisplayName("addReplica")
    class AddReplica {

        @Test
        @DisplayName("adds a replica to an existing group")
        void testAddReplica() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            registry.createReplicaGroup(region("r1"), List.of(s1), 1);

            registry.addReplica("r1", s2, ReplicaRole.SECONDARY);

            ReplicaGroup group = registry.getReplicaGroup("r1");
            assertTrue(group.getReplicas().contains(s2));
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(s2));
        }

        @Test
        @DisplayName("addReplica to unknown group throws IllegalArgumentException")
        void testAddReplicaUnknownGroup() {
            assertThrows(IllegalArgumentException.class, () ->
                registry.addReplica("no-group", server("h1", 16020), ReplicaRole.SECONDARY)
            );
        }
    }

    @Nested
    @DisplayName("removeReplica")
    class RemoveReplica {

        @Test
        @DisplayName("removes a replica from an existing group")
        void testRemoveReplica() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            registry.createReplicaGroup(region("r1"), List.of(s1, s2), 2);

            registry.removeReplica("r1", s2);

            assertFalse(registry.getReplicaGroup("r1").getReplicas().contains(s2));
        }

        @Test
        @DisplayName("removeReplica on unknown group does not throw")
        void testRemoveReplicaUnknownGroup() {
            assertDoesNotThrow(() ->
                registry.removeReplica("no-group", server("h1", 16020))
            );
        }

        @Test
        @DisplayName("removing the primary replica clears the primary field")
        void testRemovePrimaryReplica() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            registry.createReplicaGroup(region("r1"), List.of(s1, s2), 2);

            registry.removeReplica("r1", s1);
            assertNull(registry.getReplicaGroup("r1").getPrimary());
        }
    }

    @Nested
    @DisplayName("promoteToPrimary")
    class PromoteToPrimary {

        @Test
        @DisplayName("promotes a secondary to primary and demotes old primary")
        void testPromoteSecondary() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            ServerId s3 = server("h3", 16022);
            registry.createReplicaGroup(region("r1"), List.of(s1, s2, s3), 3);

            registry.promoteToPrimary("r1", s2);

            ReplicaGroup group = registry.getReplicaGroup("r1");
            assertEquals(s2, group.getPrimary());
            assertEquals(ReplicaRole.PRIMARY, group.getReplicaRole(s2));
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(s1));
            assertEquals(ReplicaRole.SECONDARY, group.getReplicaRole(s3));
        }

        @Test
        @DisplayName("promoting a server that is not in the replica list throws")
        void testPromoteNonReplicaThrows() {
            ServerId s1 = server("h1", 16020);
            ServerId outsider = server("outsider", 9999);
            registry.createReplicaGroup(region("r1"), List.of(s1), 1);

            assertThrows(IllegalArgumentException.class, () ->
                registry.promoteToPrimary("r1", outsider)
            );
        }

        @Test
        @DisplayName("promoteToPrimary on unknown group throws")
        void testPromoteUnknownGroupThrows() {
            assertThrows(IllegalArgumentException.class, () ->
                registry.promoteToPrimary("no-group", server("h1", 16020))
            );
        }
    }

    @Nested
    @DisplayName("updateReplicaProgress")
    class UpdateReplicaProgress {

        @Test
        @DisplayName("updates applied sequence ID and lag for a replica")
        void testUpdateProgress() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            registry.createReplicaGroup(region("r1"), List.of(s1, s2), 2);

            registry.updateReplicaProgress("r1", s2, 50L, 1024L);

            ReplicaGroup.ReplicaState state = registry.getReplicaGroup("r1").getReplicaState(s2);
            assertEquals(50L, state.getLastAppliedSequenceId());
            assertEquals(1024L, state.getReplicationLag());
        }

        @Test
        @DisplayName("updateReplicaProgress on unknown group throws")
        void testUpdateProgressUnknownGroupThrows() {
            assertThrows(IllegalArgumentException.class, () ->
                registry.updateReplicaProgress("no-group", server("h1", 16020), 1L, 0L)
            );
        }
    }

    @Nested
    @DisplayName("updateReplicaRole")
    class UpdateReplicaRole {

        @Test
        @DisplayName("updates the role of a replica in a group")
        void testUpdateRole() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            registry.createReplicaGroup(region("r1"), List.of(s1, s2), 2);

            registry.updateReplicaRole("r1", s2, ReplicaRole.CANDIDATE);

            assertEquals(ReplicaRole.CANDIDATE,
                registry.getReplicaGroup("r1").getReplicaRole(s2));
        }

        @Test
        @DisplayName("updateReplicaRole on unknown group throws")
        void testUpdateRoleUnknownGroupThrows() {
            assertThrows(IllegalArgumentException.class, () ->
                registry.updateReplicaRole("no-group", server("h1", 16020), ReplicaRole.PRIMARY)
            );
        }
    }

    @Nested
    @DisplayName("recordPrimaryProgress")
    class RecordPrimaryProgress {

        @Test
        @DisplayName("updates primary's sequence ID and ensures PRIMARY role")
        void testRecordPrimaryProgress() {
            ServerId s1 = server("h1", 16020);
            ServerId s2 = server("h2", 16021);
            registry.createReplicaGroup(region("r1"), List.of(s1, s2), 2);

            registry.recordPrimaryProgress("r1", 99L);

            ReplicaGroup group = registry.getReplicaGroup("r1");
            ReplicaGroup.ReplicaState state = group.getReplicaState(s1);
            assertEquals(99L, state.getLastAppliedSequenceId());
            assertEquals(0L, state.getReplicationLag());
            assertEquals(ReplicaRole.PRIMARY, state.getRole());
        }

        @Test
        @DisplayName("recordPrimaryProgress on unknown group throws")
        void testRecordPrimaryProgressUnknownGroupThrows() {
            assertThrows(IllegalArgumentException.class, () ->
                registry.recordPrimaryProgress("no-group", 1L)
            );
        }
    }
}
