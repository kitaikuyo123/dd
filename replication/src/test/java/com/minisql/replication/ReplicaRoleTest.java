package com.minisql.replication;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicaRole tests")
class ReplicaRoleTest {

    @Test
    @DisplayName("enum has exactly four roles")
    void testEnumValues() {
        ReplicaRole[] roles = ReplicaRole.values();
        assertEquals(4, roles.length);
        assertArrayEquals(
            new ReplicaRole[]{ReplicaRole.PRIMARY, ReplicaRole.SECONDARY, ReplicaRole.CANDIDATE, ReplicaRole.OBSERVER},
            roles
        );
    }

    @Test
    @DisplayName("valueOf returns the correct constant for each name")
    void testValueOf() {
        assertEquals(ReplicaRole.PRIMARY, ReplicaRole.valueOf("PRIMARY"));
        assertEquals(ReplicaRole.SECONDARY, ReplicaRole.valueOf("SECONDARY"));
        assertEquals(ReplicaRole.CANDIDATE, ReplicaRole.valueOf("CANDIDATE"));
        assertEquals(ReplicaRole.OBSERVER, ReplicaRole.valueOf("OBSERVER"));
    }

    @Test
    @DisplayName("valueOf throws for invalid name")
    void testValueOfInvalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> ReplicaRole.valueOf("UNKNOWN"));
    }

    @Test
    @DisplayName("only PRIMARY can write")
    void testCanWrite() {
        assertTrue(ReplicaRole.PRIMARY.canWrite());
        assertFalse(ReplicaRole.SECONDARY.canWrite());
        assertFalse(ReplicaRole.CANDIDATE.canWrite());
        assertFalse(ReplicaRole.OBSERVER.canWrite());
    }

    @Test
    @DisplayName("PRIMARY and SECONDARY can read; CANDIDATE and OBSERVER cannot")
    void testCanRead() {
        assertTrue(ReplicaRole.PRIMARY.canRead());
        assertTrue(ReplicaRole.SECONDARY.canRead());
        assertFalse(ReplicaRole.CANDIDATE.canRead());
        assertFalse(ReplicaRole.OBSERVER.canRead());
    }

    @Test
    @DisplayName("SECONDARY and CANDIDATE can be promoted; PRIMARY and OBSERVER cannot")
    void testCanBePromoted() {
        assertFalse(ReplicaRole.PRIMARY.canBePromoted());
        assertTrue(ReplicaRole.SECONDARY.canBePromoted());
        assertTrue(ReplicaRole.CANDIDATE.canBePromoted());
        assertFalse(ReplicaRole.OBSERVER.canBePromoted());
    }

    @Test
    @DisplayName("PRIMARY and SECONDARY participate in quorum; others do not")
    void testParticipatesInQuorum() {
        assertTrue(ReplicaRole.PRIMARY.participatesInQuorum());
        assertTrue(ReplicaRole.SECONDARY.participatesInQuorum());
        assertFalse(ReplicaRole.CANDIDATE.participatesInQuorum());
        assertFalse(ReplicaRole.OBSERVER.participatesInQuorum());
    }

    @ParameterizedTest(name = "{0} canWrite={1} canRead={2} canBePromoted={3} quorum={4}")
    @DisplayName("all role behaviours verified in one parameterised sweep")
    @CsvSource({
        "PRIMARY,   true,  true,  false, true",
        "SECONDARY, false, true,  true,  true",
        "CANDIDATE, false, false, true,  false",
        "OBSERVER,  false, false, false, false"
    })
    void testAllBehaviours(ReplicaRole role, boolean canWrite, boolean canRead, boolean canBePromoted, boolean quorum) {
        assertEquals(canWrite, role.canWrite());
        assertEquals(canRead, role.canRead());
        assertEquals(canBePromoted, role.canBePromoted());
        assertEquals(quorum, role.participatesInQuorum());
    }

    @Test
    @DisplayName("each enum constant has a human-readable toString")
    void testToString() {
        for (ReplicaRole role : ReplicaRole.values()) {
            assertNotNull(role.toString());
            assertFalse(role.toString().isEmpty());
        }
    }
}
