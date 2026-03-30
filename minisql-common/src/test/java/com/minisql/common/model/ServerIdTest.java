package com.minisql.common.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ServerId tests")
class ServerIdTest {

    @Test
    @DisplayName("constructors populate host port and start time")
    void testConstructors() {
        long startTime = 1234567890L;

        ServerId generated = new ServerId("localhost", 16020);
        ServerId explicit = new ServerId("localhost", 16020, startTime);

        assertEquals("localhost", generated.getHost());
        assertEquals(16020, generated.getPort());
        assertTrue(generated.getStartTime() > 0);

        assertEquals("localhost", explicit.getHost());
        assertEquals(16020, explicit.getPort());
        assertEquals(startTime, explicit.getStartTime());
    }

    @Test
    @DisplayName("stable identity uses host and port only")
    void testEqualsAndHashCodeIgnoreStartTime() {
        ServerId oldInstance = new ServerId("localhost", 16020, 1000L);
        ServerId newInstance = new ServerId("localhost", 16020, 2000L);
        ServerId differentPort = new ServerId("localhost", 16021, 1000L);
        ServerId differentHost = new ServerId("otherhost", 16020, 1000L);

        assertEquals(oldInstance, newInstance);
        assertEquals(oldInstance.hashCode(), newInstance.hashCode());
        assertNotEquals(oldInstance, differentPort);
        assertNotEquals(oldInstance, differentHost);
    }

    @Test
    @DisplayName("compareTo ignores start time for the same logical node")
    void testCompareToIgnoresStartTime() {
        ServerId oldInstance = new ServerId("localhost", 16020, 1000L);
        ServerId newInstance = new ServerId("localhost", 16020, 2000L);
        ServerId laterPort = new ServerId("localhost", 16021, 1000L);

        assertEquals(0, oldInstance.compareTo(newInstance));
        assertTrue(oldInstance.compareTo(laterPort) < 0);
    }

    @Test
    @DisplayName("server and instance names expose stable and runtime identity")
    void testServerAndInstanceNames() {
        ServerId serverId = new ServerId("localhost", 16020, 1234L);

        assertEquals("localhost:16020", serverId.getServerName());
        assertEquals("localhost:16020@1234", serverId.getInstanceName());
    }

    @Test
    @DisplayName("toString still includes start time for diagnostics")
    void testToStringIncludesStartTime() {
        ServerId serverId = new ServerId("localhost", 16020, 1234L);
        String text = serverId.toString();

        assertTrue(text.contains("localhost"));
        assertTrue(text.contains("16020"));
        assertTrue(text.contains("1234"));
    }
}
