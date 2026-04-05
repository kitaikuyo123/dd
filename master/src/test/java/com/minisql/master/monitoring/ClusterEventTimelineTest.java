package com.minisql.master.monitoring;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterEventTimelineTest {

    @Test
    void filtersAndLimitsEvents() {
        ClusterEventTimeline timeline = new ClusterEventTimeline();

        timeline.record("FAILOVER_TRIGGERED", "WARN", "r1", "t1", "s1", "s2", "failover", null);
        timeline.record("RECOVERY_COMPLETED", "INFO", "r1", "t1", "s2", null, "recovered", null);
        timeline.record("HOTSPOT_DETECTED", "INFO", "r2", "t2", "s3", "s4", "hot", null);

        assertEquals(2, timeline.query(Set.of("FAILOVER_TRIGGERED", "RECOVERY_COMPLETED"), 10).size());
        assertEquals(1, timeline.query(Set.of("HOTSPOT_DETECTED"), 1).size());
    }
}
