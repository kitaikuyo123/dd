package com.minisql.master;

import com.minisql.master.detect.ClusterEvent;
import com.minisql.master.detect.ClusterEventCoordinator;
import com.minisql.master.detect.ClusterEventHandler;
import com.minisql.master.detect.ClusterDetector;
import com.minisql.master.detect.ClusterEventSink;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ClusterEventCoordinator tests")
class ClusterEventCoordinatorTest {

    @Test
    @DisplayName("routes detector events to matching handlers")
    void routesDetectorEventsToMatchingHandlers() {
        ClusterEventCoordinator coordinator = new ClusterEventCoordinator();
        RecordingDetector detector = new RecordingDetector();
        RecordingHandler handler = new RecordingHandler();

        coordinator.registerHandler(handler);
        coordinator.registerDetector(detector);
        coordinator.start();

        assertTrue(detector.started);
        assertEquals("demo", handler.lastValue);

        coordinator.stop();
        assertTrue(detector.stopped);
    }

    private static final class RecordingDetector implements ClusterDetector {
        private ClusterEventSink sink = event -> { };
        private boolean started;
        private boolean stopped;

        @Override
        public String getDetectorName() {
            return "recordingDetector";
        }

        @Override
        public void start() {
            started = true;
            sink.publish(new DemoEvent("demo"));
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public void setEventSink(ClusterEventSink eventSink) {
            this.sink = eventSink;
        }
    }

    private static final class RecordingHandler implements ClusterEventHandler<DemoEvent> {
        private String lastValue;

        @Override
        public Class<DemoEvent> eventType() {
            return DemoEvent.class;
        }

        @Override
        public void handle(DemoEvent event) {
            this.lastValue = event.value;
        }
    }

    private static final class DemoEvent implements ClusterEvent {
        private final String value;

        private DemoEvent(String value) {
            this.value = value;
        }

        @Override
        public String getEventType() {
            return "demo";
        }

        @Override
        public long getOccurredAt() {
            return 0;
        }
    }
}
