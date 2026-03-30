package com.minisql.master.detect;

/**
 * Common lifecycle contract for detectors managed by ClusterEventCoordinator.
 */
public interface ClusterDetector {

    String getDetectorName();

    void start();

    void stop();

    default void setEventSink(ClusterEventSink eventSink) {
    }
}
