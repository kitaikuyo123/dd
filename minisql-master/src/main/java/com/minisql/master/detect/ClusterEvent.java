package com.minisql.master.detect;

/**
 * Marker interface for detector events routed by ClusterEventCoordinator.
 */
public interface ClusterEvent {

    String getEventType();

    long getOccurredAt();
}
