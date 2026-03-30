package com.minisql.master.detect;

@FunctionalInterface
public interface ClusterEventSink {

    void publish(ClusterEvent event);
}
