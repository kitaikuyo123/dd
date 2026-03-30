package com.minisql.master.detect;

public interface ClusterEventHandler<E extends ClusterEvent> {

    Class<E> eventType();

    void handle(E event);

    default boolean supports(ClusterEvent event) {
        return eventType().isInstance(event);
    }

    @SuppressWarnings("unchecked")
    default void handleUnchecked(ClusterEvent event) {
        handle((E) event);
    }
}
