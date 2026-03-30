package com.minisql.master.detect;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Central lifecycle hub for detector-style background components.
 */
public class ClusterEventCoordinator {

    private final List<ClusterDetector> detectors = new ArrayList<>();
    private final List<ClusterEventHandler<?>> handlers = new ArrayList<>();
    private volatile boolean running = false;

    public void registerDetector(ClusterDetector detector) {
        if (detector != null) {
            detectors.add(detector);
            detector.setEventSink(this::dispatch);
        }
    }

    public void registerHandler(ClusterEventHandler<?> handler) {
        if (handler != null) {
            handlers.add(handler);
        }
    }

    public <E extends ClusterEvent> void registerHandler(Class<E> eventType, Consumer<E> consumer) {
        if (eventType == null || consumer == null) {
            return;
        }
        registerHandler(new ClusterEventHandler<E>() {
            @Override
            public Class<E> eventType() {
                return eventType;
            }

            @Override
            public void handle(E event) {
                consumer.accept(event);
            }
        });
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        for (ClusterDetector detector : detectors) {
            detector.start();
        }
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        for (int i = detectors.size() - 1; i >= 0; i--) {
            detectors.get(i).stop();
        }
    }

    private void dispatch(ClusterEvent event) {
        for (ClusterEventHandler<?> handler : handlers) {
            if (handler.supports(event)) {
                handler.handleUnchecked(event);
            }
        }
    }
}
