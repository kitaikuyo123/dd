package com.minisql.master.detect;

import com.minisql.master.rebalance.HotSpotCoordinator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Detector adapter for hotspot monitoring.
 */
public class HotSpotDetector implements ClusterDetector {

    private final HotSpotCoordinator hotSpotCoordinator;
    private final long intervalMs;
    private ScheduledExecutorService scheduler;
    private ClusterEventSink eventSink = event -> { };

    public HotSpotDetector(HotSpotCoordinator hotSpotCoordinator) {
        this(hotSpotCoordinator, 10_000L);
    }

    public HotSpotDetector(HotSpotCoordinator hotSpotCoordinator, long intervalMs) {
        this.hotSpotCoordinator = hotSpotCoordinator;
        this.intervalMs = Math.max(1000L, intervalMs);
    }

    @Override
    public String getDetectorName() {
        return "hotSpotDetector";
    }

    @Override
    public void start() {
        if (scheduler != null && !scheduler.isShutdown()) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HotSpotDetector");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::publishActions, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    @Override
    public void setEventSink(ClusterEventSink eventSink) {
        this.eventSink = eventSink != null ? eventSink : event -> { };
    }

    private void publishActions() {
        List<HotSpotCoordinator.HotSpotAction> actions = hotSpotCoordinator.planPendingActions();
        for (HotSpotCoordinator.HotSpotAction action : actions) {
            eventSink.publish(new HotSpotActionEvent(action));
        }
    }
}
