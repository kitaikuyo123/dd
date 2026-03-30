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
    private ScheduledExecutorService scheduler;
    private ClusterEventSink eventSink = event -> { };

    public HotSpotDetector(HotSpotCoordinator hotSpotCoordinator) {
        this.hotSpotCoordinator = hotSpotCoordinator;
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
        scheduler.scheduleAtFixedRate(this::publishActions, 10, 10, TimeUnit.SECONDS);
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
