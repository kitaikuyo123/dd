package com.minisql.master.detect;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.RegionSplitCoordinator;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Detector adapter for background split checks.
 */
public class RegionSplitDetector implements ClusterDetector {

    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final RegionSplitCoordinator regionSplitCoordinator;
    private final Map<String, Long> cooldownUntilMs = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;
    private ClusterEventSink eventSink = event -> { };

    public RegionSplitDetector(ClusterManager clusterManager,
                               MetadataManager metadataManager,
                               RegionSplitCoordinator regionSplitCoordinator) {
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.regionSplitCoordinator = regionSplitCoordinator;
    }

    @Override
    public String getDetectorName() {
        return "regionSplitDetector";
    }

    @Override
    public void start() {
        regionSplitCoordinator.start();
        if (scheduler != null && !scheduler.isShutdown()) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "RegionSplitDetector");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::detectSplitSuggestions, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        regionSplitCoordinator.stop();
    }

    @Override
    public void setEventSink(ClusterEventSink eventSink) {
        this.eventSink = eventSink != null ? eventSink : event -> { };
    }

    private void detectSplitSuggestions() {
        long now = System.currentTimeMillis();
        cooldownUntilMs.entrySet().removeIf(entry -> entry.getValue() <= now);

        for (ClusterManager.ServerInfo serverInfo : clusterManager.getActiveServers()) {
            for (Map.Entry<String, ClusterManager.RegionLoad> entry : serverInfo.getRegionLoads().entrySet()) {
                String regionId = entry.getKey();
                if (cooldownUntilMs.getOrDefault(regionId, 0L) > now
                    || regionSplitCoordinator.getSplittingRegions().contains(regionId)) {
                    continue;
                }

                ClusterManager.RegionLoad load = entry.getValue();
                if (!regionSplitCoordinator.shouldSplit(load)) {
                    continue;
                }

                Region region = metadataManager.getRegion(regionId);
                if (region == null) {
                    continue;
                }

                ServerId serverId = serverInfo.getServerId();
                eventSink.publish(new RegionSplitSuggestedEvent(
                    regionId,
                    region.getTableName(),
                    serverId,
                    load
                ));
                cooldownUntilMs.put(regionId, now + TimeUnit.MINUTES.toMillis(10));
            }
        }
    }
}
