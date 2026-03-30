package com.minisql.master.detect;

import com.minisql.common.Constants;
import com.minisql.common.model.ServerId;
import com.minisql.master.monitoring.MonitoringService;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.ReplicaLifecycleManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Detects server failures and coordinates region recovery.
 *
 * <p>The detector now reports into the shared replica lifecycle map and, when
 * available, delegates promotion/bootstrap work to the unified failover and
 * recovery managers instead of maintaining a completely separate flow.</p>
 */
public class ServerFailureDetector implements ClusterDetector {

    private final ClusterManager clusterManager;
    private final LoadBalancer loadBalancer;
    private final ScheduledExecutorService scheduler;
    private final long heartbeatTimeoutMs;
    private final long checkIntervalMs;

    private ReplicaLifecycleManager lifecycleManager;
    private MonitoringService monitoringService;
    private ClusterEventSink eventSink = event -> { };

    public ServerFailureDetector(ClusterManager clusterManager,
                                 com.minisql.master.state.MetadataManager metadataManager,
                                 LoadBalancer loadBalancer) {
        this(clusterManager, metadataManager, loadBalancer,
            Constants.DEFAULT_HEARTBEAT_TIMEOUT_MS,
            Constants.DEFAULT_HEARTBEAT_INTERVAL_MS * 2);
    }

    public ServerFailureDetector(ClusterManager clusterManager,
                                 com.minisql.master.state.MetadataManager metadataManager,
                                 LoadBalancer loadBalancer,
                                 long heartbeatTimeoutMs,
                                 long checkIntervalMs) {
        this.clusterManager = clusterManager;
        this.loadBalancer = loadBalancer;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.checkIntervalMs = checkIntervalMs;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ServerFailureDetector-Scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    public void setLifecycleManager(ReplicaLifecycleManager lifecycleManager) {
        this.lifecycleManager = lifecycleManager;
    }

    public void setMonitoringService(MonitoringService monitoringService) {
        this.monitoringService = monitoringService;
    }

    @Override
    public void setEventSink(ClusterEventSink eventSink) {
        this.eventSink = eventSink != null ? eventSink : event -> { };
    }

    @Override
    public String getDetectorName() {
        return "serverFailureDetector";
    }

    @Override
    public void start() {
        scheduler.scheduleAtFixedRate(
            this::checkFailedServers,
            checkIntervalMs,
            checkIntervalMs,
            TimeUnit.MILLISECONDS
        );
        System.out.println("ServerFailureDetector started, check interval: " + checkIntervalMs +
            "ms, timeout: " + heartbeatTimeoutMs + "ms");
    }

    @Override
    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("ServerFailureDetector stopped");
    }

    private void checkFailedServers() {
        try {
            List<ServerId> failedServers = clusterManager.detectFailedServers(heartbeatTimeoutMs);
            if (failedServers.isEmpty()) {
                return;
            }

            System.out.println("Detected " + failedServers.size() + " failed servers: " + failedServers);
            for (ServerId failedServer : failedServers) {
                handleServerFailure(failedServer);
            }
        } catch (Exception e) {
            System.err.println("Error checking failed servers: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleServerFailure(ServerId failedServer) {
        System.out.println("Handling server failure: " + failedServer);
        recordEvent("HEARTBEAT_TIMEOUT", "WARN", null, failedServer, null, "Heartbeat timeout detected", null);

        clusterManager.removeServer(failedServer);
        recordEvent("SERVER_OFFLINE", "WARN", null, failedServer, null, "Server removed from active set", null);
        List<String> regionsOnServer = getRegionsOnServer(failedServer);
        if (regionsOnServer.isEmpty()) {
            System.out.println("No region assignments found for failed server: " + failedServer.getServerName());
            return;
        }

        System.out.println("Server " + failedServer + " has " + regionsOnServer.size() + " regions to recover");
        for (String regionId : regionsOnServer) {
            transition(regionId, failedServer,
                ReplicaLifecycleManager.ReplicaLifecycleState.OFFLINE,
                "ServerFailureDetector marked replica offline");
        }
        eventSink.publish(new ServerFailedEvent(failedServer, regionsOnServer));
    }

    private List<String> getRegionsOnServer(ServerId serverId) {
        List<String> regions = new ArrayList<>();
        for (Map.Entry<String, ClusterManager.RegionAssignment> entry :
            clusterManager.getRegionAssignments().entrySet()) {
            if (entry.getValue().getServerId().equals(serverId)) {
                regions.add(entry.getKey());
            }
        }
        return regions;
    }

    private void transition(String regionId,
                            ServerId serverId,
                            ReplicaLifecycleManager.ReplicaLifecycleState state,
                            String detail) {
        if (lifecycleManager != null) {
            lifecycleManager.transition(regionId, serverId, state, detail);
        }
    }

    private void recordEvent(String type, String severity, String regionId, ServerId sourceServer,
                             ServerId targetServer, String message, String details) {
        if (monitoringService != null) {
            monitoringService.recordEvent(type, severity, regionId, null,
                sourceServer == null ? null : sourceServer.getHost() + ":" + sourceServer.getPort(),
                targetServer == null ? null : targetServer.getHost() + ":" + targetServer.getPort(),
                message, details);
        }
    }
}
