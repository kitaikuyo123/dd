package com.minisql.zookeeper;

import com.minisql.common.model.ServerId;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * High-level ZooKeeper coordinator facade.
 */
public class ZkManager {

    private final ZkClient zkClient;
    private final List<ServerListener> listeners = new CopyOnWriteArrayList<>();
    private final PathBootstrapper pathBootstrapper;
    private final LeaderElectionService leaderElectionService;
    private final RegionServerRegistry regionServerRegistry;
    private final LeaderNodeWatcher leaderNodeWatcher;

    public ZkManager(String zkConnectString, ServerId serverId) {
        this(new ZkClient(zkConnectString), serverId);
    }

    ZkManager(ZkClient zkClient, ServerId serverId) {
        this.zkClient = zkClient;
        this.pathBootstrapper = new PathBootstrapper(zkClient);
        this.leaderElectionService = new LeaderElectionService(zkClient, serverId);
        this.regionServerRegistry = new RegionServerRegistry(zkClient, serverId);
        this.leaderNodeWatcher = new LeaderNodeWatcher(zkClient);
    }

    public void start() throws Exception {
        zkClient.start();
        pathBootstrapper.bootstrap();
    }

    public void close() {
        RuntimeException failure = null;
        failure = closeBestEffort(leaderNodeWatcher::close, failure);
        failure = closeBestEffort(() -> leaderElectionService.close(), failure);
        failure = closeBestEffort(() -> regionServerRegistry.close(), failure);
        failure = closeBestEffort(zkClient::close, failure);
        if (failure != null) {
            throw failure;
        }
    }

    public void participateMasterElection() throws Exception {
        leaderElectionService.start(isLeader -> notifyLeadershipChange(isLeader));
    }

    public void publishLeader() throws Exception {
        leaderElectionService.publishLeader();
    }

    public void watchLeader() throws Exception {
        leaderNodeWatcher.start(this::notifyLeaderAddressChange);
    }

    public void registerRegionServer(byte[] payload) throws Exception {
        regionServerRegistry.registerSelf(payload);
    }

    public void registerRegionServer() throws Exception {
        regionServerRegistry.registerSelf();
    }

    public void watchRegionServers() throws Exception {
        regionServerRegistry.watch(this::notifyServerAdded, this::notifyServerRemoved);
    }

    public List<String> getActiveRegionServers() throws Exception {
        return regionServerRegistry.getActiveRegionServers();
    }

    public boolean isLeader() {
        return leaderElectionService.isLeader();
    }

    public ZkClient getClient() {
        return zkClient;
    }

    public void addListener(ServerListener listener) {
        listeners.add(listener);
    }

    public void removeListener(ServerListener listener) {
        listeners.remove(listener);
    }

    private RuntimeException closeBestEffort(ThrowingRunnable runnable, RuntimeException failure) {
        try {
            runnable.run();
        } catch (Exception e) {
            if (failure == null) {
                failure = new RuntimeException("Failed to close ZkManager resources", e);
            } else {
                failure.addSuppressed(e);
            }
        }
        return failure;
    }

    private void notifyLeadershipChange(boolean isLeader) {
        for (ServerListener listener : listeners) {
            listener.onLeadershipChange(isLeader);
        }
    }

    private void notifyLeaderAddressChange(String leaderAddress) {
        for (ServerListener listener : listeners) {
            listener.onLeaderAddressChanged(leaderAddress);
        }
    }

    private void notifyServerAdded(String path) {
        for (ServerListener listener : listeners) {
            listener.onServerAdded(path);
        }
    }

    private void notifyServerRemoved(String path) {
        for (ServerListener listener : listeners) {
            listener.onServerRemoved(path);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    public interface ServerListener {
        void onLeadershipChange(boolean isLeader);

        default void onLeaderAddressChanged(String leaderAddress) {
        }

        void onServerAdded(String path);

        void onServerRemoved(String path);
    }
}
