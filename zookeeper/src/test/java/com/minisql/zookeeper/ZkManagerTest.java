package com.minisql.zookeeper;

import com.minisql.common.model.ServerId;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkManagerTest {

    private TestingServer server;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void bootstrapsPathsAndWatchesRegionServers() throws Exception {
        server = new TestingServer();
        ZkManager manager = new ZkManager(server.getConnectString(), new ServerId("localhost", 16020, 1));
        CountDownLatch added = new CountDownLatch(1);

        manager.addListener(new ZkManager.ServerListener() {
            @Override
            public void onLeadershipChange(boolean isLeader) {
            }

            @Override
            public void onServerAdded(String path) {
                added.countDown();
            }

            @Override
            public void onServerRemoved(String path) {
            }
        });

        manager.start();
        manager.watchRegionServers();
        manager.registerRegionServer();

        assertTrue(manager.getActiveRegionServers().stream().anyMatch(path -> path.contains("localhost:16020@1")));
        assertTrue(manager.isLeader() == false);
        assertTrue(added.await(5, TimeUnit.SECONDS));
        manager.close();
    }
}
