package com.minisql.zookeeper;

import com.minisql.common.Constants;
import com.minisql.common.model.ServerId;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LeaderElectionService tests")
class LeaderElectionServiceTest {

    private TestingServer server;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    private ZkClient newClient() throws Exception {
        server = new TestingServer();
        ZkClient client = new ZkClient(server.getConnectString());
        client.start();
        int retries = 0;
        while (!client.isConnected() && retries < 30) {
            Thread.sleep(100);
            retries++;
        }
        assertTrue(client.isConnected(), "Client should connect to embedded ZK");
        // Bootstrap paths that LeaderElectionService needs
        client.createPersistent(Constants.ZK_MASTERS_PATH, new byte[0]);
        return client;
    }

    @Nested
    @DisplayName("leader election")
    class LeaderElection {

        @Test
        @DisplayName("single participant becomes leader")
        void singleParticipantBecomesLeader() throws Exception {
            ZkClient client = newClient();
            LeaderElectionService service = new LeaderElectionService(client, new ServerId("master-1", 16000));

            CountDownLatch becameLeader = new CountDownLatch(1);
            service.start(isLeader -> {
                if (isLeader) becameLeader.countDown();
            });

            assertTrue(becameLeader.await(5, TimeUnit.SECONDS), "Should become leader within timeout");
            assertTrue(service.isLeader());

            // Leader path should be published
            assertTrue(client.exists(Constants.ZK_MASTER_LEADER_PATH));

            service.close();
            client.close();
        }

        @Test
        @DisplayName("second participant does not become leader while first holds it")
        void secondParticipantIsNotLeader() throws Exception {
            ZkClient client1 = newClient();
            LeaderElectionService service1 = new LeaderElectionService(client1, new ServerId("master-1", 16000));

            CountDownLatch leader1 = new CountDownLatch(1);
            service1.start(isLeader -> { if (isLeader) leader1.countDown(); });
            assertTrue(leader1.await(5, TimeUnit.SECONDS));
            assertTrue(service1.isLeader());

            // Start a second participant on the same ZK
            ZkClient client2 = new ZkClient(server.getConnectString());
            client2.start();
            int r2 = 0;
            while (!client2.isConnected() && r2 < 30) { Thread.sleep(100); r2++; }
            assertTrue(client2.isConnected(), "Client2 should connect");

            LeaderElectionService service2 = new LeaderElectionService(client2, new ServerId("master-2", 16001));
            service2.start(isLeader -> {}); // callback only fires on state change

            // Wait briefly for second latch to settle (it joins election as follower)
            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline && service1.isLeader() == service2.isLeader()) {
                Thread.sleep(50);
            }
            // First should still be leader, second should not
            assertTrue(service1.isLeader());
            assertFalse(service2.isLeader());

            service2.close();
            client2.close();
            service1.close();
            client1.close();
        }

        @Test
        @DisplayName("second participant becomes leader after first releases")
        void failoverOnRelease() throws Exception {
            ZkClient client1 = newClient();
            LeaderElectionService service1 = new LeaderElectionService(client1, new ServerId("master-1", 16000));

            CountDownLatch leader1 = new CountDownLatch(1);
            service1.start(isLeader -> { if (isLeader) leader1.countDown(); });
            assertTrue(leader1.await(5, TimeUnit.SECONDS));

            // Start second
            ZkClient client2 = new ZkClient(server.getConnectString());
            client2.start();
            int retries = 0;
            while (!client2.isConnected() && retries < 30) { Thread.sleep(100); retries++; }
            LeaderElectionService service2 = new LeaderElectionService(client2, new ServerId("master-2", 16001));

            CountDownLatch leader2 = new CountDownLatch(1);
            service2.start(isLeader -> { if (isLeader) leader2.countDown(); });
            assertTrue(service1.isLeader());

            // Release service1 — service2 should take over
            service1.close();
            client1.close();

            assertTrue(leader2.await(5, TimeUnit.SECONDS), "Second should become leader after first releases");
            assertTrue(service2.isLeader());

            service2.close();
            client2.close();
        }
    }

    @Nested
    @DisplayName("lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("close without start does not throw")
        void closeWithoutStartDoesNotThrow() throws Exception {
            ZkClient client = newClient();
            LeaderElectionService service = new LeaderElectionService(client, new ServerId("m", 16000));
            assertDoesNotThrow(() -> service.close());
            client.close();
        }

        @Test
        @DisplayName("isLeader returns false before start")
        void notLeaderBeforeStart() throws Exception {
            ZkClient client = newClient();
            LeaderElectionService service = new LeaderElectionService(client, new ServerId("m", 16000));
            assertFalse(service.isLeader());
            client.close();
        }

        @Test
        @DisplayName("double close is safe")
        void doubleCloseIsSafe() throws Exception {
            ZkClient client = newClient();
            LeaderElectionService service = new LeaderElectionService(client, new ServerId("m", 16000));
            service.start(isLeader -> {});
            service.close();
            assertDoesNotThrow(() -> service.close());
            client.close();
        }
    }
}
