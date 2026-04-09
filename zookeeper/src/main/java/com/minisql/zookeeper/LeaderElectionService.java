package com.minisql.zookeeper;

import com.minisql.common.Constants;
import com.minisql.common.model.ServerId;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import java.io.Closeable;
import java.io.IOException;

/**
 * Coordinates leader election for master nodes and publishes the winner.
 */
public class LeaderElectionService implements Closeable {

    private final ZkClient zkClient;
    private final ServerId serverId;
    private LeaderLatch leaderLatch;
    private volatile boolean leader;

    public LeaderElectionService(ZkClient zkClient, ServerId serverId) {
        this.zkClient = zkClient;
        this.serverId = serverId;
    }

    public void start(LeadershipListener leadershipListener) throws Exception {
        leaderLatch = new LeaderLatch(zkClient.getClient(), Constants.ZK_MASTER_ELECTION_PATH, serverId.getInstanceName());
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                leader = true;
                try {
                    publishLeader();
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to publish elected leader", e);
                }
                if (leadershipListener != null) {
                    leadershipListener.onLeadershipChange(true);
                }
            }

            @Override
            public void notLeader() {
                leader = false;
                if (leadershipListener != null) {
                    leadershipListener.onLeadershipChange(false);
                }
            }
        });
        leaderLatch.start();
    }

    public boolean isLeader() {
        return leader;
    }

    public void publishLeader() throws Exception {
        byte[] payload = ZkPayloads.encodeLeader(serverId);
        if (zkClient.exists(Constants.ZK_MASTER_LEADER_PATH)) {
            zkClient.setData(Constants.ZK_MASTER_LEADER_PATH, payload);
        } else {
            zkClient.createPersistent(Constants.ZK_MASTER_LEADER_PATH, payload);
        }
    }

    @Override
    public void close() throws IOException {
        if (leaderLatch != null) {
            try {
                leaderLatch.close();
            } catch (Exception e) {
                throw new IOException("Failed to close leader election service", e);
            } finally {
                leaderLatch = null;
                leader = false;
            }
        }
    }

    /**
     * Functional interface for leadership change notifications using primitive boolean.
     */
    @FunctionalInterface
    public interface LeadershipListener {
        void onLeadershipChange(boolean isLeader);
    }
}
