package com.minisql.zookeeper;

import com.minisql.common.Constants;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * Watches the elected master leader node.
 */
public class LeaderNodeWatcher implements Closeable {

    private final ZkClient zkClient;
    private CuratorCache curatorCache;

    public LeaderNodeWatcher(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void start(Consumer<String> leaderAddressListener) throws Exception {
        close();
        ensureParentPath();
        curatorCache = CuratorCache.build(zkClient.getClient(), Constants.ZK_MASTER_LEADER_PATH);
        curatorCache.listenable().addListener(CuratorCacheListener.builder()
            .forChanges((oldNode, newNode) -> {
                if (leaderAddressListener == null) {
                    return;
                }
                byte[] data = newNode == null ? null : newNode.getData();
                leaderAddressListener.accept(ZkPayloads.decodeLeaderAddress(data));
            })
            .forCreates(node -> {
                if (leaderAddressListener == null) {
                    return;
                }
                leaderAddressListener.accept(ZkPayloads.decodeLeaderAddress(node.getData()));
            })
            .build());
        curatorCache.start();
        // Notify initial value if present
        if (leaderAddressListener != null) {
            curatorCache.stream()
                .filter(node -> node.getPath().equals(Constants.ZK_MASTER_LEADER_PATH))
                .findFirst()
                .ifPresent(node -> leaderAddressListener.accept(ZkPayloads.decodeLeaderAddress(node.getData())));
        }
    }

    private void ensureParentPath() throws Exception {
        if (!zkClient.exists(Constants.ZK_MASTERS_PATH)) {
            zkClient.createPersistent(Constants.ZK_MASTERS_PATH, new byte[0]);
        }
    }

    @Override
    public void close() throws IOException {
        if (curatorCache != null) {
            try {
                curatorCache.close();
            } catch (Exception e) {
                throw new IOException("Failed to close leader node watcher", e);
            } finally {
                curatorCache = null;
            }
        }
    }
}
