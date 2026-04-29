package com.minisql.zookeeper;

import com.minisql.common.Constants;
import com.minisql.common.model.ServerId;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** RegionServer 注册管理器，处理服务注册与发现 */
public class RegionServerRegistry implements Closeable {

    private final ZkClient zkClient;
    private final ServerId serverId;
    private CuratorCache regionServerCache;

    public RegionServerRegistry(ZkClient zkClient, ServerId serverId) {
        this.zkClient = zkClient;
        this.serverId = serverId;
    }

    public void registerSelf(byte[] payload) throws Exception {
        String path = regionServerPath(serverId);
        if (zkClient.exists(path)) {
            zkClient.delete(path);
        }
        zkClient.createEphemeral(path, payload);
    }

    public void registerSelf() throws Exception {
        registerSelf(serverId.getInstanceName().getBytes(StandardCharsets.UTF_8));
    }

    public void watch(Consumer<String> onAdded, Consumer<String> onRemoved) throws Exception {
        closeCacheQuietly();
        regionServerCache = CuratorCache.build(zkClient.getClient(), Constants.ZK_REGIONSERVERS_PATH);
        regionServerCache.listenable().addListener(CuratorCacheListener.builder()
            .forCreates(node -> {
                if (onAdded != null) {
                    onAdded.accept(node.getPath());
                }
            })
            .forDeletes(node -> {
                if (onRemoved != null) {
                    onRemoved.accept(node.getPath());
                }
            })
            .build());
        regionServerCache.start();
    }

    public List<String> getActiveRegionServers() throws Exception {
        return new ArrayList<>(zkClient.getChildren(Constants.ZK_REGIONSERVERS_PATH));
    }

    private String regionServerPath(ServerId target) {
        return Constants.ZK_REGIONSERVERS_PATH + "/" + target.getInstanceName();
    }

    private void closeCacheQuietly() {
        if (regionServerCache != null) {
            try {
                regionServerCache.close();
            } catch (Exception ignored) {
                // Best effort during restart.
            } finally {
                regionServerCache = null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (regionServerCache != null) {
            try {
                regionServerCache.close();
            } catch (Exception e) {
                throw new IOException("Failed to close region server registry", e);
            } finally {
                regionServerCache = null;
            }
        }
    }
}
