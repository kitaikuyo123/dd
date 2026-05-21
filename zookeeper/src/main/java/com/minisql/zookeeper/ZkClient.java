package com.minisql.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * ZooKeeper 客户端封装
 * 负责模块: 开发者A
 */
public class ZkClient {

    private final CuratorFramework client;
    private final String connectString;

    public ZkClient(String connectString) {
        this.connectString = connectString;
        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(10000)
                .connectionTimeoutMs(10000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }

    public void start() {
        client.start();
    }

    public void close() {
        client.close();
    }

    public CuratorFramework getClient() {
        return client;
    }

    public String getConnectString() {
        return connectString;
    }

    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }

    public boolean isStarted() {
        return client.getState() == CuratorFrameworkState.STARTED;
    }

    /**
     * 创建持久化节点
     */
    public String createPersistent(String path, byte[] data) throws Exception {
        return client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, data);
    }

    /**
     * 创建临时节点
     */
    public String createEphemeral(String path, byte[] data) throws Exception {
        return client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, data);
    }

    /**
     * 创建临时顺序节点
     */
    public String createEphemeralSequential(String path, byte[] data) throws Exception {
        return client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(path, data);
    }

    /**
     * 删除节点
     */
    public void delete(String path) throws Exception {
        client.delete()
                .guaranteed()
                .deletingChildrenIfNeeded()
                .forPath(path);
    }

    /**
     * 检查节点是否存在
     */
    public boolean exists(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        return stat != null;
    }

    /**
     * 获取节点数据
     */
    public byte[] getData(String path) throws Exception {
        return client.getData().forPath(path);
    }

    /**
     * 设置节点数据
     */
    public void setData(String path, byte[] data) throws Exception {
        client.setData().forPath(path, data);
    }

    /**
     * 获取子节点列表
     */
    public List<String> getChildren(String path) throws Exception {
        return client.getChildren().forPath(path);
    }

    /**
     * 监听节点变化（一次性 Watcher，触发后不重注册）
     */
    public void watchNode(String path, NodeWatcher watcher) throws Exception {
        watchNodeOnce(path, watcher);
    }

    private void watchNodeOnce(String path, NodeWatcher watcher) throws Exception {
        client.getData().usingWatcher((org.apache.curator.framework.api.CuratorWatcher) event -> {
            watcher.onNodeChanged(event.getPath(), event.getType());
        }).forPath(path);
    }

    /**
     * 监听子节点变化（一次性 Watcher，触发后不重注册）
     */
    public void watchChildren(String path, ChildrenWatcher watcher) throws Exception {
        watchChildrenOnce(path, watcher);
    }

    private void watchChildrenOnce(String path, ChildrenWatcher watcher) throws Exception {
        client.getChildren().usingWatcher((org.apache.curator.framework.api.CuratorWatcher) event -> {
            try {
                List<String> children = client.getChildren().forPath(path);
                watcher.onChildrenChanged(event.getPath(), children);
            } catch (Exception e) {
                watcher.onChildrenChanged(event.getPath(), null);
            }
        }).forPath(path);
    }

    public interface NodeWatcher {
        void onNodeChanged(String path, org.apache.zookeeper.Watcher.Event.EventType type);
    }

    public interface ChildrenWatcher {
        void onChildrenChanged(String path, List<String> children);
    }
}
