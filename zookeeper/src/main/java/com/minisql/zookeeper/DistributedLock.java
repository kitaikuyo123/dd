package com.minisql.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.TimeUnit;

/**
 * 分布式锁实现
 * 负责模块: 开发者A
 */
public class DistributedLock {

    private final InterProcessMutex lock;

    public DistributedLock(CuratorFramework client, String lockPath) {
        this.lock = new InterProcessMutex(client, lockPath);
    }

    /**
     * 获取锁（阻塞）
     */
    public void acquire() throws Exception {
        lock.acquire();
    }

    /**
     * 尝试获取锁（带超时）
     */
    public boolean acquire(long timeout, TimeUnit unit) throws Exception {
        return lock.acquire(timeout, unit);
    }

    /**
     * 释放锁
     */
    public void release() throws Exception {
        lock.release();
    }

    /**
     * 是否持有锁
     */
    public boolean isAcquiredInThisProcess() {
        return lock.isAcquiredInThisProcess();
    }
}
