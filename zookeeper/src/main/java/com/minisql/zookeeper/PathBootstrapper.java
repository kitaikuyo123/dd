package com.minisql.zookeeper;

import com.minisql.common.Constants;

/**
 * Ensures the MiniSQL ZooKeeper path layout exists.
 */
public class PathBootstrapper {

    private final ZkClient zkClient;

    public PathBootstrapper(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void bootstrap() throws Exception {
        String[] paths = {
            Constants.ZK_ROOT_PATH,
            Constants.ZK_MASTERS_PATH,
            Constants.ZK_MASTER_ELECTION_PATH,
            Constants.ZK_REGIONSERVERS_PATH,
            Constants.ZK_TABLES_PATH,
            Constants.ZK_LOCKS_PATH,
            Constants.ZK_TABLE_LOCKS_PATH,
            Constants.ZK_REGION_LOCKS_PATH
        };

        for (String path : paths) {
            if (!zkClient.exists(path)) {
                zkClient.createPersistent(path, new byte[0]);
            }
        }
    }
}
