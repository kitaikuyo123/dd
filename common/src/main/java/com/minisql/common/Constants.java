package com.minisql.common;

/**
 * Shared system constants.
 */
public final class Constants {

    public static final String VERSION = "1.0.0";

    // ZooKeeper paths
    public static final String ZK_ROOT_PATH = "/minisql";
    public static final String ZK_MASTERS_PATH = ZK_ROOT_PATH + "/masters";
    public static final String ZK_MASTER_LEADER_PATH = ZK_MASTERS_PATH + "/leader";
    public static final String ZK_MASTER_ELECTION_PATH = ZK_MASTERS_PATH + "/election";
    public static final String ZK_REGIONSERVERS_PATH = ZK_ROOT_PATH + "/regionservers";
    public static final String ZK_TABLES_PATH = ZK_ROOT_PATH + "/tables";
    public static final String ZK_REGIONS_PATH = ZK_ROOT_PATH + "/regions";
    public static final String ZK_ASSIGNMENT_PATH = ZK_ROOT_PATH + "/assignment";
    public static final String ZK_LOCKS_PATH = ZK_ROOT_PATH + "/locks";
    public static final String ZK_TABLE_LOCKS_PATH = ZK_LOCKS_PATH + "/tables";
    public static final String ZK_REGION_LOCKS_PATH = ZK_LOCKS_PATH + "/regions";

    // Default ports
    public static final int DEFAULT_MASTER_PORT = 16000;
    public static final int DEFAULT_REGIONSERVER_PORT = 16020;
    public static final int DEFAULT_CLIENT_PORT = 9090;

    // Heartbeat defaults
    public static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 5000;

    public static final int DEFAULT_REPLICATION_FACTOR = 3;

    public static final String DEFAULT_TMP_DIR = "./tmp";

    private Constants() {
    }
}
