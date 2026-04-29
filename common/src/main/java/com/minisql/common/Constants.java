package com.minisql.common;

/** 系统共享常量 */
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

    // Region split/merge 阈值 — 字段初始化的兜底默认值。
    // 运行时由配置文件覆盖：
    //   Master:        region.split.threshold.mb / region.merge.threshold.mb
    //   RegionServer:  regionserver.region.split.threshold.mb / regionserver.region.split.min.mb
    public static final long DEFAULT_SPLIT_THRESHOLD = 10L * 1024 * 1024 * 1024;  // 10 GB
    public static final long DEFAULT_SPLIT_MIN_SIZE = 1L * 1024 * 1024 * 1024;    // 1 GB

    public static final long DEFAULT_MERGE_THRESHOLD = 100L * 1024 * 1024;         // 100 MB
    public static final long DEFAULT_MERGE_MAX_SIZE = 8L * 1024 * 1024 * 1024;    // 8 GB
    public static final long DEFAULT_MERGE_MIN_SIZE = 10L * 1024 * 1024;           // 10 MB

    private Constants() {
    }
}
