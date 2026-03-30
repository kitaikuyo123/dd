package com.minisql.common;

/**
 * 系统常量定义
 */
public class Constants {

    // 版本信息
    public static final String VERSION = "1.0.0";

    // ZooKeeper 相关路径
    public static final String ZK_ROOT_PATH = "/minisql";
    public static final String ZK_MASTER_PATH = ZK_ROOT_PATH + "/master";
    public static final String ZK_REGIONSERVERS_PATH = ZK_ROOT_PATH + "/regionservers";
    public static final String ZK_TABLES_PATH = ZK_ROOT_PATH + "/tables";
    public static final String ZK_REGIONS_PATH = ZK_ROOT_PATH + "/regions";
    public static final String ZK_ASSIGNMENT_PATH = ZK_ROOT_PATH + "/assignment";

    // 默认端口
    public static final int DEFAULT_MASTER_PORT = 16000;
    public static final int DEFAULT_REGIONSERVER_PORT = 16020;
    public static final int DEFAULT_CLIENT_PORT = 9090;

    // 心跳配置
    public static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 5000;  // 5秒
    public static final int DEFAULT_HEARTBEAT_TIMEOUT_MS = 15000;  // 15秒

    // 副本配置
    public static final int DEFAULT_REPLICATION_FACTOR = 3;

    // 临时目录（用于临时操作）
    public static final String DEFAULT_TMP_DIR = "./tmp";

    private Constants() {}
}
