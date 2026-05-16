package com.minisql.master.state;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.common.proto.CommonProto;
import com.minisql.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.List;

/**
 * 元数据管理器
 * 管理表结构和 Region 元数据，支持 ZooKeeper 持久化
 * 负责模块：开发者 A
 */
public class MetadataManager {

    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);

    // 表元数据：tableName -> Table
    private final ConcurrentMap<String, Table> tables = new ConcurrentHashMap<>();

    // Region 元数据：regionId -> Region
    private final ConcurrentMap<String, Region> regions = new ConcurrentHashMap<>();

    // ZooKeeper 客户端
    private ZkClient zkClient;

    // ZooKeeper 路径：统一使用 /minisql/tables/{tableName}/... 结构
    // - /minisql/tables/{tableName}/schema - 表结构
    // - /minisql/tables/{tableName}/regions/{regionId} - Region 元数据
    // - /minisql/tables/{tableName}/regions/{regionId}/primary - 主副本地址
    // - /minisql/tables/{tableName}/regions/{regionId}/replicas - 副本地址列表
    private static final String TABLES_BASE = "/minisql/tables";

    public MetadataManager() {
    }

    public MetadataManager(ZkClient zkClient) {
        this.zkClient = zkClient;
        initializeZkPaths();
        loadMetadataFromZk();
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
        initializeZkPaths();
        loadMetadataFromZk();
    }

    /**
     * 获取 ZooKeeper 客户端
     */
    public ZkClient getZkClient() {
        return zkClient;
    }

    /**
     * 初始化 ZK 路径
     */
    private void initializeZkPaths() {
        if (zkClient == null) return;
        try {
            // 初始化根路径
            if (!zkClient.exists(TABLES_BASE)) {
                zkClient.createPersistent(TABLES_BASE, new byte[0]);
            }
        } catch (Exception e) {
            logger.warn("Failed to initialize ZK paths: {}", e.getMessage(), e);
        }
    }

    /**
     * 从 ZooKeeper 加载元数据
     * 从 /minisql/tables/{tableName}/schema 加载表结构
     * 从 /minisql/tables/{tableName}/regions/{regionId} 加载 Region 元数据
     */
    private void loadMetadataFromZk() {
        if (zkClient == null) return;
        try {
            tables.clear();
            regions.clear();
            // 获取所有表
            List<String> tableNames = zkClient.getChildren(TABLES_BASE);
            for (String tableName : tableNames) {
                try {
                    // 跳过 regions 节点
                    if ("regions".equals(tableName)) {
                        continue;
                    }

                    // 加载表结构
                    String schemaPath = TABLES_BASE + "/" + tableName + "/schema";
                    if (zkClient.exists(schemaPath)) {
                        byte[] data = zkClient.getData(schemaPath);
                        Table table = deserializeTable(data);
                        if (table != null) {
                            tables.put(tableName, table);
                        }
                    }

                    // 加载该表的所有 Region
                    String regionsPath = TABLES_BASE + "/" + tableName + "/regions";
                    if (zkClient.exists(regionsPath)) {
                        List<String> regionIds = zkClient.getChildren(regionsPath);
                        for (String regionId : regionIds) {
                            try {
                                String regionPath = regionsPath + "/" + regionId;
                                byte[] regionData = zkClient.getData(regionPath);
                                Region region = deserializeRegion(regionData);
                                if (region != null) {
                                    restoreReplicaTopology(regionPath, region);
                                    regions.put(regionId, region);
                                }
                            } catch (Exception e) {
                                logger.warn("Failed to load region {}: {}", regionId, e.getMessage(), e);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to load table {}: {}", tableName, e.getMessage(), e);
                }
            }

            logger.info("Loaded {} tables and {} regions from ZooKeeper", tables.size(), regions.size());
        } catch (Exception e) {
            logger.error("Failed to load metadata from ZK: {}", e.getMessage(), e);
        }
    }

    /**
     * 创建表
     * 写入到 /minisql/tables/{tableName}/schema
     */
    public void createTable(Table table) {

        // 持久化到 ZK
        if (zkClient != null) {
            try {
                // 确保表目录存在
                String tablePath = TABLES_BASE + "/" + table.getTableName();
                if (!zkClient.exists(tablePath)) {
                    zkClient.createPersistent(tablePath, new byte[0]);
                }

                // 写入表结构
                String schemaPath = tablePath + "/schema";
                byte[] data = serializeTable(table);
                if (data == null || data.length == 0) {
                    throw new IllegalStateException("Serialized table schema is empty for " + table.getTableName());
                }
                if (zkClient.exists(schemaPath)) {
                    zkClient.setData(schemaPath, data);
                } else {
                    zkClient.createPersistent(schemaPath, data);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to persist table to ZK: " + e.getMessage(), e);
            }
        }

        tables.put(table.getTableName(), table);
        logger.info("Table created: {}", table.getTableName());
    }

    /**
     * 删除表
     */
    public void deleteTable(String tableName) {
        tables.remove(tableName);

        // 从 ZK 删除整个表目录（递归）
        if (zkClient != null) {
            try {
                String tablePath = TABLES_BASE + "/" + tableName;
                if (zkClient.exists(tablePath)) {
                    zkClient.delete(tablePath);
                }
            } catch (Exception e) {
                logger.warn("Failed to delete table from ZK: {}", e.getMessage(), e);
            }
        }

        logger.info("Table deleted: {}", tableName);
    }

    /**
     * 获取表
     */
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * 检查表是否存在
     */
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    /**
     * 注册 Region
     * 为了保持向后兼容，同时写入到表的 regions 目录
     */
    public void registerRegion(Region region) {
        regions.put(region.getRegionId(), region);

        // 持久化到 ZK - 使用统一路径
        if (zkClient != null) {
            try {
                String tableName = region.getTableName();
                String regionId = region.getRegionId();
                String tablePath = TABLES_BASE + "/" + tableName;
                String regionsPath = tablePath + "/regions";
                String regionPath = regionsPath + "/" + regionId;

                // 确保父路径存在
                if (!zkClient.exists(tablePath)) {
                    zkClient.createPersistent(tablePath, new byte[0]);
                }
                if (!zkClient.exists(regionsPath)) {
                    zkClient.createPersistent(regionsPath, new byte[0]);
                }

                // 写入 Region 元数据
                byte[] data = serializeRegion(region);
                if (zkClient.exists(regionPath)) {
                    zkClient.setData(regionPath, data);
                } else {
                    zkClient.createPersistent(regionPath, data);
                }
            } catch (Exception e) {
                logger.warn("Failed to persist region to ZK: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * 注册 Region 到基于表的路径结构（客户端期望的格式）
     * 同时写入 Region 元数据和主副本服务器地址
     *
     * ZooKeeper 路径：
     * - /minisql/tables/{tableName}/regions/{regionId} - Region 元数据
     * - /minisql/tables/{tableName}/regions/{regionId}/primary - 主副本地址
     */
    public void registerRegionForTable(Region region, ServerId primaryServer) {
        String tableName = region.getTableName();
        String regionId = region.getRegionId();

        // 先在内存中注册
        regions.put(regionId, region);

        // 设置主副本到 Region 对象，这样 getTableRegions() 才能获取到 MySQL 配置
        if (primaryServer != null) {
            region.setPrimary(primaryServer);
            region.addReplica(primaryServer);
        }

        if (zkClient == null) {
            logger.warn("ZooKeeper client not initialized, region registered in memory only");
            return;
        }

        try {
            String tablePath = TABLES_BASE + "/" + tableName;
            String regionsPath = tablePath + "/regions";
            String regionPath = regionsPath + "/" + regionId;
            String primaryPath = regionPath + "/primary";
            String replicasPath = regionPath + "/replicas";

            ensurePersistentPath(tablePath);
            ensurePersistentPath(regionsPath);
            upsertPersistentNode(regionPath, serializeRegion(region));

            if (primaryServer != null) {
                String serverAddress = primaryServer.getHost() + ":" + primaryServer.getPort();
                byte[] primaryData = serverAddress.getBytes(StandardCharsets.UTF_8);
                upsertPersistentNode(primaryPath, primaryData);
                logger.info("Primary server {} registered for region {}", serverAddress, regionId);
            }

            byte[] replicasData = encodeReplicas(region.getReplicas()).getBytes(StandardCharsets.UTF_8);
            upsertPersistentNode(replicasPath, replicasData);

            logger.info("Region {} registered for table {}", regionId, tableName);
        } catch (Exception e) {
            logger.error("Failed to persist region for table to ZK: {}", e.getMessage(), e);
        }
    }

    private void ensurePersistentPath(String path) throws Exception {
        if (!zkClient.exists(path)) {
            try {
                zkClient.createPersistent(path, new byte[0]);
            } catch (org.apache.zookeeper.KeeperException.NodeExistsException ignored) {
                // Another writer created the node first.
            }
        }
    }

    private void upsertPersistentNode(String path, byte[] data) throws Exception {
        try {
            if (zkClient.exists(path)) {
                zkClient.setData(path, data);
            } else {
                zkClient.createPersistent(path, data);
            }
        } catch (org.apache.zookeeper.KeeperException.NodeExistsException ignored) {
            zkClient.setData(path, data);
        }
    }

    /**
     * 移除 Region
     */
    public void removeRegion(String regionId) {
        Region region = regions.remove(regionId);

        // 从 ZK 删除 - 需要从 Region 元数据中获取 tableName
        if (zkClient != null) {
            try {
                if (region != null) {
                    String tableName = region.getTableName();
                    String regionPath = TABLES_BASE + "/" + tableName + "/regions/" + regionId;
                    if (zkClient.exists(regionPath)) {
                        zkClient.delete(regionPath);
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to delete region from ZK: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * 分配一个新的 region ID，格式 {tableName}.{自增序号}，序号持久化到 ZK。
     */
    public synchronized String allocateRegionId(String tableName) {
        long next = readNextId(tableName);
        writeNextId(tableName, next + 1);
        return tableName + "." + next;
    }

    private long readNextId(String tableName) {
        if (zkClient == null) return 1L;
        try {
            String path = TABLES_BASE + "/" + tableName + "/next-id";
            if (!zkClient.exists(path)) return 1L;
            byte[] data = zkClient.getData(path);
            return Long.parseLong(new String(data, java.nio.charset.StandardCharsets.UTF_8).trim());
        } catch (Exception e) {
            return 1L;
        }
    }

    private void writeNextId(String tableName, long nextId) {
        if (zkClient == null) return;
        try {
            String tablePath = TABLES_BASE + "/" + tableName;
            if (!zkClient.exists(tablePath)) {
                zkClient.createPersistent(tablePath, new byte[0]);
            }
            String path = tablePath + "/next-id";
            byte[] data = String.valueOf(nextId).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            if (zkClient.exists(path)) {
                zkClient.setData(path, data);
            } else {
                zkClient.createPersistent(path, data);
            }
        } catch (Exception e) {
            logger.warn("Failed to persist next-id for {}: {}", tableName, e.getMessage());
        }
    }

    /**
     * 获取 Region
     */
    public Region getRegion(String regionId) {
        return regions.get(regionId);
    }

    /**
     * 获取表的所有 Region
     */
    public java.util.Collection<Region> getRegionsForTable(String tableName) {
        java.util.List<Region> result = new java.util.ArrayList<>();
        for (Region region : regions.values()) {
            if (region.getTableName().equals(tableName)) {
                result.add(region);
            }
        }
        return result;
    }

    /**
     * 获取所有表
     */
    public java.util.Collection<Table> getAllTables() {
        return tables.values();
    }

    /**
     * 获取所有 Region
     */
    public java.util.Collection<Region> getAllRegions() {
        return regions.values();
    }

    /**
     * 根据 rowKey 查找 Region
     */
    public Region findRegion(String tableName, byte[] rowKey) {
        for (Region region : regions.values()) {
            if (region.getTableName().equals(tableName) &&
                com.minisql.common.utils.BytesUtil.compareTo(rowKey, region.getStartKey()) >= 0 &&
                com.minisql.common.utils.BytesUtil.compareTo(rowKey, region.getEndKey()) < 0) {
                return region;
            }
        }
        return null;
    }

    // ==================== 序列化和反序列化方法 ====================

    /**
     * 序列化 Table 为字节数组
     */
    private byte[] serializeTable(Table table) {
        try {
            CommonProto.TableSchema.Builder builder = CommonProto.TableSchema.newBuilder()
                .setTableName(table.getTableName() == null ? "" : table.getTableName());

            if (table.getColumns() != null) {
                for (com.minisql.common.model.Column column : table.getColumns()) {
                    CommonProto.ColumnSchema.Builder columnBuilder = CommonProto.ColumnSchema.newBuilder()
                        .setName(column.getName() == null ? "" : column.getName())
                        .setType(column.getType() == null ? "" : column.getType().name())
                        .setNullable(column.isNullable())
                        .setMaxLength(column.getLength());
                    builder.addColumns(columnBuilder.build());
                }
            }

            if (table.getPrimaryKey() != null) {
                builder.setPrimaryKey(table.getPrimaryKey());
            }
            if (table.getPartitionKeys() != null) {
                builder.addAllPartitionKeys(table.getPartitionKeys());
            }
            if (table.getClusteringKeys() != null) {
                builder.addAllClusteringKeys(table.getClusteringKeys());
            }

            if (table.getProperties() != null) {
                builder.setReplicationFactor(table.getProperties().getReplicationFactor());
            }

            return builder.build().toByteArray();
        } catch (Exception e) {
            logger.warn("Failed to serialize table: {}", e.getMessage(), e);
            return new byte[0];
        }
    }

    /**
     * 反序列化字节数组为 Table
     */
    private Table deserializeTable(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            CommonProto.TableSchema schema = CommonProto.TableSchema.parseFrom(data);
            Table table = new Table();
            table.setTableName(schema.getTableName());

            java.util.List<com.minisql.common.model.Column> columns = new java.util.ArrayList<>();
            for (CommonProto.ColumnSchema columnSchema : schema.getColumnsList()) {
                com.minisql.common.model.Column column = new com.minisql.common.model.Column();
                column.setName(columnSchema.getName());
                if (columnSchema.getType() != null && !columnSchema.getType().isEmpty()) {
                    column.setType(com.minisql.common.model.Column.ColumnType.valueOf(columnSchema.getType()));
                }
                column.setNullable(columnSchema.getNullable());
                column.setLength(columnSchema.getMaxLength());
                columns.add(column);
            }
            table.setColumns(columns);

            if (schema.getPrimaryKey() != null && !schema.getPrimaryKey().isEmpty()) {
                table.setPrimaryKey(schema.getPrimaryKey());
            }
            if (schema.getPartitionKeysCount() > 0) {
                table.setPartitionKeys(new java.util.ArrayList<>(schema.getPartitionKeysList()));
            }
            if (schema.getClusteringKeysCount() > 0) {
                table.setClusteringKeys(new java.util.ArrayList<>(schema.getClusteringKeysList()));
            }

            if (schema.getReplicationFactor() > 0) {
                Table.TableProperties props = new Table.TableProperties();
                props.setReplicationFactor(schema.getReplicationFactor());
                table.setProperties(props);
            }

            return table;
        } catch (Exception e) {
            logger.warn("Failed to deserialize table: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * 序列化 Region 为字节数组
     * 格式：
     * regionId|tableName|startKeyBase64|endKeyBase64|mysqlUrl|mysqlUser|mysqlPassword|primaryServer|replicaServers
     *
     * primaryServer 采用 host:port
     * replicaServers 采用 host:port,host:port
     *
     * 空字节数组会被编码为空字符串
     */
    private byte[] serializeRegion(Region region) {
        try {
            String data = region.getRegionId() + "|" +
                          region.getTableName() + "|" +
                          Base64.getEncoder().encodeToString(region.getStartKey()) + "|" +
                          Base64.getEncoder().encodeToString(region.getEndKey()) + "|" +
                          encodeServer(region.getPrimary()) + "|" +
                          encodeReplicas(region.getReplicas());
            return data.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.warn("Failed to serialize region: {}", e.getMessage(), e);
            return new byte[0];
        }
    }

    /**
     * 反序列化字节数组为 Region
     * 格式：regionId|tableName|startKeyBase64|endKeyBase64|primaryServer|replicaServers
     */
    private Region deserializeRegion(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            String dataStr = new String(data, "UTF-8");
            String[] parts = dataStr.split("\\|");

            if (parts.length < 4) {
                logger.warn("Invalid region data format (expected at least 4 pipe-delimited fields, got {})", parts.length);
                return null;
            }

            Region region = new Region();
            region.setRegionId(parts[0]);
            region.setTableName(parts[1]);
            region.setStartKey(Base64.getDecoder().decode(parts[2]));
            region.setEndKey(Base64.getDecoder().decode(parts[3]));

            if (parts.length >= 8 && !parts[7].isEmpty()) {
                ServerId primary = decodeServer(parts[7]);
                if (primary != null) {
                    region.setPrimary(primary);
                    region.addReplica(primary);
                }
            }

            if (parts.length >= 9 && !parts[8].isEmpty()) {
                for (ServerId replica : decodeReplicas(parts[8])) {
                    region.addReplica(replica);
                }
            }

            return region;
        } catch (Exception e) {
            logger.warn("Failed to deserialize region (Java serialization no longer supported): {}", e.getMessage(), e);
            return null;
        }
    }

    private void restoreReplicaTopology(String regionPath, Region region) {
        try {
            String primaryPath = regionPath + "/primary";
            if (zkClient.exists(primaryPath)) {
                byte[] primaryData = zkClient.getData(primaryPath);
                if (primaryData != null && primaryData.length > 0) {
                    String address = new String(primaryData, StandardCharsets.UTF_8);
                    ServerId primary = decodeServer(address);
                    if (primary != null) {
                        region.setPrimary(primary);
                        region.addReplica(primary);
                    }
                }
            }

            String replicasPath = regionPath + "/replicas";
            if (zkClient.exists(replicasPath)) {
                byte[] replicasData = zkClient.getData(replicasPath);
                if (replicasData != null && replicasData.length > 0) {
                    region.setReplicas(new ArrayList<>());
                    for (ServerId replica : decodeReplicas(new String(replicasData, StandardCharsets.UTF_8))) {
                        region.addReplica(replica);
                    }
                    if (region.getPrimary() != null && !region.getReplicas().contains(region.getPrimary())) {
                        region.addReplica(region.getPrimary());
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to restore replica topology for {}: {}", region.getRegionId(), e.getMessage(), e);
        }
    }

    private String encodeServer(ServerId serverId) {
        if (serverId == null) {
            return "";
        }
        return serverId.getHost() + ":" + serverId.getPort();
    }

    private String encodeReplicas(List<ServerId> replicas) {
        if (replicas == null || replicas.isEmpty()) {
            return "";
        }
        List<String> addresses = new ArrayList<>();
        for (ServerId replica : replicas) {
            if (replica != null) {
                addresses.add(encodeServer(replica));
            }
        }
        return String.join(",", addresses);
    }

    private List<ServerId> decodeReplicas(String encodedReplicas) {
        if (encodedReplicas == null || encodedReplicas.isEmpty()) {
            return Collections.emptyList();
        }
        List<ServerId> replicas = new ArrayList<>();
        for (String encodedReplica : encodedReplicas.split(",")) {
            ServerId replica = decodeServer(encodedReplica);
            if (replica != null) {
                replicas.add(replica);
            }
        }
        return replicas;
    }

    private ServerId decodeServer(String encodedServer) {
        if (encodedServer == null || encodedServer.isEmpty()) {
            return null;
        }
        int separator = encodedServer.lastIndexOf(':');
        if (separator <= 0 || separator >= encodedServer.length() - 1) {
            return null;
        }
        try {
            String host = encodedServer.substring(0, separator);
            int port = Integer.parseInt(encodedServer.substring(separator + 1));
            return new ServerId(host, port);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
