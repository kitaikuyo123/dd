package com.minisql.regionserver;

import com.minisql.common.model.Column;
import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.MasterProto;
import com.minisql.common.proto.MasterServiceGrpc;
import com.minisql.replication.ReplicationConfig;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.replication.ReplicationWAL;
import com.minisql.replication.GrpcReplicationTransportClient;
import com.minisql.common.rpc.GrpcChannelFactory;
import com.minisql.storage.StorageEngineFactory;
import com.minisql.storage.StorageScanFilter;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** RegionServer 主运行时，管理本节点的 Region、存储和复制 */
public class RegionServer {

    private static final Logger logger = LoggerFactory.getLogger(RegionServer.class);

    private final ServerId serverId;
    private final RegionManager regionManager;
    private final RegionSplitService splitService;
    private final RegionMergeService mergeService;
    private final StorageEngineFactory engineFactory;
    private final String masterAddress;

    private final ReplicationCoordinator replicationCoordinator;
    private final ReplicationWAL wal;

    private volatile boolean running = false;
    private volatile boolean draining = false;
    private volatile boolean shutdownHookRegistered = false;
    private static final long DRAIN_TIMEOUT_MS = 30_000L;
    private ManagedChannel masterChannel;
    private MasterServiceGrpc.MasterServiceBlockingStub masterStub;

    private Server grpcServer;

    public RegionServer(String host, int port, StorageEngineFactory engineFactory,
                        String masterAddress, int replicationFactor, String walPath) {
        this.serverId = new ServerId(host, port);
        this.engineFactory = engineFactory;
        this.masterAddress = masterAddress;

        this.regionManager = new RegionManager(this);
        this.splitService = new RegionSplitService(regionManager);
        this.mergeService = new RegionMergeService(regionManager);

        this.wal = new ReplicationWAL(walPath);
        this.replicationCoordinator = new ReplicationCoordinator(
            ReplicationConfig.builder(replicationFactor).build(),
            wal,
            new GrpcReplicationTransportClient()
        );
    }

    public void start() throws IOException {
        running = true;

        replicationCoordinator.start();
        logger.info("ReplicationCoordinator started");

        connectToMaster();
        startGrpcServer();

        logger.info("RegionServer started on {}:{}", serverId.getHost(), serverId.getPort());
        logger.info("Using storage engine: {}", engineFactory.getClass().getSimpleName().replace("EngineFactory", ""));

        if (!shutdownHookRegistered) {
            shutdownHookRegistered = true;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("*** Shutting down gRPC server since JVM is shutting down");
                try {
                    stop();
                } catch (Exception e) {
                    logger.error("Error stopping server", e);
                }
                logger.info("*** Server shut down");
            }));
        }
    }

    private void connectToMaster() {
        if (masterAddress == null || masterAddress.isEmpty()) {
            logger.warn("Master address not configured, skipping Master connection");
            return;
        }

        logger.info("Connecting to Master at {}", masterAddress);
        String[] parts = masterAddress.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 16000;

        masterChannel = GrpcChannelFactory.newChannel(host, port);
        masterStub = MasterServiceGrpc.newBlockingStub(masterChannel);
        logger.info("Connected to Master successfully");
    }

    public Table getTableSchema(String tableName) {
        if (masterStub == null) {
            logger.warn("Master connection not available");
            return null;
        }

        try {
            MasterProto.GetTableSchemaRequest request = MasterProto.GetTableSchemaRequest.newBuilder()
                .setTableName(tableName)
                .build();
            MasterProto.GetTableSchemaResponse response = masterStub.getTableSchema(request);

            if (response.getStatus().getSuccess()) {
                return convertProtoToTable(response.getSchema());
            }
            logger.warn("Failed to get table schema: {}", response.getStatus().getMessage());
            return null;
        } catch (Exception e) {
            logger.error("Error getting table schema for " + tableName, e);
            return null;
        }
    }

    private Table convertProtoToTable(CommonProto.TableSchema proto) {
        Table table = new Table();
        table.setTableName(proto.getTableName());

        String primaryKey = proto.getPrimaryKey();
        if (primaryKey != null && !primaryKey.isEmpty()) {
            table.setPrimaryKey(primaryKey);
        }

        for (CommonProto.ColumnSchema colProto : proto.getColumnsList()) {
            Column col = new Column();
            col.setName(colProto.getName());
            col.setType(Column.ColumnType.valueOf(colProto.getType()));
            col.setNullable(colProto.getNullable());
            table.addColumn(col);
        }

        return table;
    }

    private void startGrpcServer() throws IOException {
        RegionServerServiceImpl service = new RegionServerServiceImpl(this);
        logger.info("Created RegionServerServiceImpl, binding to port {}", serverId.getPort());

        grpcServer = ServerBuilder.forPort(serverId.getPort())
            .addService(service)
            .maxInboundMessageSize(64 * 1024 * 1024)
            .build()
            .start();

        logger.info("gRPC server started on port {}", serverId.getPort());
    }

    /**
     * Graceful shutdown: drain inflight requests, flush regions, then tear down
     * components in reverse dependency order.
     *
     * <p>Callers (RegionServerMain) should stop the HeartbeatSender first, which
     * causes the Master to detect this server going away via missed heartbeats
     * (faster than waiting for the ZooKeeper ephemeral node to expire at ~30s).
     */
    public void stop() throws Exception {
        running = false;
        draining = true;
        logger.info("RegionServer {} beginning graceful shutdown...", serverId);

        // 1. Shutdown gRPC server gracefully — stop accepting new requests,
        //    but allow inflight RPCs to complete within the drain timeout.
        if (grpcServer != null) {
            logger.info("Shutting down gRPC server (drain timeout: {}ms)...", DRAIN_TIMEOUT_MS);
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(DRAIN_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                    logger.warn("gRPC server did not drain in time, forcing shutdown");
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("gRPC server stopped");
        }

        // 2. Flush all open regions to persist pending writes before closing
        for (Region region : regionManager.getAllRegions()) {
            try {
                RegionStorage storage = regionManager.getRegionStorage(region.getRegionId());
                if (storage != null) {
                    storage.flush();
                }
            } catch (Exception e) {
                logger.warn("Flush failed for region {}: {}", region.getRegionId(), e.getMessage());
            }
        }

        // 3. Stop replication (stop accepting new write log entries)
        replicationCoordinator.stop();

        // 4. Close all regions
        for (Region region : regionManager.getAllRegions()) {
            try {
                regionManager.closeRegion(region.getRegionId(), false);
            } catch (Exception e) {
                logger.warn("Close region {} failed: {}", region.getRegionId(), e.getMessage());
            }
        }

        // 5. Close Master channel last
        if (masterChannel != null && !masterChannel.isShutdown()) {
            masterChannel.shutdown();
            try {
                if (!masterChannel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    masterChannel.shutdownNow();
                }
            } catch (InterruptedException e) {
                masterChannel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        logger.info("RegionServer {} stopped gracefully", serverId);
    }

    public boolean isDraining() {
        return draining;
    }

    public void put(String regionId, List<KeyValue> keyValues) throws Exception {
        put(regionId, keyValues, false);
    }

    public void put(String regionId, List<KeyValue> keyValues, boolean bypassWriteBlock) throws Exception {
        logger.debug("Starting put for region: {}", regionId);

        checkRegionOpen(regionId);
        logger.debug("Region is open: {}", regionId);

        if (!bypassWriteBlock && regionManager.isWriteBlocked(regionId)) {
            throw new IllegalStateException("Region is temporarily read-only during migration: " + regionId);
        }

        RegionStorage storage = regionManager.getRegionStorage(regionId);
        if (storage == null) {
            logger.error("Region storage not found: {}", regionId);
            throw new IllegalStateException("Region storage not found: " + regionId);
        }

        logger.debug("Storage found, writing data...");
        storage.put(keyValues);
        logger.debug("Put completed successfully for region: {}", regionId);
    }

    public List<KeyValue> get(String regionId, byte[] rowKey) {
        checkRegionOpen(regionId);

        RegionStorage storage = regionManager.getRegionStorage(regionId);
        if (storage == null) {
            throw new IllegalStateException("Region storage not found: " + regionId);
        }
        return storage.get(rowKey);
    }

    public Iterator<KeyValue> scan(String regionId, byte[] startKey, byte[] endKey) {
        checkRegionOpen(regionId);

        RegionStorage storage = regionManager.getRegionStorage(regionId);
        if (storage == null) {
            throw new IllegalStateException("Region storage not found: " + regionId);
        }
        return storage.scan(startKey, endKey);
    }

    public Iterator<KeyValue> scan(String regionId, StorageScanFilter filter) {
        checkRegionOpen(regionId);

        RegionStorage storage = regionManager.getRegionStorage(regionId);
        if (storage == null) {
            throw new IllegalStateException("Region storage not found: " + regionId);
        }
        return storage.scan(filter);
    }

    public void delete(String regionId, byte[] rowKey) throws Exception {
        delete(regionId, rowKey, false);
    }

    public void delete(String regionId, byte[] rowKey, boolean bypassWriteBlock) throws Exception {
        checkRegionOpen(regionId);

        if (!bypassWriteBlock && regionManager.isWriteBlocked(regionId)) {
            throw new IllegalStateException("Region is temporarily read-only during migration: " + regionId);
        }

        RegionStorage storage = regionManager.getRegionStorage(regionId);
        if (storage == null) {
            throw new IllegalStateException("Region storage not found: " + regionId);
        }
        storage.delete(rowKey);
    }


    private void checkRegionOpen(String regionId) {
        boolean isOpen = regionManager.isRegionOpen(regionId);
        logger.debug("Region {} is open: {}", regionId, isOpen);
        if (!isOpen) {
            RegionManager.RegionState state = regionManager.getRegionState(regionId);
            logger.error("Region {} state: {}", regionId, state);
            throw new IllegalStateException("Region is not open: " + regionId + ", state: " + state);
        }
    }

    public ServerId getServerId() {
        return serverId;
    }

    public RegionManager getRegionManager() {
        return regionManager;
    }

    public RegionSplitService getSplitService() {
        return splitService;
    }

    public RegionMergeService getMergeService() {
        return mergeService;
    }

    public boolean isRunning() {
        return running;
    }

    public StorageEngineFactory getEngineFactory() {
        return engineFactory;
    }

    public ReplicationCoordinator getReplicationCoordinator() {
        return replicationCoordinator;
    }

    public ReplicationWAL getWal() {
        return replicationCoordinator.getWal();
    }
}
