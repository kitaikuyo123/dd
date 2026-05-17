package com.minisql.regionserver;

import com.google.protobuf.ByteString;
import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.storage.RocksDBConfig;
import com.minisql.storage.RocksDBEngineFactory;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionServer error path tests")
class RegionServerErrorPathTest {

    private RegionServer regionServer;
    private RegionServerServiceImpl service;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        String testWalPath = tempDir.resolve("wal").toString();
        String dataDir = tempDir.resolve("rocksdb").toString();
        RocksDBEngineFactory engineFactory = new RocksDBEngineFactory(
            RocksDBConfig.builder(dataDir).enableWal(false).build()
        );

        regionServer = new RegionServer(
            "localhost", 16020,
            engineFactory,
            null, 3, testWalPath
        );
        regionServer.getReplicationCoordinator().start();
        service = new RegionServerServiceImpl(regionServer);
    }

    @AfterEach
    void tearDown() {
        try {
            regionServer.stop();
        } catch (Exception ignored) {
        }
    }

    private Region openTestRegion(String regionId) {
        Region region = new Region(regionId, "test_table", new byte[]{0x00}, new byte[]{0x7F});
        regionServer.getRegionManager().openRegion(region);
        return region;
    }

    private Region openReplicaRegion(String regionId) {
        Region region = new Region(regionId, "test_table", new byte[]{0x00}, new byte[]{0x7F});
        region.setPrimary(new ServerId("other-host", 16021));
        regionServer.getRegionManager().openRegion(region);
        regionServer.getRegionManager().demoteToReplica(regionId);
        return region;
    }

    // ================================
    // Put error paths
    // ================================

    @Test
    @DisplayName("put on non-primary region returns error status")
    void putWhenRegionNotPrimary() {
        String regionId = "err-not-primary";
        openReplicaRegion(regionId);

        CapturingObserver<RegionServerProto.PutResponse> obs = new CapturingObserver<>();
        service.put(
            RegionServerProto.PutRequest.newBuilder()
                .setRegionId(regionId)
                .addKeyValues(CommonProto.KeyValue.newBuilder()
                    .setRowKey(ByteString.copyFromUtf8("row1"))
                    .setColumnFamily("")
                    .setQualifier("name")
                    .setValue(ByteString.copyFromUtf8("Alice"))
                    .setType(CommonProto.KeyValueType.PUT)
                    .build())
                .build(),
            obs
        );

        assertFalse(obs.value.getStatus().getSuccess());
        assertTrue(obs.value.getStatus().getMessage().contains("not primary"));
    }

    @Test
    @DisplayName("get on non-primary region returns error status")
    void getWhenRegionNotPrimary() {
        String regionId = "err-get-not-primary";
        openReplicaRegion(regionId);

        CapturingObserver<RegionServerProto.GetResponse> obs = new CapturingObserver<>();
        service.get(
            RegionServerProto.GetRequest.newBuilder()
                .setRegionId(regionId)
                .setRowKey(ByteString.copyFromUtf8("row1"))
                .build(),
            obs
        );

        assertFalse(obs.value.getStatus().getSuccess());
        assertTrue(obs.value.getStatus().getMessage().contains("not primary"));
    }

    @Test
    @DisplayName("get on nonexistent region returns error status")
    void getWhenRegionDoesNotExist() {
        CapturingObserver<RegionServerProto.GetResponse> obs = new CapturingObserver<>();
        service.get(
            RegionServerProto.GetRequest.newBuilder()
                .setRegionId("ghost-region")
                .setRowKey(ByteString.copyFromUtf8("row1"))
                .build(),
            obs
        );

        assertFalse(obs.value.getStatus().getSuccess());
    }

    @Test
    @DisplayName("put on nonexistent region returns error status")
    void putWhenRegionDoesNotExist() {
        CapturingObserver<RegionServerProto.PutResponse> obs = new CapturingObserver<>();
        service.put(
            RegionServerProto.PutRequest.newBuilder()
                .setRegionId("ghost-region")
                .addKeyValues(CommonProto.KeyValue.newBuilder()
                    .setRowKey(ByteString.copyFromUtf8("row1"))
                    .setColumnFamily("")
                    .setQualifier("name")
                    .setValue(ByteString.copyFromUtf8("Alice"))
                    .setType(CommonProto.KeyValueType.PUT)
                    .build())
                .build(),
            obs
        );

        assertFalse(obs.value.getStatus().getSuccess());
    }

    @Test
    @DisplayName("scan on non-primary region returns error status")
    void scanWhenRegionNotPrimary() {
        String regionId = "err-scan-not-primary";
        openReplicaRegion(regionId);

        CapturingObserver<RegionServerProto.ScanResponse> obs = new CapturingObserver<>();
        service.scan(
            RegionServerProto.ScanRequest.newBuilder()
                .setRegionId(regionId)
                .build(),
            obs
        );

        assertFalse(obs.value.getStatus().getSuccess());
        assertTrue(obs.value.getStatus().getMessage().contains("not primary"));
    }

    // ================================
    // Split error paths
    // ================================

    @Test
    @DisplayName("findSplitPoint on region with no storage throws IOException")
    void findSplitPointWithNoStorage() {
        RegionSplitService splitService = new RegionSplitService(regionServer.getRegionManager());

        Region region = new Region("no-storage-region", "test_table",
            new byte[]{0x00}, new byte[]{0x7F});
        regionServer.getRegionManager().registerRegionInternal(region);

        assertThrows(java.io.IOException.class,
            () -> splitService.findBestSplitPoint("no-storage-region"));
    }

    // ================================
    // Helper
    // ================================

    private static final class CapturingObserver<T> implements StreamObserver<T> {
        T value;
        Throwable error;
        boolean completed;

        @Override
        public void onNext(T value) { this.value = value; }
        @Override
        public void onError(Throwable t) { this.error = t; }
        @Override
        public void onCompleted() { this.completed = true; }
    }
}
