package com.minisql.regionserver;

import com.google.protobuf.ByteString;
import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.storage.RocksDBConfig;
import com.minisql.storage.RocksDBEngineFactory;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionServerServiceImpl tests")
class RegionServerServiceImplTest {

    private RegionServer regionServer;
    private RegionServerServiceImpl service;
    private String testWalPath;
    private RocksDBEngineFactory engineFactory;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        testWalPath = tempDir.resolve("wal").toString();
        String dataDir = tempDir.resolve("rocksdb").toString();
        engineFactory = new RocksDBEngineFactory(
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
        } catch (Exception e) {
            // ignore
        }
    }

    private Region openTestRegion(String regionId) {
        Region region = new Region(regionId, "test_table", new byte[]{0x00}, new byte[]{0x7F});
        regionServer.getRegionManager().openRegion(region);
        return region;
    }

    private void insertRow(String regionId, String rowKey, String qualifier, byte[] value) {
        KeyValue kv = new KeyValue();
        kv.setRowKey(rowKey.getBytes());
        kv.setFamily("");
        kv.setQualifier(qualifier);
        kv.setValue(value);
        kv.setType(KeyValue.Type.PUT);
        kv.setTimestamp(System.nanoTime());
        try {
            regionServer.put(regionId, List.of(kv), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ================================
    // promoteToPrimary (existing test kept)
    // ================================

    @Test
    @DisplayName("promoteToPrimary updates local primary flag fencing token and replica group")
    void testPromoteToPrimaryUpdatesLocalState() {
        RegionServer rs = new RegionServer("localhost", 16020, null, null, 3, testWalPath + "-promote");
        Region region = new Region("region-promote", "products", "a".getBytes(), "z".getBytes());
        ServerId oldPrimary = new ServerId("other-primary", 16021);
        region.setPrimary(oldPrimary);
        region.setReplicas(List.of(oldPrimary, rs.getServerId()));
        rs.getRegionManager().registerRegionInternal(region);
        rs.getRegionManager().setRegionState(region.getRegionId(), RegionManager.RegionState.OPEN);
        rs.getRegionManager().demoteToReplica(region.getRegionId());
        ReplicationCoordinator replicationCoordinator = rs.getReplicationCoordinator();
        replicationCoordinator.createReplicaGroup(region, List.of(oldPrimary, rs.getServerId()));

        RegionServerServiceImpl svc = new RegionServerServiceImpl(rs);
        CapturingObserver<RegionServerProto.PromoteResponse> obs = new CapturingObserver<>();

        svc.promoteToPrimary(
            RegionServerProto.PromoteRequest.newBuilder()
                .setRegionId(region.getRegionId())
                .setFencingToken(7L)
                .build(),
            obs
        );

        assertTrue(obs.value.getStatus().getSuccess());
        assertNull(obs.error);
        assertTrue(obs.completed);
        assertTrue(rs.getRegionManager().isPrimary(region.getRegionId()));
        assertEquals(7L, rs.getRegionManager().getFencingToken(region.getRegionId()));

        // Cleanup
        try { rs.stop(); } catch (Exception ignored) {}
    }

    // ================================
    // scan
    // ================================

    @Nested
    @DisplayName("scan RPC")
    class ScanRpc {

        @Test
        @DisplayName("full table scan returns all rows")
        void testFullScan() {
            String regionId = "scan-test-1";
            openTestRegion(regionId);
            insertRow(regionId, "row1", "name", "Alice".getBytes());
            insertRow(regionId, "row2", "name", "Bob".getBytes());

            CapturingObserver<RegionServerProto.GetResponse> obs = new CapturingObserver<>();
            service.get(
                RegionServerProto.GetRequest.newBuilder()
                    .setRegionId(regionId)
                    .setRowKey(ByteString.copyFromUtf8("row1"))
                    .build(),
                obs
            );
            assertTrue(obs.value.getStatus().getSuccess());
            assertFalse(obs.value.getKeyValuesList().isEmpty());
        }
    }

    // ================================
    // replicate RPC
    // ================================

    @Nested
    @DisplayName("replicate RPC")
    class ReplicateRpc {

        @Test
        @DisplayName("replicate applies mutations successfully")
        void testReplicateApplies() {
            String regionId = "repl-test-1";
            openTestRegion(regionId);

            RegionServerProto.LogEntry logEntry = RegionServerProto.LogEntry.newBuilder()
                .setSequenceId(1L)
                .setTimestamp(System.currentTimeMillis())
                .addMutations(CommonProto.KeyValue.newBuilder()
                    .setRowKey(ByteString.copyFromUtf8("row1"))
                    .setColumnFamily("")
                    .setQualifier("name")
                    .setValue(ByteString.copyFromUtf8("Alice"))
                    .setType(CommonProto.KeyValueType.PUT)
                    .build())
                .build();

            CapturingObserver<RegionServerProto.ReplicateResponse> obs = new CapturingObserver<>();
            service.replicate(
                RegionServerProto.ReplicateRequest.newBuilder()
                    .setRegionId(regionId)
                    .addEntries(logEntry)
                    .build(),
                obs
            );

            assertTrue(obs.value.getStatus().getSuccess());
            assertEquals(1L, obs.value.getLastAppliedSeqId());

            // Verify data was written
            KeyValue result = regionServer.get(regionId, "row1".getBytes());
            assertNotNull(result);
            assertEquals("Alice", new String(result.getValue()));
        }

        @Test
        @DisplayName("replicate skips already-applied entries")
        void testReplicateDedup() {
            String regionId = "repl-test-2";
            openTestRegion(regionId);

            // First replicate with seqId=5
            RegionServerProto.LogEntry entry5 = RegionServerProto.LogEntry.newBuilder()
                .setSequenceId(5L)
                .setTimestamp(System.currentTimeMillis())
                .addMutations(CommonProto.KeyValue.newBuilder()
                    .setRowKey(ByteString.copyFromUtf8("row1"))
                    .setColumnFamily("")
                    .setQualifier("name")
                    .setValue(ByteString.copyFromUtf8("first"))
                    .setType(CommonProto.KeyValueType.PUT))
                .build();

            CapturingObserver<RegionServerProto.ReplicateResponse> obs1 = new CapturingObserver<>();
            service.replicate(
                RegionServerProto.ReplicateRequest.newBuilder()
                    .setRegionId(regionId)
                    .addEntries(entry5)
                    .build(),
                obs1
            );
            assertTrue(obs1.value.getStatus().getSuccess());
            assertEquals(5L, obs1.value.getLastAppliedSeqId());

            // Second replicate with seqId=3 (should be skipped)
            RegionServerProto.LogEntry entry3 = RegionServerProto.LogEntry.newBuilder()
                .setSequenceId(3L)
                .setTimestamp(System.currentTimeMillis())
                .addMutations(CommonProto.KeyValue.newBuilder()
                    .setRowKey(ByteString.copyFromUtf8("row2"))
                    .setColumnFamily("")
                    .setQualifier("name")
                    .setValue(ByteString.copyFromUtf8("stale"))
                    .setType(CommonProto.KeyValueType.PUT))
                .build();

            CapturingObserver<RegionServerProto.ReplicateResponse> obs2 = new CapturingObserver<>();
            service.replicate(
                RegionServerProto.ReplicateRequest.newBuilder()
                    .setRegionId(regionId)
                    .addEntries(entry3)
                    .build(),
                obs2
            );
            assertTrue(obs2.value.getStatus().getSuccess());
            // seqId 3 should be skipped, lastApplied stays at 5
            assertEquals(5L, obs2.value.getLastAppliedSeqId());
            // row2 should NOT exist (seqId 3 was skipped)
            KeyValue result = regionServer.get(regionId, "row2".getBytes());
            assertNull(result);
        }

        @Test
        @DisplayName("replicate rejects entry with wrong checksum")
        void testReplicateChecksumMismatch() {
            String regionId = "repl-test-3";
            openTestRegion(regionId);

            RegionServerProto.LogEntry logEntry = RegionServerProto.LogEntry.newBuilder()
                .setSequenceId(1L)
                .setTimestamp(System.currentTimeMillis())
                .setChecksum(99999L) // wrong checksum
                .addMutations(CommonProto.KeyValue.newBuilder()
                    .setRowKey(ByteString.copyFromUtf8("row1"))
                    .setColumnFamily("")
                    .setQualifier("name")
                    .setValue(ByteString.copyFromUtf8("Alice"))
                    .setType(CommonProto.KeyValueType.PUT))
                .build();

            CapturingObserver<RegionServerProto.ReplicateResponse> obs = new CapturingObserver<>();
            service.replicate(
                RegionServerProto.ReplicateRequest.newBuilder()
                    .setRegionId(regionId)
                    .addEntries(logEntry)
                    .build(),
                obs
            );

            assertFalse(obs.value.getStatus().getSuccess());
        }
    }

    // ================================
    // streamSnapshot
    // ================================

    @Nested
    @DisplayName("streamSnapshot RPC")
    class StreamSnapshotRpc {

        @Test
        @DisplayName("streaming snapshot applies data correctly")
        void testStreamSnapshotApplies() {
            String regionId = "snap-test-1";
            openTestRegion(regionId);

            // Create a mock response observer
            java.util.concurrent.CompletableFuture<RegionServerProto.StreamSnapshotResponse> future =
                new java.util.concurrent.CompletableFuture<>();

            io.grpc.stub.StreamObserver<RegionServerProto.StreamSnapshotRequest> requestObserver =
                service.streamSnapshot(new io.grpc.stub.StreamObserver<>() {
                    @Override
                    public void onNext(RegionServerProto.StreamSnapshotResponse response) {
                        future.complete(response);
                    }
                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }
                    @Override
                    public void onCompleted() {}
                });

            requestObserver.onNext(
                RegionServerProto.StreamSnapshotRequest.newBuilder()
                    .setRegionId(regionId)
                    .addBatch(CommonProto.KeyValue.newBuilder()
                        .setRowKey(ByteString.copyFromUtf8("row1"))
                        .setColumnFamily("")
                        .setQualifier("name")
                        .setValue(ByteString.copyFromUtf8("Alice"))
                        .setType(CommonProto.KeyValueType.PUT))
                    .addBatch(CommonProto.KeyValue.newBuilder()
                        .setRowKey(ByteString.copyFromUtf8("row2"))
                        .setColumnFamily("")
                        .setQualifier("name")
                        .setValue(ByteString.copyFromUtf8("Bob"))
                        .setType(CommonProto.KeyValueType.PUT))
                    .setIsFinal(true)
                    .setFinalSequenceId(10L)
                    .build()
            );
            requestObserver.onCompleted();

            try {
                RegionServerProto.StreamSnapshotResponse response = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertTrue(response.getStatus().getSuccess());
                assertEquals(2, response.getTotalApplied());

                // Verify data
                KeyValue r1 = regionServer.get(regionId, "row1".getBytes());
                assertNotNull(r1);
                assertEquals("Alice", new String(r1.getValue()));
            } catch (Exception e) {
                fail("Snapshot streaming failed: " + e.getMessage());
            }
        }
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
