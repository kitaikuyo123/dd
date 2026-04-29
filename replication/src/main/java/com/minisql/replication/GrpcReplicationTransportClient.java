package com.minisql.replication;

import com.google.protobuf.ByteString;
import com.minisql.common.model.KeyValue;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.common.proto.RegionServerServiceGrpc;
import com.minisql.common.rpc.GrpcChannelFactory;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

/** 基于 gRPC 的复制传输客户端实现 */
public class GrpcReplicationTransportClient implements ReplicationTransportClient {

    private static final Logger logger = LoggerFactory.getLogger(GrpcReplicationTransportClient.class);

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    @Override
    public boolean replicate(ServerId replica, String regionId, ReplicationLogEntry entry, long timeoutMs) {
        return replicateBatch(replica, regionId, java.util.Collections.singletonList(entry), timeoutMs);
    }

    @Override
    public boolean replicateBatch(ServerId replica, String regionId, List<ReplicationLogEntry> entries, long timeoutMs) {
        try {
            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub = newStub(replica, timeoutMs);
            RegionServerProto.ReplicateRequest.Builder requestBuilder =
                RegionServerProto.ReplicateRequest.newBuilder()
                    .setRegionId(regionId);

            for (ReplicationLogEntry entry : entries) {
                RegionServerProto.LogEntry.Builder logEntryBuilder = RegionServerProto.LogEntry.newBuilder()
                    .setSequenceId(entry.getSequenceId())
                    .setTimestamp(entry.getTimestamp());

                CRC32 crc32 = new CRC32();
                for (KeyValue kv : entry.getMutations()) {
                    CommonProto.KeyValue protoKv = toProto(kv);
                    logEntryBuilder.addMutations(protoKv);
                    crc32.update(protoKv.toByteArray());
                }
                logEntryBuilder.setChecksum(crc32.getValue());

                requestBuilder.addEntries(logEntryBuilder.build());
            }

            RegionServerProto.ReplicateResponse response = stub.replicate(requestBuilder.build());
            return response.getStatus().getSuccess();
        } catch (Exception e) {
            logger.warn("Batch replication RPC failed to {}: {}", replica, e.getMessage());
            return false;
        }
    }

    @Override
    public List<KeyValue> fetchSnapshot(ServerId primary, String regionId, long timeoutMs) {
        try {
            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub = newStub(primary, timeoutMs);
            Iterator<RegionServerProto.SnapshotResponse> responses = stub.getSnapshot(
                RegionServerProto.SnapshotRequest.newBuilder().setRegionId(regionId).build()
            );

            List<KeyValue> snapshot = new ArrayList<>();
            while (responses.hasNext()) {
                RegionServerProto.SnapshotResponse response = responses.next();
                if (!response.getStatus().getSuccess()) {
                    throw new IllegalStateException("Snapshot fetch failed: " + response.getStatus().getMessage());
                }
                for (CommonProto.KeyValue kvProto : response.getKeyValuesList()) {
                    snapshot.add(fromProto(kvProto));
                }
            }
            return snapshot;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to fetch snapshot from primary " + primary, e);
        }
    }

    @Override
    public boolean sendSnapshot(ServerId replica, String regionId, List<KeyValue> snapshot, int batchSize, long timeoutMs, long finalSequenceId) {
        try {
            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub = newStub(replica, timeoutMs);
            int effectiveBatchSize = Math.max(1, batchSize);
            for (int i = 0; i < snapshot.size(); i += effectiveBatchSize) {
                List<KeyValue> batch = snapshot.subList(i, Math.min(i + effectiveBatchSize, snapshot.size()));
                RegionServerProto.LogEntry.Builder entryBuilder = RegionServerProto.LogEntry.newBuilder()
                    .setSequenceId(0L)
                    .setTimestamp(System.currentTimeMillis());
                for (KeyValue kv : batch) {
                    entryBuilder.addMutations(toProto(kv));
                }
                RegionServerProto.ReplicateResponse response = stub.replicate(
                    RegionServerProto.ReplicateRequest.newBuilder()
                        .setRegionId(regionId)
                        .addEntries(entryBuilder.build())
                        .build()
                );
                if (!response.getStatus().getSuccess()) {
                    logger.warn("Snapshot apply failed on {} for region {}: {}",
                        replica, regionId, response.getStatus().getMessage());
                    return false;
                }
            }

            // 发送一条空 mutation + 真实 sequenceId，推进 Secondary 本地位点
            if (finalSequenceId > 0) {
                RegionServerProto.LogEntry progressEntry = RegionServerProto.LogEntry.newBuilder()
                    .setSequenceId(finalSequenceId)
                    .setTimestamp(System.currentTimeMillis())
                    .build();
                RegionServerProto.ReplicateResponse response = stub.replicate(
                    RegionServerProto.ReplicateRequest.newBuilder()
                        .setRegionId(regionId)
                        .addEntries(progressEntry)
                        .build()
                );
                if (!response.getStatus().getSuccess()) {
                    logger.warn("Snapshot progress marker failed on {} for region {}: {}",
                        replica, regionId, response.getStatus().getMessage());
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            logger.warn("Snapshot send failed to {}: {}", replica, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean sendSnapshotStreaming(ServerId replica, String regionId, List<KeyValue> snapshot,
                                          int batchSize, long timeoutMs, long finalSequenceId) {
        try {
            RegionServerServiceGrpc.RegionServerServiceStub asyncStub =
                RegionServerServiceGrpc.newStub(channelFor(replica))
                    .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);

            java.util.concurrent.CompletableFuture<RegionServerProto.StreamSnapshotResponse> future =
                new java.util.concurrent.CompletableFuture<>();

            io.grpc.stub.StreamObserver<RegionServerProto.StreamSnapshotRequest> requestObserver =
                asyncStub.streamSnapshot(new io.grpc.stub.StreamObserver<>() {
                    @Override
                    public void onNext(RegionServerProto.StreamSnapshotResponse response) {
                        future.complete(response);
                    }
                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }
                    @Override
                    public void onCompleted() {
                        // handled in onNext
                    }
                });

            int effectiveBatchSize = Math.max(1, batchSize);
            for (int i = 0; i < snapshot.size(); i += effectiveBatchSize) {
                List<KeyValue> batch = snapshot.subList(i, Math.min(i + effectiveBatchSize, snapshot.size()));
                boolean isFinal = (i + effectiveBatchSize >= snapshot.size());

                RegionServerProto.StreamSnapshotRequest.Builder reqBuilder =
                    RegionServerProto.StreamSnapshotRequest.newBuilder()
                        .setRegionId(regionId)
                        .setIsFinal(isFinal);
                if (isFinal) {
                    reqBuilder.setFinalSequenceId(finalSequenceId);
                }
                for (KeyValue kv : batch) {
                    reqBuilder.addBatch(toProto(kv));
                }
                requestObserver.onNext(reqBuilder.build());
            }
            requestObserver.onCompleted();

            RegionServerProto.StreamSnapshotResponse response = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            return response.getStatus().getSuccess();
        } catch (Exception e) {
            logger.warn("Streaming snapshot failed to {}: {}", replica, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean streamSnapshotDirect(ServerId primary, ServerId replica, String regionId,
                                         int batchSize, long timeoutMs, long finalSequenceId) {
        try {
            // Open server-streaming fetch from primary
            RegionServerServiceGrpc.RegionServerServiceBlockingStub primaryStub =
                newStub(primary, timeoutMs);
            Iterator<RegionServerProto.SnapshotResponse> responses = primaryStub.getSnapshot(
                RegionServerProto.SnapshotRequest.newBuilder().setRegionId(regionId).build());

            // Open client-streaming send to replica
            RegionServerServiceGrpc.RegionServerServiceStub replicaAsyncStub =
                RegionServerServiceGrpc.newStub(channelFor(replica))
                    .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);

            java.util.concurrent.CompletableFuture<RegionServerProto.StreamSnapshotResponse> resultFuture =
                new java.util.concurrent.CompletableFuture<>();

            io.grpc.stub.StreamObserver<RegionServerProto.StreamSnapshotRequest> requestObserver =
                replicaAsyncStub.streamSnapshot(new io.grpc.stub.StreamObserver<>() {
                    @Override
                    public void onNext(RegionServerProto.StreamSnapshotResponse response) {
                        resultFuture.complete(response);
                    }
                    @Override
                    public void onError(Throwable t) {
                        resultFuture.completeExceptionally(t);
                    }
                    @Override
                    public void onCompleted() {
                    }
                });

            // Pipe each batch from primary's snapshot response directly to replica
            int batchCount = 0;
            boolean lastBatch = false;
            while (responses.hasNext()) {
                RegionServerProto.SnapshotResponse response = responses.next();
                if (!response.getStatus().getSuccess()) {
                    requestObserver.onError(new IllegalStateException(
                        "Snapshot fetch failed: " + response.getStatus().getMessage()));
                    return false;
                }

                List<CommonProto.KeyValue> batchKvs = response.getKeyValuesList();
                lastBatch = !responses.hasNext();

                RegionServerProto.StreamSnapshotRequest.Builder reqBuilder =
                    RegionServerProto.StreamSnapshotRequest.newBuilder()
                        .setRegionId(regionId)
                        .setIsFinal(lastBatch);
                if (lastBatch) {
                    reqBuilder.setFinalSequenceId(finalSequenceId);
                }
                reqBuilder.addAllBatch(batchKvs);
                requestObserver.onNext(reqBuilder.build());
                batchCount++;
            }
            requestObserver.onCompleted();

            RegionServerProto.StreamSnapshotResponse result = resultFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
            logger.info("Streamed {} batches directly from {} to {} for region {}",
                batchCount, primary, replica, regionId);
            return result.getStatus().getSuccess();
        } catch (Exception e) {
            logger.warn("Direct snapshot streaming from {} to {} failed: {}", primary, replica, e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                        logger.warn("gRPC channel did not terminate in time");
                    }
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
    }

    private RegionServerServiceGrpc.RegionServerServiceBlockingStub newStub(ServerId serverId, long timeoutMs) {
        return RegionServerServiceGrpc.newBlockingStub(channelFor(serverId))
            .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private ManagedChannel channelFor(ServerId serverId) {
        String key = serverId.getHost() + ":" + serverId.getPort();
        return channels.computeIfAbsent(key, ignored ->
            GrpcChannelFactory.newChannel(serverId.getHost(), serverId.getPort()));
    }

    /**
     * Remove and shut down the cached channel for the given server.
     * Call this when a RegionServer is permanently decommissioned to prevent
     * unbounded growth of the channels map.
     */
    public void removeChannel(ServerId serverId) {
        String key = serverId.getHost() + ":" + serverId.getPort();
        ManagedChannel channel = channels.remove(key);
        if (channel != null) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private CommonProto.KeyValue toProto(KeyValue kv) {
        return CommonProto.KeyValue.newBuilder()
            .setRowKey(ByteString.copyFrom(kv.getRowKey()))
            .setColumnFamily(kv.getFamily() != null ? kv.getFamily() : "")
            .setQualifier(kv.getQualifier() != null ? kv.getQualifier() : "")
            .setTimestamp(kv.getTimestamp())
            .setValue(ByteString.copyFrom(kv.getValue() != null ? kv.getValue() : new byte[0]))
            .setType(kv.getType() == KeyValue.Type.PUT ? CommonProto.KeyValueType.PUT : CommonProto.KeyValueType.DELETE)
            .build();
    }

    private KeyValue fromProto(CommonProto.KeyValue kvProto) {
        KeyValue kv = new KeyValue();
        kv.setRowKey(kvProto.getRowKey().toByteArray());
        kv.setFamily(kvProto.getColumnFamily());
        kv.setQualifier(kvProto.getQualifier());
        kv.setTimestamp(kvProto.getTimestamp());
        kv.setValue(kvProto.getValue().toByteArray());
        kv.setType(kvProto.getType() == CommonProto.KeyValueType.PUT ? KeyValue.Type.PUT : KeyValue.Type.DELETE);
        return kv;
    }
}
