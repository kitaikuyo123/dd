package com.minisql.regionserver;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Row;
import com.minisql.common.model.RowAssembler;
import com.minisql.common.model.Table;
import com.minisql.common.model.Column;
import com.minisql.common.proto.*;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.common.utils.BytesUtil;
import com.minisql.replication.ReplicationCoordinator;
import com.minisql.replication.ReplicationLogEntry;
import com.minisql.replication.ReplicationWAL;
import com.minisql.sql.SQLParser;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.storage.StorageScanFilter;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import java.util.zip.CRC32;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * RegionServer gRPC 服务实现
 * 处理来自客户端和其他 RegionServer 的请求
 */
public class RegionServerServiceImpl extends RegionServerServiceGrpc.RegionServerServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(RegionServerServiceImpl.class);

    private final RegionServer regionServer;

    public RegionServerServiceImpl(RegionServer regionServer) {
        this.regionServer = regionServer;
        logger.info("RegionServerServiceImpl constructed with RegionServer: {}", regionServer.getServerId());
    }

    /**
     * 处理写入请求
     */
    @Override
    public void put(RegionServerProto.PutRequest request, StreamObserver<RegionServerProto.PutResponse> responseObserver) {
        logger.debug("PUT method called! Thread: {}", Thread.currentThread().getName());

        try {
            String regionId = request.getRegionId();
            List<KeyValue> keyValues = new ArrayList<>();

            if (!regionServer.getRegionManager().isPrimary(regionId)) {
                throw new IllegalStateException("Region is not primary on this server: " + regionId);
            }

            logger.info("Received put request for region: {}, KeyValues count: {}", regionId, request.getKeyValuesCount());

            // 转换 protobuf KeyValue 到模型 KeyValue
            for (CommonProto.KeyValue kvProto : request.getKeyValuesList()) {
                KeyValue kv = new KeyValue();
                kv.setRowKey(kvProto.getRowKey().toByteArray());
                kv.setFamily(kvProto.getColumnFamily());
                kv.setQualifier(kvProto.getQualifier());
                kv.setTimestamp(kvProto.getTimestamp());
                byte[] valueBytes = kvProto.getValue().toByteArray();
                kv.setValue(valueBytes);
                kv.setType(kvProto.getType() == CommonProto.KeyValueType.PUT ?
                    KeyValue.Type.PUT : KeyValue.Type.DELETE);
                logger.info("[RegionServer.put] KeyValue: rowKey={}, family={}, qualifier={}, value.length={}, value={}",
                    new String(kv.getRowKey()), kv.getFamily(), kv.getQualifier(),
                    kv.getValue() != null ? kv.getValue().length : 0,
                    kv.getValue() != null ? BytesUtil.bytesToHex(kv.getValue()) : "null");
                keyValues.add(kv);
            }

            logger.info("Converted keyValues, count: {}", keyValues.size());

            // === 新增：集成 WAL 和副本复制 ===

            // 1. 写入 WAL（预写日志）
            ReplicationWAL wal = regionServer.getWal();
            ReplicationCoordinator replicationCoordinator = regionServer.getReplicationCoordinator();
            ReplicationLogEntry replicationEntry = null;

            if (replicationCoordinator != null && wal != null) {
                replicationEntry = replicationCoordinator.logMutations(regionId, keyValues);
                logger.debug("WAL appended for region {}, sequenceId: {}", regionId, replicationEntry.getSequenceId());
            }

            // 2. 本地提交
            regionServer.put(regionId, keyValues, false);

            // 3. 复制到从副本（如果有副本组）
            if (replicationCoordinator != null && replicationCoordinator.getReplicaGroup(regionId) != null) {
                boolean replicationSuccess = replicationEntry != null
                    ? replicationCoordinator.replicateSync(regionId, replicationEntry)
                    : replicationCoordinator.replicateSync(regionId, keyValues);
                if (!replicationSuccess) {
                    logger.warn("Replication degraded for region {}: local write committed but replica sync failed", regionId);
                } else {
                    logger.debug("Replication completed for region {}", regionId);
                }
            } else {
                logger.debug("No replica group for region {}, skipping replication", regionId);
            }

            // 4. 标记 WAL 已应用
            if (wal != null) {
                try {
                    long appliedSequenceId = replicationEntry != null
                        ? replicationEntry.getSequenceId()
                        : wal.getCurrentSequenceId(regionId);
                    wal.markAsApplied(regionId, appliedSequenceId,
                        regionServer.getServerId().getHost() + ":" + regionServer.getServerId().getPort());
                } catch (Exception e) {
                    logger.warn("Failed to mark WAL as applied: {}", e.getMessage());
                }
            }

            logger.info("Put request completed successfully for region: {}", regionId);

            RegionServerProto.PutResponse response = RegionServerProto.PutResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error processing put request", e);
            RegionServerProto.PutResponse response = RegionServerProto.PutResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理读取请求
     */
    @Override
    public void get(RegionServerProto.GetRequest request, StreamObserver<RegionServerProto.GetResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            if (!regionServer.getRegionManager().canServeReads(regionId)) {
                throw new IOException("Region cannot serve reads on this server: " + regionId);
            }
            byte[] rowKey = request.getRowKey().toByteArray();
            RegionStorage storage = regionServer.getRegionManager().getRegionStorage(regionId);
            if (storage == null) {
                throw new IOException("Region storage not found: " + regionId);
            }

            List<KeyValue> results = storage.get(rowKey);

            RegionServerProto.GetResponse.Builder builder = RegionServerProto.GetResponse.newBuilder()
                .setStatus(createSuccessStatus());
            if (results != null) {
                for (KeyValue kv : results) {
                    builder.addKeyValues(convertToProto(kv));
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            RegionServerProto.GetResponse response = RegionServerProto.GetResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理扫描请求
     */
    @Override
    public void scan(RegionServerProto.ScanRequest request, StreamObserver<RegionServerProto.ScanResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            if (!regionServer.getRegionManager().canServeReads(regionId)) {
                throw new IOException("Region cannot serve reads on this server: " + regionId);
            }
            // proto3 中 bytes 字段没有 hasXxx() 方法，需要通过 isEmpty() 判断
            byte[] startKey = !request.getStartKey().isEmpty() ? request.getStartKey().toByteArray() : null;
            byte[] endKey = !request.getEndKey().isEmpty() ? request.getEndKey().toByteArray() : null;

            String whereClause = request.getWhereClause();
            Table tableSchema = null;
            Condition condition = null;
            PredicatePushdownPlanner.PushdownPlan pushdownPlan = PredicatePushdownPlanner.PushdownPlan.none();
            if (whereClause != null && !whereClause.isBlank()) {
                Region region = regionServer.getRegionManager().getRegion(regionId);
                if (region == null) {
                    throw new IOException("Region not found: " + regionId);
                }

                String tableName = request.getTableName();
                if (tableName == null || tableName.isBlank()) {
                    tableName = region.getTableName();
                }
                if (!region.getTableName().equalsIgnoreCase(tableName)) {
                    throw new IOException("Scan table does not match region table: " + tableName);
                }

                tableSchema = regionServer.getTableSchema(region.getTableName());
                if (tableSchema == null) {
                    throw new IOException("Table schema unavailable for: " + region.getTableName());
                }

                condition = parseWhereCondition(tableName, whereClause);
                pushdownPlan = PredicatePushdownPlanner.plan(tableSchema, condition);
                if (pushdownPlan.canPushDown()) {
                    startKey = narrowLowerBound(startKey, pushdownPlan.getStartKey());
                    endKey = narrowUpperBound(endKey, pushdownPlan.getEndKey());
                }
            }

            List<String> projectedQualifiers = request.getQualifiersList();
            StorageScanFilter storageFilter = (!pushdownPlan.getColumnPredicates().isEmpty() || !projectedQualifiers.isEmpty())
                ? new StorageScanFilter(startKey, endKey, pushdownPlan.getColumnPredicates(), projectedQualifiers)
                : null;
            java.util.Iterator<KeyValue> it = storageFilter == null
                ? regionServer.scan(regionId, startKey, endKey)
                : regionServer.scan(regionId, storageFilter);

            // Decide streaming vs materialized path
            boolean hasOrderBy = request.getOrderByCount() > 0;
            boolean canStream = !hasOrderBy
                && request.getAggregatesCount() == 0
                && (whereClause == null || whereClause.isBlank() || pushdownPlan.isFullyPushedDown());

            if (canStream) {
                streamScanResults(responseObserver, it, request.getLimit(), request.getOffset());
                return;
            }

            // Materialized path for ORDER BY, aggregation, or complex WHERE
            List<KeyValue> keyValues = new ArrayList<>();
            while (it.hasNext()) {
                keyValues.add(it.next());
            }

            // Storage engine already handles column predicates and projected qualifiers.
            // Only apply row-level filtering when pushdown was partial (complex conditions).
            if (whereClause != null && !whereClause.isBlank() && !pushdownPlan.isFullyPushedDown()) {
                List<Row> rows = RowAssembler.assemble(keyValues, tableSchema);
                List<Row> filteredRows = filterRows(rows, condition);
                Set<BytesKey> matchedRowKeys = new HashSet<>();
                for (Row row : filteredRows) {
                    if (row.getRowKey() != null) {
                        matchedRowKeys.add(new BytesKey(row.getRowKey()));
                    }
                }

                List<KeyValue> filteredKeyValues = new ArrayList<>();
                for (KeyValue kv : keyValues) {
                    if (matchedRowKeys.contains(new BytesKey(kv.getRowKey()))) {
                        filteredKeyValues.add(kv);
                    }
                }
                keyValues = filteredKeyValues;
            }

            // ORDER BY + LIMIT pushdown: sort and truncate at region server
            int limit = request.getLimit();
            int offset = request.getOffset();
            if (hasOrderBy || limit > 0) {
                tableSchema = resolveTableSchema(tableSchema, regionId);
                if (tableSchema != null) {
                    List<Row> rows = RowAssembler.assemble(keyValues, tableSchema);

                    // Sort
                    if (hasOrderBy) {
                        List<String> sortCols = new ArrayList<>();
                        List<Boolean> sortAsc = new ArrayList<>();
                        for (RegionServerProto.OrderByElement elem : request.getOrderByList()) {
                            sortCols.add(elem.getColumn());
                            sortAsc.add(elem.getAscending());
                        }
                        rows.sort(createSortComparator(sortCols, sortAsc));
                    }

                    // Apply limit + offset
                    if (offset > 0 || limit > 0) {
                        int from = Math.min(offset > 0 ? offset : 0, rows.size());
                        int to = limit > 0 ? Math.min(from + limit, rows.size()) : rows.size();
                        rows = new ArrayList<>(rows.subList(from, to));
                    }

                    // Convert back to KeyValues
                    keyValues = disassembleRows(rows, tableSchema);
                }
            }

            RegionServerProto.ScanResponse.Builder builder = RegionServerProto.ScanResponse.newBuilder()
                .setStatus(createSuccessStatus());

            // Aggregate pushdown: compute local aggregation at region server
            if (request.getAggregatesCount() > 0) {
                tableSchema = resolveTableSchema(tableSchema, regionId);
                if (tableSchema != null) {
                    List<Row> rows = RowAssembler.assemble(keyValues, tableSchema);
                    List<String> groupByCols = request.getGroupByColumnsList();
                    List<RegionServerProto.AggregateSpec> specs = request.getAggregatesList();
                    List<RegionServerProto.AggregateGroup> groups = computeLocalAggregation(rows, groupByCols, specs);
                    builder.addAllAggregateGroups(groups);
                }
            } else {
                for (KeyValue kv : keyValues) {
                    builder.addKeyValues(convertToProto(kv));
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            RegionServerProto.ScanResponse response = RegionServerProto.ScanResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * Stream scan results in batches to reduce memory pressure.
     * Used for simple scans without ORDER BY, aggregation, or complex WHERE.
     */
    private void streamScanResults(StreamObserver<RegionServerProto.ScanResponse> responseObserver,
                                    java.util.Iterator<KeyValue> it,
                                    int limit, int offset) throws IOException {
        int batchSize = 1000;
        List<CommonProto.KeyValue> batch = new ArrayList<>(batchSize);
        int totalSent = 0;
        int skipped = 0;

        while (it.hasNext()) {
            KeyValue kv = it.next();

            // Skip offset rows
            if (offset > 0 && skipped < offset) {
                skipped++;
                continue;
            }

            batch.add(convertToProto(kv));

            if (batch.size() >= batchSize) {
                boolean hasMore = it.hasNext();
                if (limit > 0) {
                    totalSent += batch.size();
                    hasMore = totalSent < limit;
                }
                responseObserver.onNext(RegionServerProto.ScanResponse.newBuilder()
                    .setStatus(createSuccessStatus())
                    .addAllKeyValues(batch)
                    .setHasMore(hasMore)
                    .build());
                batch.clear();
                if (limit > 0 && totalSent >= limit) {
                    responseObserver.onCompleted();
                    return;
                }
            }
        }

        // Send remaining or empty response
        responseObserver.onNext(RegionServerProto.ScanResponse.newBuilder()
            .setStatus(createSuccessStatus())
            .addAllKeyValues(batch)
            .setHasMore(false)
            .build());
        responseObserver.onCompleted();
    }

    /**
     * 处理删除请求
     */
    @Override
    public void getSnapshot(RegionServerProto.SnapshotRequest request,
                            StreamObserver<RegionServerProto.SnapshotResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            Iterator<KeyValue> it = regionServer.scan(regionId, null, null);
            List<CommonProto.KeyValue> batch = new ArrayList<>();
            int batchSize = 1000;
            int totalSent = 0;

            while (it.hasNext()) {
                batch.add(convertToProto(it.next()));
                if (batch.size() >= batchSize) {
                    responseObserver.onNext(RegionServerProto.SnapshotResponse.newBuilder()
                        .setStatus(createSuccessStatus())
                        .addAllKeyValues(batch)
                        .build());
                    totalSent += batch.size();
                    batch.clear();
                }
            }

            if (!batch.isEmpty() || totalSent == 0) {
                responseObserver.onNext(RegionServerProto.SnapshotResponse.newBuilder()
                    .setStatus(createSuccessStatus())
                    .addAllKeyValues(batch)
                    .build());
                totalSent += batch.size();
            }

            responseObserver.onCompleted();
            logger.info("Snapshot streamed for region {}, entries={}", regionId, totalSent);
        } catch (Exception e) {
            logger.error("Failed to stream snapshot", e);
            responseObserver.onNext(RegionServerProto.SnapshotResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void delete(RegionServerProto.DeleteRequest request, StreamObserver<RegionServerProto.DeleteResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            byte[] rowKey = request.getRowKey().toByteArray();

            regionServer.delete(regionId, rowKey, false);

            RegionServerProto.DeleteResponse response = RegionServerProto.DeleteResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            RegionServerProto.DeleteResponse response = RegionServerProto.DeleteResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理复制请求（来自主副本）
     */
    @Override
    public void replicate(RegionServerProto.ReplicateRequest request, StreamObserver<RegionServerProto.ReplicateResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            long maxSeqId = 0;
            long lastAppliedSeqId = regionServer.getRegionManager().getLastAppliedReplicationSequenceId(regionId);

            // 应用复制日志
            for (RegionServerProto.LogEntry entry : request.getEntriesList()) {
                boolean forceSnapshotApply = entry.getSequenceId() == 0 && entry.getMutationsCount() > 0;
                if (!forceSnapshotApply && entry.getSequenceId() <= lastAppliedSeqId) {
                    maxSeqId = Math.max(maxSeqId, lastAppliedSeqId);
                    continue;
                }

                // Verify checksum if present
                if (entry.getChecksum() != 0) {
                    CRC32 crc32 = new CRC32();
                    for (CommonProto.KeyValue kvProto : entry.getMutationsList()) {
                        crc32.update(kvProto.toByteArray());
                    }
                    if (crc32.getValue() != entry.getChecksum()) {
                        logger.error("Checksum mismatch for seqId={} in region {}: expected {}, actual {}",
                            entry.getSequenceId(), regionId, entry.getChecksum(), crc32.getValue());
                        RegionServerProto.ReplicateResponse response = RegionServerProto.ReplicateResponse.newBuilder()
                            .setStatus(createErrorStatus("Checksum mismatch for seqId=" + entry.getSequenceId()))
                            .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }
                }

                List<KeyValue> mutations = new ArrayList<>();
                for (CommonProto.KeyValue kvProto : entry.getMutationsList()) {
                    KeyValue kv = new KeyValue();
                    kv.setRowKey(kvProto.getRowKey().toByteArray());
                    kv.setFamily(kvProto.getColumnFamily());
                    kv.setQualifier(kvProto.getQualifier());
                    kv.setTimestamp(kvProto.getTimestamp());
                    kv.setValue(kvProto.getValue().toByteArray());
                    kv.setType(kvProto.getType() == CommonProto.KeyValueType.PUT ?
                        KeyValue.Type.PUT : KeyValue.Type.DELETE);
                    mutations.add(kv);
                }

                // 空 mutation 仅用于推进复制位点
                if (!mutations.isEmpty()) {
                    regionServer.put(regionId, mutations, true);
                }
                if (forceSnapshotApply) {
                    continue;
                }
                regionServer.getRegionManager().updateLastAppliedReplicationSequenceId(regionId, entry.getSequenceId());
                maxSeqId = Math.max(maxSeqId, entry.getSequenceId());
                lastAppliedSeqId = Math.max(lastAppliedSeqId, entry.getSequenceId());
            }

            RegionServerProto.ReplicateResponse response = RegionServerProto.ReplicateResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .setLastAppliedSeqId(Math.max(maxSeqId, lastAppliedSeqId))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            RegionServerProto.ReplicateResponse response = RegionServerProto.ReplicateResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<RegionServerProto.StreamSnapshotRequest> streamSnapshot(
            StreamObserver<RegionServerProto.StreamSnapshotResponse> responseObserver) {
        return new StreamObserver<>() {
            private String regionId;
            private long totalApplied = 0;
            private long maxSeqId = 0;

            @Override
            public void onNext(RegionServerProto.StreamSnapshotRequest request) {
                if (regionId == null) {
                    regionId = request.getRegionId();
                }

                List<KeyValue> mutations = new ArrayList<>();
                for (CommonProto.KeyValue kvProto : request.getBatchList()) {
                    mutations.add(convertFromProto(kvProto));
                }
                if (!mutations.isEmpty()) {
                    try {
                        regionServer.put(regionId, mutations, true);
                    } catch (Exception e) {
                        logger.error("Failed to apply snapshot batch: {}", e.getMessage());
                    }
                }
                totalApplied += mutations.size();

                if (request.getIsFinal() && request.getFinalSequenceId() > 0) {
                    regionServer.getRegionManager()
                        .updateLastAppliedReplicationSequenceId(regionId, request.getFinalSequenceId());
                    maxSeqId = request.getFinalSequenceId();
                }
            }

            @Override
            public void onError(Throwable t) {
                if (io.grpc.Status.fromThrowable(t).getCode() == io.grpc.Status.Code.CANCELLED) {
                    logger.info("StreamSnapshot cancelled by client (region={})", regionId);
                } else {
                    logger.error("StreamSnapshot error", t);
                }
            }

            @Override
            public void onCompleted() {
                RegionServerProto.StreamSnapshotResponse response =
                    RegionServerProto.StreamSnapshotResponse.newBuilder()
                        .setStatus(createSuccessStatus())
                        .setLastAppliedSeqId(maxSeqId)
                        .setTotalApplied(totalApplied)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void getReplicationLag(RegionServerProto.GetReplicationLagRequest request,
                                  StreamObserver<RegionServerProto.GetReplicationLagResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            long currentSequenceId = 0L;
            ReplicationWAL wal = regionServer.getWal();
            if (wal != null) {
                currentSequenceId = wal.getCurrentSequenceId(regionId);
            }

            long lagInEntries;
            long lastAppliedSequenceId;

            if (regionServer.getRegionManager().isPrimary(regionId)) {
                // Primary: lag is meaningless (it's the source of truth), report 0
                lagInEntries = 0L;
                lastAppliedSequenceId = currentSequenceId;
            } else {
                lastAppliedSequenceId = regionServer.getRegionManager()
                    .getLastAppliedReplicationSequenceId(regionId);
                lagInEntries = Math.max(0L, currentSequenceId - lastAppliedSequenceId);
            }

            RegionServerProto.GetReplicationLagResponse response = RegionServerProto.GetReplicationLagResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .setLagInEntries(lagInEntries)
                .setLagInBytes(0L)
                .setCurrentSequenceId(currentSequenceId)
                .setLastAppliedSequenceId(lastAppliedSequenceId)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            RegionServerProto.GetReplicationLagResponse response = RegionServerProto.GetReplicationLagResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理 Region 分裂请求
     */
    @Override
    public void getSplitKey(RegionServerProto.GetSplitKeyRequest request,
                            StreamObserver<RegionServerProto.GetSplitKeyResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            byte[] splitKey = regionServer.getSplitService().findBestSplitPoint(regionId);

            if (splitKey == null || splitKey.length == 0) {
                RegionServerProto.GetSplitKeyResponse response = RegionServerProto.GetSplitKeyResponse.newBuilder()
                    .setStatus(createErrorStatus("Cannot find suitable split point"))
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            RegionServerProto.GetSplitKeyResponse response = RegionServerProto.GetSplitKeyResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .setSplitKey(com.google.protobuf.ByteString.copyFrom(splitKey))
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Provided split key for region: {}", regionId);
        } catch (Exception e) {
            logger.error("Failed to compute split key", e);
            RegionServerProto.GetSplitKeyResponse response = RegionServerProto.GetSplitKeyResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理 Region 分裂请求
     */
    @Override
    public void splitRegion(RegionServerProto.SplitRegionRequest request,
                            StreamObserver<RegionServerProto.SplitRegionResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            byte[] splitKey = request.getSplitKey().isEmpty() ? null : request.getSplitKey().toByteArray();
            logger.info("Received split request for region: {} (splitKeyProvided={})", regionId, splitKey != null);

            // 如果没有指定分裂点，自动查找
            if (splitKey == null) {
                splitKey = regionServer.getSplitService().findBestSplitPoint(regionId);
            }

            if (splitKey == null) {
                RegionServerProto.SplitRegionResponse response = RegionServerProto.SplitRegionResponse.newBuilder()
                    .setStatus(createErrorStatus("Cannot find suitable split point"))
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // 执行分裂
            RegionSplitService.RegionSplitResult result =
                regionServer.getSplitService().splitRegion(regionId, splitKey,
                    request.getLeftRegionId(), request.getRightRegionId());

            // 构建响应
            CommonProto.RegionInfo leftRegion = CommonProto.RegionInfo.newBuilder()
                .setRegionId(result.getLeftRegion().getRegionId())
                .setTableName(result.getLeftRegion().getTableName())
                .setStartKey(com.google.protobuf.ByteString.copyFrom(result.getLeftRegion().getStartKey()))
                .setEndKey(com.google.protobuf.ByteString.copyFrom(result.getLeftRegion().getEndKey()))
                .setState(CommonProto.RegionState.OPEN)
                .build();

            CommonProto.RegionInfo rightRegion = CommonProto.RegionInfo.newBuilder()
                .setRegionId(result.getRightRegion().getRegionId())
                .setTableName(result.getRightRegion().getTableName())
                .setStartKey(com.google.protobuf.ByteString.copyFrom(result.getRightRegion().getStartKey()))
                .setEndKey(com.google.protobuf.ByteString.copyFrom(result.getRightRegion().getEndKey()))
                .setState(CommonProto.RegionState.OPEN)
                .build();

            RegionServerProto.SplitRegionResponse response = RegionServerProto.SplitRegionResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .setLeftRegion(leftRegion)
                .setRightRegion(rightRegion)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.info("Split request handled successfully for region: {}", regionId);

        } catch (Exception e) {
            RegionServerProto.SplitRegionResponse response = RegionServerProto.SplitRegionResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理 Region 合并请求
     */
    @Override
    public void mergeRegion(RegionServerProto.MergeRegionRequest request,
                            StreamObserver<RegionServerProto.MergeRegionResponse> responseObserver) {
        try {
            String leftRegionId = request.getLeftRegionId();
            String rightRegionId = request.getRightRegionId();

            // 使用 RegionMergeService 执行合并
            RegionMergeService mergeService = regionServer.getMergeService();

            // 检查是否可以合并
            if (!mergeService.canMerge(leftRegionId, rightRegionId)) {
                RegionServerProto.MergeRegionResponse response = RegionServerProto.MergeRegionResponse.newBuilder()
                    .setStatus(createErrorStatus("Regions cannot be merged"))
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // 执行合并
            RegionMergeService.RegionMergeResult result =
                mergeService.mergeRegions(leftRegionId, rightRegionId, request.getMergedRegionId());

            // 构建响应
            CommonProto.RegionInfo mergedRegion = CommonProto.RegionInfo.newBuilder()
                .setRegionId(result.getMergedRegion().getRegionId())
                .setTableName(result.getMergedRegion().getTableName())
                .setStartKey(com.google.protobuf.ByteString.copyFrom(result.getMergedRegion().getStartKey()))
                .setEndKey(com.google.protobuf.ByteString.copyFrom(result.getMergedRegion().getEndKey()))
                .setState(CommonProto.RegionState.OPEN)
                .build();

            RegionServerProto.MergeRegionResponse response = RegionServerProto.MergeRegionResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .setMergedRegion(mergedRegion)
                .setTotalEntries(result.getTotalEntries())
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Failed to merge regions", e);
            RegionServerProto.MergeRegionResponse response = RegionServerProto.MergeRegionResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理提升为主副本请求
     */
    @Override
    public void promoteToPrimary(RegionServerProto.PromoteRequest request, StreamObserver<RegionServerProto.PromoteResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            long fencingToken = request.getFencingToken();

            // 1. 检查Region是否存在且处于OPEN状态
            RegionManager.RegionState currentState = regionServer.getRegionManager().getRegionState(regionId);
            if (currentState == null) {
                throw new IllegalStateException("Region not found: " + regionId);
            }
            if (currentState != RegionManager.RegionState.OPEN) {
                throw new IllegalStateException("Region is not in OPEN state: " + regionId + " current state: " + currentState);
            }

            // 2. 更新Fencing Token（关键：防止脑裂）
            regionServer.getRegionManager().updateFencingToken(regionId, fencingToken);

            // 3. 执行晋升为主副本
            regionServer.getRegionManager().promoteToPrimary(regionId);
            ReplicationCoordinator replicationCoordinator = regionServer.getReplicationCoordinator();
            if (replicationCoordinator != null && replicationCoordinator.getReplicaGroup(regionId) != null) {
                replicationCoordinator.getReplicaGroup(regionId).setPrimary(regionServer.getServerId());
            }

            logger.info("Region {} successfully promoted to primary on {} with fencing token: {}", regionId, regionServer.getServerId(), fencingToken);

            RegionServerProto.PromoteResponse response = RegionServerProto.PromoteResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Failed to promote region to primary", e);
            RegionServerProto.PromoteResponse response = RegionServerProto.PromoteResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理开始迁移请求
     */
    @Override
    public void startMigration(RegionServerProto.MigrateRequest request, StreamObserver<RegionServerProto.MigrateResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            CommonProto.ServerId targetProto = request.getTargetServer();
            ServerId targetServer = new ServerId(targetProto.getHost(), targetProto.getPort());

            logger.info("Starting migration for region {} to {}", regionId, targetServer);

            long sourceSequenceId = performMigrationCatchup(regionId, targetServer);

            RegionServerProto.MigrateResponse response = RegionServerProto.MigrateResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .setSourceSequenceId(sourceSequenceId)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Failed to start migration for region {}", request.getRegionId(), e);
            RegionServerProto.MigrateResponse response = RegionServerProto.MigrateResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void finalizeMigration(RegionServerProto.FinalizeMigrationRequest request,
                                  StreamObserver<RegionServerProto.FinalizeMigrationResponse> responseObserver) {
        String regionId = request.getRegionId();
        try {
            CommonProto.ServerId targetProto = request.getTargetServer();
            ServerId targetServer = new ServerId(targetProto.getHost(), targetProto.getPort());
            long fromSequenceId = request.getFromSequenceId();

            regionServer.getRegionManager().blockWrites(regionId);
            long sourceSequenceId = performFinalMigrationCatchup(regionId, targetServer, fromSequenceId);

            RegionServerProto.FinalizeMigrationResponse response = RegionServerProto.FinalizeMigrationResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .setSourceSequenceId(sourceSequenceId)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Failed to finalize migration for region {}", regionId, e);
            regionServer.getRegionManager().unblockWrites(regionId);
            RegionServerProto.FinalizeMigrationResponse response = RegionServerProto.FinalizeMigrationResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void abortMigration(RegionServerProto.AbortMigrationRequest request,
                               StreamObserver<RegionServerProto.AbortMigrationResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            regionServer.getRegionManager().unblockWrites(regionId);

            RegionServerProto.AbortMigrationResponse response = RegionServerProto.AbortMigrationResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            RegionServerProto.AbortMigrationResponse response = RegionServerProto.AbortMigrationResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理打开 Region 请求
     */
    @Override
    public void openRegion(RegionServerProto.OpenRegionRequest request,
                           StreamObserver<RegionServerProto.OpenRegionResponse> responseObserver) {
        try {
            CommonProto.RegionInfo regionInfo = request.getRegion();
            String regionId = regionInfo.getRegionId();
            String tableName = regionInfo.getTableName();
            byte[] startKey = regionInfo.getStartKey().toByteArray();
            byte[] endKey = regionInfo.getEndKey().toByteArray();

            logger.info("Opening region: {} for table: {}", regionId, tableName);

            // 创建 Region 对象
            Region region = new Region();
            region.setRegionId(regionId);
            region.setTableName(tableName);
            region.setStartKey(startKey);
            region.setEndKey(endKey);
            if (regionInfo.hasPrimary()) {
                region.setPrimary(new ServerId(regionInfo.getPrimary().getHost(), regionInfo.getPrimary().getPort()));
            }
            if (regionInfo.getReplicasCount() > 0) {
                for (CommonProto.ServerId replicaProto : regionInfo.getReplicasList()) {
                    region.addReplica(new ServerId(replicaProto.getHost(), replicaProto.getPort()));
                }
            }

            // 打开 Region
            regionServer.getRegionManager().openRegion(region);

            if (region.getPrimary() != null && !region.getReplicas().isEmpty()) {
                ReplicationCoordinator replicationCoordinator = regionServer.getReplicationCoordinator();
                if (replicationCoordinator != null && replicationCoordinator.getReplicaGroup(regionId) == null) {
                    List<ServerId> replicaServers = new ArrayList<>();
                    replicaServers.add(region.getPrimary());
                    for (ServerId replica : region.getReplicas()) {
                        if (!replicaServers.contains(replica)) {
                            replicaServers.add(replica);
                        }
                    }
                    replicationCoordinator.createReplicaGroup(region, replicaServers);
                    logger.info("Initialized local replica group for region {} on {}", regionId, regionServer.getServerId());
                }
            }

            RegionServerProto.OpenRegionResponse response = RegionServerProto.OpenRegionResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Region opened successfully: {}", regionId);
        } catch (Exception e) {
            logger.error("Failed to open region", e);
            RegionServerProto.OpenRegionResponse response = RegionServerProto.OpenRegionResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * 处理关闭 Region 请求
     */
    @Override
    public void closeRegion(RegionServerProto.CloseRegionRequest request,
                            StreamObserver<RegionServerProto.CloseRegionResponse> responseObserver) {
        try {
            String regionId = request.getRegionId();
            boolean abort = request.getAbort();
            boolean dropTable = request.getDropTable();

            logger.info("Closing region: {}, abort: {}, dropTable: {}", regionId, abort, dropTable);

            // 关闭 Region
            regionServer.getRegionManager().closeRegion(regionId, abort, dropTable);

            RegionServerProto.CloseRegionResponse response = RegionServerProto.CloseRegionResponse.newBuilder()
                .setStatus(createSuccessStatus())
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Region closed successfully: {}", regionId);
        } catch (Exception e) {
            logger.error("Failed to close region", e);
            RegionServerProto.CloseRegionResponse response = RegionServerProto.CloseRegionResponse.newBuilder()
                .setStatus(createErrorStatus(e.getMessage()))
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private SelectStatement parseSelect(String sql) throws IOException {
        try {
            return (SelectStatement) new SQLParser(sql).parse();
        } catch (Exception e) {
            throw new IOException("Failed to parse SELECT statement", e);
        }
    }

    private Condition parseWhereCondition(String tableName, String whereClause) throws IOException {
        if (whereClause == null || whereClause.isBlank()) {
            return null;
        }
        String syntheticSql = "SELECT * FROM " + tableName + " WHERE " + whereClause;
        SelectStatement select = parseSelect(syntheticSql);
        return select.getWhere();
    }

    private byte[] narrowLowerBound(byte[] existing, byte[] pushed) {
        if (existing == null) {
            return pushed;
        }
        if (pushed == null) {
            return existing;
        }
        return BytesUtil.compareTo(existing, pushed) >= 0 ? existing : pushed;
    }

    private byte[] narrowUpperBound(byte[] existing, byte[] pushed) {
        if (existing == null) {
            return pushed;
        }
        if (pushed == null) {
            return existing;
        }
        return BytesUtil.compareTo(existing, pushed) <= 0 ? existing : pushed;
    }

    private List<Row> filterRows(List<Row> rows, Condition condition) {
        if (condition == null || rows.isEmpty()) {
            return rows;
        }

        List<Row> filtered = new ArrayList<>();
        for (Row row : rows) {
            com.minisql.sql.execution.Row evalRow = new com.minisql.sql.execution.Row();
            for (String columnName : row.getColumnNames()) {
                evalRow.addColumn(columnName, row.getColumn(columnName));
            }
            if (condition.evaluate(evalRow)) {
                filtered.add(row);
            }
        }
        return filtered;
    }

    private int compareValues(Object left, Object right) {
        return com.minisql.common.utils.ValueComparator.compare(left, right);
    }

    private static final class BytesKey {
        private final byte[] value;

        private BytesKey(byte[] value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof BytesKey)) {
                return false;
            }
            BytesKey other = (BytesKey) obj;
            return Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }

    private long performMigrationCatchup(String regionId, ServerId targetServer) throws Exception {
        ReplicationWAL wal = regionServer.getWal();
        long snapshotSequenceId = wal != null ? wal.getCurrentSequenceId(regionId) : 0L;

        performFullSync(regionId, targetServer);
        advanceReplicaSequence(targetServer, regionId, snapshotSequenceId);

        if (wal == null) {
            return snapshotSequenceId;
        }

        long currentSequenceId = wal.getCurrentSequenceId(regionId);
        if (currentSequenceId > snapshotSequenceId) {
            sendIncrementalEntries(targetServer, regionId, wal.getEntries(regionId, snapshotSequenceId));
        }
        return currentSequenceId;
    }

    private long performFinalMigrationCatchup(String regionId, ServerId targetServer, long fromSequenceId) throws Exception {
        ReplicationWAL wal = regionServer.getWal();
        long currentSequenceId = wal != null ? wal.getCurrentSequenceId(regionId) : 0L;
        if (wal == null) {
            return currentSequenceId;
        }

        if (wal != null && currentSequenceId > fromSequenceId) {
            sendIncrementalEntries(targetServer, regionId, wal.getEntries(regionId, fromSequenceId));
        } else {
            advanceReplicaSequence(targetServer, regionId, currentSequenceId);
        }
        return wal.getCurrentSequenceId(regionId);
    }

    private void advanceReplicaSequence(ServerId targetServer, String regionId, long sequenceId) {
        if (sequenceId <= 0) {
            return;
        }

        RegionServerProto.LogEntry marker = RegionServerProto.LogEntry.newBuilder()
            .setSequenceId(sequenceId)
            .setTimestamp(System.currentTimeMillis())
            .build();
        sendReplicationEntries(targetServer, regionId, java.util.Collections.singletonList(marker));
    }

    private void sendIncrementalEntries(ServerId targetServer, String regionId, List<ReplicationLogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        List<RegionServerProto.LogEntry> protoEntries = new ArrayList<>();
        for (ReplicationLogEntry entry : entries) {
            RegionServerProto.LogEntry.Builder builder = RegionServerProto.LogEntry.newBuilder()
                .setSequenceId(entry.getSequenceId())
                .setTimestamp(entry.getTimestamp());

            for (KeyValue mutation : entry.getMutations()) {
                builder.addMutations(convertToProto(mutation));
            }
            protoEntries.add(builder.build());
        }

        sendReplicationEntries(targetServer, regionId, protoEntries);
    }

    private void sendReplicationEntries(ServerId targetServer, String regionId, List<RegionServerProto.LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        io.grpc.ManagedChannel channel = com.minisql.common.rpc.GrpcChannelFactory
            .forAddress(targetServer.getHost(), targetServer.getPort());

        try {
            RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                RegionServerServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(300000, java.util.concurrent.TimeUnit.MILLISECONDS);

            RegionServerProto.ReplicateRequest request = RegionServerProto.ReplicateRequest.newBuilder()
                .setRegionId(regionId)
                .addAllEntries(entries)
                .build();

            RegionServerProto.ReplicateResponse response = stub.replicate(request);
            if (!response.getStatus().getSuccess()) {
                throw new RuntimeException("Incremental replication failed: " + response.getStatus().getMessage());
            }
        } finally {
            channel = null;
        }
    }

    /**
     * 执行全量同步
     */
    private void performFullSync(String regionId, ServerId targetServer) {
        try {
            io.grpc.ManagedChannel channel = com.minisql.common.rpc.GrpcChannelFactory
                .forAddress(targetServer.getHost(), targetServer.getPort());

            try {
                RegionServerServiceGrpc.RegionServerServiceBlockingStub stub =
                    RegionServerServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(300000, java.util.concurrent.TimeUnit.MILLISECONDS);

                // 获取 Region 的所有数据
                java.util.Iterator<KeyValue> iterator = regionServer.scan(regionId, null, null);
                List<CommonProto.KeyValue> batch = new ArrayList<>();
                int batchSize = 1000;
                int totalSent = 0;

                while (iterator.hasNext()) {
                    KeyValue kv = iterator.next();
                    batch.add(convertToProto(kv));

                    if (batch.size() >= batchSize) {
                        sendSnapshotBatch(stub, regionId, batch);
                        totalSent += batch.size();
                        batch.clear();
                    }
                }

                // 发送剩余数据
                if (!batch.isEmpty()) {
                    sendSnapshotBatch(stub, regionId, batch);
                    totalSent += batch.size();
                }

                logger.info("Full sync completed for region {}, sent {} entries to {}", regionId, totalSent, targetServer);

            } finally {
                channel = null;
            }
        } catch (Exception e) {
            logger.error("Error performing full sync", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 发送批量数据
     */
    private void sendSnapshotBatch(RegionServerServiceGrpc.RegionServerServiceBlockingStub stub,
                                   String regionId, List<CommonProto.KeyValue> batch) {
        RegionServerProto.LogEntry snapshotEntry = RegionServerProto.LogEntry.newBuilder()
            .setSequenceId(0L)
            .setTimestamp(System.currentTimeMillis())
            .addAllMutations(batch)
            .build();
        RegionServerProto.ReplicateRequest request = RegionServerProto.ReplicateRequest.newBuilder()
            .setRegionId(regionId)
            .addEntries(snapshotEntry)
            .build();

        RegionServerProto.ReplicateResponse response = stub.replicate(request);
        if (!response.getStatus().getSuccess()) {
            throw new RuntimeException("Failed to send snapshot batch: " + response.getStatus().getMessage());
        }
    }

    private Table resolveTableSchema(Table tableSchema, String regionId) {
        if (tableSchema != null) return tableSchema;
        Region region = regionServer.getRegionManager().getRegion(regionId);
        return region != null ? regionServer.getTableSchema(region.getTableName()) : null;
    }

    /**
     * Create a comparator for sorting rows by specified columns.
     */
    private Comparator<Row> createSortComparator(List<String> columns, List<Boolean> ascending) {
        return (left, right) -> {
            for (int i = 0; i < columns.size(); i++) {
                Object leftVal = left.getColumn(columns.get(i));
                Object rightVal = right.getColumn(columns.get(i));
                int cmp = compareValues(leftVal, rightVal);
                if (cmp != 0) {
                    boolean asc = ascending.size() > i ? ascending.get(i) : true;
                    return asc ? cmp : -cmp;
                }
            }
            return 0;
        };
    }

    /**
     * Convert Row objects back to KeyValue list for proto serialization.
     */
    private List<KeyValue> disassembleRows(List<Row> rows, Table schema) {
        List<KeyValue> result = new ArrayList<>();
        for (Row row : rows) {
            byte[] rowKey = row.getRowKey();
            long timestamp = row.getTimestamp() > 0 ? row.getTimestamp() : System.currentTimeMillis();
            for (Column col : schema.getColumns()) {
                // Skip primary key column (encoded in rowKey)
                if (col.getName().equals(schema.getPrimaryKey())) continue;
                Object value = row.getColumn(col.getName());
                if (value == null) continue;
                byte[] valueBytes = com.minisql.common.utils.RowKeySerializer.serialize(value, col.getType());
                KeyValue kv = KeyValue.builder(rowKey)
                    .family("")
                    .qualifier(col.getName())
                    .timestamp(timestamp)
                    .value(valueBytes)
                    .type(KeyValue.Type.PUT)
                    .build();
                result.add(kv);
            }
        }
        return result;
    }

    /**
     * Compute local aggregation for a set of rows, producing AggregateGroup protos.
     */
    private List<RegionServerProto.AggregateGroup> computeLocalAggregation(
            List<Row> rows,
            List<String> groupByCols,
            List<RegionServerProto.AggregateSpec> specs) {

        // Group rows by groupBy columns
        Map<List<Object>, List<Row>> buckets = new LinkedHashMap<>();
        for (Row row : rows) {
            List<Object> key = new ArrayList<>();
            if (groupByCols != null) {
                for (String col : groupByCols) {
                    key.add(row.getColumn(col));
                }
            }
            buckets.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        // If no rows and no GROUP BY, produce one group with zero counts
        if (buckets.isEmpty() && (groupByCols == null || groupByCols.isEmpty())) {
            buckets.put(Collections.emptyList(), Collections.emptyList());
        }

        List<RegionServerProto.AggregateGroup> result = new ArrayList<>();
        for (Map.Entry<List<Object>, List<Row>> entry : buckets.entrySet()) {
            RegionServerProto.AggregateGroup.Builder groupBuilder =
                RegionServerProto.AggregateGroup.newBuilder();

            // Encode group key values as bytes
            for (Object keyVal : entry.getKey()) {
                if (keyVal == null) {
                    groupBuilder.addGroupByKey(com.google.protobuf.ByteString.EMPTY);
                } else {
                    groupBuilder.addGroupByKey(com.google.protobuf.ByteString.copyFromUtf8(
                        String.valueOf(keyVal)));
                }
            }

            List<Row> groupRows = entry.getValue();

            // Compute each aggregate
            for (RegionServerProto.AggregateSpec spec : specs) {
                String func = spec.getFunction().toUpperCase();
                double sumVal = 0;
                long countVal = 0;
                Object maxVal = null;
                Object minVal = null;

                for (Row row : groupRows) {
                    Object val = "COUNT".equals(func) ? null : row.getColumn(spec.getColumn());
                    switch (func) {
                        case "COUNT":
                            countVal++;
                            break;
                        case "SUM":
                            if (val instanceof Number) {
                                sumVal += ((Number) val).doubleValue();
                            }
                            break;
                        case "AVG":
                            if (val instanceof Number) {
                                sumVal += ((Number) val).doubleValue();
                                countVal++;
                            }
                            break;
                        case "MAX":
                            if (val != null && (maxVal == null || compareValues(val, maxVal) > 0)) {
                                maxVal = val;
                            }
                            break;
                        case "MIN":
                            if (val != null && (minVal == null || compareValues(val, minVal) < 0)) {
                                minVal = val;
                            }
                            break;
                        default:
                            break;
                    }
                }

                RegionServerProto.AggregateResult.Builder resBuilder =
                    RegionServerProto.AggregateResult.newBuilder()
                        .setOutputName(spec.getOutputName())
                        .setFunction(func)
                        .setSumValue(sumVal)
                        .setCountValue(countVal);
                if (maxVal != null) {
                    resBuilder.setMaxValue(com.google.protobuf.ByteString.copyFromUtf8(
                        String.valueOf(maxVal)));
                }
                if (minVal != null) {
                    resBuilder.setMinValue(com.google.protobuf.ByteString.copyFromUtf8(
                        String.valueOf(minVal)));
                }
                groupBuilder.addResults(resBuilder);
            }

            result.add(groupBuilder.build());
        }
        return result;
    }

    private CommonProto.Status createSuccessStatus() {
        return CommonProto.Status.newBuilder()
            .setCode(0)
            .setSuccess(true)
            .setMessage("OK")
            .build();
    }

    /**
     * 创建错误状态
     */
    private CommonProto.Status createErrorStatus(String message) {
        return CommonProto.Status.newBuilder()
            .setCode(-1)
            .setSuccess(false)
            .setMessage(message)
            .build();
    }

    /**
     * 转换为 Protobuf KeyValue
     */
    private CommonProto.KeyValue convertToProto(KeyValue kv) {
        return CommonProto.KeyValue.newBuilder()
            .setRowKey(com.google.protobuf.ByteString.copyFrom(kv.getRowKey()))
            .setColumnFamily(kv.getFamily() != null ? kv.getFamily() : "")
            .setQualifier(kv.getQualifier() != null ? kv.getQualifier() : "")
            .setTimestamp(kv.getTimestamp())
            .setValue(com.google.protobuf.ByteString.copyFrom(kv.getValue() != null ? kv.getValue() : new byte[0]))
            .setType(kv.getType() == KeyValue.Type.PUT ? CommonProto.KeyValueType.PUT : CommonProto.KeyValueType.DELETE)
            .build();
    }

    private KeyValue convertFromProto(CommonProto.KeyValue kvProto) {
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
