package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.ServerId;

import java.util.List;

/**
 * Transport abstraction for replication-related RPCs.
 */
public interface ReplicationTransportClient extends AutoCloseable {

    boolean replicate(ServerId replica, String regionId, ReplicationLogEntry entry, long timeoutMs);

    boolean replicateBatch(ServerId replica, String regionId, List<ReplicationLogEntry> entries, long timeoutMs);

    List<KeyValue> fetchSnapshot(ServerId primary, String regionId, long timeoutMs);

    boolean sendSnapshot(ServerId replica, String regionId, List<KeyValue> snapshot, int batchSize, long timeoutMs, long finalSequenceId);

    boolean sendSnapshotStreaming(ServerId replica, String regionId, List<KeyValue> snapshot, int batchSize, long timeoutMs, long finalSequenceId);

    @Override
    void close();
}
