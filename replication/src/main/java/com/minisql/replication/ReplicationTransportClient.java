package com.minisql.replication;

import com.minisql.common.model.KeyValue;
import com.minisql.common.model.ServerId;

import java.util.List;

/** 复制传输客户端接口，定义副本间数据传输的抽象 */
public interface ReplicationTransportClient extends AutoCloseable {

    boolean replicate(ServerId replica, String regionId, ReplicationLogEntry entry, long timeoutMs);

    boolean replicateBatch(ServerId replica, String regionId, List<ReplicationLogEntry> entries, long timeoutMs);

    @Deprecated
    List<KeyValue> fetchSnapshot(ServerId primary, String regionId, long timeoutMs);

    /**
     * Stream snapshot directly from primary to replica without materializing the
     * entire dataset in the coordinator's memory.
     *
     * @return true if the full snapshot was streamed successfully
     */
    boolean streamSnapshotDirect(ServerId primary, ServerId replica, String regionId,
                                  int batchSize, long timeoutMs, long finalSequenceId);

    @Deprecated
    boolean sendSnapshot(ServerId replica, String regionId, List<KeyValue> snapshot, int batchSize, long timeoutMs, long finalSequenceId);

    @Deprecated
    boolean sendSnapshotStreaming(ServerId replica, String regionId, List<KeyValue> snapshot, int batchSize, long timeoutMs, long finalSequenceId);

    @Override
    void close();

    /**
     * Remove and shut down the cached channel for the given server.
     * Default no-op for implementations that don't cache channels.
     */
    default void removeChannel(ServerId serverId) {}
}
