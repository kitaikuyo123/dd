package com.minisql.replication;

import com.minisql.common.model.KeyValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 复制日志条目，记录一次变更的序列号、时间戳和变更数据 */
public class ReplicationLogEntry {

    private final long sequenceId;
    private final long timestamp;
    private final List<KeyValue> mutations;

    public ReplicationLogEntry(long sequenceId, long timestamp, List<KeyValue> mutations) {
        this.sequenceId = sequenceId;
        this.timestamp = timestamp;
        this.mutations = mutations == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(mutations));
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<KeyValue> getMutations() {
        return mutations;
    }
}
