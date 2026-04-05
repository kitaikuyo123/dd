package com.minisql.replication;

import com.minisql.common.model.KeyValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unified replication log entry shared by in-memory replication and WAL.
 */
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
