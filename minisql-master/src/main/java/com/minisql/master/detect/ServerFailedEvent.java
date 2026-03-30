package com.minisql.master.detect;

import com.minisql.common.model.ServerId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServerFailedEvent implements ClusterEvent {

    private final ServerId failedServer;
    private final List<String> affectedRegionIds;
    private final long occurredAt;

    public ServerFailedEvent(ServerId failedServer, List<String> affectedRegionIds) {
        this.failedServer = failedServer;
        this.affectedRegionIds = affectedRegionIds == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(affectedRegionIds));
        this.occurredAt = System.currentTimeMillis();
    }

    public ServerId getFailedServer() {
        return failedServer;
    }

    public List<String> getAffectedRegionIds() {
        return affectedRegionIds;
    }

    @Override
    public String getEventType() {
        return "serverFailed";
    }

    @Override
    public long getOccurredAt() {
        return occurredAt;
    }
}
