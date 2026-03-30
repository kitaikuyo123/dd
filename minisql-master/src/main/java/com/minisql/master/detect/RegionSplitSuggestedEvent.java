package com.minisql.master.detect;

import com.minisql.common.model.ServerId;
import com.minisql.master.state.ClusterManager;

public class RegionSplitSuggestedEvent implements ClusterEvent {

    private final String regionId;
    private final String tableName;
    private final ServerId serverId;
    private final ClusterManager.RegionLoad regionLoad;
    private final long occurredAt;

    public RegionSplitSuggestedEvent(String regionId,
                                     String tableName,
                                     ServerId serverId,
                                     ClusterManager.RegionLoad regionLoad) {
        this.regionId = regionId;
        this.tableName = tableName;
        this.serverId = serverId;
        this.regionLoad = regionLoad;
        this.occurredAt = System.currentTimeMillis();
    }

    public String getRegionId() {
        return regionId;
    }

    public String getTableName() {
        return tableName;
    }

    public ServerId getServerId() {
        return serverId;
    }

    public ClusterManager.RegionLoad getRegionLoad() {
        return regionLoad;
    }

    @Override
    public String getEventType() {
        return "regionSplitSuggested";
    }

    @Override
    public long getOccurredAt() {
        return occurredAt;
    }
}
