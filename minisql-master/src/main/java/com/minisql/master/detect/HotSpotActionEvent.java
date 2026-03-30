package com.minisql.master.detect;

import com.minisql.master.rebalance.HotSpotCoordinator;

public class HotSpotActionEvent implements ClusterEvent {

    private final HotSpotCoordinator.HotSpotAction action;
    private final long occurredAt;

    public HotSpotActionEvent(HotSpotCoordinator.HotSpotAction action) {
        this.action = action;
        this.occurredAt = System.currentTimeMillis();
    }

    public HotSpotCoordinator.HotSpotAction getAction() {
        return action;
    }

    @Override
    public String getEventType() {
        return "hotSpotAction";
    }

    @Override
    public long getOccurredAt() {
        return occurredAt;
    }
}
