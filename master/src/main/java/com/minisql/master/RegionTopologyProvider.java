package com.minisql.master;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.state.MetadataManager;
import com.minisql.replication.TopologyProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于 Region 元数据的只读拓扑提供者
 *
 * 用于 Master 端，使 ReplicaGroup 的拓扑始终与 Region 元数据一致，
 * 无需在迁移、故障转移等操作后手动同步 ReplicaGroup。
 */
public class RegionTopologyProvider implements TopologyProvider {

    private final MetadataManager metadataManager;
    private final String regionId;

    public RegionTopologyProvider(MetadataManager metadataManager, String regionId) {
        this.metadataManager = metadataManager;
        this.regionId = regionId;
    }

    @Override
    public ServerId getPrimary() {
        Region region = metadataManager.getRegion(regionId);
        return region != null ? region.getPrimary() : null;
    }

    @Override
    public List<ServerId> getReplicas() {
        Region region = metadataManager.getRegion(regionId);
        if (region == null || region.getReplicas() == null) {
            return new ArrayList<>();
        }
        return region.getReplicas();
    }
}
