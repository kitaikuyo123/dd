package com.minisql.regionserver;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.RegionServerProto;
import com.minisql.replication.ReplicationCoordinator;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RegionServerServiceImpl component tests")
class RegionServerServiceImplTest {

    @Test
    @DisplayName("promoteToPrimary updates local primary flag fencing token and replica group")
    void testPromoteToPrimaryUpdatesLocalState() {
        RegionServer regionServer = new RegionServer(
            "localhost",
            16020,
            null,
            null,
            3,
            "./data/test-wal-serviceimpl"
        );

        Region region = new Region("region-1", "products", "a".getBytes(), "z".getBytes());
        ServerId oldPrimary = new ServerId("other-primary", 16021);
        region.setPrimary(oldPrimary);
        region.setReplicas(List.of(oldPrimary, regionServer.getServerId()));
        regionServer.getRegionManager().registerRegionInternal(region);
        regionServer.getRegionManager().setRegionState(region.getRegionId(), RegionManager.RegionState.OPEN);
        regionServer.getRegionManager().demoteToReplica(region.getRegionId());
        ReplicationCoordinator replicationCoordinator = regionServer.getReplicationCoordinator();
        replicationCoordinator.createReplicaGroup(region, List.of(oldPrimary, regionServer.getServerId()));

        RegionServerServiceImpl service = new RegionServerServiceImpl(regionServer);
        CapturingObserver<RegionServerProto.PromoteResponse> responseObserver = new CapturingObserver<>();

        service.promoteToPrimary(
            RegionServerProto.PromoteRequest.newBuilder()
                .setRegionId(region.getRegionId())
                .setFencingToken(7L)
                .build(),
            responseObserver
        );

        assertTrue(responseObserver.value.getStatus().getSuccess());
        assertNull(responseObserver.error);
        assertTrue(responseObserver.completed);
        assertTrue(regionServer.getRegionManager().isPrimary(region.getRegionId()));
        assertEquals(7L, regionServer.getRegionManager().getFencingToken(region.getRegionId()));
        assertEquals(regionServer.getServerId(),
            replicationCoordinator.getReplicaGroup(region.getRegionId()).getPrimary());
    }

    private static final class CapturingObserver<T> implements StreamObserver<T> {
        private T value;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }
    }
}
