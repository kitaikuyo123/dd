package com.minisql.replication;

import com.minisql.common.Constants;
import com.minisql.common.model.ServerId;
import com.minisql.common.proto.CommonProto;
import com.minisql.common.proto.MasterProto;
import com.minisql.common.proto.MasterServiceGrpc;
import com.minisql.zookeeper.ZkClient;
import com.minisql.zookeeper.ZkPayloads;
import com.minisql.common.rpc.GrpcChannelFactory;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** 主副本变更通知器，通过 ZooKeeper 发布主副本变更事件 */
public class PrimaryChangeNotifier {

    private static final Logger logger = LoggerFactory.getLogger(PrimaryChangeNotifier.class);

    private ZkClient zkClient;

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void notifyPrimaryChange(String regionId, ServerId oldPrimary, ServerId newPrimary) {
        notifyMaster(regionId, oldPrimary, newPrimary);
    }

    private void notifyMaster(String regionId, ServerId oldPrimary, ServerId newPrimary) {
        if (zkClient == null || newPrimary == null) {
            return;
        }

        String masterAddress = getMasterAddressFromZk();
        if (masterAddress == null || masterAddress.isEmpty()) {
            return;
        }

        String[] parts = masterAddress.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : Constants.DEFAULT_MASTER_PORT;

        ManagedChannel channel = GrpcChannelFactory.forAddress(host, port);
        try {
            MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5000, TimeUnit.MILLISECONDS);
            MasterProto.PrimaryChangeResponse response = stub.reportPrimaryChange(
                MasterProto.PrimaryChangeRequest.newBuilder()
                    .setRegionId(regionId)
                    .setOldPrimary(toProto(oldPrimary))
                    .setNewPrimary(toProto(newPrimary))
                    .setTimestamp(System.currentTimeMillis())
                    .build()
            );
            if (!response.getStatus().getSuccess()) {
                logger.warn("Master notification failed: {}", response.getStatus().getMessage());
            }
        } catch (Exception e) {
            logger.warn("Failed to notify Master about primary change: {}", e.getMessage());
        }
    }

    private String getMasterAddressFromZk() {
        if (zkClient == null) {
            return null;
        }
        try {
            String masterPath = Constants.ZK_MASTER_LEADER_PATH;
            if (zkClient.exists(masterPath)) {
                return ZkPayloads.decodeLeaderAddress(zkClient.getData(masterPath));
            }
        } catch (Exception e) {
            logger.warn("Failed to read master address from ZooKeeper: {}", e.getMessage());
        }
        return null;
    }

    private CommonProto.ServerId toProto(ServerId serverId) {
        if (serverId == null) {
            return CommonProto.ServerId.getDefaultInstance();
        }
        return CommonProto.ServerId.newBuilder()
            .setHost(serverId.getHost())
            .setPort(serverId.getPort())
            .build();
    }
}
