package com.minisql.common.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/** 集中式 gRPC 通道工厂，管理和复用 gRPC 连接 */
public class GrpcChannelFactory {

    private static final Logger logger = Logger.getLogger(GrpcChannelFactory.class.getName());

    // Per-address channel cache (host:port -> ManagedChannel)
    private static final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    private GrpcChannelFactory() {
    }

    /**
     * Create (or retrieve cached) channel for the given address.
     */
    public static ManagedChannel forAddress(String host, int port) {
        String key = host + ":" + port;
        return channels.computeIfAbsent(key, ignored -> buildChannel(host, port));
    }

    /**
     * Create a new channel without caching.
     */
    public static ManagedChannel newChannel(String host, int port) {
        return buildChannel(host, port);
    }

    /**
     * Remove and shut down a cached channel.
     */
    static void removeChannel(String host, int port) {
        String key = host + ":" + port;
        ManagedChannel channel = channels.remove(key);
        if (channel != null) {
            shutdownChannel(channel);
        }
    }

    /**
     * Shut down all cached channels.
     */
    static void shutdownAll() {
        for (ManagedChannel channel : channels.values()) {
            shutdownChannel(channel);
        }
        channels.clear();
    }

    private static ManagedChannel buildChannel(String host, int port) {
        logger.fine("Creating plaintext gRPC channel to " + host + ":" + port);
        return ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .maxInboundMessageSize(64 * 1024 * 1024)
            .build();
    }

    static void shutdownChannel(ManagedChannel channel) {
        try {
            channel.shutdown();
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warning("gRPC channel did not terminate in time");
                }
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
