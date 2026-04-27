package com.minisql.common.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Centralized gRPC channel factory.
 *
 * <p>All gRPC channel creation should go through this factory to ensure
 * consistent TLS/plaintext configuration and channel reuse.
 *
 * <p>By default, plaintext is used (backward compatible). Set a {@link GrpcSslConfig}
 * to enable TLS across all channels.
 */
public class GrpcChannelFactory {

    private static final Logger logger = Logger.getLogger(GrpcChannelFactory.class.getName());

    private static volatile GrpcSslConfig sslConfig;

    // Per-address channel cache (host:port -> ManagedChannel)
    private static final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    private GrpcChannelFactory() {
    }

    /**
     * Configure global TLS settings. Set to null to fall back to plaintext.
     */
    public static void setSslConfig(GrpcSslConfig config) {
        sslConfig = config;
        if (config != null && config.isTlsEnabled()) {
            logger.info("gRPC TLS enabled (trustCert=" + config.getTrustCertPath() + ")");
        } else {
            logger.info("gRPC plaintext mode (no TLS configured)");
        }
    }

    public static GrpcSslConfig getSslConfig() {
        return sslConfig;
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
    public static void removeChannel(String host, int port) {
        String key = host + ":" + port;
        ManagedChannel channel = channels.remove(key);
        if (channel != null) {
            shutdownChannel(channel);
        }
    }

    /**
     * Shut down all cached channels.
     */
    public static void shutdownAll() {
        for (ManagedChannel channel : channels.values()) {
            shutdownChannel(channel);
        }
        channels.clear();
    }

    private static ManagedChannel buildChannel(String host, int port) {
        if (sslConfig != null && sslConfig.isTlsEnabled()) {
            SslContext sslContext = sslConfig.buildClientSslContext();
            if (sslContext != null) {
                logger.fine("Creating TLS gRPC channel to " + host + ":" + port);
                return NettyChannelBuilder.forAddress(host, port)
                    .sslContext(sslContext)
                    .maxInboundMessageSize(64 * 1024 * 1024)
                    .build();
            }
        }
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
