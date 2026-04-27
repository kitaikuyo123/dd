package com.minisql.common.rpc;

import javax.net.ssl.SSLException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.File;

/**
 * TLS/SSL configuration for gRPC channels.
 *
 * When no trust/cert paths are configured, plaintext is used (backward compatible).
 * When configured, all gRPC channels use TLS with the provided certificates.
 */
public class GrpcSslConfig {

    private final String trustCertPath;
    private final String keyCertPath;
    private final String keyPath;

    private GrpcSslConfig(Builder builder) {
        this.trustCertPath = builder.trustCertPath;
        this.keyCertPath = builder.keyCertPath;
        this.keyPath = builder.keyPath;
    }

    public boolean isTlsEnabled() {
        return trustCertPath != null && !trustCertPath.isEmpty();
    }

    public String getTrustCertPath() { return trustCertPath; }
    public String getKeyCertPath() { return keyCertPath; }
    public String getKeyPath() { return keyPath; }

    /**
     * Build the client-side SSL context. Returns null when TLS is not configured
     * (fall back to plaintext).
     */
    public SslContext buildClientSslContext() {
        if (!isTlsEnabled()) {
            return null;
        }
        try {
            SslContextBuilder builder = GrpcSslContexts.forClient();
            if (trustCertPath != null && !trustCertPath.isEmpty()) {
                builder.trustManager(new File(trustCertPath));
            }
            if (keyCertPath != null && !keyCertPath.isEmpty() && keyPath != null && !keyPath.isEmpty()) {
                builder.keyManager(new File(keyCertPath), new File(keyPath));
            }
            return builder.build();
        } catch (SSLException e) {
            throw new RuntimeException("Failed to build gRPC SSL context", e);
        }
    }

    /**
     * Build the server-side SSL context. Returns null when TLS is not configured.
     */
    public SslContext buildServerSslContext() {
        if (!isTlsEnabled() || keyCertPath == null || keyPath == null) {
            return null;
        }
        try {
            return GrpcSslContexts.forServer(new File(keyCertPath), new File(keyPath))
                .trustManager(new File(trustCertPath))
                .build();
        } catch (SSLException e) {
            throw new RuntimeException("Failed to build gRPC server SSL context", e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String trustCertPath;
        private String keyCertPath;
        private String keyPath;

        public Builder trustCertPath(String path) {
            this.trustCertPath = path;
            return this;
        }

        public Builder keyCertPath(String path) {
            this.keyCertPath = path;
            return this;
        }

        public Builder keyPath(String path) {
            this.keyPath = path;
            return this;
        }

        public GrpcSslConfig build() {
            return new GrpcSslConfig(this);
        }
    }
}
