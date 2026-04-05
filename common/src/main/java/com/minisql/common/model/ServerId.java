package com.minisql.common.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 服务器唯一标识
 */
public class ServerId implements Serializable, Comparable<ServerId> {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final long startTime;

    public ServerId(String host, int port) {
        this(host, port, System.currentTimeMillis());
    }

    public ServerId(String host, int port, long startTime) {
        this.host = host;
        this.port = port;
        this.startTime = startTime;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getServerName() {
        return host + ":" + port;
    }

    public String getInstanceName() {
        return host + ":" + port + "@" + startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerId serverId = (ServerId) o;
        return port == serverId.port && Objects.equals(host, serverId.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public int compareTo(ServerId other) {
        int cmp = this.host.compareTo(other.host);
        if (cmp != 0) return cmp;
        return Integer.compare(this.port, other.port);
    }

    @Override
    public String toString() {
        return "ServerId{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", startTime=" + startTime +
                '}';
    }
}
