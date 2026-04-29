package com.minisql.zookeeper;

import com.minisql.common.model.ServerId;
import java.nio.charset.StandardCharsets;

/** ZooKeeper 节点数据的 JSON 序列化工具 */
public final class ZkPayloads {

    private ZkPayloads() {
    }

    public static byte[] encodeLeader(ServerId serverId) {
        return ("{\"host\":\"" + escape(serverId.getHost()) + "\"," +
            "\"port\":" + serverId.getPort() + "," +
            "\"instanceName\":\"" + escape(serverId.getInstanceName()) + "\"}")
            .getBytes(StandardCharsets.UTF_8);
    }

    public static String decodeLeaderAddress(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        String json = new String(data, StandardCharsets.UTF_8);
        String host = extractString(json, "host");
        Integer port = extractInt(json, "port");
        if (host == null || port == null) {
            return null;
        }
        return host + ":" + port;
    }

    public static byte[] encodeRegionServerNode(ServerId serverId,
                                                String grpcAddress,
                                                String mysqlUrl,
                                                String mysqlUser,
                                                long startedAt,
                                                long metricsVersion) {
        return ("{\"serverId\":\"" + escape(serverId.getInstanceName()) + "\"," +
            "\"host\":\"" + escape(serverId.getHost()) + "\"," +
            "\"port\":" + serverId.getPort() + "," +
            "\"grpcAddress\":\"" + escape(grpcAddress) + "\"," +
            "\"mysqlUrl\":\"" + escape(mysqlUrl) + "\"," +
            "\"mysqlUser\":\"" + escape(mysqlUser) + "\"," +
            "\"startedAt\":" + startedAt + "," +
            "\"metricsVersion\":" + metricsVersion + "}")
            .getBytes(StandardCharsets.UTF_8);
    }

    public static String extractString(String json, String field) {
        if (json == null) {
            return null;
        }
        String marker = "\"" + field + "\":\"";
        int start = json.indexOf(marker);
        if (start < 0) {
            return null;
        }
        int valueStart = start + marker.length();
        int end = valueStart;
        while (end < json.length()) {
            if (json.charAt(end) == '"' && json.charAt(end - 1) != '\\') {
                break;
            }
            end++;
        }
        if (end >= json.length()) {
            return null;
        }
        return json.substring(valueStart, end).replace("\\\"", "\"").replace("\\\\", "\\");
    }

    public static Integer extractInt(String json, String field) {
        Long value = extractLong(json, field);
        return value == null ? null : value.intValue();
    }

    public static Long extractLong(String json, String field) {
        if (json == null) {
            return null;
        }
        String marker = "\"" + field + "\":";
        int start = json.indexOf(marker);
        if (start < 0) {
            return null;
        }
        int valueStart = start + marker.length();
        int end = valueStart;
        while (end < json.length()) {
            char ch = json.charAt(end);
            if ((ch < '0' || ch > '9') && ch != '-') {
                break;
            }
            end++;
        }
        if (end == valueStart) {
            return null;
        }
        try {
            return Long.parseLong(json.substring(valueStart, end));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
