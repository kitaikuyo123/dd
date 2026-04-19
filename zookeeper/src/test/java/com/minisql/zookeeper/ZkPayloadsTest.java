package com.minisql.zookeeper;

import com.minisql.common.model.ServerId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ZkPayloads 单元测试")
class ZkPayloadsTest {

    // ---- encodeLeader / decodeLeaderAddress ----

    @Test
    @DisplayName("encodeLeader + decodeLeaderAddress 往返")
    void encodeDecodeLeaderRoundTrip() {
        ServerId server = new ServerId("host1", 9090);
        byte[] encoded = ZkPayloads.encodeLeader(server);
        String address = ZkPayloads.decodeLeaderAddress(encoded);

        assertEquals("host1:9090", address);
    }

    @Test
    @DisplayName("decodeLeaderAddress null 输入返回 null")
    void decodeLeaderNull() {
        assertNull(ZkPayloads.decodeLeaderAddress(null));
    }

    @Test
    @DisplayName("decodeLeaderAddress 空数组返回 null")
    void decodeLeaderEmpty() {
        assertNull(ZkPayloads.decodeLeaderAddress(new byte[0]));
    }

    @Test
    @DisplayName("decodeLeaderAddress 无效 JSON 返回 null")
    void decodeLeaderInvalidJson() {
        assertNull(ZkPayloads.decodeLeaderAddress("not json".getBytes()));
    }

    @Test
    @DisplayName("encodeLeader 输出包含 host/port/instanceName")
    void encodeLeaderContent() {
        ServerId server = new ServerId("myhost", 8080);
        byte[] encoded = ZkPayloads.encodeLeader(server);
        String json = new String(encoded);

        assertTrue(json.contains("\"host\":\"myhost\""));
        assertTrue(json.contains("\"port\":8080"));
        assertTrue(json.contains("\"instanceName\":"));
    }

    // ---- encodeRegionServerNode ----

    @Test
    @DisplayName("encodeRegionServerNode 包含所有字段")
    void encodeRegionServerNode() {
        ServerId server = new ServerId("rs1", 16020);
        byte[] encoded = ZkPayloads.encodeRegionServerNode(server, "grpc:123", "mysql:3306", "root", 1000L, 5L);
        String json = new String(encoded);

        assertTrue(json.contains("\"serverId\":"));
        assertTrue(json.contains("\"host\":\"rs1\""));
        assertTrue(json.contains("\"port\":16020"));
        assertTrue(json.contains("\"grpcAddress\":\"grpc:123\""));
        assertTrue(json.contains("\"mysqlUrl\":\"mysql:3306\""));
        assertTrue(json.contains("\"mysqlUser\":\"root\""));
        assertTrue(json.contains("\"startedAt\":1000"));
        assertTrue(json.contains("\"metricsVersion\":5"));
    }

    // ---- extractString ----

    @Test
    @DisplayName("extractString 正常提取")
    void extractStringBasic() {
        assertEquals("hello", ZkPayloads.extractString("{\"name\":\"hello\"}", "name"));
    }

    @Test
    @DisplayName("extractString 多字段提取")
    void extractStringMultipleFields() {
        String json = "{\"a\":\"1\",\"b\":\"2\"}";
        assertEquals("1", ZkPayloads.extractString(json, "a"));
        assertEquals("2", ZkPayloads.extractString(json, "b"));
    }

    @Test
    @DisplayName("extractString 字段不存在返回 null")
    void extractStringNotFound() {
        assertNull(ZkPayloads.extractString("{\"a\":\"1\"}", "b"));
    }

    @Test
    @DisplayName("extractString null 输入返回 null")
    void extractStringNull() {
        assertNull(ZkPayloads.extractString(null, "field"));
    }

    @Test
    @DisplayName("extractString 含转义引号")
    void extractStringEscapedQuotes() {
        assertEquals("he\"llo", ZkPayloads.extractString("{\"v\":\"he\\\"llo\"}", "v"));
    }

    // ---- extractInt / extractLong ----

    @Test
    @DisplayName("extractInt 正常提取")
    void extractIntBasic() {
        assertEquals(42, ZkPayloads.extractInt("{\"count\":42}", "count"));
    }

    @Test
    @DisplayName("extractInt 字段不存在返回 null")
    void extractIntNotFound() {
        assertNull(ZkPayloads.extractInt("{\"a\":1}", "b"));
    }

    @Test
    @DisplayName("extractInt null 输入返回 null")
    void extractIntNull() {
        assertNull(ZkPayloads.extractInt(null, "field"));
    }

    @Test
    @DisplayName("extractLong 正常提取")
    void extractLongBasic() {
        assertEquals(123456789L, ZkPayloads.extractLong("{\"ts\":123456789}", "ts"));
    }

    @Test
    @DisplayName("extractLong 负数")
    void extractLongNegative() {
        assertEquals(-1L, ZkPayloads.extractLong("{\"val\":-1}", "val"));
    }

    @Test
    @DisplayName("extractLong 字段不存在返回 null")
    void extractLongNotFound() {
        assertNull(ZkPayloads.extractLong("{\"a\":1}", "b"));
    }

    @Test
    @DisplayName("extractLong 后跟逗号分隔符")
    void extractLongFollowedByComma() {
        assertEquals(100L, ZkPayloads.extractLong("{\"a\":100,\"b\":200}", "a"));
        assertEquals(200L, ZkPayloads.extractLong("{\"a\":100,\"b\":200}", "b"));
    }
}
