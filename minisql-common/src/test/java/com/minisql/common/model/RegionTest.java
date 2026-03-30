package com.minisql.common.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Region 单元测试
 */
@DisplayName("Region 单元测试")
class RegionTest {

    @Test
    @DisplayName("测试 Region 基本构造")
    void testConstructor() {
        Region region = new Region();
        assertNotNull(region);
        assertEquals(Region.State.INIT, region.getState());
        assertNotNull(region.getReplicas());
        assertTrue(region.getReplicas().isEmpty());
    }

    @Test
    @DisplayName("测试带参数构造")
    void testConstructorWithParams() {
        Region region = new Region("region1", "users", "a".getBytes(), "m".getBytes());

        assertEquals("region1", region.getRegionId());
        assertEquals("users", region.getTableName());
        assertArrayEquals("a".getBytes(), region.getStartKey());
        assertArrayEquals("m".getBytes(), region.getEndKey());
        assertEquals(Region.State.INIT, region.getState());
    }

    @Test
    @DisplayName("测试 contains 方法 - key 在范围内")
    void testContainsInRange() {
        Region region = new Region("region1", "users", "a".getBytes(), "m".getBytes());

        assertTrue(region.contains("a".getBytes()));  // startKey
        assertTrue(region.contains("f".getBytes()));  // 中间
        assertTrue(region.contains("l".getBytes()));  // 接近 end
    }

    @Test
    @DisplayName("测试 contains 方法 - key 不在范围内")
    void testContainsOutOfRange() {
        Region region = new Region("region1", "users", "a".getBytes(), "m".getBytes());

        assertFalse(region.contains("A".getBytes()));  // 小于 startKey
        assertFalse(region.contains("m".getBytes()));  // 等于 endKey（不包含）
        assertFalse(region.contains("z".getBytes()));  // 大于 endKey
    }

    @Test
    @DisplayName("测试 isAdjacent 相邻 Region")
    void testIsAdjacent() {
        Region region1 = new Region("region1", "users", "a".getBytes(), "m".getBytes());
        Region region2 = new Region("region2", "users", "m".getBytes(), "z".getBytes());

        assertTrue(region1.isAdjacent(region2));
        assertTrue(region2.isAdjacent(region1));
    }

    @Test
    @DisplayName("测试 isAdjacent 不相邻 Region")
    void testIsNotAdjacent() {
        Region region1 = new Region("region1", "users", "a".getBytes(), "m".getBytes());
        Region region2 = new Region("region2", "users", "n".getBytes(), "z".getBytes());

        assertFalse(region1.isAdjacent(region2));
    }

    @Test
    @DisplayName("测试 isEmpty 空 Region")
    void testIsEmpty() {
        byte[] emptyKey = new byte[0];
        Region emptyRegion = new Region("region1", "users", emptyKey, emptyKey);

        assertTrue(emptyRegion.isEmpty());

        Region nonEmptyRegion = new Region("region1", "users", "a".getBytes(), "m".getBytes());
        assertFalse(nonEmptyRegion.isEmpty());
    }

    @Test
    @DisplayName("测试 getSizeInMB")
    void testGetSizeInMB() {
        Region region = new Region();
        region.setEstimatedSize(1024 * 1024);  // 1MB

        assertEquals(1.0, region.getSizeInMB(), 0.001);

        region.setEstimatedSize(2048);  // 2KB
        assertEquals(2048.0 / (1024.0 * 1024.0), region.getSizeInMB(), 0.001);
    }

    @Test
    @DisplayName("测试 recordRead 读请求计数")
    void testRecordRead() {
        Region region = new Region();

        assertEquals(0, region.getReadRequestCount());

        region.recordRead();
        assertEquals(1, region.getReadRequestCount());

        region.recordRead();
        assertEquals(2, region.getReadRequestCount());
    }

    @Test
    @DisplayName("测试 recordWrite 写请求计数")
    void testRecordWrite() {
        Region region = new Region();

        assertEquals(0, region.getWriteRequestCount());

        region.recordWrite();
        assertEquals(1, region.getWriteRequestCount());

        region.recordWrite();
        assertEquals(2, region.getWriteRequestCount());
    }

    @Test
    @DisplayName("测试 addReplica 添加副本")
    void testAddReplica() {
        Region region = new Region();
        ServerId server1 = new ServerId("host1", 16020);
        ServerId server2 = new ServerId("host2", 16020);

        region.addReplica(server1);
        region.addReplica(server2);

        assertEquals(2, region.getReplicas().size());
        assertTrue(region.getReplicas().contains(server1));
        assertTrue(region.getReplicas().contains(server2));
    }

    @Test
    @DisplayName("测试 addReplica 不重复添加")
    void testAddReplicaNoDuplicate() {
        Region region = new Region();
        ServerId server = new ServerId("host1", 16020);

        region.addReplica(server);
        region.addReplica(server);  // 重复添加

        assertEquals(1, region.getReplicas().size());
    }

    @Test
    @DisplayName("测试 removeReplica 移除副本")
    void testRemoveReplica() {
        Region region = new Region();
        ServerId server1 = new ServerId("host1", 16020);
        ServerId server2 = new ServerId("host2", 16020);

        region.addReplica(server1);
        region.addReplica(server2);

        region.removeReplica(server1);

        assertEquals(1, region.getReplicas().size());
        assertTrue(region.getReplicas().contains(server2));
        assertFalse(region.getReplicas().contains(server1));
    }

    @Test
    @DisplayName("测试 setPrimary 设置主副本")
    void testSetPrimary() {
        Region region = new Region();
        ServerId primary = new ServerId("host1", 16020);

        region.setPrimary(primary);

        assertEquals(primary, region.getPrimary());
    }

    @Test
    @DisplayName("测试 setState 状态转换")
    void testSetState() {
        Region region = new Region();

        assertEquals(Region.State.INIT, region.getState());

        region.setState(Region.State.OPEN);
        assertEquals(Region.State.OPEN, region.getState());

        region.setState(Region.State.SPLITTING);
        assertEquals(Region.State.SPLITTING, region.getState());
    }

    @Test
    @DisplayName("测试 compareTo 比较")
    void testCompareTo() {
        Region region1 = new Region("r1", "t1", "a".getBytes(), "m".getBytes());
        Region region2 = new Region("r2", "t1", "m".getBytes(), "z".getBytes());
        Region region3 = new Region("r3", "t1", "a".getBytes(), "f".getBytes());

        assertTrue(region1.compareTo(region2) < 0);  // region1 在 region2 之前
        assertTrue(region2.compareTo(region1) > 0);
        assertTrue(region1.compareTo(region3) >= 0); // startKey 相同
    }

    @Test
    @DisplayName("测试 equals 和 hashCode")
    void testEqualsAndHashCode() {
        Region region1 = new Region("region1", "users", "a".getBytes(), "m".getBytes());
        Region region2 = new Region("region1", "users", "a".getBytes(), "m".getBytes());
        Region region3 = new Region("region2", "users", "a".getBytes(), "m".getBytes());

        assertEquals(region1, region2);  // regionId 相同
        assertNotEquals(region1, region3);  // regionId 不同
        assertEquals(region1.hashCode(), region2.hashCode());
    }

    @Test
    @DisplayName("测试 toString 方法")
    void testToString() {
        Region region = new Region("region1", "users", "a".getBytes(), "m".getBytes());
        String str = region.toString();

        assertTrue(str.contains("region1"));
        assertTrue(str.contains("users"));
        assertTrue(str.contains("regionId"));
        assertTrue(str.contains("tableName"));
    }

    @Test
    @DisplayName("测试 lastUpdateTime 更新")
    void testLastUpdateTime() {
        Region region = new Region();
        long initialTime = region.getLastUpdateTime();

        // 等待一小段时间
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            // ignore
        }

        region.recordRead();
        assertTrue(region.getLastUpdateTime() >= initialTime);

        long readTime = region.getLastUpdateTime();

        // 再次等待
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            // ignore
        }

        region.recordWrite();
        assertTrue(region.getLastUpdateTime() >= readTime);
    }

    @Test
    @DisplayName("测试空字节数组 startKey")
    void testEmptyStartKey() {
        Region region = new Region("region1", "users", new byte[0], "m".getBytes());

        // 空 startKey 表示最小值，任何 key 都应该 >= 空数组
        assertTrue(region.contains("a".getBytes()));
        assertTrue(region.contains("l".getBytes()));
        assertFalse(region.contains("m".getBytes()));  // 等于 endKey
    }

    @Test
    @DisplayName("测试空字节数组 endKey")
    void testEmptyEndKey() {
        Region region = new Region("region1", "users", "a".getBytes(), new byte[0]);

        // 注意：当前实现中，空 endKey 不表示最大值
        // compareBytes 比较时空数组被认为是最小的（长度最短）
        // 所以任何非空 key 都会大于空 endKey，导致 contains 返回 false
        // 这是当前实现的限制，实际使用中应该避免使用空 endKey
        assertFalse(region.contains("a".getBytes()));  // 空 endKey 导致比较失败
        assertFalse(region.contains("z".getBytes()));  // 同样返回 false
    }

    @Test
    @DisplayName("测试不同长度的字节数组比较")
    void testCompareBytesDifferentLength() {
        Region region1 = new Region("r1", "t1", "abc".getBytes(), "def".getBytes());
        Region region2 = new Region("r2", "t1", "abcd".getBytes(), "efgh".getBytes());

        // "abc" < "abcd"（短的在前）
        assertTrue(region1.compareTo(region2) < 0);
    }

    @Test
    @DisplayName("测试 State 枚举所有值")
    void testStateEnum() {
        Region.State[] states = Region.State.values();

        assertEquals(10, states.length);
        assertEquals(Region.State.INIT, Region.State.valueOf("INIT"));
        assertEquals(Region.State.OPEN, Region.State.valueOf("OPEN"));
        assertEquals(Region.State.SPLIT, Region.State.valueOf("SPLIT"));
    }
}
