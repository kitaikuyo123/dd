package com.minisql.master;

import com.minisql.common.model.ServerId;
import com.minisql.master.state.ClusterManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

import static com.minisql.master.rebalance.LoadBalancer.LoadCalculator;

@DisplayName("LoadCalculator 测试")
class LoadCalculatorTest {

    @Test
    @DisplayName("测试创建 LoadCalculator")
    void testCreateLoadCalculator() {
        LoadCalculator calculator = new LoadCalculator();
        assertNotNull(calculator);
    }

    @Test
    @DisplayName("测试计算服务器负载分数 — 每个 Region 贡献 10 分")
    void testCalculateLoadScore() {
        LoadCalculator calculator = new LoadCalculator();

        ServerId server = new ServerId("host1", 8080);
        ClusterManager.ServerInfo info = new ClusterManager.ServerInfo(server, System.currentTimeMillis());

        // 0 个 Region → 0 分
        assertEquals(0.0, calculator.calculateLoadScore(info));

        // 添加 5 个 Region → 50 分
        for (int i = 0; i < 5; i++) {
            info.updateRegionLoad("region-" + i, new ClusterManager.RegionLoad());
        }
        assertEquals(50.0, calculator.calculateLoadScore(info));
    }

    @Test
    @DisplayName("测试计算剩余容量")
    void testCalculateRemainingCapacity() {
        LoadCalculator calculator = new LoadCalculator();

        ServerId server = new ServerId("host1", 8080);
        ClusterManager.ServerInfo info = new ClusterManager.ServerInfo(server, System.currentTimeMillis());

        // 0 个 Region → 剩余容量 100
        assertEquals(100.0, calculator.getRemainingCapacity(info));

        // 3 个 Region → 剩余容量 70
        for (int i = 0; i < 3; i++) {
            info.updateRegionLoad("region-" + i, new ClusterManager.RegionLoad());
        }
        assertEquals(70.0, calculator.getRemainingCapacity(info));
    }

    @Test
    @DisplayName("测试判断服务器是否过载 — Region > 7 时过载")
    void testIsOverloaded() {
        LoadCalculator calculator = new LoadCalculator();

        ServerId server = new ServerId("host1", 8080);
        ClusterManager.ServerInfo info = new ClusterManager.ServerInfo(server, System.currentTimeMillis());

        // 7 个 Region → 70 分 → 不过载（需要 > 70）
        for (int i = 0; i < 7; i++) {
            info.updateRegionLoad("region-" + i, new ClusterManager.RegionLoad());
        }
        assertFalse(calculator.isOverloaded(info));

        // 8 个 Region → 80 分 → 过载
        info.updateRegionLoad("region-7", new ClusterManager.RegionLoad());
        assertTrue(calculator.isOverloaded(info));
    }

    @Test
    @DisplayName("测试判断服务器是否空闲 — Region < 3 时空闲")
    void testIsIdle() {
        LoadCalculator calculator = new LoadCalculator();

        ServerId server = new ServerId("host1", 8080);
        ClusterManager.ServerInfo info = new ClusterManager.ServerInfo(server, System.currentTimeMillis());

        // 0 个 Region → 空闲
        assertTrue(calculator.isIdle(info));

        // 2 个 Region → 20 分 → 空闲（需要 < 30）
        info.updateRegionLoad("region-0", new ClusterManager.RegionLoad());
        info.updateRegionLoad("region-1", new ClusterManager.RegionLoad());
        assertTrue(calculator.isIdle(info));

        // 3 个 Region → 30 分 → 不空闲
        info.updateRegionLoad("region-2", new ClusterManager.RegionLoad());
        assertFalse(calculator.isIdle(info));
    }
}
