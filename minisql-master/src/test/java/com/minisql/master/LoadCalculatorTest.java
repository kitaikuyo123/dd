package com.minisql.master;

import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.state.ClusterManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

// 导入静态内部类
import static com.minisql.master.rebalance.LoadBalancer.LoadCalculator;

/**
 * LoadCalculator 单元测试
 */
@DisplayName("LoadCalculator 测试")
class LoadCalculatorTest {

    @Test
    @DisplayName("测试创建 LoadCalculator")
    void testCreateLoadCalculator() {
        LoadCalculator calculator = new LoadCalculator();
        assertNotNull(calculator);
    }

    @Test
    @DisplayName("测试计算服务器负载分数")
    void testCalculateLoadScore() {
        LoadCalculator calculator = new LoadCalculator();

        ServerId server = new ServerId("host1", 8080);
        ClusterManager.ServerInfo info = new ClusterManager.ServerInfo(server, System.currentTimeMillis());

        // 测试空指标
        double load = calculator.calculateLoadScore(info);
        assertTrue(load >= 0.0);

        // 测试正常指标
        ClusterManager.ServerMetrics metrics = new ClusterManager.ServerMetrics();
        metrics.setCpuUsage(50.0);
        metrics.setMemoryUsage(60.0);
        metrics.setTotalSpace(1000L);
        metrics.setAvailableSpace(900L);
        info.setMetrics(metrics);

        load = calculator.calculateLoadScore(info);
        assertTrue(load >= 0.0 && load <= 100.0);
    }

    @Test
    @DisplayName("测试计算剩余容量")
    void testCalculateRemainingCapacity() {
        LoadCalculator calculator = new LoadCalculator();

        ServerId server = new ServerId("host1", 8080);
        ClusterManager.ServerInfo info = new ClusterManager.ServerInfo(server, System.currentTimeMillis());

        ClusterManager.ServerMetrics metrics = new ClusterManager.ServerMetrics();
        metrics.setCpuUsage(30.0);
        metrics.setMemoryUsage(30.0);
        metrics.setTotalSpace(1000L);
        metrics.setAvailableSpace(700L);
        info.setMetrics(metrics);

        double remaining = calculator.getRemainingCapacity(info);
        assertTrue(remaining > 0);
    }

    @Test
    @DisplayName("测试判断服务器是否过载")
    void testIsOverloaded() {
        LoadCalculator calculator = new LoadCalculator();

        ServerId idleServer = new ServerId("host1", 8080);
        ServerId busyServer = new ServerId("host2", 8080);

        ClusterManager.ServerInfo idleInfo = new ClusterManager.ServerInfo(idleServer, System.currentTimeMillis());
        ClusterManager.ServerInfo busyInfo = new ClusterManager.ServerInfo(busyServer, System.currentTimeMillis());

        ClusterManager.ServerMetrics idleMetrics = new ClusterManager.ServerMetrics();
        idleMetrics.setCpuUsage(10.0);
        idleMetrics.setMemoryUsage(10.0);
        idleMetrics.setTotalSpace(1000L);
        idleMetrics.setAvailableSpace(900L);
        idleInfo.setMetrics(idleMetrics);

        // 设置高负载指标使服务器过载（需要超过 80 分）
        ClusterManager.ServerMetrics busyMetrics = new ClusterManager.ServerMetrics();
        busyMetrics.setCpuUsage(90.0);
        busyMetrics.setMemoryUsage(85.0);
        busyMetrics.setTotalSpace(1000L);
        busyMetrics.setAvailableSpace(10L);  // 99% 磁盘使用率
        busyInfo.setMetrics(busyMetrics);

        // 添加 Region 负载增加分数（每个 Region 约 1 分）
        for (int i = 0; i < 10; i++) {
            ClusterManager.RegionLoad load = new ClusterManager.RegionLoad();
            load.setRegionId("region-" + i);
            load.setReadRequests(10000);
            load.setWriteRequests(5000);
            load.setStoreFileSize(100 * 1024 * 1024); // 100MB
            busyInfo.updateRegionLoad("region-" + i, load);
        }

        assertFalse(calculator.isOverloaded(idleInfo));
        assertTrue(calculator.isOverloaded(busyInfo));
    }

    @Test
    @DisplayName("测试判断服务器是否空闲")
    void testIsIdle() {
        LoadCalculator calculator = new LoadCalculator();

        ServerId idleServer = new ServerId("host1", 8080);
        ClusterManager.ServerInfo info = new ClusterManager.ServerInfo(idleServer, System.currentTimeMillis());

        ClusterManager.ServerMetrics metrics = new ClusterManager.ServerMetrics();
        metrics.setCpuUsage(10.0);
        metrics.setMemoryUsage(10.0);
        metrics.setTotalSpace(1000L);
        metrics.setAvailableSpace(900L);
        info.setMetrics(metrics);

        assertTrue(calculator.isIdle(info));
    }
}
