package com.minisql.master.monitoring;

import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.master.rebalance.LoadBalancer;
import com.minisql.master.rebalance.RegionMigrationCoordinator;
import com.minisql.master.rebalance.RegionSplitCoordinator;
import com.minisql.master.state.ClusterManager;
import com.minisql.master.state.MetadataManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 演示场景编排服务
 *
 * 提供演示模式所需的后端操作：建表、插入数据、杀节点、恢复、触发负载均衡。
 * 前端通过 MonitorHttpServer 的演示 API 调用。
 */
public class DemoService {

    private static final String DEMO_SQL_SETUP =
        "CREATE TABLE demo_users (id INT PRIMARY KEY, name STRING, age INT);\n" +
        "CREATE TABLE demo_orders (id INT PRIMARY KEY, user_id INT, amount DOUBLE, status STRING);\n" +
        "INSERT INTO demo_users (id, name, age) VALUES (1, 'alice', 25);\n" +
        "INSERT INTO demo_users (id, name, age) VALUES (2, 'bob', 30);\n" +
        "INSERT INTO demo_users (id, name, age) VALUES (3, 'carol', 28);\n" +
        "INSERT INTO demo_users (id, name, age) VALUES (4, 'dave', 35);\n" +
        "INSERT INTO demo_users (id, name, age) VALUES (5, 'eve', 22);\n" +
        "INSERT INTO demo_users (id, name, age) VALUES (6, 'frank', 40);\n" +
        "INSERT INTO demo_users (id, name, age) VALUES (7, 'grace', 27);\n" +
        "INSERT INTO demo_users (id, name, age) VALUES (8, 'henry', 33);\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (1, 1, 100.0, 'paid');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (2, 2, 200.0, 'pending');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (3, 1, 50.0, 'paid');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (4, 3, 150.0, 'paid');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (5, 2, 80.0, 'paid');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (6, 4, 300.0, 'pending');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (7, 6, 250.0, 'shipped');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (8, 7, 90.0, 'paid');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (9, 1, 75.0, 'shipped');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (10, 3, 60.0, 'pending');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (11, 8, 180.0, 'cancelled');\n" +
        "INSERT INTO demo_orders (id, user_id, amount, status) VALUES (12, 4, 120.0, 'paid');";

    private final MonitoringService monitoringService;
    private final ClusterManager clusterManager;
    private final MetadataManager metadataManager;
    private final LoadBalancer loadBalancer;
    private final RegionMigrationCoordinator migrationCoordinator;
    private final RegionSplitCoordinator splitCoordinator;
    private final SqlConsoleService sqlConsoleService;
    private final String projectRoot;

    public DemoService(MonitoringService monitoringService,
                       ClusterManager clusterManager,
                       MetadataManager metadataManager,
                       LoadBalancer loadBalancer,
                       RegionMigrationCoordinator migrationCoordinator,
                       RegionSplitCoordinator splitCoordinator,
                       SqlConsoleService sqlConsoleService) {
        this.monitoringService = monitoringService;
        this.clusterManager = clusterManager;
        this.metadataManager = metadataManager;
        this.loadBalancer = loadBalancer;
        this.migrationCoordinator = migrationCoordinator;
        this.splitCoordinator = splitCoordinator;
        this.sqlConsoleService = sqlConsoleService;
        this.projectRoot = detectProjectRoot();
        System.out.println("[DemoService] projectRoot detected: " + this.projectRoot);
    }

    /** 建表 + 插入演示数据（先尝试删除旧表） */
    public Map<String, Object> setupDemoData() {
        // 先尝试删除旧表（忽略错误）
        try { sqlConsoleService.execute("DROP TABLE demo_orders;"); } catch (Exception ignored) {}
        try { sqlConsoleService.execute("DROP TABLE demo_users;"); } catch (Exception ignored) {}
        return executeSqlAndWrap(DEMO_SQL_SETUP);
    }

    /** 获取第一个表的 Primary RS 端口 */
    public Map<String, Object> getPrimaryServerPort(String tableName) {
        Map<String, Object> result = new HashMap<>();
        for (Region region : getAllRegions()) {
            if (tableName != null && !tableName.equals(region.getTableName())) {
                continue;
            }
            ServerId primary = clusterManager.getPrimaryServerForRegion(region.getRegionId());
            if (primary != null) {
                result.put("port", primary.getPort());
                result.put("host", primary.getHost());
                result.put("regionId", region.getRegionId());
                result.put("tableName", region.getTableName());
                return result;
            }
        }
        result.put("error", "No primary found for table: " + tableName);
        return result;
    }

    /** 杀掉指定端口的 RS 进程 */
    public Map<String, Object> killServer(int port) {
        Map<String, Object> result = new HashMap<>();
        try {
            String scriptPath = projectRoot + File.separator + "scripts" + File.separator + "stop-port.ps1";
            File scriptFile = new File(scriptPath);
            if (!scriptFile.exists()) {
                result.put("success", false);
                result.put("error", "Script not found: " + scriptPath + " (projectRoot=" + projectRoot + ")");
                return result;
            }
            ProcessBuilder pb = new ProcessBuilder("powershell", "-ExecutionPolicy", "Bypass",
                "-File", scriptPath, "-Port", String.valueOf(port));
            pb.redirectErrorStream(true);
            Process process = pb.start();
            int exitCode = process.waitFor();
            result.put("success", exitCode == 0);
            result.put("port", port);
            if (exitCode != 0) {
                // 读取脚本输出以便调试
                String output = new String(process.getInputStream().readAllBytes());
                result.put("error", "Process exited with code " + exitCode + ": " + output.trim());
            }
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /** 重启指定编号的 RS */
    public Map<String, Object> restartServer(int instanceNumber) {
        Map<String, Object> result = new HashMap<>();
        try {
            String scriptPath = projectRoot + File.separator + "scripts" + File.separator + "start-regionserver.bat";
            File scriptFile = new File(scriptPath);
            if (!scriptFile.exists()) {
                result.put("success", false);
                result.put("error", "Script not found: " + scriptPath + " (projectRoot=" + projectRoot + ")");
                return result;
            }
            ProcessBuilder pb = new ProcessBuilder("cmd", "/c", "start",
                "\"MiniSQL RS " + instanceNumber + "\"", scriptPath, String.valueOf(instanceNumber), "--skip-compile");
            pb.redirectErrorStream(true);
            Process process = pb.start();
            result.put("success", true);
            result.put("instance", instanceNumber);
            result.put("scriptPath", scriptPath);
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /** 强制分裂指定表的 region */
    public Map<String, Object> forceSplit(String tableName) {
        Map<String, Object> result = new HashMap<>();
        if (splitCoordinator == null) {
            result.put("success", false);
            result.put("error", "Split coordinator not available");
            return result;
        }
        for (Region region : getAllRegions()) {
            if (region.getTableName() != null && region.getTableName().equals(tableName)) {
                String regionId = region.getRegionId();
                try {
                    boolean accepted = splitCoordinator.checkAndSplitRegion(regionId);
                    result.put("success", accepted);
                    result.put("regionId", regionId);
                    result.put("message", accepted
                        ? "Split scheduled for region " + regionId
                        : "Split rejected (already splitting or region not ready)");
                } catch (Exception e) {
                    result.put("success", false);
                    result.put("error", e.getMessage());
                }
                return result;
            }
        }
        result.put("success", false);
        result.put("error", "No region found for table: " + tableName);
        return result;
    }

    /** 触发负载均衡 */
    public Map<String, Object> triggerBalance() {
        Map<String, Object> result = new HashMap<>();
        try {
            List<ClusterManager.ServerInfo> servers = new ArrayList<>(clusterManager.getActiveServers());
            if (servers.size() < 2) {
                result.put("success", true);
                result.put("message", "Not enough servers for balancing");
                result.put("actions", 0);
                return result;
            }
            List<LoadBalancer.BalanceAction> actions = loadBalancer.computeBalanceActions(servers);
            for (LoadBalancer.BalanceAction action : actions) {
                try {
                    migrationCoordinator.execute(action);
                } catch (Exception e) {
                    monitoringService.recordEvent("BALANCE_ERROR", "WARN",
                        action.getRegionId(), null,
                        action.getSource() != null ? action.getSource().getHost() + ":" + action.getSource().getPort() : null,
                        action.getTarget() != null ? action.getTarget().getHost() + ":" + action.getTarget().getPort() : null,
                        "Balance action failed", e.getMessage());
                }
            }
            result.put("success", true);
            result.put("actions", actions.size());
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /** 执行 SQL */
    public Map<String, Object> executeSql(String sql) {
        return executeSqlAndWrap(sql);
    }

    private Map<String, Object> executeSqlAndWrap(String sql) {
        Map<String, Object> result = new HashMap<>();
        try {
            if (sqlConsoleService == null) {
                result.put("success", false);
                result.put("error", "SQL console not available");
                return result;
            }
            List<Map<String, Object>> rows = sqlConsoleService.execute(sql);
            result.put("success", true);
            result.put("results", rows);
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    private List<Region> getAllRegions() {
        if (metadataManager != null) {
            return new ArrayList<>(metadataManager.getAllRegions());
        }
        List<Region> allRegions = new ArrayList<>();
        for (ClusterManager.ServerInfo info : clusterManager.getActiveServers()) {
            for (String regionId : info.getRegionLoads().keySet()) {
                Region region = metadataManager.getRegion(regionId);
                if (region != null) {
                    allRegions.add(region);
                }
            }
        }
        return allRegions;
    }

    private String detectProjectRoot() {
        String cwd = System.getProperty("user.dir");
        File dir = new File(cwd);
        while (dir != null) {
            if (new File(dir, "scripts" + File.separator + "start-master.bat").exists()) {
                return dir.getAbsolutePath();
            }
            dir = dir.getParentFile();
        }
        return cwd;
    }
}
