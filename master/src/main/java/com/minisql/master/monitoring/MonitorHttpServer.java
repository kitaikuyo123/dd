package com.minisql.master.monitoring;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 监控 HTTP 服务器
 *
 * 提供 Web 监控面板的静态资源和 REST API，以及 SSE 实时推送和演示模式 API。
 */
public class MonitorHttpServer {

    private final MonitoringService monitoringService;
    private final SqlConsoleService sqlConsoleService;
    private DemoService demoService;
    private HttpServer httpServer;

    // SSE 基础设施
    private final CopyOnWriteArrayList<SseClient> sseClients = new CopyOnWriteArrayList<>();
    private ScheduledExecutorService sseScheduler;

    public MonitorHttpServer(MonitoringService monitoringService) {
        this(monitoringService, null);
    }

    public MonitorHttpServer(MonitoringService monitoringService, SqlConsoleService sqlConsoleService) {
        this.monitoringService = monitoringService;
        this.sqlConsoleService = sqlConsoleService;
    }

    public void setDemoService(DemoService demoService) {
        this.demoService = demoService;
    }

    public void start(String host, int port) throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(host, port), 0);

        // 静态资源
        httpServer.createContext("/monitor", this::handleMonitorRoot);
        httpServer.createContext("/monitor/", this::handleStatic);

        // 数据 API
        httpServer.createContext("/monitor/api/overview", exchange -> writeJson(exchange, monitoringService.overview()));
        httpServer.createContext("/monitor/api/servers", exchange -> writeJson(exchange, monitoringService.servers()));
        httpServer.createContext("/monitor/api/regions", exchange -> writeJson(exchange, monitoringService.regions()));
        httpServer.createContext("/monitor/api/tables", exchange -> writeJson(exchange, monitoringService.tables()));
        httpServer.createContext("/monitor/api/sql/summary", exchange -> {
            Map<String, String> params = queryParams(exchange.getRequestURI());
            writeJson(exchange, monitoringService.sqlSummary(params.getOrDefault("window", "5m")));
        });
        httpServer.createContext("/monitor/api/hotspots", exchange -> {
            Map<String, String> params = queryParams(exchange.getRequestURI());
            writeJson(exchange, monitoringService.hotspots(
                params.getOrDefault("scope", "region"),
                params.getOrDefault("window", "5m")));
        });
        httpServer.createContext("/monitor/api/region-replicas", exchange -> writeJson(exchange, monitoringService.regionReplicas()));
        httpServer.createContext("/monitor/api/events", exchange -> {
            Map<String, String> params = queryParams(exchange.getRequestURI());
            Set<String> types = new HashSet<>();
            String rawType = params.get("type");
            if (rawType != null && !rawType.isBlank()) {
                types.addAll(Arrays.asList(rawType.split(",")));
            }
            int limit = 200;
            try {
                limit = Integer.parseInt(params.getOrDefault("limit", "200"));
            } catch (NumberFormatException ignored) {}
            writeJson(exchange, monitoringService.events(types, limit));
        });
        httpServer.createContext("/monitor/api/health", exchange -> writeJson(exchange, monitoringService.health()));
        httpServer.createContext("/monitor/api/sql/execute", this::handleSqlExecute);

        // SSE 实时推送
        httpServer.createContext("/monitor/api/stream", this::handleSseStream);

        // 演示模式 API
        httpServer.createContext("/monitor/api/demo/setup", this::handleDemoSetup);
        httpServer.createContext("/monitor/api/demo/primary-port", this::handleDemoPrimaryPort);
        httpServer.createContext("/monitor/api/demo/kill-server", this::handleDemoKillServer);
        httpServer.createContext("/monitor/api/demo/restart-server", this::handleDemoRestartServer);
        httpServer.createContext("/monitor/api/demo/trigger-balance", this::handleDemoTriggerBalance);
        httpServer.createContext("/monitor/api/demo/force-split", this::handleDemoForceSplit);
        httpServer.createContext("/monitor/api/demo/force-merge", this::handleDemoForceMerge);
        httpServer.createContext("/monitor/api/demo/sql", this::handleDemoSql);

        httpServer.setExecutor(Executors.newCachedThreadPool());
        httpServer.start();

        // 启动 SSE 后台任务
        sseScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sse-scheduler");
            t.setDaemon(true);
            return t;
        });
        // 每 30s 打印 SSE 状态日志
        sseScheduler.scheduleAtFixedRate(() -> {
            if (!sseClients.isEmpty()) {
                System.out.println("[SSE] clients=" + sseClients.size()
                    + ", servers=" + monitoringService.servers().size());
            }
        }, 30, 30, TimeUnit.SECONDS);
        monitoringService.setEventSubscriber(this::broadcastEvent);
        sseScheduler.scheduleAtFixedRate(this::sendHeartbeats, 15, 15, TimeUnit.SECONDS);
        sseScheduler.scheduleAtFixedRate(this::sendSnapshot, 2, 2, TimeUnit.SECONDS);
    }

    public void stop() {
        if (sseScheduler != null) {
            sseScheduler.shutdownNow();
            sseScheduler = null;
        }
        for (SseClient client : sseClients) {
            client.close();
        }
        sseClients.clear();
        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }
    }

    // ==================== SSE ====================

    private void handleSseStream(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Allow", "GET");
            writeText(exchange, 405, "Method Not Allowed", "text/plain; charset=UTF-8");
            return;
        }

        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "text/event-stream; charset=UTF-8");
        headers.set("Cache-Control", "no-cache");
        headers.set("Connection", "keep-alive");
        headers.set("X-Accel-Buffering", "no");
        exchange.sendResponseHeaders(200, 0); // chunked mode

        SseClient client = new SseClient(exchange);
        sseClients.add(client);

        // 发送初始快照
        try {
            String snapshotJson = JsonUtil.toJson(monitoringService.snapshot());
            client.send("snapshot", snapshotJson);
        } catch (Exception e) {
            sseClients.remove(client);
            client.close();
            return;
        }

        // 阻塞线程直到客户端断连
        try {
            InputStream is = exchange.getRequestBody();
            byte[] buf = new byte[1024];
            while (is.read(buf) >= 0) {
                // drain
            }
        } catch (IOException ignored) {
        } finally {
            sseClients.remove(client);
            client.close();
        }
    }

    private void broadcastEvent(ClusterEventTimeline.ClusterEvent event) {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("timestamp", event.getTimestamp());
        eventData.put("type", event.getType());
        eventData.put("severity", event.getSeverity());
        eventData.put("regionId", event.getRegionId());
        eventData.put("tableName", event.getTableName());
        eventData.put("sourceServer", event.getSourceServer());
        eventData.put("targetServer", event.getTargetServer());
        eventData.put("message", event.getMessage());
        eventData.put("details", event.getDetails());

        String json = JsonUtil.toJson(eventData);
        List<SseClient> dead = new ArrayList<>();
        for (SseClient client : sseClients) {
            try {
                client.send("event", json);
            } catch (Exception e) {
                dead.add(client);
            }
        }
        sseClients.removeAll(dead);
    }

    private void sendHeartbeats() {
        List<SseClient> dead = new ArrayList<>();
        for (SseClient client : sseClients) {
            try {
                client.sendHeartbeat();
            } catch (Exception e) {
                dead.add(client);
            }
        }
        sseClients.removeAll(dead);
    }

    private void sendSnapshot() {
        if (sseClients.isEmpty()) return;
        String json;
        try {
            json = JsonUtil.toJson(monitoringService.snapshot());
        } catch (Exception e) {
            System.err.println("[SSE] Failed to serialize snapshot: " + e.getMessage());
            return;
        }
        List<SseClient> dead = new ArrayList<>();
        for (SseClient client : sseClients) {
            try {
                client.send("snapshot", json);
            } catch (Exception e) {
                dead.add(client);
            }
        }
        if (!dead.isEmpty()) {
            System.out.println("[SSE] Removed " + dead.size() + " dead clients");
        }
        sseClients.removeAll(dead);
    }

    // ==================== Demo API ====================

    private void handleDemoSetup(HttpExchange exchange) throws IOException {
        if (demoService == null) { writeJson(exchange, Map.of("success", false, "error", "Demo not available")); return; }
        writeJson(exchange, demoService.setupDemoData());
    }

    private void handleDemoPrimaryPort(HttpExchange exchange) throws IOException {
        if (demoService == null) { writeJson(exchange, Map.of("success", false, "error", "Demo not available")); return; }
        Map<String, String> params = queryParams(exchange.getRequestURI());
        writeJson(exchange, demoService.getPrimaryServerPort(params.getOrDefault("table", null)));
    }

    private void handleDemoKillServer(HttpExchange exchange) throws IOException {
        if (demoService == null) { writeJson(exchange, Map.of("success", false, "error", "Demo not available")); return; }
        Map<String, Object> body = readJsonBody(exchange);
        int port = ((Number) body.getOrDefault("port", 0)).intValue();
        writeJson(exchange, demoService.killServer(port));
    }

    private void handleDemoRestartServer(HttpExchange exchange) throws IOException {
        if (demoService == null) { writeJson(exchange, Map.of("success", false, "error", "Demo not available")); return; }
        Map<String, Object> body = readJsonBody(exchange);
        int instance = ((Number) body.getOrDefault("instance", 1)).intValue();
        writeJson(exchange, demoService.restartServer(instance));
    }

    private void handleDemoTriggerBalance(HttpExchange exchange) throws IOException {
        if (demoService == null) { writeJson(exchange, Map.of("success", false, "error", "Demo not available")); return; }
        writeJson(exchange, demoService.triggerBalance());
    }

    private void handleDemoForceSplit(HttpExchange exchange) throws IOException {
        if (demoService == null) { writeJson(exchange, Map.of("success", false, "error", "Demo not available")); return; }
        Map<String, Object> body = readJsonBody(exchange);
        String tableName = String.valueOf(body.getOrDefault("tableName", "demo_users"));
        writeJson(exchange, demoService.forceSplit(tableName));
    }

    private void handleDemoForceMerge(HttpExchange exchange) throws IOException {
        if (demoService == null) { writeJson(exchange, Map.of("success", false, "error", "Demo not available")); return; }
        Map<String, Object> body = readJsonBody(exchange);
        String tableName = String.valueOf(body.getOrDefault("tableName", "demo_users"));
        writeJson(exchange, demoService.forceMerge(tableName));
    }

    private void handleDemoSql(HttpExchange exchange) throws IOException {
        if (demoService == null) { writeJson(exchange, Map.of("success", false, "error", "Demo not available")); return; }
        String sql;
        try (InputStream is = exchange.getRequestBody()) {
            sql = new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
        writeJson(exchange, demoService.executeSql(sql));
    }

    // ==================== SQL Console ====================

    private void handleSqlExecute(HttpExchange exchange) throws IOException {
        if (sqlConsoleService == null) {
            writeText(exchange, 404, "SQL console is not enabled", "text/plain; charset=UTF-8");
            return;
        }
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Allow", "POST");
            writeText(exchange, 405, "Method Not Allowed", "text/plain; charset=UTF-8");
            return;
        }
        String sql;
        try (InputStream is = exchange.getRequestBody()) {
            sql = new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
        try {
            writeJson(exchange, sqlConsoleService.execute(sql));
        } catch (RuntimeException e) {
            writeBytes(exchange, 400,
                JsonUtil.toJson(Map.of("error", e.getMessage())).getBytes(StandardCharsets.UTF_8),
                "application/json; charset=UTF-8");
        }
    }

    // ==================== Static & Utility ====================

    private void handleMonitorRoot(HttpExchange exchange) throws IOException {
        if ("/monitor".equals(exchange.getRequestURI().getPath())) {
            exchange.getResponseHeaders().set("Location", "/monitor/");
            exchange.sendResponseHeaders(302, -1);
            return;
        }
        handleStatic(exchange);
    }

    private void handleStatic(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String resourcePath = "/monitor/index.html";
        if (!"/monitor/".equals(path)) {
            resourcePath = path.replaceFirst("^/monitor", "/monitor");
        }
        try (InputStream inputStream = getClass().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                writeText(exchange, 404, "Not Found", "text/plain; charset=UTF-8");
                return;
            }
            writeBytes(exchange, 200, inputStream.readAllBytes(), contentType(resourcePath));
        }
    }

    private void writeJson(HttpExchange exchange, Object payload) throws IOException {
        writeText(exchange, 200, JsonUtil.toJson(payload), "application/json; charset=UTF-8");
    }

    private void writeText(HttpExchange exchange, int status, String body, String contentType) throws IOException {
        writeBytes(exchange, status, body.getBytes(StandardCharsets.UTF_8), contentType);
    }

    private void writeBytes(HttpExchange exchange, int status, byte[] body, String contentType) throws IOException {
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", contentType);
        headers.set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private String contentType(String resourcePath) {
        if (resourcePath.endsWith(".js")) return "application/javascript; charset=UTF-8";
        if (resourcePath.endsWith(".css")) return "text/css; charset=UTF-8";
        return "text/html; charset=UTF-8";
    }

    private Map<String, String> queryParams(URI uri) {
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) return Collections.emptyMap();
        Map<String, String> result = new HashMap<>();
        for (String pair : query.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = java.net.URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length > 1 ? java.net.URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            result.put(key, value);
        }
        return result;
    }

    /** 读取 JSON body 为 Map（简单解析） */
    @SuppressWarnings("unchecked")
    private Map<String, Object> readJsonBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            String body = new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
            if (body.isEmpty() || !body.startsWith("{")) {
                return new HashMap<>();
            }
            return (Map<String, Object>) parseSimpleJson(body);
        }
    }

    /** 极简 JSON 解析器，只支持 { "key": number } 格式 */
    private static Map<String, Object> parseSimpleJson(String json) {
        Map<String, Object> result = new HashMap<>();
        String content = json.trim();
        if (content.startsWith("{")) content = content.substring(1);
        if (content.endsWith("}")) content = content.substring(0, content.length() - 1);
        for (String pair : content.split(",")) {
            String[] kv = pair.split(":", 2);
            if (kv.length != 2) continue;
            String key = kv[0].trim().replace("\"", "");
            String val = kv[1].trim();
            try {
                result.put(key, Double.parseDouble(val));
            } catch (NumberFormatException e) {
                result.put(key, val.replace("\"", ""));
            }
        }
        return result;
    }
}
