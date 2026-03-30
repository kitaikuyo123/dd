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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

public class MonitorHttpServer {

    private final MonitoringService monitoringService;
    private final SqlConsoleService sqlConsoleService;
    private HttpServer httpServer;

    public MonitorHttpServer(MonitoringService monitoringService) {
        this(monitoringService, null);
    }

    public MonitorHttpServer(MonitoringService monitoringService, SqlConsoleService sqlConsoleService) {
        this.monitoringService = monitoringService;
        this.sqlConsoleService = sqlConsoleService;
    }

    public void start(String host, int port) throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(host, port), 0);
        httpServer.createContext("/monitor", this::handleMonitorRoot);
        httpServer.createContext("/monitor/", this::handleStatic);
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
                params.getOrDefault("window", "5m")
            ));
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
            } catch (NumberFormatException ignored) {
            }
            writeJson(exchange, monitoringService.events(types, limit));
        });
        httpServer.createContext("/monitor/api/health", exchange -> writeJson(exchange, monitoringService.health()));
        httpServer.createContext("/monitor/api/sql/execute", this::handleSqlExecute);
        httpServer.setExecutor(Executors.newCachedThreadPool());
        httpServer.start();
    }

    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }
    }

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
        try (InputStream inputStream = exchange.getRequestBody()) {
            sql = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8).trim();
        }

        try {
            writeJson(exchange, sqlConsoleService.execute(sql));
        } catch (RuntimeException e) {
            writeBytes(exchange, 400,
                JsonUtil.toJson(Map.of("error", e.getMessage())).getBytes(StandardCharsets.UTF_8),
                "application/json; charset=UTF-8");
        }
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
        if (resourcePath.endsWith(".js")) {
            return "application/javascript; charset=UTF-8";
        }
        if (resourcePath.endsWith(".css")) {
            return "text/css; charset=UTF-8";
        }
        return "text/html; charset=UTF-8";
    }

    private Map<String, String> queryParams(URI uri) {
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new HashMap<>();
        for (String pair : query.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = java.net.URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length > 1 ? java.net.URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            result.put(key, value);
        }
        return result;
    }
}
