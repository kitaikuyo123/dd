package com.minisql.master.monitoring;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * SSE 客户端连接封装
 *
 * 持有一个 HttpExchange 的 OutputStream，用于向浏览器推送 SSE 事件。
 * 每次写入 synchronized 防止多线程并发写入交错。
 */
public class SseClient {

    private final HttpExchange exchange;
    private final OutputStream out;
    private volatile boolean alive = true;

    public SseClient(HttpExchange exchange) throws IOException {
        this.exchange = exchange;
        this.out = exchange.getResponseBody();
    }

    /** 发送一个 SSE 事件：event: type\ndata: json\n\n */
    public synchronized void send(String eventType, String jsonData) throws IOException {
        if (!alive) return;
        StringBuilder frame = new StringBuilder();
        frame.append("event: ").append(eventType).append("\n");
        for (String line : jsonData.split("\n")) {
            frame.append("data: ").append(line).append("\n");
        }
        frame.append("\n");
        out.write(frame.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /** 发送 SSE 心跳注释 */
    public synchronized void sendHeartbeat() throws IOException {
        if (!alive) return;
        out.write(": heartbeat\n\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /** 关闭连接 */
    public void close() {
        alive = false;
        try { out.close(); } catch (IOException ignored) {}
        try { exchange.close(); } catch (Exception ignored) {}
    }

    public boolean isAlive() {
        return alive;
    }
}
