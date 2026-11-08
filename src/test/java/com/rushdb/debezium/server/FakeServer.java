package com.rushdb.debezium.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/** An in-process HTTP server that records requests and plays scripted responses. */
final class FakeServer implements AutoCloseable {

    record Request(String path, Map<String, List<String>> headers, JsonNode body) {
        String header(String name) {
            List<String> values = headers.get(name);
            return values == null ? null : values.get(0);
        }
    }

    record Response(int status, String body) {}

    private static final ObjectMapper JSON = new ObjectMapper();

    final List<Request> requests = new ArrayList<>();
    private final Deque<Response> scripted = new ArrayDeque<>();
    private final HttpServer server;

    FakeServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            JsonNode body = JSON.readTree(exchange.getRequestBody().readAllBytes());
            Response response;
            synchronized (this) {
                requests.add(new Request(exchange.getRequestURI().getPath(), exchange.getRequestHeaders(), body));
                response = scripted.isEmpty() ? new Response(200, "{\"response\":{}}") : scripted.poll();
            }
            byte[] bytes = response.body().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(response.status(), bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }

    /** Queues responses for the next requests; later requests get 200 {@code {"response":{}}}. */
    synchronized FakeServer respond(int status, String body) {
        scripted.add(new Response(status, body));
        return this;
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
