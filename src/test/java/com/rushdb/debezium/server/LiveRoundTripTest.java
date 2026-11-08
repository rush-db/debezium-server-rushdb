package com.rushdb.debezium.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The sink's round trip against a real RushDB, once per configured target:
 * <ul>
 *   <li>a self-hosted engine: {@code RUSHDB_TEST_ENGINE_URL}, {@code RUSHDB_TEST_ENGINE_TOKEN}</li>
 *   <li>a multitenant engine: {@code RUSHDB_TEST_MT_ENGINE_URL}, {@code RUSHDB_TEST_MT_ENGINE_TOKEN}</li>
 *   <li>RushDB Cloud: {@code RUSHDB_TEST_API_KEY} (and optionally {@code RUSHDB_TEST_URL})</li>
 * </ul>
 * Each run writes a label of its own and deletes its records afterwards.
 */
class LiveRoundTripTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Test
    @EnabledIfEnvironmentVariable(named = "RUSHDB_TEST_ENGINE_URL", matches = ".+")
    void selfHostedEngine() throws Exception {
        roundTrip(new EngineTarget(new Connection.Engine(
                System.getenv("RUSHDB_TEST_ENGINE_URL"), System.getenv("RUSHDB_TEST_ENGINE_TOKEN"), null)));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "RUSHDB_TEST_MT_ENGINE_URL", matches = ".+")
    void multitenantEngine() throws Exception {
        roundTrip(new EngineTarget(new Connection.Engine(
                System.getenv("RUSHDB_TEST_MT_ENGINE_URL"), System.getenv("RUSHDB_TEST_MT_ENGINE_TOKEN"),
                "t" + UUID.randomUUID().toString().replace("-", "").substring(0, 12))));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "RUSHDB_TEST_API_KEY", matches = ".+")
    void rushDbCloud() throws Exception {
        String url = Optional.ofNullable(System.getenv("RUSHDB_TEST_URL"))
                .filter(s -> !s.isBlank()).orElse("https://api.rushdb.com");
        roundTrip(new CloudTarget(new Connection.Cloud(url, System.getenv("RUSHDB_TEST_API_KEY"))));
    }

    private void roundTrip(Target target) throws Exception {
        // A table name of its own per run: its label is unique, so runs never interfere.
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String table = "orders_" + suffix;
        String label = "ORDERS_" + suffix.toUpperCase();
        try {
            RushDbChangeConsumer consumer = new RushDbChangeConsumer();
            consumer.configure(target.connection(), "append", true, 500, true, ".", "src", Set.of("email"),
                    Map.of(table, label), "syncKey", "syncedAt", true);

            consumer.handleBatch(List.of(
                    event(table, 1, "c", null, "{\"id\":1,\"total\":9.5,\"email\":\"a@x\",\"customer\":{\"city\":\"Berlin\"}}"),
                    event(table, 2, "c", null, "{\"id\":2,\"total\":3}"),
                    event(table, 3, "c", null, "{\"id\":3,\"total\":7}")), new Committer());
            consumer.handleBatch(List.of(
                    event(table, 1, "u", "{\"id\":1}", "{\"id\":1,\"total\":11,\"customer\":{\"city\":\"Berlin\"}}"),
                    event(table, 2, "d", "{\"id\":2,\"total\":3}", null)), new Committer());

            List<JsonNode> records = target.records(label);
            assertEquals(2, records.size(), "row 2 deleted, rows 1 and 3 kept");
            JsonNode first = byKey(records, "app.public." + table + ":1");
            assertEquals(11, first.path("total").asInt(), "update merged");
            assertEquals("Berlin", first.path("customer.city").asText(), "nested field flattened");
            assertFalse(first.has("email"), "ignored field never written");
            assertTrue(first.has("syncedAt"));
            target.syncKeyIndexed(label).ifPresent(indexed ->
                    assertTrue(indexed, "an ordered index on syncKey was declared for the label"));

            // rewrite mirrors the row exactly: fields absent from the new image are removed
            RushDbChangeConsumer mirror = new RushDbChangeConsumer();
            mirror.configure(target.connection(), "rewrite", true, 500, true, ".", "src", Set.of(),
                    Map.of(table, label), "syncKey", "syncedAt", true);
            mirror.handleBatch(List.of(event(table, 1, "u", "{\"id\":1}", "{\"id\":1,\"total\":12}")),
                    new Committer());
            JsonNode rewritten = byKey(target.records(label), "app.public." + table + ":1");
            assertEquals(12, rewritten.path("total").asInt());
            assertFalse(rewritten.has("customer.city"), "rewrite dropped the field the row no longer has");
        } finally {
            target.deleteAll(label);
        }
    }

    // --- targets ------------------------------------------------------------

    /** How a test reads a target back, independently of the sink's own client. */
    private interface Target {
        Connection connection();

        /** The properties of every record of the label. */
        List<JsonNode> records(String label) throws Exception;

        /** Whether the identity index exists; empty where the sink does not declare one. */
        Optional<Boolean> syncKeyIndexed(String label) throws Exception;

        void deleteAll(String label) throws Exception;
    }

    private record EngineTarget(Connection.Engine connection) implements Target {
        public List<JsonNode> records(String label) throws Exception {
            List<JsonNode> out = new ArrayList<>();
            execute("{\"kind\":\"query\",\"query\":{\"kind\":\"records\",\"query\":{\"labels\":[\"" + label + "\"]}}}")
                    .path("response").path("records").forEach(r -> out.add(r.path("properties")));
            return out;
        }

        public Optional<Boolean> syncKeyIndexed(String label) throws Exception {
            for (JsonNode index : execute("{\"kind\":\"describe\",\"describe\":{\"kind\":\"indexes\"}}")
                    .path("response").path("indexes")) {
                if (label.equals(index.path("label").asText()) && "synckey".equals(index.path("property").asText())) {
                    return Optional.of(true);
                }
            }
            return Optional.of(false);
        }

        public void deleteAll(String label) throws Exception {
            execute("{\"kind\":\"mutate\",\"mutation\":{\"kind\":\"deleteRecords\",\"query\":{\"labels\":[\""
                    + label + "\"]}}}");
        }

        private JsonNode execute(String operation) throws Exception {
            String scope = connection.scope();
            String body = "{\"protocolVersion\":1,"
                    + (scope == null ? "" : "\"context\":{\"scope\":\"" + scope + "\"},")
                    + "\"operation\":" + operation + "}";
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(connection.url() + "/protocol/v1/execute"))
                    .header("Authorization", "Bearer " + connection.token())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body));
            if (scope != null) {
                request.header("X-RushDB-Tenant", scope);
            }
            return send(request.build());
        }
    }

    private record CloudTarget(Connection.Cloud connection) implements Target {
        public List<JsonNode> records(String label) throws Exception {
            List<JsonNode> out = new ArrayList<>();
            post("/records/search", "{\"labels\":[\"" + label + "\"],\"limit\":1000}")
                    .path("data").forEach(out::add);
            return out;
        }

        /** RushDB Cloud manages indexing; the sink declares no index there. */
        public Optional<Boolean> syncKeyIndexed(String label) {
            return Optional.empty();
        }

        public void deleteAll(String label) throws Exception {
            post("/records/delete", "{\"labels\":[\"" + label + "\"]}");
        }

        private JsonNode post(String path, String body) throws Exception {
            String base = connection.url().endsWith("/")
                    ? connection.url().substring(0, connection.url().length() - 1) : connection.url();
            if (!base.contains("/api/")) {
                base += "/api/v1";
            }
            return send(HttpRequest.newBuilder(URI.create(base + path))
                    .header("Authorization", "Bearer " + connection.apiKey())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build());
        }
    }

    private static JsonNode send(HttpRequest request) throws Exception {
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() < 300,
                request.uri().getPath() + " returned " + response.statusCode() + ": " + response.body());
        return JSON.readTree(response.body());
    }

    // --- events ---------------------------------------------------------------

    private static ChangeEvent<Object, Object> event(String table, int id, String op, String before, String after) {
        String envelope = "{\"op\":\"" + op + "\",\"ts_ms\":1718000000000,"
                + "\"source\":{\"table\":\"" + table + "\",\"db\":\"app\"},"
                + "\"before\":" + before + ",\"after\":" + after + "}";
        String key = "{\"id\":" + id + "}";
        String destination = "app.public." + table;
        return new ChangeEvent<>() {
            public Object key() { return key; }
            public Object value() { return envelope; }
            public String destination() { return destination; }
            public Integer partition() { return null; }
        };
    }

    private static final class Committer implements DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> {
        public void markProcessed(ChangeEvent<Object, Object> record) {}
        public void markBatchFinished() {}
        public void markProcessed(ChangeEvent<Object, Object> record, DebeziumEngine.Offsets offsets) {}
        public DebeziumEngine.Offsets buildOffsets() { return null; }
    }

    private static JsonNode byKey(List<JsonNode> records, String syncKey) {
        for (JsonNode r : records) {
            if (syncKey.equals(r.path("syncKey").asText())) {
                return r;
            }
        }
        fail("no record with syncKey " + syncKey + " in " + records);
        return null;
    }
}
