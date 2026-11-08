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
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The sink against a real RushDB engine: {@code RUSHDB_TEST_ENGINE_URL} and
 * {@code RUSHDB_TEST_ENGINE_TOKEN}; a multitenant engine as
 * {@code RUSHDB_TEST_MT_ENGINE_URL} / {@code RUSHDB_TEST_MT_ENGINE_TOKEN}.
 */
class EngineLiveTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @EnabledIfEnvironmentVariable(named = "RUSHDB_TEST_ENGINE_URL", matches = ".+")
    void streamsCreatesUpdatesAndDeletes() throws Exception {
        roundTrip(new Connection.Engine(
                System.getenv("RUSHDB_TEST_ENGINE_URL"), System.getenv("RUSHDB_TEST_ENGINE_TOKEN"), null));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "RUSHDB_TEST_MT_ENGINE_URL", matches = ".+")
    void streamsIntoATenant() throws Exception {
        roundTrip(new Connection.Engine(
                System.getenv("RUSHDB_TEST_MT_ENGINE_URL"), System.getenv("RUSHDB_TEST_MT_ENGINE_TOKEN"),
                "t" + UUID.randomUUID().toString().replace("-", "").substring(0, 12)));
    }

    private void roundTrip(Connection.Engine engine) throws Exception {
        // A table name of its own per run: its label is unique, so earlier runs never interfere.
        String table = "orders_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String label = "ORDERS_" + table.substring(7).toUpperCase();
        RushDbChangeConsumer consumer = new RushDbChangeConsumer();
        consumer.configure(engine, "append", true, 500, true, ".", "src", Set.of("email"),
                Map.of(table, label), "syncKey", "syncedAt", true);

        consumer.handleBatch(List.of(
                event(table, 1, "c", null, "{\"id\":1,\"total\":9.5,\"email\":\"a@x\",\"customer\":{\"city\":\"Berlin\"}}"),
                event(table, 2, "c", null, "{\"id\":2,\"total\":3}"),
                event(table, 3, "c", null, "{\"id\":3,\"total\":7}")), new Committer());
        consumer.handleBatch(List.of(
                event(table, 1, "u", "{\"id\":1}", "{\"id\":1,\"total\":11,\"customer\":{\"city\":\"Berlin\"}}"),
                event(table, 2, "d", "{\"id\":2,\"total\":3}", null)), new Committer());

        JsonNode records = query(engine, label);
        assertEquals(2, records.size(), "row 2 deleted, rows 1 and 3 kept");
        JsonNode first = byKey(records, "app.public." + table + ":1");
        assertEquals(11, first.path("total").asInt(), "update merged");
        assertEquals("Berlin", first.path("customer.city").asText(), "nested field flattened");
        assertFalse(first.has("email"), "ignored field never written");
        assertTrue(first.has("syncedAt"));

        JsonNode indexes = execute(engine, "{\"kind\":\"describe\",\"describe\":{\"kind\":\"indexes\"}}")
                .path("response").path("indexes");
        boolean indexed = false;
        for (JsonNode index : indexes) {
            indexed |= label.equals(index.path("label").asText()) && "synckey".equals(index.path("property").asText());
        }
        assertTrue(indexed, "an ordered index on syncKey was declared for the label");

        // rewrite mirrors the row exactly: fields absent from the new image are removed
        RushDbChangeConsumer mirror = new RushDbChangeConsumer();
        mirror.configure(engine, "rewrite", true, 500, true, ".", "src", Set.of(),
                Map.of(table, label), "syncKey", "syncedAt", true);
        mirror.handleBatch(List.of(event(table, 1, "u", "{\"id\":1}", "{\"id\":1,\"total\":12}")), new Committer());
        JsonNode rewritten = byKey(query(engine, label), "app.public." + table + ":1");
        assertEquals(12, rewritten.path("total").asInt());
        assertFalse(rewritten.has("customer.city"), "rewrite dropped the field the row no longer has");
    }

    // --- helpers ------------------------------------------------------------

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
        final List<ChangeEvent<Object, Object>> processed = new ArrayList<>();
        boolean finished;
        public void markProcessed(ChangeEvent<Object, Object> record) { processed.add(record); }
        public void markBatchFinished() { finished = true; }
        public void markProcessed(ChangeEvent<Object, Object> record, DebeziumEngine.Offsets offsets) {
            processed.add(record);
        }
        public DebeziumEngine.Offsets buildOffsets() { return null; }
    }

    private static JsonNode query(Connection.Engine engine, String label) throws Exception {
        JsonNode records = execute(engine, "{\"kind\":\"query\",\"query\":{\"kind\":\"records\","
                + "\"query\":{\"labels\":[\"" + label + "\"]}}}").path("response").path("records");
        List<JsonNode> flat = new ArrayList<>();
        records.forEach(r -> flat.add(r.path("properties")));
        return JSON.valueToTree(flat);
    }

    private static JsonNode byKey(JsonNode records, String syncKey) {
        for (JsonNode r : records) {
            if (syncKey.equals(r.path("syncKey").asText())) {
                return r;
            }
        }
        fail("no record with syncKey " + syncKey + " in " + records);
        return null;
    }

    private static JsonNode execute(Connection.Engine engine, String operation) throws Exception {
        String scope = engine.scope();
        String body = "{\"protocolVersion\":1,"
                + (scope == null ? "" : "\"context\":{\"scope\":\"" + scope + "\"},")
                + "\"operation\":" + operation + "}";
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(engine.url() + "/protocol/v1/execute"))
                .header("Authorization", "Bearer " + engine.token())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (scope != null) {
            request.header("X-RushDB-Tenant", scope);
        }
        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request.build(), HttpResponse.BodyHandlers.ofString());
        return JSON.readTree(response.body());
    }
}
