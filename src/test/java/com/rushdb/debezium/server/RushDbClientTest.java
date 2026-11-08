package com.rushdb.debezium.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RushDbClientTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static List<ObjectNode> rows() {
        return List.of(JSON.createObjectNode().put("syncKey", "app.public.orders:1").put("total", 9.5));
    }

    private static EngineClient engine(FakeServer server, String scope) {
        return new EngineClient(new Connection.Engine(server.url(), "tok", scope), "append", true, 3);
    }

    // --- engine protocol ----------------------------------------------------

    @Test
    void engineUpsertIsAnIngestWithMergeBy() throws Exception {
        try (FakeServer server = new FakeServer()) {
            engine(server, null).upsert("ORDER", rows(), "syncKey");
            FakeServer.Request request = server.requests.get(0);
            assertEquals("/protocol/v1/execute", request.path());
            assertEquals("Bearer tok", request.header("Authorization"));
            assertNull(request.header("X-RushDB-Tenant"));
            JsonNode body = request.body();
            assertEquals(1, body.get("protocolVersion").asInt());
            assertFalse(body.has("context"));
            JsonNode ingest = body.at("/operation/ingest");
            assertEquals("ingest", body.at("/operation/kind").asText());
            assertEquals("documents", ingest.get("kind").asText());
            assertEquals("ORDER", ingest.get("label").asText());
            assertEquals("app.public.orders:1", ingest.at("/documents/0/syncKey").asText());
            assertEquals("syncKey", ingest.at("/options/mergeBy/0").asText());
            assertEquals("append", ingest.at("/options/mergeStrategy").asText());
            assertTrue(ingest.at("/options/suggestTypes").asBoolean());
            assertFalse(ingest.at("/options/returnResult").asBoolean());
        }
    }

    @Test
    void engineDeleteIsADeleteRecordsByIdentity() throws Exception {
        try (FakeServer server = new FakeServer()) {
            engine(server, null).deleteBySyncKeys("ORDER", List.of("k1", "k2"), "syncKey");
            JsonNode mutation = server.requests.get(0).body().at("/operation/mutation");
            assertEquals("mutate", server.requests.get(0).body().at("/operation/kind").asText());
            assertEquals("deleteRecords", mutation.get("kind").asText());
            assertEquals("ORDER", mutation.at("/query/labels/0").asText());
            assertEquals("k2", mutation.at("/query/predicate/syncKey/$in/1").asText());
        }
    }

    @Test
    void engineScopeGoesInHeaderAndContext() throws Exception {
        try (FakeServer server = new FakeServer()) {
            engine(server, "acme").upsert("ORDER", rows(), "syncKey");
            FakeServer.Request request = server.requests.get(0);
            assertEquals("acme", request.header("X-RushDB-Tenant"));
            assertEquals("acme", request.body().at("/context/scope").asText());
        }
    }

    @Test
    void engineRetriesRetryableErrors() throws Exception {
        try (FakeServer server = new FakeServer()) {
            server.respond(409, "{\"error\":{\"code\":\"aborted\",\"message\":\"transaction_conflict\",\"retryable\":true}}");
            engine(server, null).upsert("ORDER", rows(), "syncKey");
            assertEquals(2, server.requests.size());
        }
    }

    @Test
    void engineFailsFastOnNonRetryableErrors() throws Exception {
        try (FakeServer server = new FakeServer()) {
            server.respond(422, "{\"error\":{\"code\":\"invalid_request\",\"message\":\"bad\",\"retryable\":false}}");
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> engine(server, null).upsert("ORDER", rows(), "syncKey"));
            assertTrue(e.getMessage().contains("invalid_request"));
            assertEquals(1, server.requests.size());
        }
    }

    @Test
    void engineTreatsAnErrorEnvelopeWithHttp200AsFailure() throws Exception {
        try (FakeServer server = new FakeServer()) {
            server.respond(200, "{\"error\":{\"code\":\"invalid_request\",\"message\":\"bad\",\"retryable\":false}}");
            assertThrows(IllegalStateException.class,
                    () -> engine(server, null).upsert("ORDER", rows(), "syncKey"));
        }
    }

    @Test
    void engineCreatesTheIdentityIndexOncePerLabel() throws Exception {
        try (FakeServer server = new FakeServer()) {
            server.respond(200, "{\"response\":{\"kind\":\"indexes\",\"indexes\":[]}}");
            EngineClient client = engine(server, null);
            client.ensureSyncKeyIndex("ORDER", "syncKey");
            client.ensureSyncKeyIndex("ORDER", "syncKey");
            assertEquals(2, server.requests.size(), "describe + create, then cached");
            JsonNode definition = server.requests.get(1).body().at("/operation/index/definition");
            assertEquals("create", server.requests.get(1).body().at("/operation/index/kind").asText());
            assertEquals("order_synckey", definition.get("name").asText());
            assertEquals("ORDER", definition.get("label").asText());
            assertEquals("syncKey", definition.get("property").asText());
        }
    }

    @Test
    void engineSkipsAnExistingIdentityIndex() throws Exception {
        try (FakeServer server = new FakeServer()) {
            server.respond(200, "{\"response\":{\"kind\":\"indexes\",\"indexes\":["
                    + "{\"name\":\"x\",\"label\":\"ORDER\",\"property\":\"synckey\",\"kind\":\"ordered\"}]}}");
            engine(server, null).ensureSyncKeyIndex("ORDER", "syncKey");
            assertEquals(1, server.requests.size());
        }
    }

    @Test
    void engineAcceptsAnIndexCreatedConcurrently() throws Exception {
        try (FakeServer server = new FakeServer()) {
            server.respond(200, "{\"response\":{\"kind\":\"indexes\",\"indexes\":[]}}")
                    .respond(409, "{\"error\":{\"code\":\"conflict\","
                            + "\"message\":\"index order_synckey already exists\",\"retryable\":false}}");
            engine(server, null).ensureSyncKeyIndex("ORDER", "syncKey");
            assertEquals(2, server.requests.size());
        }
    }

    // --- RushDB Cloud REST ----------------------------------------------------

    @Test
    void cloudUpsertIsAJsonImportWithMergeBy() throws Exception {
        try (FakeServer server = new FakeServer()) {
            new CloudClient(new Connection.Cloud(server.url(), "key"), "rewrite", false, 3)
                    .upsert("ORDER", rows(), "syncKey");
            FakeServer.Request request = server.requests.get(0);
            assertEquals("/api/v1/records/import/json", request.path());
            assertEquals("Bearer key", request.header("Authorization"));
            JsonNode body = request.body();
            assertEquals("ORDER", body.get("label").asText());
            assertEquals("app.public.orders:1", body.at("/data/0/syncKey").asText());
            assertEquals("syncKey", body.at("/options/mergeBy/0").asText());
            assertEquals("rewrite", body.at("/options/mergeStrategy").asText());
            assertFalse(body.at("/options/suggestTypes").asBoolean());
        }
    }

    @Test
    void cloudDeleteFiltersByIdentity() throws Exception {
        try (FakeServer server = new FakeServer()) {
            new CloudClient(new Connection.Cloud(server.url() + "/api/v1/", "key"), "append", true, 3)
                    .deleteBySyncKeys("ORDER", List.of("k1"), "syncKey");
            FakeServer.Request request = server.requests.get(0);
            assertEquals("/api/v1/records/delete", request.path());
            assertEquals("k1", request.body().at("/where/syncKey/$in/0").asText());
        }
    }

    @Test
    void cloudRetriesServerErrorsButNotClientErrors() throws Exception {
        try (FakeServer server = new FakeServer()) {
            server.respond(503, "unavailable");
            CloudClient client = new CloudClient(new Connection.Cloud(server.url(), "key"), "append", true, 3);
            client.upsert("ORDER", rows(), "syncKey");
            assertEquals(2, server.requests.size());
            server.respond(400, "{\"message\":\"bad\"}");
            assertThrows(IllegalStateException.class, () -> client.upsert("ORDER", rows(), "syncKey"));
            assertEquals(3, server.requests.size());
        }
    }

    // --- target selection -----------------------------------------------------

    @Test
    void selectsTheEngineOrCloudFromConfig() {
        Connection engine = RushDbChangeConsumer.connection(Optional.empty(), Optional.empty(),
                Optional.of("http://rushdb:3000"), Optional.of("t"), Optional.of("acme"));
        assertEquals(new Connection.Engine("http://rushdb:3000", "t", "acme"), engine);
        Connection cloud = RushDbChangeConsumer.connection(Optional.of("k"), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
        assertEquals(new Connection.Cloud("https://api.rushdb.com", "k"), cloud);
    }

    @Test
    void refusesAmbiguousOrIncompleteTargets() {
        assertThrows(IllegalStateException.class, () -> RushDbChangeConsumer.connection(
                Optional.of("k"), Optional.empty(), Optional.of("http://rushdb:3000"), Optional.of("t"),
                Optional.empty()));
        assertThrows(IllegalStateException.class, () -> RushDbChangeConsumer.connection(
                Optional.empty(), Optional.empty(), Optional.of("http://rushdb:3000"), Optional.empty(),
                Optional.empty()));
        assertThrows(IllegalStateException.class, () -> RushDbChangeConsumer.connection(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
    }
}
