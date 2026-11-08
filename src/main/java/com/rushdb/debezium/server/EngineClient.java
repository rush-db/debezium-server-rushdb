package com.rushdb.debezium.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A self-hosted RushDB engine, over its native protocol
 * ({@code POST /protocol/v1/execute}): upserts are a document {@code ingest}
 * with {@code mergeBy}, deletes a {@code deleteRecords} mutation, and the
 * identity property gets an ordered index per label so upserts and deletes
 * look records up through it instead of walking the label.
 */
final class EngineClient extends RushDbClient {

    private final String executeUrl;
    private final Map<String, String> headers;
    private final String scope;
    /** Labels whose identity index is known to exist in this process. */
    private final Set<String> indexed = ConcurrentHashMap.newKeySet();

    EngineClient(Connection.Engine connection, String mergeStrategy, boolean suggestTypes, int maxAttempts) {
        super(mergeStrategy, suggestTypes, maxAttempts);
        this.executeUrl = CloudClient.stripSlash(connection.url()) + "/protocol/v1/execute";
        this.scope = connection.scope();
        Map<String, String> h = new HashMap<>();
        h.put("Authorization", "Bearer " + connection.token());
        if (scope != null) {
            h.put("X-RushDB-Tenant", scope);
        }
        this.headers = Map.copyOf(h);
    }

    @Override
    public void upsert(String label, List<ObjectNode> records, String syncKeyField) throws InterruptedException {
        ObjectNode ingest = JSON.createObjectNode();
        ingest.put("kind", "documents");
        ingest.put("label", label);
        ArrayNode documents = ingest.putArray("documents");
        records.forEach(documents::add);
        ObjectNode options = ingest.putObject("options");
        options.put("suggestTypes", suggestTypes);
        options.putArray("mergeBy").add(syncKeyField);
        options.put("mergeStrategy", mergeStrategy);
        options.put("returnResult", false);
        execute("ingest", "ingest", ingest, "ingest");
    }

    @Override
    public void deleteBySyncKeys(String label, List<String> syncKeys, String syncKeyField)
            throws InterruptedException {
        ObjectNode mutation = JSON.createObjectNode();
        mutation.put("kind", "deleteRecords");
        ObjectNode query = mutation.putObject("query");
        query.putArray("labels").add(label);
        ArrayNode in = query.putObject("predicate").putObject(syncKeyField).putArray("$in");
        syncKeys.forEach(in::add);
        execute("mutate", "mutation", mutation, "deleteRecords");
    }

    @Override
    public void ensureSyncKeyIndex(String label, String syncKeyField) throws InterruptedException {
        if (indexed.contains(label)) {
            return;
        }
        String property = normalize(syncKeyField);
        ObjectNode describe = JSON.createObjectNode().put("kind", "indexes");
        JsonNode response = execute("describe", "describe", describe, "describe indexes");
        for (JsonNode index : response.path("indexes")) {
            if (label.equals(index.path("label").asText())
                    && property.equals(index.path("property").asText())
                    && "ordered".equals(index.path("kind").asText("ordered"))) {
                indexed.add(label);
                return;
            }
        }
        ObjectNode create = JSON.createObjectNode().put("kind", "create");
        create.putObject("definition")
                .put("name", indexName(label, property))
                .put("label", label)
                .put("property", syncKeyField);
        try {
            execute("index", "index", create, "create index");
        } catch (IllegalStateException e) {
            // Another sink instance created it between our check and create.
            if (e.getMessage() == null || !e.getMessage().contains("already exists")) {
                throw e;
            }
        }
        indexed.add(label);
    }

    /**
     * Runs one protocol operation and returns its {@code response}. The payload
     * sits under {@code field}: the operation's kind, except {@code mutate}'s
     * {@code mutation}.
     */
    private JsonNode execute(String kind, String field, ObjectNode payload, String what)
            throws InterruptedException {
        ObjectNode envelope = JSON.createObjectNode();
        envelope.put("protocolVersion", 1);
        if (scope != null) {
            envelope.putObject("context").put("scope", scope);
        }
        ObjectNode operation = envelope.putObject("operation");
        operation.put("kind", kind);
        operation.set(field, payload);
        JsonNode body = parseOrNull(post(executeUrl, headers, envelope, what));
        return body == null ? JSON.createObjectNode() : body.path("response");
    }

    @Override
    protected Failure failureOf(int status, String body) {
        JsonNode parsed = parseOrNull(body);
        JsonNode error = parsed == null ? null : parsed.get("error");
        if (status >= 200 && status < 300 && (error == null || error.isNull())) {
            return null;
        }
        if (error != null && error.isObject()) {
            String message = error.path("code").asText() + ": " + error.path("message").asText();
            return new Failure(error.path("retryable").asBoolean(false) || retryableStatus(status), message);
        }
        return new Failure(retryableStatus(status), "HTTP " + status + ": " + body);
    }

    /** The engine stores index property names lower-case, non-alphanumerics as {@code _}. */
    static String normalize(String property) {
        return property.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "_");
    }

    static String indexName(String label, String normalizedProperty) {
        return normalize(label) + "_" + normalizedProperty;
    }
}
