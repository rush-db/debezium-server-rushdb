package com.rushdb.debezium.server;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Map;

/**
 * RushDB Cloud: merge-upserts through {@code POST /api/v1/records/import/json}
 * and deletes through {@code POST /api/v1/records/delete}.
 */
final class CloudClient extends RushDbClient {

    private final String baseUrl;
    private final Map<String, String> headers;

    CloudClient(Connection.Cloud connection, String mergeStrategy, boolean suggestTypes, int maxAttempts) {
        super(mergeStrategy, suggestTypes, maxAttempts);
        String url = stripSlash(connection.url());
        this.baseUrl = url.contains("/api/") ? url : url + "/api/v1";
        this.headers = Map.of("Authorization", "Bearer " + connection.apiKey());
    }

    @Override
    public void upsert(String label, List<ObjectNode> records, String syncKeyField) throws InterruptedException {
        ObjectNode body = JSON.createObjectNode();
        body.put("label", label);
        ArrayNode data = body.putArray("data");
        records.forEach(data::add);
        ObjectNode options = body.putObject("options");
        options.put("suggestTypes", suggestTypes);
        options.putArray("mergeBy").add(syncKeyField);
        options.put("mergeStrategy", mergeStrategy);
        options.put("returnResult", false);
        post(baseUrl + "/records/import/json", headers, body, "import");
    }

    @Override
    public void deleteBySyncKeys(String label, List<String> syncKeys, String syncKeyField)
            throws InterruptedException {
        ObjectNode body = JSON.createObjectNode();
        body.putArray("labels").add(label);
        ArrayNode in = body.putObject("where").putObject(syncKeyField).putArray("$in");
        syncKeys.forEach(in::add);
        post(baseUrl + "/records/delete", headers, body, "delete");
    }

    /** RushDB Cloud manages indexing; its API does not declare ordered indexes. */
    @Override
    public void ensureSyncKeyIndex(String label, String syncKeyField) {}

    @Override
    protected Failure failureOf(int status, String body) {
        if (status >= 200 && status < 300) {
            return null;
        }
        return new Failure(retryableStatus(status), "HTTP " + status + ": " + body);
    }

    static String stripSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
