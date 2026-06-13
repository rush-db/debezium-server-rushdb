package com.rushdb.debezium.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Minimal RushDB REST client: merge-upsert via the JSON import endpoint and
 * delete-by-query on syncKey. Retries 408/429/5xx and transport errors with
 * jittered exponential backoff.
 */
public class RushDbClient {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpClient http;
    private final String baseUrl;
    private final String apiKey;
    private final String mergeStrategy;
    private final boolean suggestTypes;
    private final int maxAttempts;

    public RushDbClient(String baseUrl, String apiKey, String mergeStrategy, boolean suggestTypes, int maxAttempts) {
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.apiKey = apiKey;
        this.mergeStrategy = mergeStrategy;
        this.suggestTypes = suggestTypes;
        this.maxAttempts = maxAttempts;
    }

    /** Bulk upsert: records matching an existing identity are merged, the rest created. */
    public void importRecords(String label, List<ObjectNode> records, String syncKeyField)
            throws InterruptedException {
        ObjectNode body = JSON.createObjectNode();
        body.put("label", label);
        ArrayNode data = body.putArray("data");
        records.forEach(data::add);
        ObjectNode options = body.putObject("options");
        options.put("suggestTypes", suggestTypes);
        options.putArray("mergeBy").add(syncKeyField);
        options.put("mergeStrategy", mergeStrategy);
        post("/api/v1/records/import/json", body);
    }

    /** Deletes all records of the label whose identity value is in the set. */
    public void deleteBySyncKeys(String label, List<String> syncKeys, String syncKeyField)
            throws InterruptedException {
        ObjectNode body = JSON.createObjectNode();
        body.putArray("labels").add(label);
        ArrayNode in = body.putObject("where").putObject(syncKeyField).putArray("$in");
        syncKeys.forEach(in::add);
        post("/api/v1/records/delete", body);
    }

    private void post(String path, ObjectNode body) throws InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(60))
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        IOException lastIo = null;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (attempt > 0) {
                long cap = Math.min(10_000, 250L * (1L << attempt));
                Thread.sleep(ThreadLocalRandom.current().nextLong(cap / 2, cap + 1));
            }
            try {
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                    return;
                }
                if (status == 408 || status == 429 || status >= 500) {
                    lastIo = new IOException("RushDB " + path + " returned " + status);
                    continue;
                }
                throw new IllegalStateException(
                        "RushDB " + path + " returned " + status + ": " + response.body());
            } catch (IOException e) {
                lastIo = e;
            }
        }
        throw new IllegalStateException("RushDB " + path + " failed after " + maxAttempts + " attempts", lastIo);
    }
}
