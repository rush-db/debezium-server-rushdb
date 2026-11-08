package com.rushdb.debezium.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Applies sink operations to RushDB: merge-upserts by the identity property
 * and deletes by it. {@link #forConnection} picks the RushDB Cloud REST client
 * or the engine protocol client; both have the same semantics.
 *
 * <p>Requests are retried with jittered exponential backoff on transport
 * errors, HTTP 408/429/5xx, and errors RushDB marks retryable.
 */
public abstract class RushDbClient {

    static final ObjectMapper JSON = new ObjectMapper();

    private final HttpClient http;
    protected final String mergeStrategy;
    protected final boolean suggestTypes;
    private final int maxAttempts;

    protected RushDbClient(String mergeStrategy, boolean suggestTypes, int maxAttempts) {
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.mergeStrategy = mergeStrategy;
        this.suggestTypes = suggestTypes;
        this.maxAttempts = maxAttempts;
    }

    public static RushDbClient forConnection(
            Connection connection, String mergeStrategy, boolean suggestTypes, int maxAttempts) {
        if (connection instanceof Connection.Cloud cloud) {
            return new CloudClient(cloud, mergeStrategy, suggestTypes, maxAttempts);
        }
        return new EngineClient((Connection.Engine) connection, mergeStrategy, suggestTypes, maxAttempts);
    }

    /** Bulk upsert: records matching an existing identity are merged, the rest created. */
    public abstract void upsert(String label, List<ObjectNode> records, String syncKeyField)
            throws InterruptedException;

    /** Deletes all records of the label whose identity value is in the list. */
    public abstract void deleteBySyncKeys(String label, List<String> syncKeys, String syncKeyField)
            throws InterruptedException;

    /**
     * Makes identity lookups on the label cheap. A no-op where the destination
     * offers no way to declare an index.
     */
    public abstract void ensureSyncKeyIndex(String label, String syncKeyField) throws InterruptedException;

    /** One response that is not a success: whether to try again, and why. */
    protected record Failure(boolean retryable, String message) {}

    /** Inspects a response; {@code null} means success. */
    protected abstract Failure failureOf(int status, String body);

    /** Sends a JSON POST, retrying retryable failures; returns the response body. */
    protected String post(String url, Map<String, String> headers, ObjectNode body, String what)
            throws InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
        headers.forEach(builder::header);
        HttpRequest request = builder.build();

        Exception last = null;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (attempt > 0) {
                long cap = Math.min(10_000, 250L * (1L << attempt));
                Thread.sleep(ThreadLocalRandom.current().nextLong(cap / 2, cap + 1));
            }
            try {
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                Failure failure = failureOf(response.statusCode(), response.body());
                if (failure == null) {
                    return response.body();
                }
                if (!failure.retryable()) {
                    throw new IllegalStateException("RushDB " + what + " failed: " + failure.message());
                }
                last = new IOException("RushDB " + what + ": " + failure.message());
            } catch (IOException e) {
                last = e;
            }
        }
        throw new IllegalStateException("RushDB " + what + " failed after " + maxAttempts + " attempts", last);
    }

    static boolean retryableStatus(int status) {
        return status == 408 || status == 429 || status >= 500;
    }

    static JsonNode parseOrNull(String body) {
        try {
            return body == null || body.isBlank() ? null : JSON.readTree(body);
        } catch (IOException e) {
            return null;
        }
    }
}
