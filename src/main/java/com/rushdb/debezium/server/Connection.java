package com.rushdb.debezium.server;

/**
 * Where the sink writes: RushDB Cloud (REST API, API key) or a self-hosted
 * RushDB engine (native protocol, engine token, optional tenant scope).
 * Both modes apply the same upserts and deletes.
 */
public sealed interface Connection permits Connection.Cloud, Connection.Engine {

    /** RushDB Cloud: {@code url} is the API base, e.g. {@code https://api.rushdb.com}. */
    record Cloud(String url, String apiKey) implements Connection {
        public Cloud {
            requireText(url, "url");
            requireText(apiKey, "api-key");
        }
    }

    /**
     * A self-hosted RushDB engine: {@code url} is the engine's HTTP address,
     * e.g. {@code http://127.0.0.1:3000}; {@code scope} selects the tenant on a
     * multitenant engine and is {@code null} otherwise.
     */
    record Engine(String url, String token, String scope) implements Connection {
        public Engine {
            requireText(url, "engine-url");
            requireText(token, "engine-token");
            if (scope != null && scope.isBlank()) {
                scope = null;
            }
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " must not be empty");
        }
    }
}
