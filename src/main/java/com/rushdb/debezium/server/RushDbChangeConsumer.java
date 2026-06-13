package com.rushdb.debezium.server;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Debezium Server sink for RushDB. Activate with:
 *
 * <pre>
 * debezium.sink.type=rushdb
 * debezium.sink.rushdb.url=https://api.rushdb.com     # or $RUSHDB_BASE_URL
 * debezium.sink.rushdb.api-key=...                    # or $RUSHDB_API_KEY
 * </pre>
 *
 * Delivery: each Debezium batch is collapsed by syncKey (last write wins),
 * upserts go through the import API's native {@code mergeBy} merge, deletes
 * through delete-by-query. Offsets are committed only after RushDB accepts
 * the batch, so failures replay — at-least-once transport + idempotent apply.
 * No event data is ever persisted by this sink.
 */
@Named("rushdb")
@Dependent
public class RushDbChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final String PREFIX = "debezium.sink.rushdb.";

    private EventMapper mapper;
    private RushDbClient client;
    private int importChunk;

    @PostConstruct
    void init() {
        Config config = ConfigProvider.getConfig();
        configure(
                get(config, "url").or(() -> env("RUSHDB_BASE_URL")).orElse("https://api.rushdb.com"),
                get(config, "api-key")
                        .or(() -> env("RUSHDB_API_KEY"))
                        .orElseThrow(() -> new IllegalStateException(
                                "set debezium.sink.rushdb.api-key or RUSHDB_API_KEY")),
                get(config, "merge-strategy").orElse("append"),
                get(config, "suggest-types").map(Boolean::parseBoolean).orElse(true),
                get(config, "import-chunk").map(Integer::parseInt).orElse(500),
                get(config, "singularize").map(Boolean::parseBoolean).orElse(true),
                get(config, "flatten-separator").orElse("."),
                get(config, "reserved-prefix").orElse("src"),
                get(config, "ignore-fields").map(RushDbChangeConsumer::csv).orElse(Set.of()),
                get(config, "labels").map(RushDbChangeConsumer::labelMap).orElse(Map.of()),
                get(config, "sync-key-field").orElse("syncKey"),
                get(config, "synced-at-field").orElse("syncedAt"));
    }

    /** Also callable directly when used with the embedded DebeziumEngine. */
    public void configure(
            String url,
            String apiKey,
            String mergeStrategy,
            boolean suggestTypes,
            int importChunk,
            boolean singularize,
            String flattenSeparator,
            String reservedPrefix,
            Set<String> ignoreFields,
            Map<String, String> labelOverrides,
            String syncKeyField,
            String syncedAtField) {
        if (!"append".equals(mergeStrategy) && !"rewrite".equals(mergeStrategy)) {
            throw new IllegalStateException("merge-strategy must be append|rewrite");
        }
        this.mapper = new EventMapper(singularize, flattenSeparator, reservedPrefix,
                ignoreFields, labelOverrides, syncKeyField, syncedAtField);
        this.client = new RushDbClient(url, apiKey, mergeStrategy, suggestTypes, 8);
        this.importChunk = importChunk;
    }

    @Override
    public void handleBatch(
            List<ChangeEvent<Object, Object>> records,
            DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        // Collapse by (label, syncKey): last write wins — also required so one
        // import call never carries two records with the same mergeBy key.
        Map<String, SinkOp> collapsed = new LinkedHashMap<>();
        for (ChangeEvent<Object, Object> record : records) {
            Optional<SinkOp> op = mapper.map(
                    record.destination(), asString(record.key()), asString(record.value()));
            op.ifPresent(o -> collapsed.put(o.label() + "" + o.syncKey(), o));
        }

        Map<String, List<ObjectNode>> upserts = new HashMap<>();
        Map<String, List<String>> deletes = new HashMap<>();
        for (SinkOp op : collapsed.values()) {
            if (op instanceof SinkOp.Upsert u) {
                upserts.computeIfAbsent(u.label(), l -> new ArrayList<>()).add(u.data());
            } else if (op instanceof SinkOp.Delete d) {
                deletes.computeIfAbsent(d.label(), l -> new ArrayList<>()).add(d.syncKey());
            }
        }

        // Any failure throws → Debezium does not commit offsets → replay.
        // Replays are idempotent on the RushDB side (mergeBy the identity field).
        String syncKeyField = mapper.syncKeyField();
        for (Map.Entry<String, List<ObjectNode>> entry : upserts.entrySet()) {
            for (List<ObjectNode> chunk : chunks(entry.getValue(), importChunk)) {
                client.importRecords(entry.getKey(), chunk, syncKeyField);
            }
        }
        for (Map.Entry<String, List<String>> entry : deletes.entrySet()) {
            for (List<String> chunk : chunks(entry.getValue(), importChunk)) {
                client.deleteBySyncKeys(entry.getKey(), chunk, syncKeyField);
            }
        }

        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    private static String asString(Object o) {
        return o == null ? null : o.toString();
    }

    private static <T> List<List<T>> chunks(List<T> items, int size) {
        List<List<T>> out = new ArrayList<>();
        for (int i = 0; i < items.size(); i += size) {
            out.add(items.subList(i, Math.min(items.size(), i + size)));
        }
        return out;
    }

    private static Optional<String> get(Config config, String key) {
        return config.getOptionalValue(PREFIX + key, String.class);
    }

    private static Optional<String> env(String name) {
        return Optional.ofNullable(System.getenv(name)).filter(s -> !s.isBlank());
    }

    private static Set<String> csv(String value) {
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    /** {@code orders=ORDER;public.users=USER} → map. */
    private static Map<String, String> labelMap(String value) {
        return Arrays.stream(value.split(";"))
                .map(String::trim)
                .filter(s -> s.contains("="))
                .collect(Collectors.toMap(
                        s -> s.substring(0, s.indexOf('=')).trim(),
                        s -> s.substring(s.indexOf('=') + 1).trim()));
    }
}
