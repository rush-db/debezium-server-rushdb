package com.rushdb.debezium.server;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
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
 * # RushDB Cloud
 * debezium.sink.rushdb.api-key=...                    # or $RUSHDB_API_KEY
 * # or a self-hosted RushDB engine
 * debezium.sink.rushdb.engine-url=http://rushdb:3000  # or $RUSHDB_ENGINE_URL
 * debezium.sink.rushdb.engine-token=...               # or $RUSHDB_ENGINE_TOKEN
 * </pre>
 *
 * Delivery: each Debezium batch is collapsed by syncKey (last write wins),
 * upserts merge by the identity property ({@code mergeBy}), deletes go by the
 * same property. Offsets are committed only after RushDB accepts the batch,
 * so failures replay — at-least-once transport + idempotent apply. No event
 * data is ever persisted by this sink.
 *
 * <p>Debezium Server (3.7+) calls {@link #handle}, the embedded
 * {@code DebeziumEngine} calls {@link #handleBatch}; both apply the same way.
 */
@Named("rushdb")
@Dependent
public class RushDbChangeConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>,
        DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    /** One change event as the sink reads it, from either Debezium entry point. */
    private record Event(String destination, String key, String value) {}

    private static final String PREFIX = "debezium.sink.rushdb.";

    private EventMapper mapper;
    private RushDbClient client;
    private int importChunk;
    private boolean indexSyncKey;

    @PostConstruct
    void init() {
        Config config = ConfigProvider.getConfig();
        configure(
                connection(
                        get(config, "api-key").or(() -> env("RUSHDB_API_KEY")),
                        get(config, "url").or(() -> env("RUSHDB_BASE_URL")),
                        get(config, "engine-url").or(() -> env("RUSHDB_ENGINE_URL")),
                        get(config, "engine-token").or(() -> env("RUSHDB_ENGINE_TOKEN")),
                        get(config, "scope").or(() -> env("RUSHDB_SCOPE"))),
                get(config, "merge-strategy").orElse("append"),
                get(config, "suggest-types").map(Boolean::parseBoolean).orElse(true),
                get(config, "import-chunk").map(Integer::parseInt).orElse(500),
                get(config, "singularize").map(Boolean::parseBoolean).orElse(true),
                get(config, "flatten-separator").orElse("."),
                get(config, "reserved-prefix").orElse("src"),
                get(config, "ignore-fields").map(RushDbChangeConsumer::csv).orElse(Set.of()),
                get(config, "labels").map(RushDbChangeConsumer::labelMap).orElse(Map.of()),
                get(config, "sync-key-field").orElse("syncKey"),
                get(config, "synced-at-field").orElse("syncedAt"),
                get(config, "index-sync-key").map(Boolean::parseBoolean).orElse(true));
    }

    /**
     * Cloud ({@code api-key}, optional {@code url}) or a self-hosted engine
     * ({@code engine-url}, {@code engine-token}, optional {@code scope}).
     * Setting both is refused, so a deployment never picks a target by accident.
     */
    static Connection connection(
            Optional<String> apiKey,
            Optional<String> url,
            Optional<String> engineUrl,
            Optional<String> engineToken,
            Optional<String> scope) {
        if (engineUrl.isPresent()) {
            if (apiKey.isPresent()) {
                throw new IllegalStateException(
                        "both a RushDB Cloud api-key and an engine-url are set; configure one target");
            }
            return new Connection.Engine(
                    engineUrl.get(),
                    engineToken.orElseThrow(() -> new IllegalStateException(
                            "engine-url is set: also set debezium.sink.rushdb.engine-token or RUSHDB_ENGINE_TOKEN")),
                    scope.orElse(null));
        }
        if (apiKey.isPresent()) {
            return new Connection.Cloud(url.orElse("https://api.rushdb.com"), apiKey.get());
        }
        throw new IllegalStateException(
                "no RushDB target: set debezium.sink.rushdb.api-key (RushDB Cloud) or "
                        + "debezium.sink.rushdb.engine-url and engine-token (self-hosted engine)");
    }

    /** Also callable directly when used with the embedded DebeziumEngine. */
    public void configure(
            Connection connection,
            String mergeStrategy,
            boolean suggestTypes,
            int importChunk,
            boolean singularize,
            String flattenSeparator,
            String reservedPrefix,
            Set<String> ignoreFields,
            Map<String, String> labelOverrides,
            String syncKeyField,
            String syncedAtField,
            boolean indexSyncKey) {
        if (!"append".equals(mergeStrategy) && !"rewrite".equals(mergeStrategy)) {
            throw new IllegalStateException("merge-strategy must be append|rewrite");
        }
        this.mapper = new EventMapper(singularize, flattenSeparator, reservedPrefix,
                ignoreFields, labelOverrides, syncKeyField, syncedAtField);
        this.client = RushDbClient.forConnection(connection, mergeStrategy, suggestTypes, 8);
        this.importChunk = importChunk;
        this.indexSyncKey = indexSyncKey;
    }

    /** Debezium Server: each event is committed once its batch is applied. */
    @Override
    public void handle(CapturingEvents<BatchEvent> events) throws InterruptedException {
        List<BatchEvent> records = events.records();
        List<Event> mapped = new ArrayList<>(records.size());
        for (BatchEvent record : records) {
            mapped.add(new Event(record.destination(), asString(record.key()), asString(record.value())));
        }
        apply(mapped);
        for (BatchEvent record : records) {
            record.commit();
        }
    }

    /** The embedded DebeziumEngine. */
    @Override
    public void handleBatch(
            List<ChangeEvent<Object, Object>> records,
            DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        List<Event> mapped = new ArrayList<>(records.size());
        for (ChangeEvent<Object, Object> record : records) {
            mapped.add(new Event(record.destination(), asString(record.key()), asString(record.value())));
        }
        apply(mapped);
        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    /** Applies a batch; throws when RushDB does not accept it, so nothing is committed. */
    private void apply(List<Event> events) throws InterruptedException {
        // Collapse by (label, syncKey): last write wins — also required so one
        // import call never carries two records with the same mergeBy key.
        Map<String, SinkOp> collapsed = new LinkedHashMap<>();
        for (Event event : events) {
            Optional<SinkOp> op = mapper.map(event.destination(), event.key(), event.value());
            op.ifPresent(o -> collapsed.put(o.label() + "\u001f" + o.syncKey(), o));
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
        if (indexSyncKey) {
            Set<String> labels = new java.util.LinkedHashSet<>(upserts.keySet());
            labels.addAll(deletes.keySet());
            for (String label : labels) {
                client.ensureSyncKeyIndex(label, syncKeyField);
            }
        }
        for (Map.Entry<String, List<ObjectNode>> entry : upserts.entrySet()) {
            for (List<ObjectNode> chunk : chunks(entry.getValue(), importChunk)) {
                client.upsert(entry.getKey(), chunk, syncKeyField);
            }
        }
        for (Map.Entry<String, List<String>> entry : deletes.entrySet()) {
            for (List<String> chunk : chunks(entry.getValue(), importChunk)) {
                client.deleteBySyncKeys(entry.getKey(), chunk, syncKeyField);
            }
        }
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
