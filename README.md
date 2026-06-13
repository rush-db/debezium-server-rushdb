# debezium-server-rushdb

A [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html)
sink that streams change events from **any Debezium source connector**
(PostgreSQL, MySQL, MongoDB, SQL Server, Oracle, Cassandra, Db2, …) into
[RushDB](https://rushdb.com) — no Kafka required.

This is the Debezium-ecosystem sibling of
[`rushdb-synx`](../rushdb-synx): same destination semantics (merge-upserts by
`syncKey`, source-verbatim property names, PII suppression, reserved-key
sanitization), but the capture side is delegated entirely to Debezium.

## How it works

Implements Debezium Server's documented custom-sink contract: a CDI bean
implementing `DebeziumEngine.ChangeConsumer`, registered as
`@Named("rushdb")` and selected with `debezium.sink.type=rushdb`.

Per batch:

1. Parse each event — raw Debezium envelope (`op`/`before`/`after`/`source`),
   `ExtractNewRecordState`-flattened records (`__deleted` marker), and the
   `schema`/`payload` wrapper are all handled; MongoDB's after-as-string and
   extended JSON (`$oid`, `$date`, `$numberLong`) are normalized to scalars.
2. Map to RushDB shape:
   - label = `UPPER_CASE` singular of table/collection (overridable)
   - property names **preserved verbatim**; nested objects flatten to
     `address.city`; arrays of objects kept as JSON strings
   - identity property (default `syncKey`, renamable via `sync-key-field`) =
     `<topic>:<key values>` — deterministic
   - RushDB-reserved names (`$…`, `__…`) renamed `src_…`; configured
     `ignore-fields` (PII) dropped in-process, never transferred
3. Collapse the batch by the identity (last write wins), then apply:
   upserts → `POST /records/import/json` with `mergeBy: ["<sync-key-field>"]`;
   deletes → `POST /records/delete` by the same field filter.
4. Mark offsets processed **only after RushDB accepts** — failures replay,
   and replays are idempotent. The sink never persists event data.

## Install

Grab a prebuilt jar from the [GitHub Releases](../../releases) page, or build
it yourself:

```sh
mvn package          # → target/debezium-server-rushdb-<version>.jar (thin jar)
```

1. Download and unpack the Debezium Server distribution.
2. Copy `debezium-server-rushdb-<version>.jar` into `debezium-server/lib/`.
3. Use [`example/application.properties`](example/application.properties) as
   the starting point for `debezium-server/config/application.properties`.
4. `export RUSHDB_API_KEY=...` (and `RUSHDB_BASE_URL` if self-hosted), then
   `./run.sh`.

The jar ships `META-INF/beans.xml` and a Jandex index so CDI can discover the
bean. If your Debezium Server build does not pick up beans from `lib/`
(Quarkus builds vary by version), add this module as a dependency of the
Debezium Server assembly instead — the documented path for custom sinks.

## Configuration

| Property (`debezium.sink.rushdb.`) | Default | Notes |
|---|---|---|
| `url` | `$RUSHDB_BASE_URL` → `https://api.rushdb.com` | |
| `api-key` | `$RUSHDB_API_KEY` | required |
| `merge-strategy` | `append` | `append` keeps destination extras; `rewrite` mirrors rows exactly |
| `suggest-types` | `true` | RushDB type inference |
| `import-chunk` | `500` | records per import call |
| `labels` | — | `entity=LABEL;entity2=LABEL2` overrides |
| `ignore-fields` | — | comma-separated PII fields, any depth, case/separator-insensitive |
| `reserved-prefix` | `src` | `$type` → `src_type` |
| `flatten-separator` | `.` | nested path joiner |
| `singularize` | `true` | `orders` → `ORDER` |
| `sync-key-field` | `syncKey` | identity property name + upsert merge key / delete filter |
| `synced-at-field` | `syncedAt` | sync-timestamp property name |

Recommended format settings (the sink also unwraps `schema`/`payload` if you
leave schemas on):

```properties
debezium.format.key=json
debezium.format.value=json
debezium.format.key.schemas.enable=false
debezium.format.value.schemas.enable=false
```

## Embedded engine

The consumer also works with the embedded `DebeziumEngine` (no server):

```java
var consumer = new RushDbChangeConsumer();
consumer.configure("https://api.rushdb.com", apiKey, "append", true, 500,
        true, ".", "src", Set.of("email"), Map.of(), "syncKey", "syncedAt");
try (var engine = DebeziumEngine.create(Json.class)
        .using(connectorProps)
        .notifying(consumer)
        .build()) {
    executor.execute(engine);
}
```

## Development

```sh
mvn test     # envelope-mapping unit tests (raw, flattened, Mongo extjson,
             # reserved keys, PII ignore, composite keys, tombstones)
```

## CI / releases

GitHub Actions ([`.github/workflows`](.github/workflows)):

- **CI** (`ci.yml`) — runs `mvn verify` on every push / PR to `main`.
- **Release** (`release.yml`) — triggered by pushing a version tag; builds
  the jar (versioned from the tag), runs the tests, and publishes a GitHub
  Release with the jar attached:

  ```sh
  git tag v0.2.0
  git push origin v0.2.0     # → Release v0.2.0 with debezium-server-rushdb-0.2.0.jar
  ```

  The tag drives the artifact version, so the `pom.xml` version doesn't need
  bumping per release.
