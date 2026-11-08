# debezium-server-rushdb

A [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html)
sink that streams change events from **any Debezium source connector**
(PostgreSQL, MySQL, MariaDB, MongoDB, SQL Server, Oracle, Db2, …) into
[RushDB](https://rushdb.com) — no Kafka required. It writes to **RushDB Cloud**
or to a **self-hosted RushDB engine** with the same results.

## How it works

The sink is a CDI bean registered as `@Named("rushdb")` and selected with
`debezium.sink.type=rushdb`. It implements Debezium Server's sink interface
(`DebeziumServerConsumer`, Debezium Server 3.7+) and the embedded engine's
`DebeziumEngine.ChangeConsumer`.

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

   | | RushDB Cloud | Self-hosted engine |
   |---|---|---|
   | Upserts | `POST /api/v1/records/import/json`, `mergeBy: [syncKey]` | `ingest` documents, `mergeBy: [syncKey]` |
   | Deletes | `POST /api/v1/records/delete` by `syncKey` | `deleteRecords` by `syncKey` |
   | Identity index | managed by RushDB Cloud | an ordered index on `syncKey` per label, declared on first write |

   On a self-hosted engine the index keeps upserts and deletes on large
   tables from walking every record of the label (`index-sync-key=false`
   turns it off).
4. Commit offsets **only after RushDB accepts** the batch — failures replay,
   and replays are idempotent. Requests are retried with backoff on network
   errors, HTTP 408/429/5xx and errors RushDB marks retryable. The sink never
   persists event data.

## Install

Debezium Server registers sinks when the server itself is built, so a sink jar
copied into an existing server's `lib/` is never loaded. Use the server this
repository builds instead: Debezium Server 3.7.1 with the RushDB sink built
in, and the same source connectors as the official distribution.

**Docker** (the official image with the RushDB sink built in):

```sh
docker run --rm \
  -v $PWD/config:/debezium/config \
  -v $PWD/data:/debezium/data \
  -e RUSHDB_ENGINE_URL=http://rushdb:3000 -e RUSHDB_ENGINE_TOKEN=... \
  ghcr.io/rush-db/debezium-server-rushdb:<version>
```

**Build it yourself:**

```sh
mvn install                                        # the sink
mvn -f distribution/pom.xml package                # distribution/target/*-runner.jar + lib/
docker build -f distribution/Dockerfile -t debezium-server-rushdb .
```

Start from [`example/application.properties`](example/application.properties)
for `config/application.properties`.

## Connect

| Target | Settings |
|---|---|
| RushDB Cloud | `RUSHDB_API_KEY` (or `debezium.sink.rushdb.api-key`); `url` defaults to `https://api.rushdb.com` |
| Self-hosted engine | `RUSHDB_ENGINE_URL` + `RUSHDB_ENGINE_TOKEN` (or `engine-url` / `engine-token`); `RUSHDB_SCOPE` (or `scope`) selects the tenant on a multitenant engine |

Setting both a Cloud key and an engine URL is refused, so a deployment never
picks a target by accident. **Pass the API key and engine token as
environment variables:** Debezium Server prints its `debezium.*` configuration
at startup, and these keys would appear there in clear text.

## Configuration

| Property (`debezium.sink.rushdb.`) | Default | Notes |
|---|---|---|
| `api-key` | `$RUSHDB_API_KEY` | RushDB Cloud |
| `url` | `$RUSHDB_BASE_URL` → `https://api.rushdb.com` | RushDB Cloud API base |
| `engine-url` | `$RUSHDB_ENGINE_URL` | self-hosted engine, e.g. `http://rushdb:3000` |
| `engine-token` | `$RUSHDB_ENGINE_TOKEN` | the engine's token |
| `scope` | `$RUSHDB_SCOPE` | tenant on a multitenant engine |
| `merge-strategy` | `append` | `append` keeps destination extras; `rewrite` mirrors rows exactly |
| `suggest-types` | `true` | RushDB type inference (ISO-8601 strings become datetimes) |
| `import-chunk` | `500` | records per write request |
| `index-sync-key` | `true` | self-hosted engine: declare an ordered index on the identity property per label |
| `labels` | — | `entity=LABEL;entity2=LABEL2` overrides |
| `ignore-fields` | — | comma-separated PII fields, any depth, case/separator-insensitive |
| `reserved-prefix` | `src` | `$type` → `src_type` |
| `flatten-separator` | `.` | nested path joiner |
| `singularize` | `true` | `orders` → `ORDER` |
| `sync-key-field` | `syncKey` | identity property name + upsert merge key / delete filter |
| `synced-at-field` | `syncedAt` | sync-timestamp property name |

Recommended source and format settings:

```properties
debezium.format.key=json
debezium.format.value=json
debezium.format.key.schemas.enable=false
debezium.format.value.schemas.enable=false
# Without schemas, DECIMAL/NUMERIC columns would arrive as base64 bytes;
# double stores them as numbers (string keeps exact digits, as strings).
debezium.source.decimal.handling.mode=double
```

JSON columns (PostgreSQL `json`/`jsonb`, MySQL `JSON`) arrive from Debezium as
strings and are stored as strings.

## Embedded engine

The consumer also works with the embedded `DebeziumEngine` (no server):

```java
var consumer = new RushDbChangeConsumer();
consumer.configure(
        new Connection.Engine("http://127.0.0.1:3000", engineToken, null), // or new Connection.Cloud(url, apiKey)
        "append", true, 500, true, ".", "src", Set.of("email"), Map.of(),
        "syncKey", "syncedAt", true);
try (var engine = DebeziumEngine.create(Json.class)
        .using(connectorProps)
        .notifying(consumer)
        .build()) {
    executor.execute(engine);
}
```

## Development

Java 21 (Debezium Server 3.7's runtime) and Maven.

```sh
mvn verify   # unit tests: event mapping, request shapes for both targets,
             # retries, target selection

# with live engines, the sink's round trip against real RushDB engines:
docker run -d -e RUSHDB_TOKEN=test-token -p 127.0.0.1:3937:3000 rushdb/rushdb:3.0.5
docker run -d -e RUSHDB_TOKEN=test-token -p 127.0.0.1:3938:3000 rushdb/rushdb:3.0.5 --multitenant --pgwire-port 0
RUSHDB_TEST_ENGINE_URL=http://127.0.0.1:3937 RUSHDB_TEST_ENGINE_TOKEN=test-token \
RUSHDB_TEST_MT_ENGINE_URL=http://127.0.0.1:3938 RUSHDB_TEST_MT_ENGINE_TOKEN=test-token mvn verify
```

## CI / releases

GitHub Actions ([`.github/workflows`](.github/workflows)):

- **CI** (`ci.yml`) — `mvn verify` against a single-tenant and a multitenant
  RushDB engine, then builds the distribution and its Docker image.
- **Release** (`release.yml`) — on a version tag: builds and tests the sink
  (versioned from the tag), builds the distribution, pushes
  `ghcr.io/rush-db/debezium-server-rushdb:<version>` (and `latest`), and
  publishes a GitHub Release with the sink jar and the distribution archive:

  ```sh
  git tag v1.1.0
  git push origin v1.1.0
  ```

  The tag drives the artifact version, so `pom.xml` versions don't need
  bumping per release.
