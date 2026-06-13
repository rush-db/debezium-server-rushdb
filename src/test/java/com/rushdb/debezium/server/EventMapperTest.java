package com.rushdb.debezium.server;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class EventMapperTest {

    private EventMapper mapper(Set<String> ignore) {
        return new EventMapper(true, ".", "src", ignore, Map.of(), "syncKey", "syncedAt");
    }

    @Test
    void rawEnvelopeInsert() {
        Optional<SinkOp> op = mapper(Set.of()).map(
                "server1.public.orders",
                "{\"id\": 42}",
                """
                {"op":"c","ts_ms":1718000000000,
                 "source":{"table":"orders","db":"app"},
                 "before":null,
                 "after":{"id":42,"total_amount":99.5,"customer":{"name":"Ada","city":"London"}}}
                """);
        SinkOp.Upsert upsert = assertInstanceOf(SinkOp.Upsert.class, op.orElseThrow());
        assertEquals("ORDER", upsert.label());
        assertEquals("server1.public.orders:42", upsert.syncKey());
        // source names preserved verbatim, nesting flattened with '.'
        assertEquals(99.5, upsert.data().get("total_amount").asDouble());
        assertEquals("Ada", upsert.data().get("customer.name").asText());
        assertEquals(upsert.syncKey(), upsert.data().get("syncKey").asText());
        assertTrue(upsert.data().has("syncedAt"));
    }

    @Test
    void rawEnvelopeDeleteUsesKey() {
        Optional<SinkOp> op = mapper(Set.of()).map(
                "server1.public.orders",
                "{\"id\": 7}",
                "{\"op\":\"d\",\"source\":{\"table\":\"orders\"},\"before\":{\"id\":7},\"after\":null}");
        SinkOp.Delete delete = assertInstanceOf(SinkOp.Delete.class, op.orElseThrow());
        assertEquals("ORDER", delete.label());
        assertEquals("server1.public.orders:7", delete.syncKey());
    }

    @Test
    void tombstoneAndTruncateAreSkipped() {
        EventMapper m = mapper(Set.of());
        assertTrue(m.map("t.a.b", "{\"id\":1}", null).isEmpty());
        assertTrue(m.map("t.a.b", null,
                "{\"op\":\"t\",\"source\":{\"table\":\"b\"},\"before\":null,\"after\":null}").isEmpty());
    }

    @Test
    void flattenedRecordWithDeletedMarker() {
        EventMapper m = mapper(Set.of());
        Optional<SinkOp> upsert = m.map("s.db.customers", "{\"id\":1}",
                "{\"id\":1,\"plan\":\"pro\",\"__deleted\":\"false\"}");
        assertInstanceOf(SinkOp.Upsert.class, upsert.orElseThrow());

        Optional<SinkOp> delete = m.map("s.db.customers", "{\"id\":1}",
                "{\"id\":1,\"__deleted\":\"true\"}");
        assertInstanceOf(SinkOp.Delete.class, delete.orElseThrow());
        assertEquals("CUSTOMER", delete.orElseThrow().label());
    }

    @Test
    void schemaPayloadWrapperIsUnwrapped() {
        Optional<SinkOp> op = mapper(Set.of()).map(
                "s.public.users",
                "{\"schema\":{},\"payload\":{\"id\":5}}",
                """
                {"schema":{},"payload":{"op":"c","source":{"table":"users"},
                 "after":{"id":5,"name":"Ada"},"before":null}}
                """);
        SinkOp.Upsert upsert = assertInstanceOf(SinkOp.Upsert.class, op.orElseThrow());
        assertEquals("USER", upsert.label());
        assertEquals("s.public.users:5", upsert.syncKey());
    }

    @Test
    void mongoAfterStringAndExtendedJson() {
        Optional<SinkOp> op = mapper(Set.of()).map(
                "s.app.customers",
                "{\"id\":\"{\\\"$oid\\\": \\\"65f1aa\\\"}\"}",
                """
                {"op":"c","source":{"collection":"customers"},
                 "after":"{\\"_id\\": {\\"$oid\\": \\"65f1aa\\"}, \\"balance\\": {\\"$numberLong\\": \\"42\\"}, \\"created\\": {\\"$date\\": 1718000000000}, \\"plan\\": \\"pro\\"}"}
                """);
        SinkOp.Upsert upsert = assertInstanceOf(SinkOp.Upsert.class, op.orElseThrow());
        assertEquals("CUSTOMER", upsert.label());
        // extended JSON collapsed to scalars, names preserved
        assertEquals("65f1aa", upsert.data().get("_id").asText());
        assertEquals(42.0, upsert.data().get("balance").asDouble());
        assertEquals("2024-06-10T06:13:20Z", upsert.data().get("created").asText());
    }

    @Test
    void reservedKeysRenamedIgnoreFieldsDropped() {
        EventMapper m = mapper(Set.of("email", "fullName", "sin"));
        Optional<SinkOp> op = m.map(
                "s.app.docs",
                "{\"id\":1}",
                """
                {"op":"c","source":{"table":"docs"},
                 "after":{"id":1,"$type":"vip","__score":0.9,"syncKey":"spoof",
                          "Email":"a@b.c","full_name":"Ada L","sin":"123",
                          "meta":{"$relation":"r","keep":"yes"}}}
                """);
        SinkOp.Upsert upsert = assertInstanceOf(SinkOp.Upsert.class, op.orElseThrow());
        assertEquals("vip", upsert.data().get("src_type").asText());
        assertEquals(0.9, upsert.data().get("src_score").asDouble());
        assertEquals("spoof", upsert.data().get("src_syncKey").asText());
        assertEquals("r", upsert.data().get("meta.src_relation").asText());
        assertEquals("yes", upsert.data().get("meta.keep").asText());
        // real syncKey is the pipeline identity, not the spoofed value
        assertEquals("s.app.docs:1", upsert.data().get("syncKey").asText());
        // ignore matching is case- and separator-insensitive
        for (String leaked : new String[] {"Email", "email", "full_name", "fullName", "sin"}) {
            assertFalse(upsert.data().has(leaked), leaked + " leaked");
        }
    }

    @Test
    void compositeKeysSortedDeterministically() {
        EventMapper m = mapper(Set.of());
        Optional<SinkOp> op = m.map("s.public.t", "{\"b\": 2, \"a\": 1}",
                "{\"op\":\"c\",\"source\":{\"table\":\"t\"},\"after\":{\"a\":1,\"b\":2}}");
        assertEquals("s.public.t:1|2", op.orElseThrow().syncKey());
    }

    @Test
    void labelDerivation() {
        EventMapper m = mapper(Set.of());
        assertEquals("ORDER_ITEM", m.labelFor("order_items"));
        assertEquals("COMPANY", m.labelFor("companies"));
        assertEquals("STATUS", m.labelFor("status"));
        EventMapper withOverride =
                new EventMapper(true, ".", "src", Set.of(), Map.of("users", "APP_USER"), "syncKey", "syncedAt");
        assertEquals("APP_USER", withOverride.labelFor("users"));
    }

    @Test
    void customSyncKeyFieldName() {
        EventMapper m = new EventMapper(true, ".", "src", Set.of(), Map.of(),
                "_sync_id_rushdb", "_synced_at");
        assertEquals("_sync_id_rushdb", m.syncKeyField());

        Optional<SinkOp> op = m.map(
                "s.public.orders",
                "{\"id\":1}",
                """
                {"op":"c","source":{"table":"orders"},
                 "after":{"id":1,"amount":50,"_sync_id_rushdb":"spoof"}}
                """);
        SinkOp.Upsert upsert = assertInstanceOf(SinkOp.Upsert.class, op.orElseThrow());
        // identity written under the configured names
        assertEquals("s.public.orders:1", upsert.data().get("_sync_id_rushdb").asText());
        assertTrue(upsert.data().has("_synced_at"));
        assertFalse(upsert.data().has("syncKey"));
        assertFalse(upsert.data().has("syncedAt"));
        // a colliding source field is renamed, never overwrites the identity
        assertEquals("spoof", upsert.data().get("src_sync_id_rushdb").asText());
    }
}
