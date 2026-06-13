package com.rushdb.debezium.server;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** A destination-ready operation derived from one Debezium change event. */
public sealed interface SinkOp permits SinkOp.Upsert, SinkOp.Delete {

    String label();

    String syncKey();

    record Upsert(String label, String syncKey, ObjectNode data) implements SinkOp {}

    record Delete(String label, String syncKey) implements SinkOp {}
}
