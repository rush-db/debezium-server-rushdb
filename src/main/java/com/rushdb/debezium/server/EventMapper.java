package com.rushdb.debezium.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Maps Debezium change events (raw envelope or ExtractNewRecordState-flattened)
 * to RushDB sink operations.
 *
 * <p>Design choices mirror rushdb-synx: source field names are preserved
 * verbatim (nested paths flatten with a separator, default {@code .}); labels
 * are UPPER_CASE singular; every record carries a deterministic
 * {@code syncKey} used for idempotent merge-upserts and deletes; RushDB
 * reserved names ({@code $...}, {@code __...}) are renamed with a prefix;
 * configured ignore fields are dropped in-process and never transferred.
 */
public final class EventMapper {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final boolean singularize;
    private final String flattenSeparator;
    private final String reservedPrefix;
    /** Field names to drop at any depth; matched case/separator-insensitively. */
    private final Set<String> ignoreFields;
    private final Map<String, String> labelOverrides;
    /** Property name the identity is stored under (default {@code syncKey}). */
    private final String syncKeyField;
    /** Property name for the sync timestamp (default {@code syncedAt}). */
    private final String syncedAtField;

    public EventMapper(
            boolean singularize,
            String flattenSeparator,
            String reservedPrefix,
            Set<String> ignoreFields,
            Map<String, String> labelOverrides,
            String syncKeyField,
            String syncedAtField) {
        this.singularize = singularize;
        this.flattenSeparator = flattenSeparator;
        this.reservedPrefix = reservedPrefix;
        this.ignoreFields = ignoreFields.stream()
                .map(EventMapper::normalize)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        this.labelOverrides = labelOverrides;
        this.syncKeyField = syncKeyField;
        this.syncedAtField = syncedAtField;
    }

    public String syncKeyField() {
        return syncKeyField;
    }

    /**
     * @param destination Debezium topic/destination name, e.g. {@code server.public.orders}
     * @param keyJson     serialized key, may be null
     * @param valueJson   serialized value, null for tombstones
     * @return the sink operation, or empty for events that carry no row data
     */
    public Optional<SinkOp> map(String destination, String keyJson, String valueJson) {
        if (valueJson == null || valueJson.isBlank()) {
            return Optional.empty(); // tombstone — the preceding op=d already deleted
        }
        JsonNode value = unwrapPayload(parse(valueJson));
        if (value == null || value.isNull()) {
            return Optional.empty();
        }

        String entity;
        String op;
        JsonNode image;
        Long tsMs = null;

        JsonNode source = value.get("source");
        if (value.has("op") && source != null && source.isObject()) {
            // Raw Debezium envelope.
            op = value.get("op").asText();
            entity = entityFromSource(source, destination);
            if (value.hasNonNull("ts_ms")) {
                tsMs = value.get("ts_ms").asLong();
            }
            image = switch (op) {
                case "c", "u", "r" -> value.get("after");
                case "d" -> value.get("before");
                default -> null; // t(runcate), m(essage) — nothing to write
            };
            if (image == null || image.isNull()) {
                if ("d".equals(op)) {
                    image = JSON.createObjectNode(); // identity comes from the key
                } else {
                    return Optional.empty();
                }
            }
        } else {
            // ExtractNewRecordState-flattened record.
            entity = lastSegment(destination);
            boolean deleted = value.has("__deleted") && "true".equals(value.get("__deleted").asText());
            op = deleted ? "d" : "u";
            image = value;
        }

        // MongoDB connectors serialize after/before as a JSON *string*.
        if (image.isTextual()) {
            image = parse(image.asText());
        }
        image = normalizeExtendedJson(image);

        String label = labelFor(entity);
        String syncKey = destination + ":" + identity(keyJson, image);

        if ("d".equals(op)) {
            return Optional.of(new SinkOp.Delete(label, syncKey));
        }

        ObjectNode data = JSON.createObjectNode();
        flatten(data, "", new ArrayList<>(), image);
        data.put(syncKeyField, syncKey);
        data.put(syncedAtField, Instant.ofEpochMilli(tsMs != null ? tsMs : System.currentTimeMillis()).toString());
        return Optional.of(new SinkOp.Upsert(label, syncKey, data));
    }

    // --- entity / label ----------------------------------------------------

    private static String entityFromSource(JsonNode source, String destination) {
        for (String field : new String[] {"table", "collection"}) {
            if (source.hasNonNull(field)) {
                return source.get(field).asText();
            }
        }
        return lastSegment(destination);
    }

    private static String lastSegment(String destination) {
        int idx = destination.lastIndexOf('.');
        return idx >= 0 ? destination.substring(idx + 1) : destination;
    }

    String labelFor(String entity) {
        String override = labelOverrides.get(entity);
        if (override != null) {
            return override;
        }
        String upper = toUpperSnake(entity);
        return singularize ? singularizeUpper(upper) : upper;
    }

    private static String toUpperSnake(String s) {
        StringBuilder out = new StringBuilder(s.length() + 4);
        boolean prevLower = false;
        for (char c : s.toCharArray()) {
            if (Character.isLetterOrDigit(c)) {
                if (Character.isUpperCase(c) && prevLower) {
                    out.append('_');
                }
                prevLower = Character.isLowerCase(c);
                out.append(Character.toUpperCase(c));
            } else if (out.length() > 0 && out.charAt(out.length() - 1) != '_') {
                prevLower = false;
                out.append('_');
            }
        }
        while (out.length() > 0 && out.charAt(out.length() - 1) == '_') {
            out.setLength(out.length() - 1);
        }
        return out.toString();
    }

    private static String singularizeUpper(String upper) {
        if (upper.endsWith("IES") && upper.length() > 4) {
            return upper.substring(0, upper.length() - 3) + "Y";
        }
        for (String suffix : new String[] {"SES", "XES", "ZES", "CHES", "SHES"}) {
            if (upper.endsWith(suffix)) {
                return upper.substring(0, upper.length() - 2);
            }
        }
        boolean nonPlural = upper.endsWith("SS") || upper.endsWith("US") || upper.endsWith("IS");
        if (upper.length() > 3 && upper.endsWith("S") && !nonPlural) {
            return upper.substring(0, upper.length() - 1);
        }
        return upper;
    }

    // --- identity ----------------------------------------------------------

    private static String identity(String keyJson, JsonNode image) {
        if (keyJson != null && !keyJson.isBlank()) {
            JsonNode key = unwrapPayload(parse(keyJson));
            if (key != null && key.isObject() && key.size() > 0) {
                TreeMap<String, String> sorted = new TreeMap<>();
                key.fields().forEachRemaining(e -> sorted.put(e.getKey(), scalarToString(e.getValue())));
                return String.join("|", sorted.values());
            }
            if (key != null && !key.isNull()) {
                return scalarToString(key);
            }
        }
        for (String candidate : new String[] {"id", "_id", "uuid", "pk"}) {
            if (image.hasNonNull(candidate)) {
                return scalarToString(image.get(candidate));
            }
        }
        return Integer.toHexString(image.toString().hashCode());
    }

    private static String scalarToString(JsonNode v) {
        return v.isTextual() ? v.asText() : v.toString();
    }

    // --- flattening + field rules -------------------------------------------

    private void flatten(ObjectNode out, String prefix, List<String> path, JsonNode node) {
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String rawKey = entry.getKey();
            JsonNode value = entry.getValue();

            if (ignoreFields.contains(normalize(rawKey))) {
                continue; // sensitive field: never transferred
            }
            String keyName = isReserved(rawKey) ? renamedReserved(rawKey) : rawKey;
            String full = prefix.isEmpty() ? keyName : prefix + flattenSeparator + keyName;

            if (value.isObject()) {
                path.add(rawKey);
                flatten(out, full, path, value);
                path.remove(path.size() - 1);
            } else if (value.isArray() && containsContainers((ArrayNode) value)) {
                out.put(uniqueKey(out, full), value.toString());
            } else {
                out.set(uniqueKey(out, full), value);
            }
        }
    }

    private static boolean containsContainers(ArrayNode array) {
        for (JsonNode item : array) {
            if (item.isContainerNode()) {
                return true;
            }
        }
        return false;
    }

    private boolean isReserved(String key) {
        return key.startsWith("$") || key.startsWith("__")
                || key.equals(syncKeyField) || key.equals(syncedAtField);
    }

    private String renamedReserved(String key) {
        String cleaned = key.replaceFirst("^[$_]+", "");
        if (cleaned.isEmpty()) {
            cleaned = "field";
        }
        return reservedPrefix + "_" + cleaned;
    }

    private static String uniqueKey(ObjectNode out, String base) {
        if (!out.has(base)) {
            return base;
        }
        int n = 2;
        while (out.has(base + n)) {
            n++;
        }
        return base + n;
    }

    static String normalize(String fieldName) {
        return fieldName.replace("_", "").replace("-", "").toLowerCase();
    }

    // --- JSON helpers --------------------------------------------------------

    private static JsonNode parse(String json) {
        try {
            return JSON.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("unparseable event JSON", e);
        }
    }

    /** Strips the {"schema": ..., "payload": ...} wrapper if schemas are enabled. */
    private static JsonNode unwrapPayload(JsonNode node) {
        if (node != null && node.isObject() && node.has("payload") && node.has("schema")) {
            return node.get("payload");
        }
        return node;
    }

    /**
     * Collapses MongoDB extended-JSON scalars ({@code {"$oid": ...}},
     * {@code {"$date": ...}}, {@code {"$numberLong": ...}}) into plain values
     * so they flatten as single properties instead of {@code field.$oid}.
     */
    private static JsonNode normalizeExtendedJson(JsonNode node) {
        if (node.isObject()) {
            if (node.size() == 1) {
                JsonNode oid = node.get("$oid");
                if (oid != null) {
                    return oid;
                }
                JsonNode date = node.get("$date");
                if (date != null) {
                    return date.isNumber()
                            ? JSON.getNodeFactory().textNode(Instant.ofEpochMilli(date.asLong()).toString())
                            : date;
                }
                for (String num : new String[] {"$numberLong", "$numberInt", "$numberDecimal", "$numberDouble"}) {
                    JsonNode n = node.get(num);
                    if (n != null) {
                        try {
                            return JSON.getNodeFactory().numberNode(Double.parseDouble(n.asText()));
                        } catch (NumberFormatException e) {
                            return n;
                        }
                    }
                }
            }
            ObjectNode out = JSON.createObjectNode();
            node.fields().forEachRemaining(e -> out.set(e.getKey(), normalizeExtendedJson(e.getValue())));
            return out;
        }
        if (node.isArray()) {
            ArrayNode out = JSON.createArrayNode();
            node.forEach(item -> out.add(normalizeExtendedJson(item)));
            return out;
        }
        return node;
    }
}
