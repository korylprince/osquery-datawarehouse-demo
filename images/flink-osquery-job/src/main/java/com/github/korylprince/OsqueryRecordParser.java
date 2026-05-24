package com.github.korylprince;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.TimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Parses osquery JSON log messages into Iceberg-compatible rows with cached schemas.
 *
 * <h2>Osquery message formats</h2>
 * Osquery produces two types of result logs:
 * <ul>
 *   <li><b>Differential</b>: contains a single {@code "columns"} object with key-value pairs
 *       for the query result, plus an {@code "action"} field ("added" or "removed").</li>
 *   <li><b>Snapshot</b>: contains a {@code "snapshot"} array of objects, each representing
 *       one row of the query result.</li>
 * </ul>
 *
 * <h2>Dynamic metadata extraction</h2>
 * Every top-level JSON key except {@code "name"}, {@code "columns"}, and {@code "snapshot"}
 * is treated as a metadata field. These become Iceberg columns prefixed with {@code "meta_"}.
 * Types are inferred from the JSON value:
 * <ul>
 *   <li>{@code "unixTime"} → {@code TimestampType} as {@code meta_time} (epoch seconds converted to milliseconds)</li>
 *   <li>Integral numbers → {@code LongType}</li>
 *   <li>Booleans → {@code BooleanType}</li>
 *   <li>Everything else (strings, nulls, objects) → {@code StringType}</li>
 * </ul>
 * Payload columns (from {@code "columns"} or {@code "snapshot"} entries) are always StringType.
 *
 * <h2>Schema caching and canonical meta fields</h2>
 * {@link org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink} maintains an LRU cache
 * ({@code TableMetadataCache.CacheItem.inputSchemas}) that maps input {@link Schema} objects to
 * pre-computed schema-comparison results. Because {@link org.apache.iceberg.Schema} does
 * <em>not</em> override {@code equals()} or {@code hashCode()}, the map uses Java reference
 * identity ({@code ==}) as the effective key. Each distinct Schema heap object - even if
 * structurally identical to another - occupies a separate LRU slot. When more distinct Schema
 * objects accumulate for a single table than the LRU maximum
 * ({@code inputSchemasPerTableCacheMaximumSize}, default 10), the oldest entry is evicted and the
 * "Performance degraded" warning fires; on the next occurrence of that schema the full
 * {@code CompareSchemasVisitor} walk must be repeated.
 * <p>
 * This class addresses the problem on two fronts:
 * <ol>
 *   <li><b>Instance reuse</b>: Schema instances are cached here keyed by a
 *       <em>column signature</em> (an ordered list of "columnName:typeId" strings). When a
 *       message has the same set of columns as a previous message, the exact same Schema heap
 *       object is returned, so the LRU lookup hits via identity on the first comparison.</li>
 *   <li><b>Canonical meta fields</b>: The well-known osquery meta keys
 *       ({@code unixTime}, {@code hostIdentifier}, {@code action}, {@code epoch},
 *       {@code counter}, {@code numerics}, {@code decorations}) are always emitted as schema
 *       columns with a fixed type, using {@code null} when the key is absent from the JSON.
 *       This ensures all messages for the same osquery table share the same meta-column set
 *       regardless of osquery version or host configuration - eliminating meta-field variation
 *       as the primary driver of LRU eviction. Non-canonical top-level keys are still captured
 *       dynamically and appended after the canonical columns.</li>
 * </ol>
 * When new <em>payload</em> columns appear (genuine osquery schema evolution), the signature
 * changes, a new Schema is created, and the dynamic sink automatically adds the new columns to
 * the Iceberg table.
 */
public class OsqueryRecordParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern NON_IDENTIFIER_CHARS = Pattern.compile("[^a-z0-9_]");
    private static final Pattern COLLAPSE_UNDERSCORES = Pattern.compile("_+");
    private static final Pattern STRIP_UNDERSCORES    = Pattern.compile("^_+|_+$");

    /** Root-level JSON keys that have special handling and are NOT included as meta columns. */
    private static final Set<String> EXCLUDED_META_KEYS = Set.of("name", "columns", "snapshot", "calendarTime", "decorations");

    /** This osquery field gets special treatment: epoch seconds → Iceberg TimestampType. */
    private static final String UNIX_TIME_KEY = "unixTime";

    /** The column name for the unixTime field (overrides default "meta_unixtime" sanitization). */
    private static final String TIME_COLUMN_NAME = "meta_time";

    /** Ingestion-time column added by the Flink job (epoch millis from System.currentTimeMillis()). */
    private static final String INGEST_TIME_COLUMN_NAME = "meta_ingest_time";

    private static final String TABLE_PREFIX = "osquery_";

    /**
     * Schema cache: maps column signature → Schema instance.
     * Transient because Schema is not serializable; the cache rebuilds at runtime.
     * Thread safety is not needed - Flink operators run single-threaded per subtask.
     */
    private transient Map<List<String>, Schema> schemaCache;

    /** PartitionSpec cache: maps Schema → PartitionSpec. Kept in sync with schemaCache. */
    private transient Map<Schema, PartitionSpec> partitionSpecCache;

    /** LRU cache for sanitizeIdentifier results; avoids recompiling regex for repeated column names. */
    private final Map<String, String> identifierCache = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > 2048;
        }
    };

    // -------------------------------------------------------------------------
    // Data carriers
    // -------------------------------------------------------------------------

    /** A fully built row: its (cached) Iceberg Schema, PartitionSpec, and the Flink RowData. */
    record RowEnvelope(Schema schema, PartitionSpec partitionSpec, RowData rowData) {}

    /** The complete parse result: table name and one RowEnvelope per payload row. */
    record ParsedMessage(String tableName, List<RowEnvelope> rows) {}

    /** A single metadata field extracted from the JSON root. */
    private record MetaField(String columnName, Type icebergType, Object flinkValue) {}

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Parses one osquery JSON message into a {@link ParsedMessage}.
     * <p>
     * A differential message produces one row; a snapshot message produces one row
     * per array entry. Each row shares the same metadata columns (from the message
     * envelope) but may have different payload columns.
     * <p>
     * Uses a single-pass Jackson streaming {@link JsonParser} to avoid building a full
     * DOM tree. Payload rows are buffered as raw string maps so that meta fields
     * appearing anywhere in the object (before or after the payload) are all collected
     * before schema construction. For snapshot messages, the schema is computed once for
     * the first row and reused for all subsequent rows.
     *
     * @param json raw JSON bytes from Kafka
     * @return parsed message with table name and row envelopes
     */
    public ParsedMessage parse(byte[] json) throws IOException {
        String queryName = null;
        MetaField timeField = null;
        List<MetaField> otherMetaFields = new ArrayList<>();
        List<Map<String, String>> payloadRows = null;
        boolean isSnapshot = false;

        try (JsonParser p = OBJECT_MAPPER.getFactory().createParser(json)) {
            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw new IllegalArgumentException("Expected a JSON object payload");
            }

            while (p.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = p.currentName();
                p.nextToken(); // advance to value token

                if ("name".equals(fieldName)) {
                    queryName = p.getText();

                } else if (UNIX_TIME_KEY.equals(fieldName)) {
                    // unixTime may be sent as a JSON number or a JSON string
                    long seconds = p.currentToken() == JsonToken.VALUE_NUMBER_INT
                            ? p.getLongValue()
                            : Long.parseLong(p.getText().trim());
                    timeField = new MetaField(TIME_COLUMN_NAME, TimestampType.withoutZone(),
                            TimestampData.fromEpochMillis(seconds * 1000L));

                } else if ("columns".equals(fieldName)) {
                    // Differential message: single payload row - buffer it
                    payloadRows = new ArrayList<>();
                    payloadRows.add(parseRowObjectFromParser(p));

                } else if ("snapshot".equals(fieldName)) {
                    // Snapshot message: array of row objects - buffer all rows
                    if (p.currentToken() != JsonToken.START_ARRAY) {
                        throw new IllegalArgumentException("snapshot must be a JSON array");
                    }
                    payloadRows = new ArrayList<>();
                    isSnapshot = true;
                    while (p.nextToken() != JsonToken.END_ARRAY) {
                        payloadRows.add(parseRowObjectFromParser(p));
                    }

                } else if ("decorations".equals(fieldName)) {
                    // Flatten decorations into decoration_<field> meta columns
                    otherMetaFields.addAll(parseDecorationsFromParser(p));

                } else if (EXCLUDED_META_KEYS.contains(fieldName)) {
                    // calendarTime and other excluded fields - skip the value (scalar or container)
                    p.skipChildren();

                } else {
                    // Dynamic meta field
                    otherMetaFields.add(parseStreamingMetaField(p, fieldName));
                }
            }
        }

        if (queryName == null) {
            throw new IllegalArgumentException("Missing required field: name");
        }
        if (payloadRows == null) {
            throw new IllegalArgumentException(
                "Message must contain either a columns object or a snapshot array");
        }

        // All meta fields are now collected regardless of their position in the JSON.
        String tableName = TABLE_PREFIX + sanitizeIdentifier(queryName, "table");
        List<MetaField> allMeta = assembleFinalMetaFields(timeField, otherMetaFields);

        List<RowEnvelope> results = new ArrayList<>(payloadRows.size());
        Schema sharedSchema = null;
        PartitionSpec sharedSpec = null;
        for (Map<String, String> rawRow : payloadRows) {
            if (sharedSchema == null) {
                RowEnvelope env = buildRowEnvelopeFromMap(allMeta, rawRow);
                if (isSnapshot) {
                    // Snapshot: cache schema from first row and reuse for rows 2..N
                    sharedSchema = env.schema();
                    sharedSpec = env.partitionSpec();
                }
                results.add(env);
            } else {
                // Snapshot rows 2..N share the same schema - skip signature/cache work
                results.add(buildRowDataOnlyFromMap(allMeta, rawRow, sharedSchema, sharedSpec));
            }
        }

        return new ParsedMessage(tableName, results);
    }

    // -------------------------------------------------------------------------
    // Metadata helpers
    // -------------------------------------------------------------------------

    /**
     * Assembles the final sorted meta field list: {@code meta_time} first, then the
     * injected ingest-time field, then remaining meta fields sorted by column name.
     */
    private List<MetaField> assembleFinalMetaFields(MetaField timeField, List<MetaField> otherMeta) {
        otherMeta.sort(Comparator.comparing(MetaField::columnName));
        List<MetaField> result = new ArrayList<>(otherMeta.size() + 2);
        if (timeField != null) {
            result.add(timeField);
        }
        result.add(new MetaField(INGEST_TIME_COLUMN_NAME, TimestampType.withoutZone(),
                TimestampData.fromEpochMillis(System.currentTimeMillis())));
        result.addAll(otherMeta);
        return result;
    }

    /**
     * Converts a {@link JsonParser} positioned at a value token into a {@link MetaField}.
     * Mirrors the type-inference logic of the old {@code inferMetaType} / {@code convertMetaValue}
     * methods but works directly on the streaming token without building an intermediate node.
     */
    private MetaField parseStreamingMetaField(JsonParser p, String key) throws IOException {
        String columnName = "meta_" + sanitizeIdentifier(key, "field");
        JsonToken token = p.currentToken();
        if (token == JsonToken.VALUE_NUMBER_INT) {
            return new MetaField(columnName, Types.LongType.get(), p.getLongValue());
        }
        if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
            return new MetaField(columnName, Types.BooleanType.get(), p.getBooleanValue());
        }
        if (token.isStructStart()) {
            // Container value: serialize to JSON string (rare case)
            String serialized = OBJECT_MAPPER.writeValueAsString(p.readValueAsTree());
            return new MetaField(columnName, Types.StringType.get(), StringData.fromString(serialized));
        }
        if (token == JsonToken.VALUE_NULL) {
            return new MetaField(columnName, Types.StringType.get(), null);
        }
        // String, floating-point, etc. → StringType
        return new MetaField(columnName, Types.StringType.get(), StringData.fromString(p.getText()));
    }

    /**
     * Reads the decorations JSON object (parser positioned at {@code START_OBJECT}) and
     * flattens each nested field into a {@link MetaField} with column name
     * {@code decoration_<sanitized_field>}.
     */
    private List<MetaField> parseDecorationsFromParser(JsonParser p) throws IOException {
        List<MetaField> fields = new ArrayList<>();
        while (p.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = p.currentName();
            p.nextToken(); // advance to value
            String columnName = "decoration_" + sanitizeIdentifier(fieldName, "field");
            JsonToken token = p.currentToken();
            if (token == JsonToken.VALUE_NUMBER_INT) {
                fields.add(new MetaField(columnName, Types.LongType.get(), p.getLongValue()));
            } else if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
                fields.add(new MetaField(columnName, Types.BooleanType.get(), p.getBooleanValue()));
            } else if (token == JsonToken.VALUE_NULL) {
                fields.add(new MetaField(columnName, Types.StringType.get(), null));
            } else {
                fields.add(new MetaField(columnName, Types.StringType.get(), StringData.fromString(p.getText())));
            }
        }
        return fields;
    }

    /**
     * Reads a JSON object (parser positioned at {@code START_OBJECT}) into a
     * {@link LinkedHashMap} of raw string values, preserving insertion (field) order.
     * Container values are serialized to a JSON string.
     */
    private static Map<String, String> parseRowObjectFromParser(JsonParser p) throws IOException {
        // p is at START_OBJECT
        Map<String, String> result = new LinkedHashMap<>();
        while (p.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = p.currentName();
            p.nextToken(); // advance to value
            if (p.currentToken().isStructStart()) {
                result.put(fieldName, OBJECT_MAPPER.writeValueAsString(p.readValueAsTree()));
            } else if (p.currentToken() == JsonToken.VALUE_NULL) {
                result.put(fieldName, null);
            } else {
                result.put(fieldName, p.getText());
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Row building and schema caching
    // -------------------------------------------------------------------------

    /**
     * Builds a {@link RowEnvelope} from a pre-parsed payload map: builds the
     * column signature, looks up (or creates) the cached Schema and PartitionSpec, and
     * populates a {@link GenericRowData}.
     */
    private RowEnvelope buildRowEnvelopeFromMap(List<MetaField> metaFields, Map<String, String> rawPayload) {
        Map<String, String> payloadColumns = sanitizePayloadColumns(metaFields, rawPayload);

        List<String> signature = buildSignature(metaFields, payloadColumns.keySet());
        Schema schema = getOrCreateSchema(signature, metaFields, payloadColumns.keySet());
        PartitionSpec partitionSpec = getOrCreatePartitionSpec(schema);

        GenericRowData rowData = buildRowData(metaFields, payloadColumns,
                metaFields.size() + payloadColumns.size());
        return new RowEnvelope(schema, partitionSpec, rowData);
    }

    /**
     * Builds a {@link RowEnvelope} for snapshot rows 2..N reusing a pre-computed schema.
     * No signature building or cache lookup - the schema is guaranteed identical to the first row.
     */
    private RowEnvelope buildRowDataOnlyFromMap(List<MetaField> metaFields, Map<String, String> rawPayload,
            Schema sharedSchema, PartitionSpec sharedSpec) {
        Map<String, String> payloadColumns = sanitizePayloadColumns(metaFields, rawPayload);
        GenericRowData rowData = buildRowData(metaFields, payloadColumns,
                metaFields.size() + payloadColumns.size());
        return new RowEnvelope(sharedSchema, sharedSpec, rowData);
    }

    /**
     * Returns a cached Schema for the given signature, or creates and caches a new one.
     * The cache is lazily initialized (it's transient for serialization support).
     */
    private Schema getOrCreateSchema(
            List<String> signature,
            List<MetaField> metaFields,
            Set<String> payloadColumnNames) {
        if (schemaCache == null) {
            schemaCache = new HashMap<>();
        }
        return schemaCache.computeIfAbsent(signature, _key -> {
            List<Types.NestedField> fields = new ArrayList<>();
            int fieldId = 1;
            for (MetaField mf : metaFields) {
                fields.add(Types.NestedField.optional(fieldId++, mf.columnName(), mf.icebergType()));
            }
            for (String colName : payloadColumnNames) {
                fields.add(Types.NestedField.optional(fieldId++, colName, Types.StringType.get()));
            }
            return new Schema(fields);
        });
    }

    /**
     * Returns a cached PartitionSpec for the given Schema, or creates one.
     * Uses Iceberg's day() transform on the meta_time column for hidden partitioning.
     * If the schema doesn't contain meta_time (shouldn't happen), falls back to unpartitioned.
     */
    private PartitionSpec getOrCreatePartitionSpec(Schema schema) {
        if (partitionSpecCache == null) {
            partitionSpecCache = new HashMap<>();
        }
        return partitionSpecCache.computeIfAbsent(schema, s -> {
            if (s.findField(TIME_COLUMN_NAME) != null) {
                return PartitionSpec.builderFor(s).day(TIME_COLUMN_NAME).build();
            }
            return PartitionSpec.unpartitioned();
        });
    }

    // -------------------------------------------------------------------------
    // Column sanitization helpers
    // -------------------------------------------------------------------------

    /**
     * Sanitizes payload column names to be valid Iceberg identifiers.
     * <p>
     * Names are lowercased, special characters replaced with underscores, and
     * collisions with metadata column names are resolved by appending numeric suffixes.
     * Entries are sorted by original key for deterministic column ordering.
     */
    private Map<String, String> sanitizePayloadColumns(
            List<MetaField> metaFields, Map<String, String> rawPayload) {
        List<Map.Entry<String, String>> sorted = new ArrayList<>(rawPayload.entrySet());
        sorted.sort(Comparator.comparing(Map.Entry::getKey));

        Set<String> reserved = new LinkedHashSet<>();
        for (MetaField mf : metaFields) {
            reserved.add(mf.columnName());
        }

        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : sorted) {
            String sanitized = sanitizeIdentifier(entry.getKey(), "field");
            String candidate = sanitized;
            int suffix = 2;
            while (reserved.contains(candidate) || result.containsKey(candidate)) {
                candidate = sanitized + "_" + suffix++;
            }
            result.put(candidate, entry.getValue());
        }
        return result;
    }

    /**
     * Builds the column signature used for schema cache lookups.
     * Format: {@code ["meta_action:STRING", "meta_epoch:LONG", ..., "gid:STRING", "uid:STRING"]}
     */
    private static List<String> buildSignature(List<MetaField> metaFields,
            Set<String> payloadColumnNames) {
        List<String> sig = new ArrayList<>(metaFields.size() + payloadColumnNames.size());
        for (MetaField mf : metaFields) {
            sig.add(mf.columnName() + ":" + mf.icebergType().typeId());
        }
        for (String name : payloadColumnNames) {
            sig.add(name + ":STRING");
        }
        return sig;
    }

    /**
     * Populates a {@link GenericRowData}: meta fields first, then payload columns in order.
     */
    private static GenericRowData buildRowData(List<MetaField> metaFields,
            Map<String, String> payloadColumns, int totalColumns) {
        GenericRowData rowData = new GenericRowData(totalColumns);
        int idx = 0;
        for (MetaField mf : metaFields) {
            rowData.setField(idx++, mf.flinkValue());
        }
        for (String value : payloadColumns.values()) {
            rowData.setField(idx++, value != null ? StringData.fromString(value) : null);
        }
        return rowData;
    }

    // -------------------------------------------------------------------------
    // Identifier sanitization
    // -------------------------------------------------------------------------

    /**
     * Sanitizes a raw string into a valid Iceberg identifier:
     * lowercases, replaces non-alphanumeric characters with underscores,
     * collapses runs of underscores, strips leading/trailing underscores,
     * and ensures the result starts with a letter.
     * Results are cached in an LRU map since the same column names repeat across messages.
     */
    String sanitizeIdentifier(String rawValue, String kind) {
        return identifierCache.computeIfAbsent(rawValue, raw -> {
            String lower = raw.toLowerCase(Locale.ROOT);
            String normalized = STRIP_UNDERSCORES.matcher(
                COLLAPSE_UNDERSCORES.matcher(
                    NON_IDENTIFIER_CHARS.matcher(lower).replaceAll("_")
                ).replaceAll("_")
            ).replaceAll("");

            if (normalized.isEmpty()) {
                normalized = kind;
            }
            if (!Character.isLetter(normalized.charAt(0)) && normalized.charAt(0) != '_') {
                normalized = kind + "_" + normalized;
            }
            return normalized;
        });
    }
}
