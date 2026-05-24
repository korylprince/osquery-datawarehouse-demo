package com.github.korylprince;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.hadoop.conf.Configuration;

/**
 * Flink streaming job that ingests osquery logs from Kafka and writes them
 * to Iceberg tables via the DynamicIcebergSink.
 *
 * JSON parsing and row construction are handled by {@link OsqueryRecordParser}.
 */
public class OsqueryJob {
    private static final int WRITE_PARALLELISM = 3;
    private static final String DEFAULT_BRANCH = "main";

    public static void main(String[] args) throws Exception {
        String polarisUri = System.getenv("POLARIS_URI");
        String warehouse = System.getenv("POLARIS_WAREHOUSE");
        String clientId = System.getenv("POLARIS_CLIENT_ID");
        String clientSecret = System.getenv("POLARIS_CLIENT_SECRET");
        String s3Endpoint = System.getenv("S3_ENDPOINT");
        String awsAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String kafkaBootstrapServers = requiredEnv("KAFKA_BOOTSTRAP_SERVERS");
        String kafkaTopic = requiredEnv("KAFKA_TOPIC");
        String kafkaGroupId = envOrDefault("KAFKA_GROUP_ID", "flink-osquery-job-iceberg");
        String icebergDatabase = envOrDefault("ICEBERG_DATABASE", "default");
        int parserParallelism = Integer.parseInt(envOrDefault("PARSER_PARALLELISM", "2"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.setBufferTimeout(200);

        CatalogLoader catalogLoader = createCatalogLoader(
            polarisUri,
            warehouse,
            clientId,
            clientSecret,
            s3Endpoint,
            awsAccessKey,
            awsSecretKey
        );

        KafkaSource<byte[]> kafkaSource = createKafkaSource(
            kafkaBootstrapServers,
            kafkaTopic,
            kafkaGroupId
        );

        DataStream<DynamicRecord> dynamicStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "osquery-kafka-source")
            .uid("osquery-kafka-source")
            .setParallelism(1)          // source is always 1 (single Kafka partition)
            .rebalance()                // round-robin to parser subtasks
            .flatMap(new OsqueryFlatMapFunction(icebergDatabase))
            .setParallelism(parserParallelism)
            .name("osquery-parser-metrics")
            .uid("osquery-parser-metrics")
            .returns(TypeInformation.of(DynamicRecord.class));

        DynamicIcebergSink.forInput(dynamicStream)
            .generator(new CachingGenerator())
            .catalogLoader(catalogLoader)
            .uidPrefix("osquery-dynamic-iceberg")
            .writeParallelism(WRITE_PARALLELISM)
            .immediateTableUpdate(true)
            // Refresh table metadata every 5 minutes instead of the 1-second default.
            // With immediateTableUpdate=true, schema evolution is applied inline as needed;
            // the periodic refresh only guards against out-of-band table changes, so a long
            // interval avoids continuous catalog.loadTable() calls under load.
            .cacheRefreshMs(60_000L)
            // Hold up to 200 distinct input schemas per table in the TableMetadataCache LRU.
            // Safety net for any schema variation; with CachingGenerator normalizing identity
            // the effective count per table is 1 in steady state.
            .inputSchemasPerTableCacheMaxSize(200)
            .append();

        env.execute("Flink Kafka Dynamic Iceberg Job");
    }

    private static CatalogLoader createCatalogLoader(
        String polarisUri,
        String warehouse,
        String clientId,
        String clientSecret,
        String s3Endpoint,
        String awsAccessKey,
        String awsSecretKey
    ) {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", polarisUri);
        catalogProps.put("warehouse", warehouse);
        catalogProps.put("credential", clientId + ":" + clientSecret);
        catalogProps.put("scope", "PRINCIPAL_ROLE:ALL");
        catalogProps.put("client.factory", "com.github.korylprince.GarageS3ClientFactory");
        catalogProps.put("s3.endpoint", s3Endpoint);
        catalogProps.put("s3.region", "garage");
        catalogProps.put("s3.path-style-access", "true");
        catalogProps.put("s3.access-key-id", awsAccessKey);
        catalogProps.put("s3.secret-access-key", awsSecretKey);
        return CatalogLoader.custom(
            "iceberg",
            catalogProps,
            new Configuration(),
            "org.apache.iceberg.rest.RESTCatalog"
        );
    }

    private static KafkaSource<byte[]> createKafkaSource(
        String bootstrapServers,
        String topic,
        String groupId
    ) {
        return KafkaSource.<byte[]>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new DeserializationSchema<byte[]>() {
                @Override public byte[] deserialize(byte[] message) { return message; }
                @Override public boolean isEndOfStream(byte[] nextElement) { return false; }
                @Override public TypeInformation<byte[]> getProducedType() {
                    return TypeInformation.of(byte[].class);
                }
            })
            .build();
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable: " + name);
        }
        return value;
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    /**
     * {@link DynamicRecordGenerator} that normalizes {@link Schema} object identity before
     * records enter {@link DynamicIcebergSink}.
     *
     * <h2>Why this is needed</h2>
     * Flink's default {@code CopyingChainingOutput} copies every record between chained operators
     * using the type serializer (object reuse is off by default). {@link DynamicRecord} falls back
     * to Kryo, which round-trips the {@link Schema} field through byte serialization and produces
     * a <em>new heap object</em> on every copy - even when the schema is structurally identical.
     * <p>
     * {@link org.apache.iceberg.flink.sink.dynamic.TableMetadataCache} stores schema comparison
     * results in an LRU keyed by Schema instance (via {@link Object#hashCode}/{@link Object#equals}
     * which {@link Schema} does not override, so identity {@code ==} is used). Every distinct
     * heap object becomes a separate LRU entry. Once the LRU overflows its limit, evictions
     * fire the "Performance degraded" warning and the full
     * {@code CompareSchemasVisitor} structural walk is repeated for every record.
     *
     * <h2>Fix</h2>
     * This generator maintains a per-table canonical Schema. On each call to
     * {@link #generate}, if the incoming schema is a different heap object than the cached one:
     * <ul>
     *   <li>Structurally equal (Kryo copy): {@link DynamicRecord#setSchema} normalizes the record
     *       back to the canonical instance so {@code TableMetadataCache} sees a stable reference.</li>
     *   <li>Structurally different (genuine schema evolution): the cache is updated so new columns
     *       trigger the normal Iceberg schema-evolution path.</li>
     * </ul>
     */
    static final class CachingGenerator
            implements DynamicRecordGenerator<DynamicRecord>, Serializable {

        private static final long serialVersionUID = 1L;

        /** Per-table canonical Schema instance, keyed by table identifier. Transient: rebuilt on open(). */
        private transient Map<TableIdentifier, Schema> tableSchemas;

        @Override
        public void open(OpenContext openContext) {
            tableSchemas = new HashMap<>();
        }

        @Override
        public void generate(DynamicRecord record, Collector<DynamicRecord> out) {
            TableIdentifier tableId = record.tableIdentifier();
            Schema incoming = record.schema();
            Schema canonical = tableSchemas.get(tableId);

            if (canonical == null) {
                // First record for this table: establish the canonical instance.
                tableSchemas.put(tableId, incoming);
            } else if (canonical != incoming) {
                // Different heap object (Kryo copy or genuine evolution).
                if (incoming.asStruct().equals(canonical.asStruct())) {
                    // Structurally identical (Kryo copy): normalize to canonical instance so
                    // TableMetadataCache.inputSchemas always sees the same key object.
                    record.setSchema(canonical);
                } else {
                    // Genuine schema evolution (new/removed columns): update canonical so
                    // the sink can detect and apply the change.
                    tableSchemas.put(tableId, incoming);
                }
            }

            out.collect(record);
        }
    }

    /**
     * Parses each Kafka JSON message into {@link DynamicRecord} objects for
     * {@link DynamicIcebergSink}, while maintaining per-table {@code rows_processed}
     * counters exposed through Flink's metrics system (and forwarded to Prometheus).
     *
     * <p>JSON is parsed exactly once here; the downstream identity generator in the
     * sink receives pre-built records with no additional parsing.
     */
    static final class OsqueryFlatMapFunction extends RichFlatMapFunction<byte[], DynamicRecord> {
        private final String database;
        private transient OsqueryRecordParser parser;
        private transient Map<String, Counter> tableCounters;
        private transient Counter droppedCounter;
        private transient Map<String, TableIdentifier> tableIds;

        OsqueryFlatMapFunction(String database) {
            this.database = database;
        }

        @Override
        public void open(OpenContext openContext) {
            parser = new OsqueryRecordParser();
            tableCounters = new HashMap<>();
            tableIds = new HashMap<>();
            droppedCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("messages_dropped");
        }

        @Override
        public void flatMap(byte[] value, Collector<DynamicRecord> out) {
            OsqueryRecordParser.ParsedMessage msg;
            try {
                msg = parser.parse(value);
            } catch (Exception e) {
                droppedCounter.inc();
                return;
            }

            tableCounters
                .computeIfAbsent(msg.tableName(), name ->
                    getRuntimeContext().getMetricGroup()
                        .addGroup("table", name)
                        .counter("rows_processed"))
                .inc(msg.rows().size());

            for (OsqueryRecordParser.RowEnvelope envelope : msg.rows()) {
                out.collect(new DynamicRecord(
                    tableIds.computeIfAbsent(msg.tableName(), name -> TableIdentifier.of(database, name)),
                    DEFAULT_BRANCH,
                    envelope.schema(),
                    envelope.rowData(),
                    envelope.partitionSpec(),
                    DistributionMode.NONE,
                    WRITE_PARALLELISM
                ));
            }
        }
    }
}
