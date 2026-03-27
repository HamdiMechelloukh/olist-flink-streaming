package com.olist.streaming.jobs;

import com.olist.streaming.functions.KpiWindowFunction;
import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.models.RealtimeKpi;
import com.olist.streaming.serialization.JsonSerializationSchemaFactory;
import com.olist.streaming.serialization.OrderEventDeserializationSchema;
import com.olist.streaming.sinks.IcebergSinkBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class RealtimeKpiJob {

    private static final String SOURCE_TOPIC = "orders";
    private static final String SINK_TOPIC = "realtime-kpis";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        KafkaSource<OrderEvent> source = KafkaSource.<OrderEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(SOURCE_TOPIC)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new OrderEventDeserializationSchema())
                .build();

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getPurchaseTimestamp().toEpochMilli());

        DataStream<OrderEvent> orderStream = env
                .fromSource(source, watermarkStrategy, "orders_source");

        KafkaSink<RealtimeKpi> sink = KafkaSink.<RealtimeKpi>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<RealtimeKpi>builder()
                        .setTopic(SINK_TOPIC)
                        .setValueSerializationSchema(JsonSerializationSchemaFactory.create(RealtimeKpi.class))
                        .build())
                .build();

        DataStream<RealtimeKpi> kpiStream = defineWorkflow(orderStream);

        kpiStream
                .sinkTo(sink)
                .name("kpi_kafka_sink")
                .uid("kpi_kafka_sink");

        String icebergEnabled = System.getenv().getOrDefault("ICEBERG_ENABLED", "false");
        if ("true".equalsIgnoreCase(icebergEnabled)) {
            IcebergSinkBuilder icebergSink = new IcebergSinkBuilder(
                    "olist_catalog",
                    System.getenv().getOrDefault("ICEBERG_CATALOG_URI", "http://localhost:8181"),
                    System.getenv().getOrDefault("ICEBERG_WAREHOUSE", "s3a://warehouse/"),
                    System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000"),
                    System.getenv().getOrDefault("MINIO_ROOT_USER", "admin"),
                    System.getenv().getOrDefault("MINIO_ROOT_PASSWORD", "password123")
            );
            icebergSink.attachKpiSink(kpiStream, "olist", "realtime_kpis");
        }

        env.execute("RealtimeKpi");
    }

    public static DataStream<RealtimeKpi> defineWorkflow(DataStream<OrderEvent> orderStream) {
        return orderStream
                .filter(event -> event.getPrice() != null)
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new KpiWindowFunction());
    }
}
