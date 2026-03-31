package com.olist.streaming.jobs;

import com.olist.streaming.functions.KpiWindowFunction;
import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.models.RealtimeKpi;
import com.olist.streaming.serialization.JsonSerializationSchemaFactory;
import com.olist.streaming.serialization.OrderEventDeserializationSchema;
import com.olist.streaming.sinks.IcebergCatalogConfig;
import com.olist.streaming.sinks.IcebergSinkBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

/**
 * Flink streaming job that computes real-time KPIs over 1-minute tumbling event-time windows:
 * total orders, total revenue, average order value, and orders per minute.
 *
 * <p>Events with a null price are filtered out before windowing.
 * Results are written to a Kafka topic and optionally to an Iceberg table.
 *
 * <p>Topics and checkpoint settings are read from environment variables via {@link JobConfig}.
 */
public class RealtimeKpiJob {

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(config.checkpointIntervalMs());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<OrderEvent> source = KafkaSource.<OrderEvent>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.ordersTopic())
                .setGroupId("realtime-kpi-job")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new OrderEventDeserializationSchema())
                .build();

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getPurchaseTimestamp().toEpochMilli())
                .withIdleness(Duration.ofSeconds(5));

        DataStream<OrderEvent> orderStream = env
                .fromSource(source, watermarkStrategy, "orders_source")
                .uid("orders_source");

        KafkaSink<RealtimeKpi> sink = KafkaSink.<RealtimeKpi>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.<RealtimeKpi>builder()
                        .setTopic(config.kpiSinkTopic())
                        .setValueSerializationSchema(JsonSerializationSchemaFactory.create(RealtimeKpi.class))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<RealtimeKpi> kpiStream = defineWorkflow(orderStream);

        kpiStream
                .sinkTo(sink)
                .name("kpi_kafka_sink")
                .uid("kpi_kafka_sink");

        if (config.icebergEnabled()) {
            IcebergSinkBuilder icebergSink = new IcebergSinkBuilder(IcebergCatalogConfig.fromEnvironment());
            icebergSink.attachKpiSink(kpiStream, IcebergSinkBuilder.DATABASE, IcebergSinkBuilder.TABLE_REALTIME_KPIS);
        }

        env.execute("RealtimeKpi");
    }

    public static DataStream<RealtimeKpi> defineWorkflow(DataStream<OrderEvent> orderStream) {
        return orderStream
                .filter(event -> event.getPrice() != null)
                .uid("filter_null_price")
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new KpiWindowFunction.KpiAggregateFunction(), new KpiWindowFunction())
                .uid("kpi_window");
    }
}
