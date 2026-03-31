package com.olist.streaming.jobs;

import com.olist.streaming.functions.RevenueWindowFunction;
import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.models.RevenueByCategory;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Flink streaming job that computes total revenue per product category
 * over 1-minute tumbling event-time windows.
 *
 * <p>Orders with null price or category are routed to a side output and logged.
 * Results are written to a Kafka topic and optionally to an Iceberg table.
 *
 * <p>Topics and checkpoint settings are read from environment variables via {@link JobConfig}.
 */
public class RevenueAggregationJob {

    public static final OutputTag<OrderEvent> INVALID_EVENTS_TAG =
            new OutputTag<OrderEvent>("invalid-events"){};

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(config.checkpointIntervalMs());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<OrderEvent> source = KafkaSource.<OrderEvent>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.ordersTopic())
                .setGroupId("revenue-aggregation-job")
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

        KafkaSink<RevenueByCategory> sink = KafkaSink.<RevenueByCategory>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.<RevenueByCategory>builder()
                        .setTopic(config.revenueSinkTopic())
                        .setValueSerializationSchema(JsonSerializationSchemaFactory.create(RevenueByCategory.class))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        SingleOutputStreamOperator<OrderEvent> validatedStream = orderStream
                .process(new InvalidEventRouter()).name("event_validator").uid("event_validator");

        DataStream<RevenueByCategory> revenueStream = aggregate(validatedStream);

        validatedStream.getSideOutput(INVALID_EVENTS_TAG)
                .print("INVALID_EVENT").name("invalid_events_log").uid("invalid_events_log");

        revenueStream
                .sinkTo(sink)
                .name("revenue_kafka_sink")
                .uid("revenue_kafka_sink");

        if (config.icebergEnabled()) {
            IcebergSinkBuilder icebergSink = new IcebergSinkBuilder(IcebergCatalogConfig.fromEnvironment());
            icebergSink.attachRevenueSink(revenueStream, IcebergSinkBuilder.DATABASE, IcebergSinkBuilder.TABLE_REVENUE_BY_CATEGORY);
        }

        env.execute("RevenueAggregation");
    }

    public static DataStream<RevenueByCategory> defineWorkflow(DataStream<OrderEvent> orderStream) {
        return aggregate(orderStream.process(new InvalidEventRouter()).name("event_validator"));
    }

    private static DataStream<RevenueByCategory> aggregate(DataStream<OrderEvent> validStream) {
        return validStream
                .map(RevenueByCategory::fromOrderEvent)
                .uid("map_to_revenue")
                .keyBy(RevenueByCategory::getProductCategory)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .reduce(RevenueByCategory::merge, new RevenueWindowFunction())
                .uid("revenue_window");
    }

    static class InvalidEventRouter extends ProcessFunction<OrderEvent, OrderEvent> {
        @Override
        public void processElement(OrderEvent event, Context ctx, Collector<OrderEvent> out) {
            if (event.getProductCategory() != null && event.getPrice() != null) {
                out.collect(event);
            } else {
                ctx.output(INVALID_EVENTS_TAG, event);
            }
        }
    }
}
