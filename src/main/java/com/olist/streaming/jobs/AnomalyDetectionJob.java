package com.olist.streaming.jobs;

import com.olist.streaming.models.OrderAlert;
import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.serialization.OrderEventDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import com.olist.streaming.serialization.JsonSerializationSchemaFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.time.Duration;

public class AnomalyDetectionJob {

    private static final String SOURCE_TOPIC = "orders";
    private static final String SINK_TOPIC = "order-alerts";
    private static final int SUSPICIOUS_ORDER_COUNT = 3;
    private static final BigDecimal PRICE_ANOMALY_THRESHOLD = new BigDecimal("500");

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

        KafkaSink<OrderAlert> sink = KafkaSink.<OrderAlert>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<OrderAlert>builder()
                        .setTopic(SINK_TOPIC)
                        .setValueSerializationSchema(JsonSerializationSchemaFactory.create(OrderAlert.class))
                        .build())
                .build();

        DataStream<OrderAlert> suspiciousFrequency = detectSuspiciousFrequency(orderStream);
        DataStream<OrderAlert> priceAnomalies = detectPriceAnomaly(orderStream);

        suspiciousFrequency.union(priceAnomalies)
                .sinkTo(sink)
                .name("alerts_sink")
                .uid("alerts_sink");

        env.execute("AnomalyDetection");
    }

    public static DataStream<OrderAlert> detectSuspiciousFrequency(DataStream<OrderEvent> orderStream) {
        DataStream<OrderEvent> keyedStream = orderStream.keyBy(OrderEvent::getCustomerId);

        Pattern<OrderEvent, ?> pattern = Pattern.<OrderEvent>begin("first")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.getOrderId() != null;
                    }
                })
                .next("second")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.getOrderId() != null;
                    }
                })
                .next("third")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.getOrderId() != null;
                    }
                })
                .within(Duration.ofMinutes(5));

        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        return patternStream.select((PatternSelectFunction<OrderEvent, OrderAlert>) matchedPattern -> {
            OrderEvent third = matchedPattern.get("third").getFirst();
            return new OrderAlert(
                    OrderAlert.AlertType.SUSPICIOUS_FREQUENCY,
                    third.getCustomerId(),
                    SUSPICIOUS_ORDER_COUNT + " orders in less than 5 minutes",
                    null,
                    third.getPurchaseTimestamp()
            );
        });
    }

    public static DataStream<OrderAlert> detectPriceAnomaly(DataStream<OrderEvent> orderStream) {
        return orderStream
                .filter(event -> event.getPrice() != null
                        && event.getPrice().compareTo(PRICE_ANOMALY_THRESHOLD) > 0)
                .map(event -> new OrderAlert(
                        OrderAlert.AlertType.PRICE_ANOMALY,
                        event.getSellerId(),
                        "Order price " + event.getPrice() + " exceeds threshold " + PRICE_ANOMALY_THRESHOLD,
                        event.getPrice(),
                        event.getPurchaseTimestamp()
                ));
    }
}
