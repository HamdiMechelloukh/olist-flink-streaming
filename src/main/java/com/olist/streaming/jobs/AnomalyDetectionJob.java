package com.olist.streaming.jobs;

import com.olist.streaming.models.OrderAlert;
import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.serialization.OrderEventDeserializationSchema;
import com.olist.streaming.sinks.IcebergCatalogConfig;
import com.olist.streaming.sinks.IcebergSinkBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import com.olist.streaming.serialization.JsonSerializationSchemaFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * Flink streaming job that detects two types of order anomalies:
 * <ul>
 *   <li><b>Suspicious frequency</b>: a customer placing {@code suspiciousOrderCount} or more
 *       orders within a 5-minute window, detected via CEP.</li>
 *   <li><b>Price anomaly</b>: a single order whose price exceeds {@code priceAnomalyThreshold} BRL.</li>
 * </ul>
 *
 * <p>Both alert streams are merged and written to a Kafka topic and optionally to an Iceberg table.
 * Thresholds and topics are read from environment variables via {@link JobConfig}.
 */
public class AnomalyDetectionJob {

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(config.checkpointIntervalMs());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<OrderEvent> source = KafkaSource.<OrderEvent>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.ordersTopic())
                .setGroupId("anomaly-detection-job")
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

        KafkaSink<OrderAlert> sink = KafkaSink.<OrderAlert>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.<OrderAlert>builder()
                        .setTopic(config.alertsSinkTopic())
                        .setValueSerializationSchema(JsonSerializationSchemaFactory.create(OrderAlert.class))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<OrderAlert> suspiciousFrequency = detectSuspiciousFrequency(orderStream, config.suspiciousOrderCount());
        DataStream<OrderAlert> priceAnomalies = detectPriceAnomaly(orderStream, config.priceAnomalyThreshold());
        DataStream<OrderAlert> allAlerts = suspiciousFrequency.union(priceAnomalies);

        allAlerts
                .sinkTo(sink)
                .name("alerts_sink")
                .uid("alerts_sink");

        if (config.icebergEnabled()) {
            IcebergSinkBuilder icebergSink = new IcebergSinkBuilder(IcebergCatalogConfig.fromEnvironment());
            icebergSink.attachAlertSink(allAlerts, IcebergSinkBuilder.DATABASE, IcebergSinkBuilder.TABLE_ORDER_ALERTS);
        }

        env.execute("AnomalyDetection");
    }

    public static DataStream<OrderAlert> detectSuspiciousFrequency(DataStream<OrderEvent> orderStream, int suspiciousOrderCount) {
        KeyedStream<OrderEvent, String> keyedStream = orderStream.keyBy(OrderEvent::getCustomerId);

        Pattern<OrderEvent, ?> pattern = Pattern.<OrderEvent>begin("orders")
                .timesOrMore(suspiciousOrderCount)
                .within(Duration.ofMinutes(5));

        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        return patternStream
                .select((PatternSelectFunction<OrderEvent, OrderAlert>) matchedPattern -> {
                    List<OrderEvent> orders = matchedPattern.get("orders");
                    OrderEvent last = orders.get(orders.size() - 1);
                    return new OrderAlert(
                            OrderAlert.AlertType.SUSPICIOUS_FREQUENCY,
                            last.getCustomerId(),
                            orders.size() + " orders in less than 5 minutes",
                            null,
                            last.getPurchaseTimestamp()
                    );
                })
                .uid("cep_suspicious_frequency")
                .map(new AlertCounter("suspiciousFrequencyAlertsEmitted"))
                .uid("counter_suspicious_frequency");
    }

    public static DataStream<OrderAlert> detectPriceAnomaly(DataStream<OrderEvent> orderStream, BigDecimal priceAnomalyThreshold) {
        return orderStream
                .filter(event -> event.getPrice() != null
                        && event.getPrice().compareTo(priceAnomalyThreshold) > 0)
                .uid("filter_price_anomaly")
                .map(event -> new OrderAlert(
                        OrderAlert.AlertType.PRICE_ANOMALY,
                        event.getSellerId(),
                        "Order price " + event.getPrice() + " exceeds threshold " + priceAnomalyThreshold,
                        event.getPrice(),
                        event.getPurchaseTimestamp()
                ))
                .uid("map_price_anomaly")
                .map(new AlertCounter("priceAnomalyAlertsEmitted"))
                .uid("counter_price_anomaly");
    }

    private static class AlertCounter extends RichMapFunction<OrderAlert, OrderAlert> {
        private final String metricName;
        private transient Counter alertsEmitted;

        AlertCounter(String metricName) {
            this.metricName = metricName;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            alertsEmitted = getRuntimeContext().getMetricGroup().counter(metricName);
        }

        @Override
        public OrderAlert map(OrderAlert alert) {
            alertsEmitted.inc();
            return alert;
        }
    }
}
