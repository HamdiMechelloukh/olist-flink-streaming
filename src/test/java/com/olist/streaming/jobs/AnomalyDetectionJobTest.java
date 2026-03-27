package com.olist.streaming.jobs;

import com.olist.streaming.models.OrderAlert;
import com.olist.streaming.models.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AnomalyDetectionJobTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(4)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    void detectsPriceAnomaly() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        List<OrderEvent> events = List.of(
                createOrderEvent("order1", "customer1", "seller1", new BigDecimal("50.00"), baseTime),
                createOrderEvent("order2", "customer1", "seller2", new BigDecimal("600.00"), baseTime.plusSeconds(10)),
                createOrderEvent("order3", "customer1", "seller3", new BigDecimal("1000.00"), baseTime.plusSeconds(20))
        );

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getPurchaseTimestamp().toEpochMilli());

        DataStream<OrderEvent> source = env
                .fromData(events)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        List<OrderAlert> results = new ArrayList<>();
        AnomalyDetectionJob.detectPriceAnomaly(source)
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertEquals(2, results.size(), "Should detect 2 price anomalies (600 and 1000)");
        assertTrue(results.stream().allMatch(a -> a.getAlertType() == OrderAlert.AlertType.PRICE_ANOMALY));
    }

    @Test
    void detectsSuspiciousFrequency() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // 3 orders from same customer within 5 minutes → should trigger alert
        // 1 order from a different customer → should not trigger alert
        List<OrderEvent> events = List.of(
                createOrderEvent("order1", "suspicious-customer", "seller1", new BigDecimal("50.00"), baseTime),
                createOrderEvent("order2", "suspicious-customer", "seller1", new BigDecimal("60.00"), baseTime.plusSeconds(30)),
                createOrderEvent("order3", "suspicious-customer", "seller1", new BigDecimal("70.00"), baseTime.plusSeconds(60)),
                createOrderEvent("order4", "normal-customer", "seller2", new BigDecimal("80.00"), baseTime.plusSeconds(90))
        );

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getPurchaseTimestamp().toEpochMilli());

        DataStream<OrderEvent> source = env
                .fromData(events)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        List<OrderAlert> results = new ArrayList<>();
        AnomalyDetectionJob.detectSuspiciousFrequency(source)
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertEquals(1, results.size(), "Should detect 1 suspicious frequency alert");
        assertEquals(OrderAlert.AlertType.SUSPICIOUS_FREQUENCY, results.get(0).getAlertType());
        assertEquals("suspicious-customer", results.get(0).getEntityId());
    }

    @Test
    void twoOrdersDoNotTriggerSuspiciousFrequency() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        List<OrderEvent> events = List.of(
                createOrderEvent("order1", "customer1", "seller1", new BigDecimal("50.00"), baseTime),
                createOrderEvent("order2", "customer1", "seller1", new BigDecimal("60.00"), baseTime.plusSeconds(30))
        );

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getPurchaseTimestamp().toEpochMilli());

        DataStream<OrderEvent> source = env
                .fromData(events)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        List<OrderAlert> results = new ArrayList<>();
        AnomalyDetectionJob.detectSuspiciousFrequency(source)
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertTrue(results.isEmpty(), "Two orders should not trigger a suspicious frequency alert");
    }

    @Test
    void priceAtThresholdIsNotAnomaly() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        List<OrderEvent> events = List.of(
                createOrderEvent("order1", "customer1", "seller1", new BigDecimal("500.00"), baseTime),
                createOrderEvent("order2", "customer1", "seller1", new BigDecimal("499.99"), baseTime.plusSeconds(10))
        );

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getPurchaseTimestamp().toEpochMilli());

        DataStream<OrderEvent> source = env
                .fromData(events)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        List<OrderAlert> results = new ArrayList<>();
        AnomalyDetectionJob.detectPriceAnomaly(source)
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertTrue(results.isEmpty(), "Prices at or below 500 BRL should not trigger a price anomaly alert");
    }

    private static OrderEvent createOrderEvent(String orderId, String customerId, String sellerId,
                                                BigDecimal price, Instant timestamp) {
        OrderEvent event = new OrderEvent();
        event.setOrderId(orderId);
        event.setCustomerId(customerId);
        event.setOrderStatus("delivered");
        event.setPurchaseTimestamp(timestamp);
        event.setProductCategory("electronics");
        event.setPrice(price);
        event.setSellerId(sellerId);
        return event;
    }
}
