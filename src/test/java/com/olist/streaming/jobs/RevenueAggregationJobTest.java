package com.olist.streaming.jobs;

import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.models.RevenueByCategory;
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

class RevenueAggregationJobTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(4)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    void revenueAggregationGroupsByCategory() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        List<OrderEvent> events = List.of(
                createOrderEvent("order1", "electronics", new BigDecimal("100.00"), baseTime),
                createOrderEvent("order2", "electronics", new BigDecimal("200.00"), baseTime.plusSeconds(10)),
                createOrderEvent("order3", "books", new BigDecimal("30.00"), baseTime.plusSeconds(20))
        );

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getPurchaseTimestamp().toEpochMilli());

        DataStream<OrderEvent> source = env
                .fromData(events)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        List<RevenueByCategory> results = new ArrayList<>();
        RevenueAggregationJob.defineWorkflow(source)
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty(), "Should produce at least one result");

        RevenueByCategory electronics = results.stream()
                .filter(r -> "electronics".equals(r.getProductCategory()))
                .reduce(RevenueByCategory::merge)
                .orElse(null);

        assertNotNull(electronics, "Should have electronics category");
        assertEquals(0, new BigDecimal("300.00").compareTo(electronics.getTotalRevenue()),
                "Electronics revenue should be 300.00");
        assertEquals(2, electronics.getOrderCount(), "Electronics should have 2 orders");
    }

    private static OrderEvent createOrderEvent(String orderId, String category,
                                                BigDecimal price, Instant timestamp) {
        OrderEvent event = new OrderEvent();
        event.setOrderId(orderId);
        event.setCustomerId("customer1");
        event.setOrderStatus("delivered");
        event.setPurchaseTimestamp(timestamp);
        event.setProductCategory(category);
        event.setPrice(price);
        event.setSellerId("seller1");
        return event;
    }
}
