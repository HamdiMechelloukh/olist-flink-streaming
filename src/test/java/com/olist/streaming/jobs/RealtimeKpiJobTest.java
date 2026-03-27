package com.olist.streaming.jobs;

import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.models.RealtimeKpi;
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

class RealtimeKpiJobTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(4)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    void computesKpisCorrectly() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        List<OrderEvent> events = List.of(
                createOrderEvent("order1", new BigDecimal("100.00"), baseTime),
                createOrderEvent("order2", new BigDecimal("200.00"), baseTime.plusSeconds(10)),
                createOrderEvent("order3", new BigDecimal("300.00"), baseTime.plusSeconds(20))
        );

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getPurchaseTimestamp().toEpochMilli());

        DataStream<OrderEvent> source = env
                .fromData(events)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        List<RealtimeKpi> results = new ArrayList<>();
        RealtimeKpiJob.defineWorkflow(source)
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty(), "Should produce at least one KPI result");

        RealtimeKpi kpi = results.getFirst();
        assertEquals(0, new BigDecimal("600.00").compareTo(kpi.getTotalRevenue()),
                "Total revenue should be 600.00");
        assertEquals(3, kpi.getTotalOrders(), "Should have 3 total orders");
        assertEquals(0, new BigDecimal("200.00").compareTo(kpi.getAverageOrderValue()),
                "Average order value should be 200.00");
    }

    @Test
    void nullPriceEventsAreFiltered() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        OrderEvent nullPriceEvent = new OrderEvent();
        nullPriceEvent.setOrderId("order-null");
        nullPriceEvent.setCustomerId("customer1");
        nullPriceEvent.setOrderStatus("delivered");
        nullPriceEvent.setPurchaseTimestamp(baseTime.plusSeconds(5));
        nullPriceEvent.setProductCategory("electronics");
        nullPriceEvent.setPrice(null);
        nullPriceEvent.setSellerId("seller1");

        List<OrderEvent> events = List.of(
                createOrderEvent("order1", new BigDecimal("100.00"), baseTime),
                createOrderEvent("order2", new BigDecimal("200.00"), baseTime.plusSeconds(10)),
                nullPriceEvent
        );

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getPurchaseTimestamp().toEpochMilli());

        DataStream<OrderEvent> source = env
                .fromData(events)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        List<RealtimeKpi> results = new ArrayList<>();
        RealtimeKpiJob.defineWorkflow(source)
                .executeAndCollect()
                .forEachRemaining(results::add);

        assertFalse(results.isEmpty(), "Should produce at least one KPI result");
        RealtimeKpi kpi = results.getFirst();
        assertEquals(2, kpi.getTotalOrders(), "Null-price event should be filtered out");
        assertEquals(0, new BigDecimal("300.00").compareTo(kpi.getTotalRevenue()),
                "Revenue should be 300.00 excluding the null-price event");
    }

    private static OrderEvent createOrderEvent(String orderId, BigDecimal price, Instant timestamp) {
        OrderEvent event = new OrderEvent();
        event.setOrderId(orderId);
        event.setCustomerId("customer1");
        event.setOrderStatus("delivered");
        event.setPurchaseTimestamp(timestamp);
        event.setProductCategory("electronics");
        event.setPrice(price);
        event.setSellerId("seller1");
        return event;
    }
}
