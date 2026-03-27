package com.olist.streaming.functions;

import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.models.RealtimeKpi;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

public class KpiWindowFunction
        extends ProcessAllWindowFunction<OrderEvent, RealtimeKpi, TimeWindow> {

    @Override
    public void process(Context context, Iterable<OrderEvent> elements, Collector<RealtimeKpi> out) {
        long orderCount = 0;
        BigDecimal totalRevenue = BigDecimal.ZERO;

        for (OrderEvent event : elements) {
            orderCount++;
            if (event.getPrice() != null) {
                totalRevenue = totalRevenue.add(event.getPrice());
            }
        }

        if (orderCount == 0) return;

        TimeWindow window = context.window();
        long windowDurationMs = window.getEnd() - window.getStart();
        double windowMinutes = windowDurationMs / 60_000.0;

        RealtimeKpi kpi = new RealtimeKpi();
        kpi.setTotalOrders(orderCount);
        kpi.setTotalRevenue(totalRevenue);
        kpi.setAverageOrderValue(totalRevenue.divide(BigDecimal.valueOf(orderCount), 2, RoundingMode.HALF_UP));
        kpi.setOrdersPerMinute(orderCount / windowMinutes);
        kpi.setWindowStart(Instant.ofEpochMilli(window.getStart()));
        kpi.setWindowEnd(Instant.ofEpochMilli(window.getEnd()));

        out.collect(kpi);
    }
}
