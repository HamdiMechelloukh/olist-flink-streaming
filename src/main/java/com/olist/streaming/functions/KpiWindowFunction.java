package com.olist.streaming.functions;

import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.models.RealtimeKpi;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

/**
 * Window function that converts a {@link KpiAccumulator} into a {@link RealtimeKpi} result
 * containing total orders, total revenue, average order value, and orders per minute,
 * computed over a 1-minute tumbling event-time window.
 *
 * <p>Exposes three Flink metrics: {@code kpiWindowsEmitted} (counter),
 * {@code lastWindowOrderCount} (gauge), and {@code orderValueDistribution} (histogram).
 */
public class KpiWindowFunction
        extends ProcessAllWindowFunction<KpiWindowFunction.KpiAccumulator, RealtimeKpi, TimeWindow> {

    private transient Counter kpiWindowsEmitted;
    private transient volatile long lastWindowOrderCount;
    private transient BoundedHistogram orderValueHistogram;

    @Override
    public void open(OpenContext openContext) throws Exception {
        kpiWindowsEmitted = getRuntimeContext().getMetricGroup().counter("kpiWindowsEmitted");
        getRuntimeContext().getMetricGroup().gauge("lastWindowOrderCount",
                (Gauge<Long>) () -> lastWindowOrderCount);
        orderValueHistogram = new BoundedHistogram(1000);
        getRuntimeContext().getMetricGroup().histogram("orderValueDistribution", orderValueHistogram);
    }

    @Override
    public void process(Context context, Iterable<KpiAccumulator> elements, Collector<RealtimeKpi> out) {
        KpiAccumulator acc = elements.iterator().next();
        if (acc.orderCount == 0) return;

        TimeWindow window = context.window();
        double windowMinutes = (window.getEnd() - window.getStart()) / 60_000.0;

        RealtimeKpi kpi = new RealtimeKpi();
        kpi.setTotalOrders(acc.orderCount);
        kpi.setTotalRevenue(acc.totalRevenue);
        kpi.setAverageOrderValue(acc.totalRevenue.divide(BigDecimal.valueOf(acc.orderCount), 2, RoundingMode.HALF_UP));
        kpi.setOrdersPerMinute(acc.orderCount / windowMinutes);
        kpi.setWindowStart(Instant.ofEpochMilli(window.getStart()));
        kpi.setWindowEnd(Instant.ofEpochMilli(window.getEnd()));

        lastWindowOrderCount = acc.orderCount;
        orderValueHistogram.update(kpi.getAverageOrderValue().setScale(0, RoundingMode.HALF_UP).longValue());
        kpiWindowsEmitted.inc();
        out.collect(kpi);
    }

    public static class KpiAccumulator {
        long orderCount;
        BigDecimal totalRevenue = BigDecimal.ZERO;
    }

    public static class KpiAggregateFunction
            implements AggregateFunction<OrderEvent, KpiAccumulator, KpiAccumulator> {

        @Override
        public KpiAccumulator createAccumulator() {
            return new KpiAccumulator();
        }

        @Override
        public KpiAccumulator add(OrderEvent event, KpiAccumulator acc) {
            acc.orderCount++;
            acc.totalRevenue = acc.totalRevenue.add(event.getTotalValue());
            return acc;
        }

        @Override
        public KpiAccumulator getResult(KpiAccumulator acc) {
            return acc;
        }

        @Override
        public KpiAccumulator merge(KpiAccumulator a, KpiAccumulator b) {
            a.orderCount += b.orderCount;
            a.totalRevenue = a.totalRevenue.add(b.totalRevenue);
            return a;
        }
    }
}
