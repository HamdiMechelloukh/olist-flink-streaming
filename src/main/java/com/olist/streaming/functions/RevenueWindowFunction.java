package com.olist.streaming.functions;

import com.olist.streaming.models.RevenueByCategory;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * Window function that finalises a reduced {@link RevenueByCategory} result by attaching
 * the event-time window boundaries, then emits it downstream.
 *
 * <p>Also increments the {@code windowsEmitted} Flink metric on each fired window,
 * making throughput visible in the Flink Web UI and Prometheus.
 */
public class RevenueWindowFunction
        extends ProcessWindowFunction<RevenueByCategory, RevenueByCategory, String, TimeWindow> {

    private transient Counter windowsEmitted;

    @Override
    public void open(OpenContext openContext) throws Exception {
        windowsEmitted = getRuntimeContext().getMetricGroup().counter("windowsEmitted");
    }

    @Override
    public void process(String category, Context context, Iterable<RevenueByCategory> elements,
                        Collector<RevenueByCategory> out) {
        RevenueByCategory result = elements.iterator().next();
        TimeWindow window = context.window();
        result.setWindowStart(Instant.ofEpochMilli(window.getStart()));
        result.setWindowEnd(Instant.ofEpochMilli(window.getEnd()));
        windowsEmitted.inc();
        out.collect(result);
    }
}
