package com.olist.streaming.functions;

import com.olist.streaming.models.RevenueByCategory;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class RevenueWindowFunction
        extends ProcessWindowFunction<RevenueByCategory, RevenueByCategory, String, TimeWindow> {

    @Override
    public void process(String category, Context context, Iterable<RevenueByCategory> elements,
                        Collector<RevenueByCategory> out) {
        RevenueByCategory result = null;

        for (RevenueByCategory element : elements) {
            if (result == null) {
                result = element;
            } else {
                result = result.merge(element);
            }
        }

        if (result != null) {
            TimeWindow window = context.window();
            result.setWindowStart(Instant.ofEpochMilli(window.getStart()));
            result.setWindowEnd(Instant.ofEpochMilli(window.getEnd()));
            out.collect(result);
        }
    }
}
