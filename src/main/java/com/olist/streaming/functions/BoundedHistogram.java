package com.olist.streaming.functions;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Circular-buffer histogram that retains the last {@code capacity} values.
 */
class BoundedHistogram implements Histogram {

    private final long[] buffer;
    private final AtomicLong totalCount = new AtomicLong(0);
    private int writeIndex = 0;

    BoundedHistogram(int capacity) {
        this.buffer = new long[capacity];
    }

    @Override
    public synchronized void update(long value) {
        buffer[writeIndex % buffer.length] = value;
        writeIndex++;
        totalCount.incrementAndGet();
    }

    @Override
    public synchronized long getCount() {
        return totalCount.get();
    }

    @Override
    public synchronized HistogramStatistics getStatistics() {
        int size = (int) Math.min(totalCount.get(), buffer.length);
        long[] snapshot = Arrays.copyOf(buffer, size);
        Arrays.sort(snapshot);
        return new BoundedHistogramStatistics(snapshot);
    }

    private static class BoundedHistogramStatistics extends HistogramStatistics {

        private final long[] sorted;

        BoundedHistogramStatistics(long[] sorted) {
            this.sorted = sorted;
        }

        @Override
        public double getQuantile(double q) {
            if (sorted.length == 0) return 0;
            int index = (int) Math.ceil(q * sorted.length) - 1;
            return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
        }

        @Override
        public long[] getValues() {
            return Arrays.copyOf(sorted, sorted.length);
        }

        @Override
        public int size() {
            return sorted.length;
        }

        @Override
        public double getMean() {
            if (sorted.length == 0) return 0;
            long sum = 0;
            for (long v : sorted) sum += v;
            return (double) sum / sorted.length;
        }

        @Override
        public double getStdDev() {
            if (sorted.length < 2) return 0;
            double mean = getMean();
            double variance = 0;
            for (long v : sorted) variance += (v - mean) * (v - mean);
            return Math.sqrt(variance / sorted.length);
        }

        @Override
        public long getMax() {
            return sorted.length == 0 ? 0 : sorted[sorted.length - 1];
        }

        @Override
        public long getMin() {
            return sorted.length == 0 ? 0 : sorted[0];
        }
    }
}
