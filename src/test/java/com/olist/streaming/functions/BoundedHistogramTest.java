package com.olist.streaming.functions;

import org.apache.flink.metrics.HistogramStatistics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BoundedHistogramTest {

    @Test
    void emptyHistogramReturnsZeroCount() {
        BoundedHistogram histogram = new BoundedHistogram(10);
        assertEquals(0, histogram.getCount());
    }

    @Test
    void countIncreasesWithEachUpdate() {
        BoundedHistogram histogram = new BoundedHistogram(10);
        histogram.update(100);
        histogram.update(200);
        assertEquals(2, histogram.getCount());
    }

    @Test
    void statisticsReflectInsertedValues() {
        BoundedHistogram histogram = new BoundedHistogram(10);
        histogram.update(100);
        histogram.update(200);
        histogram.update(300);

        HistogramStatistics stats = histogram.getStatistics();
        assertEquals(3, stats.size());
        assertEquals(100, stats.getMin());
        assertEquals(300, stats.getMax());
        assertEquals(200.0, stats.getMean(), 0.001);
    }

    @Test
    void quantileP50ReturnsMedian() {
        BoundedHistogram histogram = new BoundedHistogram(10);
        histogram.update(10);
        histogram.update(20);
        histogram.update(30);
        histogram.update(40);
        histogram.update(50);

        HistogramStatistics stats = histogram.getStatistics();
        assertEquals(30.0, stats.getQuantile(0.5), 0.001);
    }

    @Test
    void bufferEvictsOldestValuesWhenFull() {
        BoundedHistogram histogram = new BoundedHistogram(3);
        histogram.update(1);
        histogram.update(2);
        histogram.update(3);
        // Buffer full: [1, 2, 3]
        histogram.update(100);
        // Buffer wraps: one slot overwritten with 100

        // Count keeps growing beyond capacity
        assertEquals(4, histogram.getCount());
        // Statistics only reflect the 3 values currently in the buffer
        HistogramStatistics stats = histogram.getStatistics();
        assertEquals(3, stats.size());
        assertEquals(100, stats.getMax());
    }

    @Test
    void emptyStatisticsReturnSafeDefaults() {
        BoundedHistogram histogram = new BoundedHistogram(10);
        HistogramStatistics stats = histogram.getStatistics();
        assertEquals(0, stats.size());
        assertEquals(0.0, stats.getMean(), 0.001);
        assertEquals(0.0, stats.getQuantile(0.5), 0.001);
        assertEquals(0, stats.getMin());
        assertEquals(0, stats.getMax());
    }

    @Test
    void stdDevIsZeroForSingleValue() {
        BoundedHistogram histogram = new BoundedHistogram(10);
        histogram.update(42);
        assertEquals(0.0, histogram.getStatistics().getStdDev(), 0.001);
    }

    @Test
    void stdDevIsCorrectForKnownValues() {
        BoundedHistogram histogram = new BoundedHistogram(10);
        // Values: 2, 4, 4, 4, 5, 5, 7, 9 — classic textbook stddev example (mean=5, stddev=2)
        for (long v : new long[]{2, 4, 4, 4, 5, 5, 7, 9}) {
            histogram.update(v);
        }
        assertEquals(2.0, histogram.getStatistics().getStdDev(), 0.001);
    }
}
