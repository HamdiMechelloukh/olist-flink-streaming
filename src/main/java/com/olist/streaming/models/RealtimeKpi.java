package com.olist.streaming.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RealtimeKpi {

    private BigDecimal averageOrderValue;
    private double ordersPerMinute;
    private long totalOrders;
    private BigDecimal totalRevenue;
    private Instant windowStart;
    private Instant windowEnd;

    public RealtimeKpi() {}

    public BigDecimal getAverageOrderValue() { return averageOrderValue; }
    public void setAverageOrderValue(BigDecimal averageOrderValue) { this.averageOrderValue = averageOrderValue; }

    public double getOrdersPerMinute() { return ordersPerMinute; }
    public void setOrdersPerMinute(double ordersPerMinute) { this.ordersPerMinute = ordersPerMinute; }

    public long getTotalOrders() { return totalOrders; }
    public void setTotalOrders(long totalOrders) { this.totalOrders = totalOrders; }

    public BigDecimal getTotalRevenue() { return totalRevenue; }
    public void setTotalRevenue(BigDecimal totalRevenue) { this.totalRevenue = totalRevenue; }

    public Instant getWindowStart() { return windowStart; }
    public void setWindowStart(Instant windowStart) { this.windowStart = windowStart; }

    public Instant getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Instant windowEnd) { this.windowEnd = windowEnd; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RealtimeKpi that = (RealtimeKpi) o;
        return totalOrders == that.totalOrders
                && Objects.equals(averageOrderValue, that.averageOrderValue)
                && Objects.equals(totalRevenue, that.totalRevenue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(averageOrderValue, totalOrders, totalRevenue);
    }

    @Override
    public String toString() {
        return "RealtimeKpi{avgOrder=" + averageOrderValue + ", orders/min=" + ordersPerMinute
                + ", totalOrders=" + totalOrders + ", revenue=" + totalRevenue
                + ", window=[" + windowStart + " - " + windowEnd + "]}";
    }
}
