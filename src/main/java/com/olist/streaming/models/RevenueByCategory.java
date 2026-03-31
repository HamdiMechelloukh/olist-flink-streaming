package com.olist.streaming.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RevenueByCategory {

    private String productCategory;
    private BigDecimal totalRevenue;
    private long orderCount;
    private Instant windowStart;
    private Instant windowEnd;

    public RevenueByCategory() {}

    public RevenueByCategory(String productCategory, BigDecimal totalRevenue, long orderCount,
                             Instant windowStart, Instant windowEnd) {
        this.productCategory = productCategory;
        this.totalRevenue = totalRevenue;
        this.orderCount = orderCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public static RevenueByCategory fromOrderEvent(OrderEvent event) {
        RevenueByCategory revenue = new RevenueByCategory();
        revenue.setProductCategory(event.getProductCategory());
        revenue.setTotalRevenue(event.getTotalValue());
        revenue.setOrderCount(1);
        return revenue;
    }

    public RevenueByCategory merge(RevenueByCategory other) {
        RevenueByCategory merged = new RevenueByCategory();
        merged.setProductCategory(this.productCategory);
        merged.setTotalRevenue(
                Objects.requireNonNullElse(this.totalRevenue, BigDecimal.ZERO)
                        .add(Objects.requireNonNullElse(other.totalRevenue, BigDecimal.ZERO)));
        merged.setOrderCount(this.orderCount + other.orderCount);
        merged.setWindowStart(this.windowStart);
        merged.setWindowEnd(this.windowEnd);
        return merged;
    }

    public String getProductCategory() { return productCategory; }
    public void setProductCategory(String productCategory) { this.productCategory = productCategory; }

    public BigDecimal getTotalRevenue() { return totalRevenue; }
    public void setTotalRevenue(BigDecimal totalRevenue) { this.totalRevenue = totalRevenue; }

    public long getOrderCount() { return orderCount; }
    public void setOrderCount(long orderCount) { this.orderCount = orderCount; }

    public Instant getWindowStart() { return windowStart; }
    public void setWindowStart(Instant windowStart) { this.windowStart = windowStart; }

    public Instant getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Instant windowEnd) { this.windowEnd = windowEnd; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RevenueByCategory that = (RevenueByCategory) o;
        return orderCount == that.orderCount
                && Objects.equals(productCategory, that.productCategory)
                && Objects.equals(totalRevenue, that.totalRevenue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productCategory, totalRevenue, orderCount);
    }

    @Override
    public String toString() {
        return "RevenueByCategory{category='" + productCategory + "', revenue=" + totalRevenue
                + ", orders=" + orderCount + ", window=[" + windowStart + " - " + windowEnd + "]}";
    }
}
