package com.olist.streaming.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderEvent {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("customer_id")
    private String customerId;

    @JsonProperty("order_status")
    private String orderStatus;

    @JsonProperty("order_purchase_timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = TIMESTAMP_FORMAT, timezone = "UTC")
    private Instant purchaseTimestamp;

    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("seller_id")
    private String sellerId;

    @JsonProperty("product_category")
    private String productCategory;

    @JsonProperty("price")
    private BigDecimal price;

    @JsonProperty("freight_value")
    private BigDecimal freightValue;

    public OrderEvent() {}

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getOrderStatus() { return orderStatus; }
    public void setOrderStatus(String orderStatus) { this.orderStatus = orderStatus; }

    public Instant getPurchaseTimestamp() { return purchaseTimestamp; }
    public void setPurchaseTimestamp(Instant purchaseTimestamp) { this.purchaseTimestamp = purchaseTimestamp; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public String getSellerId() { return sellerId; }
    public void setSellerId(String sellerId) { this.sellerId = sellerId; }

    public String getProductCategory() { return productCategory; }
    public void setProductCategory(String productCategory) { this.productCategory = productCategory; }

    public BigDecimal getPrice() { return price; }
    public void setPrice(BigDecimal price) { this.price = price; }

    public BigDecimal getFreightValue() { return freightValue; }
    public void setFreightValue(BigDecimal freightValue) { this.freightValue = freightValue; }

    public BigDecimal getTotalValue() {
        BigDecimal p = price != null ? price : BigDecimal.ZERO;
        BigDecimal f = freightValue != null ? freightValue : BigDecimal.ZERO;
        return p.add(f);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderEvent that = (OrderEvent) o;
        return Objects.equals(orderId, that.orderId)
                && Objects.equals(productId, that.productId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, productId);
    }

    @Override
    public String toString() {
        return "OrderEvent{orderId='" + orderId + "', category='" + productCategory
                + "', price=" + price + ", seller='" + sellerId + "'}";
    }
}
