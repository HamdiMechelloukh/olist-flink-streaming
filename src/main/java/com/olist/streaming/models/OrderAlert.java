package com.olist.streaming.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderAlert {

    public enum AlertType {
        SUSPICIOUS_FREQUENCY,
        PRICE_ANOMALY
    }

    private AlertType alertType;
    private String entityId;
    private String description;
    private BigDecimal value;
    private Instant detectedAt;

    public OrderAlert() {}

    public OrderAlert(AlertType alertType, String entityId, String description,
                      BigDecimal value, Instant detectedAt) {
        this.alertType = alertType;
        this.entityId = entityId;
        this.description = description;
        this.value = value;
        this.detectedAt = detectedAt;
    }

    public AlertType getAlertType() { return alertType; }
    public void setAlertType(AlertType alertType) { this.alertType = alertType; }

    public String getEntityId() { return entityId; }
    public void setEntityId(String entityId) { this.entityId = entityId; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public BigDecimal getValue() { return value; }
    public void setValue(BigDecimal value) { this.value = value; }

    public Instant getDetectedAt() { return detectedAt; }
    public void setDetectedAt(Instant detectedAt) { this.detectedAt = detectedAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderAlert that = (OrderAlert) o;
        return alertType == that.alertType
                && Objects.equals(entityId, that.entityId)
                && Objects.equals(detectedAt, that.detectedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alertType, entityId, detectedAt);
    }

    @Override
    public String toString() {
        return "OrderAlert{type=" + alertType + ", entity='" + entityId
                + "', description='" + description + "', value=" + value + "}";
    }
}
