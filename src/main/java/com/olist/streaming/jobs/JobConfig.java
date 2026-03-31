package com.olist.streaming.jobs;

import java.math.BigDecimal;

public record JobConfig(
        String bootstrapServers,
        long checkpointIntervalMs,
        boolean icebergEnabled,
        String ordersTopic,
        String revenueSinkTopic,
        String alertsSinkTopic,
        String kpiSinkTopic,
        int suspiciousOrderCount,
        BigDecimal priceAnomalyThreshold
) {
    public static JobConfig fromEnvironment() {
        return new JobConfig(
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                Long.parseLong(System.getenv().getOrDefault("CHECKPOINT_INTERVAL_MS", "30000")),
                "true".equalsIgnoreCase(System.getenv().getOrDefault("ICEBERG_ENABLED", "false")),
                System.getenv().getOrDefault("ORDERS_TOPIC", "orders"),
                System.getenv().getOrDefault("REVENUE_SINK_TOPIC", "revenue-by-category"),
                System.getenv().getOrDefault("ALERTS_SINK_TOPIC", "order-alerts"),
                System.getenv().getOrDefault("KPI_SINK_TOPIC", "realtime-kpis"),
                Integer.parseInt(System.getenv().getOrDefault("SUSPICIOUS_ORDER_COUNT", "3")),
                new BigDecimal(System.getenv().getOrDefault("PRICE_ANOMALY_THRESHOLD", "500"))
        );
    }
}
