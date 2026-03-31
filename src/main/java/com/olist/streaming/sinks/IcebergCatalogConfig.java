package com.olist.streaming.sinks;

import java.util.Map;

public record IcebergCatalogConfig(
        String catalogName,
        String warehouse,
        String s3Endpoint,
        String s3AccessKey,
        String s3SecretKey
) {
    public static IcebergCatalogConfig fromEnvironment() {
        return new IcebergCatalogConfig(
                System.getenv().getOrDefault("ICEBERG_CATALOG_NAME", "olist_catalog"),
                System.getenv().getOrDefault("ICEBERG_WAREHOUSE", "s3a://warehouse/"),
                System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000"),
                System.getenv().getOrDefault("MINIO_ROOT_USER", "admin"),
                System.getenv().getOrDefault("MINIO_ROOT_PASSWORD", "password123")
        );
    }

    public Map<String, String> toProperties() {
        return Map.of(
                "warehouse", warehouse,
                "fs.s3a.endpoint", s3Endpoint,
                "fs.s3a.access.key", s3AccessKey,
                "fs.s3a.secret.key", s3SecretKey,
                "fs.s3a.path.style.access", "true",
                "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        );
    }
}
