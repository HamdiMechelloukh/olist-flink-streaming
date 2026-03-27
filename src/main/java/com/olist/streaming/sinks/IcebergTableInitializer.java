package com.olist.streaming.sinks;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IcebergTableInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergTableInitializer.class);
    private static final String DATABASE = "olist";

    public static void main(String[] args) {
        String catalogUri = System.getenv().getOrDefault("ICEBERG_CATALOG_URI", "http://localhost:8181");
        String warehouse = System.getenv().getOrDefault("ICEBERG_WAREHOUSE", "s3a://warehouse/");
        String s3Endpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
        String s3AccessKey = System.getenv().getOrDefault("MINIO_ROOT_USER", "admin");
        String s3SecretKey = System.getenv().getOrDefault("MINIO_ROOT_PASSWORD", "password123");

        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize("olist_catalog", Map.of(
                "uri", catalogUri,
                "warehouse", warehouse,
                "s3.endpoint", s3Endpoint,
                "s3.access-key-id", s3AccessKey,
                "s3.secret-access-key", s3SecretKey,
                "s3.path-style-access", "true"
        ));

        Namespace namespace = Namespace.of(DATABASE);
        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
            LOG.info("Created namespace: {}", DATABASE);
        }

        createTableIfNotExists(catalog, DATABASE, "order_events",
                IcebergSinkBuilder.orderEventSchema());
        createTableIfNotExists(catalog, DATABASE, "revenue_by_category",
                IcebergSinkBuilder.revenueByCategorySchema());
        createTableIfNotExists(catalog, DATABASE, "realtime_kpis",
                IcebergSinkBuilder.realtimeKpiSchema());

        LOG.info("All Iceberg tables initialized successfully");
    }

    private static void createTableIfNotExists(Catalog catalog, String database,
                                                 String tableName, Schema schema) {
        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        if (!catalog.tableExists(tableId)) {
            catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());
            LOG.info("Created table: {}.{}", database, tableName);
        } else {
            LOG.info("Table already exists: {}.{}", database, tableName);
        }
    }
}
