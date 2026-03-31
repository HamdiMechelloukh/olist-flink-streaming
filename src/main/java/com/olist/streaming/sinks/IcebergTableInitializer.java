package com.olist.streaming.sinks;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergTableInitializer.class);
    public static void main(String[] args) throws Exception {
        IcebergCatalogConfig config = IcebergCatalogConfig.fromEnvironment();

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        config.toProperties().forEach(hadoopConf::set);

        try (org.apache.iceberg.hadoop.HadoopCatalog catalog = new org.apache.iceberg.hadoop.HadoopCatalog(
                hadoopConf, config.warehouse())) {
            
            Schema revenueSchema = IcebergSinkBuilder.revenueByCategorySchema();
            createTableIfNotExists(catalog, IcebergSinkBuilder.DATABASE, IcebergSinkBuilder.TABLE_REVENUE_BY_CATEGORY,
                    revenueSchema,
                    PartitionSpec.builderFor(revenueSchema).day("window_start").build());
            createTableIfNotExists(catalog, IcebergSinkBuilder.DATABASE, IcebergSinkBuilder.TABLE_REALTIME_KPIS,
                    IcebergSinkBuilder.realtimeKpiSchema(), PartitionSpec.unpartitioned());
            createTableIfNotExists(catalog, IcebergSinkBuilder.DATABASE, IcebergSinkBuilder.TABLE_ORDER_ALERTS,
                    IcebergSinkBuilder.orderAlertSchema(), PartitionSpec.unpartitioned());

            LOG.info("All Iceberg tables initialized successfully");
        }
    }

    private static void createTableIfNotExists(Catalog catalog, String database,
                                                 String tableName, Schema schema, PartitionSpec partitionSpec) {
        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        if (!catalog.tableExists(tableId)) {
            catalog.createTable(tableId, schema, partitionSpec);
            LOG.info("Created table: {}.{}", database, tableName);
        } else {
            LOG.info("Table already exists: {}.{}", database, tableName);
        }
    }
}
