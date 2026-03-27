package com.olist.streaming.sinks;

import com.olist.streaming.models.OrderEvent;
import com.olist.streaming.models.RevenueByCategory;
import com.olist.streaming.models.RealtimeKpi;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.*;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

public class IcebergSinkBuilder {

    private final CatalogLoader catalogLoader;

    public IcebergSinkBuilder(String catalogName, String catalogUri, String warehouse,
                               String s3Endpoint, String s3AccessKey, String s3SecretKey) {
        Map<String, String> catalogProperties = Map.of(
                "type", "rest",
                "uri", catalogUri,
                "warehouse", warehouse,
                "s3.endpoint", s3Endpoint,
                "s3.access-key-id", s3AccessKey,
                "s3.secret-access-key", s3SecretKey,
                "s3.path-style-access", "true"
        );

        this.catalogLoader = CatalogLoader.custom(
                catalogName,
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.rest.RESTCatalog"
        );
    }

    public void attachOrderEventSink(DataStream<OrderEvent> stream, String database, String table) {
        TableIdentifier tableId = TableIdentifier.of(database, table);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        FlinkSink.builderFor(
                        stream,
                        event -> {
                            GenericRowData row = new GenericRowData(10);
                            row.setField(0, toStringData(event.getOrderId()));
                            row.setField(1, toStringData(event.getCustomerId()));
                            row.setField(2, toStringData(event.getOrderStatus()));
                            row.setField(3, toTimestampData(event.getPurchaseTimestamp()));
                            row.setField(4, toStringData(event.getProductId()));
                            row.setField(5, toStringData(event.getSellerId()));
                            row.setField(6, toStringData(event.getProductCategory()));
                            row.setField(7, toDecimalData(event.getPrice()));
                            row.setField(8, toDecimalData(event.getFreightValue()));
                            row.setField(9, toStringData(event.getPaymentType()));
                            return row;
                        },
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(RowData.class))
                .tableLoader(tableLoader)
                .append();
    }

    public void attachRevenueSink(DataStream<RevenueByCategory> stream, String database, String table) {
        TableIdentifier tableId = TableIdentifier.of(database, table);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        FlinkSink.builderFor(
                        stream,
                        revenue -> {
                            GenericRowData row = new GenericRowData(5);
                            row.setField(0, toStringData(revenue.getProductCategory()));
                            row.setField(1, toDecimalData(revenue.getTotalRevenue()));
                            row.setField(2, revenue.getOrderCount());
                            row.setField(3, toTimestampData(revenue.getWindowStart()));
                            row.setField(4, toTimestampData(revenue.getWindowEnd()));
                            return row;
                        },
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(RowData.class))
                .tableLoader(tableLoader)
                .append();
    }

    public void attachKpiSink(DataStream<RealtimeKpi> stream, String database, String table) {
        TableIdentifier tableId = TableIdentifier.of(database, table);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        FlinkSink.builderFor(
                        stream,
                        kpi -> {
                            GenericRowData row = new GenericRowData(6);
                            row.setField(0, toDecimalData(kpi.getAverageOrderValue()));
                            row.setField(1, kpi.getOrdersPerMinute());
                            row.setField(2, kpi.getTotalOrders());
                            row.setField(3, toDecimalData(kpi.getTotalRevenue()));
                            row.setField(4, toTimestampData(kpi.getWindowStart()));
                            row.setField(5, toTimestampData(kpi.getWindowEnd()));
                            return row;
                        },
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(RowData.class))
                .tableLoader(tableLoader)
                .append();
    }

    public static Schema orderEventSchema() {
        return new Schema(
                Types.NestedField.optional(1, "order_id", Types.StringType.get()),
                Types.NestedField.optional(2, "customer_id", Types.StringType.get()),
                Types.NestedField.optional(3, "order_status", Types.StringType.get()),
                Types.NestedField.optional(4, "purchase_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(5, "product_id", Types.StringType.get()),
                Types.NestedField.optional(6, "seller_id", Types.StringType.get()),
                Types.NestedField.optional(7, "product_category", Types.StringType.get()),
                Types.NestedField.optional(8, "price", Types.DecimalType.of(10, 2)),
                Types.NestedField.optional(9, "freight_value", Types.DecimalType.of(10, 2)),
                Types.NestedField.optional(10, "payment_type", Types.StringType.get())
        );
    }

    public static Schema revenueByCategorySchema() {
        return new Schema(
                Types.NestedField.optional(1, "product_category", Types.StringType.get()),
                Types.NestedField.optional(2, "total_revenue", Types.DecimalType.of(12, 2)),
                Types.NestedField.optional(3, "order_count", Types.LongType.get()),
                Types.NestedField.optional(4, "window_start", Types.TimestampType.withZone()),
                Types.NestedField.optional(5, "window_end", Types.TimestampType.withZone())
        );
    }

    public static Schema realtimeKpiSchema() {
        return new Schema(
                Types.NestedField.optional(1, "average_order_value", Types.DecimalType.of(10, 2)),
                Types.NestedField.optional(2, "orders_per_minute", Types.DoubleType.get()),
                Types.NestedField.optional(3, "total_orders", Types.LongType.get()),
                Types.NestedField.optional(4, "total_revenue", Types.DecimalType.of(12, 2)),
                Types.NestedField.optional(5, "window_start", Types.TimestampType.withZone()),
                Types.NestedField.optional(6, "window_end", Types.TimestampType.withZone())
        );
    }

    private static StringData toStringData(String value) {
        return value != null ? StringData.fromString(value) : null;
    }

    private static TimestampData toTimestampData(Instant instant) {
        return instant != null ? TimestampData.fromInstant(instant) : null;
    }

    private static org.apache.flink.table.data.DecimalData toDecimalData(BigDecimal value) {
        if (value == null) return null;
        return org.apache.flink.table.data.DecimalData.fromBigDecimal(value, 12, 2);
    }
}
