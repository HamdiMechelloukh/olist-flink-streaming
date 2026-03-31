package com.olist.streaming.sinks;

import com.olist.streaming.models.RevenueByCategory;
import com.olist.streaming.models.OrderAlert;
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

public class IcebergSinkBuilder {

    public static final String DATABASE = "olist";
    public static final String TABLE_REVENUE_BY_CATEGORY = "revenue_by_category";
    public static final String TABLE_REALTIME_KPIS = "realtime_kpis";
    public static final String TABLE_ORDER_ALERTS = "order_alerts";

    private final CatalogLoader catalogLoader;

    public IcebergSinkBuilder(IcebergCatalogConfig config) {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        config.toProperties().forEach(hadoopConf::set);
        this.catalogLoader = CatalogLoader.hadoop(
                config.catalogName(),
                hadoopConf,
                config.toProperties()
        );
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

    public static Schema revenueByCategorySchema() {
        return new Schema(
                Types.NestedField.optional(1, "product_category", Types.StringType.get()),
                Types.NestedField.optional(2, "total_revenue", Types.DecimalType.of(12, 2)),
                Types.NestedField.optional(3, "order_count", Types.LongType.get()),
                Types.NestedField.optional(4, "window_start", Types.TimestampType.withZone()),
                Types.NestedField.optional(5, "window_end", Types.TimestampType.withZone())
        );
    }

    public void attachAlertSink(DataStream<OrderAlert> stream, String database, String table) {
        TableIdentifier tableId = TableIdentifier.of(database, table);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        FlinkSink.builderFor(
                        stream,
                        alert -> {
                            GenericRowData row = new GenericRowData(5);
                            row.setField(0, toStringData(alert.getAlertType() != null ? alert.getAlertType().name() : null));
                            row.setField(1, toStringData(alert.getEntityId()));
                            row.setField(2, toStringData(alert.getDescription()));
                            row.setField(3, toDecimalData(alert.getValue()));
                            row.setField(4, toTimestampData(alert.getDetectedAt()));
                            return row;
                        },
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(RowData.class))
                .tableLoader(tableLoader)
                .append();
    }

    public static Schema orderAlertSchema() {
        return new Schema(
                Types.NestedField.optional(1, "alert_type", Types.StringType.get()),
                Types.NestedField.optional(2, "entity_id", Types.StringType.get()),
                Types.NestedField.optional(3, "description", Types.StringType.get()),
                Types.NestedField.optional(4, "value", Types.DecimalType.of(12, 2)),
                Types.NestedField.optional(5, "detected_at", Types.TimestampType.withZone())
        );
    }

    public static Schema realtimeKpiSchema() {
        return new Schema(
                Types.NestedField.optional(1, "average_order_value", Types.DecimalType.of(12, 2)),
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
