package com.olist.streaming.simulator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.olist.streaming.models.OrderEvent;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class OlistEventSimulator {

    private static final Logger LOG = LoggerFactory.getLogger(OlistEventSimulator.class);
    private static final String TOPIC = "orders";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final String bootstrapServers;
    private final Path dataPath;
    private final int eventsPerSecond;
    private final ObjectMapper objectMapper;

    public OlistEventSimulator(String bootstrapServers, Path dataPath, int eventsPerSecond) {
        this.bootstrapServers = bootstrapServers;
        this.dataPath = dataPath;
        this.eventsPerSecond = eventsPerSecond;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public void run() throws IOException, InterruptedException {
        Map<String, String> productCategories = loadProductCategories();
        Map<String, List<ItemRecord>> orderItems = loadOrderItems();
        List<OrderEvent> events = loadAndEnrichOrders(productCategories, orderItems);

        LOG.info("Loaded {} events, sending at {} events/second", events.size(), eventsPerSecond);

        try (KafkaProducer<String, String> producer = createProducer()) {
            long delayMs = 1000L / eventsPerSecond;
            int sent = 0;

            for (OrderEvent event : events) {
                long sendStart = System.currentTimeMillis();
                String json = objectMapper.writeValueAsString(event);
                producer.send(new ProducerRecord<>(TOPIC, event.getOrderId(), json));
                sent++;

                if (sent % 100 == 0) {
                    LOG.info("Sent {} / {} events", sent, events.size());
                }

                long remaining = delayMs - (System.currentTimeMillis() - sendStart);
                if (remaining > 0) {
                    Thread.sleep(remaining);
                }
            }

            producer.flush();
            LOG.info("Finished sending {} events", sent);
        }
    }

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setIgnoreSurroundingSpaces(true)
            .build();

    private List<OrderEvent> loadAndEnrichOrders(Map<String, String> productCategories,
                                                  Map<String, List<ItemRecord>> orderItems) throws IOException {
        List<OrderEvent> events = new ArrayList<>();
        Path ordersFile = dataPath.resolve("olist_orders_dataset.csv");

        try (CSVParser parser = CSV_FORMAT.parse(Files.newBufferedReader(ordersFile))) {
            for (CSVRecord record : parser) {
                String orderId = record.get("order_id");
                String customerId = record.get("customer_id");
                String orderStatus = record.get("order_status");
                Instant purchaseTimestamp = parseTimestamp(record.get("order_purchase_timestamp"));

                if (purchaseTimestamp == null) continue;

                List<ItemRecord> items = orderItems.getOrDefault(orderId, List.of());
                if (items.isEmpty()) {
                    OrderEvent event = new OrderEvent();
                    event.setOrderId(orderId);
                    event.setCustomerId(customerId);
                    event.setOrderStatus(orderStatus);
                    event.setPurchaseTimestamp(purchaseTimestamp);
                    events.add(event);
                } else {
                    for (ItemRecord item : items) {
                        OrderEvent event = new OrderEvent();
                        event.setOrderId(orderId);
                        event.setCustomerId(customerId);
                        event.setOrderStatus(orderStatus);
                        event.setPurchaseTimestamp(purchaseTimestamp);
                        event.setProductId(item.productId);
                        event.setSellerId(item.sellerId);
                        event.setPrice(item.price);
                        event.setFreightValue(item.freightValue);
                        event.setProductCategory(productCategories.get(item.productId));
                        events.add(event);
                    }
                }
            }
        }

        events.sort(Comparator.comparing(OrderEvent::getPurchaseTimestamp));
        return events;
    }

    private Map<String, List<ItemRecord>> loadOrderItems() throws IOException {
        Map<String, List<ItemRecord>> orderItems = new HashMap<>();
        Path itemsFile = dataPath.resolve("olist_order_items_dataset.csv");

        try (CSVParser parser = CSV_FORMAT.parse(Files.newBufferedReader(itemsFile))) {
            for (CSVRecord record : parser) {
                String orderId = record.get("order_id");
                ItemRecord item = new ItemRecord(
                        record.get("product_id"),
                        record.get("seller_id"),
                        parseBigDecimal(record.get("price")),
                        parseBigDecimal(record.get("freight_value"))
                );
                orderItems.computeIfAbsent(orderId, k -> new ArrayList<>()).add(item);
            }
        }

        return orderItems;
    }

    private Map<String, String> loadProductCategories() throws IOException {
        Map<String, String> productCategories = new HashMap<>();
        Path productsFile = dataPath.resolve("olist_products_dataset.csv");

        try (CSVParser parser = CSV_FORMAT.parse(Files.newBufferedReader(productsFile))) {
            for (CSVRecord record : parser) {
                String productId = record.get("product_id");
                String category = record.get("product_category_name");
                if (category != null && !category.isEmpty()) {
                    productCategories.put(productId, category);
                }
            }
        }

        return productCategories;
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        return new KafkaProducer<>(props);
    }

    private static Instant parseTimestamp(String value) {
        if (value == null || value.isEmpty()) return null;
        try {
            return LocalDateTime.parse(value, TIMESTAMP_FORMAT).toInstant(ZoneOffset.UTC);
        } catch (Exception e) {
            return null;
        }
    }

    private static BigDecimal parseBigDecimal(String value) {
        try {
            return new BigDecimal(value.trim());
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    private record ItemRecord(String productId, String sellerId, BigDecimal price, BigDecimal freightValue) {}

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String dataPathStr = System.getenv().getOrDefault("OLIST_DATA_PATH", "./data/");
        int eventsPerSecond = Integer.parseInt(System.getenv().getOrDefault("EVENTS_PER_SECOND", "50"));

        OlistEventSimulator simulator = new OlistEventSimulator(bootstrapServers, Path.of(dataPathStr), eventsPerSecond);
        simulator.run();
    }
}
