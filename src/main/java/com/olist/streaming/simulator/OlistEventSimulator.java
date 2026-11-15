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
    /** En dessous, la granularite du timer de l'OS coute plus cher que le retard a corriger. */
    private static final long SLEEP_THRESHOLD_NANOS = 2_000_000L;

    private final String bootstrapServers;
    private final Path dataPath;
    private final int eventsPerSecond;
    private final int durationSeconds;
    private final ObjectMapper objectMapper;

    public OlistEventSimulator(String bootstrapServers, Path dataPath, int eventsPerSecond) {
        this(bootstrapServers, dataPath, eventsPerSecond, 0);
    }

    public OlistEventSimulator(String bootstrapServers, Path dataPath, int eventsPerSecond, int durationSeconds) {
        this.bootstrapServers = bootstrapServers;
        this.dataPath = dataPath;
        this.eventsPerSecond = eventsPerSecond;
        this.durationSeconds = durationSeconds;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public void run() throws IOException, InterruptedException {
        Map<String, String> productCategories = loadProductCategories();
        Map<String, List<ItemRecord>> orderItems = loadOrderItems();
        List<OrderEvent> events = loadAndEnrichOrders(productCategories, orderItems);

        if (events.isEmpty()) {
            LOG.warn("Aucun evenement charge depuis {}", dataPath);
            return;
        }

        LOG.info("Loaded {} events, sending at {} events/second{}", events.size(), eventsPerSecond,
                durationSeconds > 0 ? " for " + durationSeconds + "s (dataset rejoue)" : "");

        // Valeurs d'origine : en mode duree le dataset est rejoue, et chaque passage doit
        // porter des cles distinctes et des horodatages decales.
        int count = events.size();
        String[] baseOrderIds = new String[count];
        String[] baseCustomerIds = new String[count];
        long[] baseTimestamps = new long[count];
        for (int i = 0; i < count; i++) {
            OrderEvent event = events.get(i);
            baseOrderIds[i] = event.getOrderId();
            baseCustomerIds[i] = event.getCustomerId();
            baseTimestamps[i] = event.getPurchaseTimestamp().toEpochMilli();
        }

        // Les fenetres sont en temps evenementiel sur la date d'achat, et les evenements
        // sont tries. Rejouer le dataset sans decaler ferait reculer le temps : le
        // watermark etant deja avance, tout le passage suivant serait traite comme
        // retardataire et les fenetres resteraient vides. D'ou un decalage d'une
        // amplitude complete du dataset a chaque passage.
        long spanMillis = baseTimestamps[count - 1] - baseTimestamps[0] + 60_000L;

        // Echeance absolue au nanoseconde plutot qu'un sleep par evenement.
        //
        // La division entiere 1000/eventsPerSecond rendait le reglage inoperant :
        // elle vaut 1 ms pour toute cible entre 501 et 1000 ev/s, puis 0 au-dela.
        // Et un sleep par evenement accumule la derive au lieu de la rattraper,
        // d'autant plus vite que la granularite du timer de l'OS est grossiere.
        //
        // Ici l'echeance est absolue, donc l'ecart se resorbe au lieu de se cumuler,
        // et on ne dort que si le retard vaut la peine d'un appel systeme.
        long intervalNanos = Math.max(1L, 1_000_000_000L / eventsPerSecond);
        long startNanos = System.nanoTime();
        long deadlineNanos = durationSeconds > 0
                ? startNanos + durationSeconds * 1_000_000_000L
                : Long.MAX_VALUE;

        try (KafkaProducer<String, String> producer = createProducer()) {
            long sent = 0;
            int pass = 0;

            emission:
            while (true) {
                for (int i = 0; i < count; i++) {
                    if (System.nanoTime() >= deadlineNanos) {
                        break emission;
                    }

                    OrderEvent event = events.get(i);
                    if (pass > 0) {
                        String suffix = "-r" + pass;
                        event.setOrderId(baseOrderIds[i] + suffix);
                        event.setCustomerId(baseCustomerIds[i] + suffix);
                        event.setPurchaseTimestamp(Instant.ofEpochMilli(baseTimestamps[i] + pass * spanMillis));
                    }

                    String json = objectMapper.writeValueAsString(event);
                    producer.send(new ProducerRecord<>(TOPIC, event.getOrderId(), json));
                    sent++;

                    if (sent % 10_000 == 0) {
                        LOG.info("Sent {} events, {} ev/s effectifs", sent, achievedRate(sent, startNanos));
                    }

                    long sleepNanos = startNanos + sent * intervalNanos - System.nanoTime();
                    if (sleepNanos > SLEEP_THRESHOLD_NANOS) {
                        Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
                    }
                }

                pass++;
                if (durationSeconds <= 0) {
                    break;
                }
            }

            producer.flush();
            LOG.info("Finished: {} events in {}s, {} ev/s effectifs (cible {} ev/s)",
                    sent, String.format("%.1f", (System.nanoTime() - startNanos) / 1e9),
                    achievedRate(sent, startNanos), eventsPerSecond);
        }
    }

    private static String achievedRate(long sent, long startNanos) {
        double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;
        return elapsedSeconds > 0 ? String.format("%.0f", sent / elapsedSeconds) : "n/a";
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
        int durationSeconds = Integer.parseInt(System.getenv().getOrDefault("SIMULATOR_DURATION_SECONDS", "0"));

        OlistEventSimulator simulator =
                new OlistEventSimulator(bootstrapServers, Path.of(dataPathStr), eventsPerSecond, durationSeconds);
        simulator.run();
    }
}
