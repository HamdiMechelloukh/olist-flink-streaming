package com.olist.streaming.serialization;

import com.olist.streaming.models.OrderEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class OrderEventDeserializationSchemaTest {

    private final OrderEventDeserializationSchema schema = new OrderEventDeserializationSchema();

    @Test
    void deserializesValidMessage() throws Exception {
        String json = """
                {
                  "order_id": "abc123",
                  "customer_id": "cust1",
                  "order_status": "delivered",
                  "order_purchase_timestamp": "2024-01-01 00:00:00",
                  "product_category": "electronics",
                  "price": 99.90,
                  "freight_value": 10.00,
                  "seller_id": "seller1"
                }
                """;

        OrderEvent result = schema.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertNotNull(result);
        assertEquals("abc123", result.getOrderId());
        assertEquals("electronics", result.getProductCategory());
    }

    @Test
    void returnsNullForMalformedJson() throws Exception {
        byte[] malformed = "not valid json {{{".getBytes(StandardCharsets.UTF_8);

        OrderEvent result = schema.deserialize(malformed);

        assertNull(result, "Malformed JSON should return null without throwing");
    }

    @Test
    void returnsNullForEmptyMessage() throws Exception {
        OrderEvent result = schema.deserialize(new byte[0]);

        assertNull(result, "Empty message should return null without throwing");
    }
}
