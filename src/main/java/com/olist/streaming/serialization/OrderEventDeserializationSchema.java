package com.olist.streaming.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.olist.streaming.models.OrderEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class OrderEventDeserializationSchema implements DeserializationSchema<OrderEvent> {

    private transient ObjectMapper objectMapper;

    @Override
    public OrderEvent deserialize(byte[] message) throws IOException {
        return getObjectMapper().readValue(message, OrderEvent.class);
    }

    @Override
    public boolean isEndOfStream(OrderEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<OrderEvent> getProducedType() {
        return TypeInformation.of(OrderEvent.class);
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        }
        return objectMapper;
    }
}
