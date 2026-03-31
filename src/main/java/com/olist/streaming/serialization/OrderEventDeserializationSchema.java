package com.olist.streaming.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.olist.streaming.models.OrderEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class OrderEventDeserializationSchema implements DeserializationSchema<OrderEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OrderEventDeserializationSchema.class);
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());

    private transient Counter deserializationErrors;

    @Override
    public void open(InitializationContext context) throws Exception {
        deserializationErrors = context.getMetricGroup().counter("deserializationErrors");
    }

    @Override
    public OrderEvent deserialize(byte[] message) {
        try {
            return OBJECT_MAPPER.readValue(message, OrderEvent.class);
        } catch (IOException e) {
            LOG.warn("Skipping malformed message: {}", new String(message, StandardCharsets.UTF_8), e);
            if (deserializationErrors != null) {
                deserializationErrors.inc();
            }
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(OrderEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<OrderEvent> getProducedType() {
        return TypeInformation.of(OrderEvent.class);
    }
}
