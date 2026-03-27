package com.olist.streaming.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class JsonSerializationSchemaFactory {

    public static <T> SerializationSchema<T> create(Class<T> clazz) {
        return new SerializationSchema<>() {
            private transient ObjectMapper objectMapper;

            @Override
            public byte[] serialize(T element) {
                try {
                    return getObjectMapper().writeValueAsBytes(element);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to serialize " + clazz.getSimpleName(), e);
                }
            }

            private ObjectMapper getObjectMapper() {
                if (objectMapper == null) {
                    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
                }
                return objectMapper;
            }
        };
    }
}
