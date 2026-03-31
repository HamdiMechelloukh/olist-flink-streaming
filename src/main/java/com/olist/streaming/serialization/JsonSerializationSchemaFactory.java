package com.olist.streaming.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class JsonSerializationSchemaFactory {

    public static <T> SerializationSchema<T> create(Class<T> clazz) {
        return new JsonSerializationSchema<>(clazz);
    }

    private static final class JsonSerializationSchema<T> implements SerializationSchema<T> {

        private static final long serialVersionUID = 1L;
        private static final ObjectMapper OBJECT_MAPPER =
                new ObjectMapper().registerModule(new JavaTimeModule());

        private final Class<T> clazz;

        JsonSerializationSchema(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public byte[] serialize(T element) {
            try {
                return OBJECT_MAPPER.writeValueAsBytes(element);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize " + clazz.getSimpleName(), e);
            }
        }
    }
}
