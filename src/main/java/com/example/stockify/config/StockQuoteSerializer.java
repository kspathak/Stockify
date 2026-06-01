package com.example.stockify.config;

import com.example.stockify.domain.StockQuote;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class StockQuoteSerializer implements Serializer<StockQuote> {

    // Jackson 3.x (used by Spring Boot 4) has Java Time support built-in
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, StockQuote data) {
        if (data == null) return null;
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize StockQuote", e);
        }
    }
}
