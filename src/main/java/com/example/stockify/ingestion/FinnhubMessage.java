package com.example.stockify.ingestion;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record FinnhubMessage(
        List<Trade> data,
        String type
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Trade(
            @JsonProperty("p") double price,
            @JsonProperty("s") String symbol,
            @JsonProperty("t") long timestamp,
            @JsonProperty("v") double volume
    ) {}
}
