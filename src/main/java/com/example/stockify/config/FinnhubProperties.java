package com.example.stockify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "stockify.finnhub")
public record FinnhubProperties(
        String apiKey,
        String wsUrl,
        List<String> watchlist
) {}
