package com.example.stockify.domain;

import java.time.Instant;

public record StockQuote(
        String symbol,
        double price,
        double volume,
        Instant timestamp,
        String source
) {}
