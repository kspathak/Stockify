package com.example.stockify.ingestion;

import com.example.stockify.domain.StockQuote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class StockQuoteProducer {

    private static final Logger log = LoggerFactory.getLogger(StockQuoteProducer.class);
    private static final String TOPIC = "stock.prices.raw";

    private final KafkaTemplate<String, StockQuote> kafkaTemplate;

    public StockQuoteProducer(KafkaTemplate<String, StockQuote> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(StockQuote quote) {
        kafkaTemplate.send(TOPIC, quote.symbol(), quote)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send quote for {}: {}", quote.symbol(), ex.getMessage());
                    } else {
                        log.debug("Sent quote: {} @ {}", quote.symbol(), quote.price());
                    }
                });
    }
}
