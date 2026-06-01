package com.example.stockify;

import com.example.stockify.ingestion.FinnhubWebSocketClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"stock.prices.raw"})
class StockifyApplicationTests {

    // Prevent the WS client from trying to connect to Finnhub during context tests
    @MockitoBean
    private FinnhubWebSocketClient finnhubWebSocketClient;

    @Test
    void contextLoads() {
    }
}
