package com.example.stockify.ingestion;

import com.example.stockify.config.FinnhubProperties;
import com.example.stockify.domain.StockQuote;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;

@Component
public class FinnhubWebSocketClient implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(FinnhubWebSocketClient.class);

    private final FinnhubProperties properties;
    private final StockQuoteProducer producer;
    private final ObjectMapper objectMapper;

    private volatile Disposable connection;
    private volatile boolean running = false;

    public FinnhubWebSocketClient(FinnhubProperties properties,
                                  StockQuoteProducer producer,
                                  ObjectMapper objectMapper) {
        this.properties = properties;
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    @Override
    public void start() {
        running = true;
        log.info("Connecting to Finnhub WebSocket for {} symbols", properties.watchlist().size());
        connection = connect()
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(5))
                        .doBeforeRetry(s -> log.warn("Reconnecting to Finnhub (attempt {})", s.totalRetries() + 1)))
                .doOnError(e -> log.error("Unrecoverable Finnhub WS error", e))
                .subscribe();
    }

    @Override
    public void stop() {
        running = false;
        if (connection != null && !connection.isDisposed()) {
            connection.dispose();
            log.info("Finnhub WebSocket connection closed");
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private Mono<Void> connect() {
        var client = new ReactorNettyWebSocketClient();
        var uri = URI.create(properties.wsUrl() + "?token=" + properties.apiKey());

        return client.execute(uri, session -> {
            // Send a subscribe message for every symbol in the watchlist
            Flux<WebSocketMessage> subscriptions = Flux.fromIterable(properties.watchlist())
                    .map(symbol -> session.textMessage(
                            "{\"type\":\"subscribe\",\"symbol\":\"%s\"}".formatted(symbol)));

            Mono<Void> sendSubscriptions = session.send(subscriptions)
                    .doOnSuccess(v -> log.info("Subscribed to {} symbols", properties.watchlist().size()));

            // Receive and process trade messages; ignore pings
            Flux<Void> receive = session.receive()
                    .filter(msg -> msg.getType() == WebSocketMessage.Type.TEXT)
                    .map(WebSocketMessage::getPayloadAsText)
                    .filter(text -> text.contains("\"type\":\"trade\""))
                    .flatMap(this::processMessage);

            return sendSubscriptions.thenMany(receive).then();
        });
    }

    private Mono<Void> processMessage(String text) {
        try {
            FinnhubMessage message = objectMapper.readValue(text, FinnhubMessage.class);
            if (message.data() == null) return Mono.empty();

            for (FinnhubMessage.Trade trade : message.data()) {
                StockQuote quote = new StockQuote(
                        trade.symbol(),
                        trade.price(),
                        trade.volume(),
                        Instant.ofEpochMilli(trade.timestamp()),
                        "finnhub"
                );
                producer.send(quote);
                log.info("Tick  {}  price={}  vol={}", quote.symbol(), quote.price(), quote.volume());
            }
        } catch (Exception e) {
            log.warn("Failed to parse Finnhub message: {}", text, e);
        }
        return Mono.empty();
    }
}
