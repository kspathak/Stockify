# Stockify — Code Reference (Phase 1)

> Phase 1 covers real-time price ingestion from the Finnhub WebSocket API into a Kafka topic.
> This document describes every class: its responsibility, inputs, outputs, and the design
> principles it applies.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Package Structure](#2-package-structure)
3. [Class Reference](#3-class-reference)
   - [StockifyApplication](#31-stockifyapplication)
   - [FinnhubProperties](#32-finnhubproperties)
   - [KafkaConfig](#33-kafkaconfig)
   - [StockQuoteSerializer](#34-stockquoteserializer)
   - [StockQuote](#35-stockquote)
   - [FinnhubMessage](#36-finnhubmessage)
   - [FinnhubWebSocketClient](#37-finnhubwebsocketclient)
   - [StockQuoteProducer](#38-stockquoteproducer)
   - [StockifyApplicationTests](#39-stockifyapplicationtests)
4. [Data Flow Diagram](#4-data-flow-diagram)
5. [Dependency Graph](#5-dependency-graph)
6. [Design Principles Summary](#6-design-principles-summary)

---

## 1. System Overview

```
Finnhub WebSocket API
        │
        │  WSS (JSON trade events)
        ▼
FinnhubWebSocketClient          ← connects, subscribes, receives
        │
        │  FinnhubMessage (raw JSON → Java record)
        ▼
   processMessage()             ← maps Trade → StockQuote
        │
        │  StockQuote domain record
        ▼
  StockQuoteProducer            ← wraps KafkaTemplate
        │
        │  Kafka topic: stock.prices.raw
        ▼
  [downstream consumers]        ← Phase 2: TimescaleDB writer
                                   Phase 3: Sentiment enricher
                                   Phase 4: Prediction engine
```

---

## 2. Package Structure

```
com.example.stockify
│
├── StockifyApplication.java          Entry point, context bootstrap
│
├── config/
│   ├── FinnhubProperties.java        Typed configuration binding
│   ├── KafkaConfig.java              Kafka infrastructure beans
│   └── StockQuoteSerializer.java     Kafka value serializer
│
├── domain/
│   └── StockQuote.java               Core domain record (immutable)
│
└── ingestion/
    ├── FinnhubMessage.java           Finnhub WebSocket message shape
    ├── FinnhubWebSocketClient.java   Reactive WS connection + lifecycle
    └── StockQuoteProducer.java       Kafka producer wrapper
```

---

## 3. Class Reference

---

### 3.1 `StockifyApplication`

**File:** `src/main/java/com/example/stockify/StockifyApplication.java`

**Responsibility:** Application entry point. Bootstraps the Spring context and activates automatic
scanning of `@ConfigurationProperties` classes.

#### Input
| Source | What |
|--------|------|
| JVM args (`args[]`) | Standard Spring Boot command-line arguments (e.g. `--server.port=8081`) |
| `application.properties` | All environment configuration loaded at startup |

#### Output
| What | Where |
|------|-------|
| Running Spring Application Context | In-process; all beans instantiated and lifecycle started |

#### Annotations
| Annotation | Purpose |
|------------|---------|
| `@SpringBootApplication` | Combines `@Configuration`, `@EnableAutoConfiguration`, `@ComponentScan` — single-annotation bootstrap |
| `@ConfigurationPropertiesScan` | Tells Spring to discover all `@ConfigurationProperties` records/classes in the package tree without needing `@EnableConfigurationProperties` on each individually |

#### Design Principles

**Convention over Configuration** — `@SpringBootApplication` eliminates hundreds of lines of
explicit XML or Java configuration by relying on classpath conventions and auto-configuration.

**Single Responsibility** — The entry point does exactly one thing: hand control to Spring Boot.
No business logic lives here.

---

### 3.2 `FinnhubProperties`

**File:** `src/main/java/com/example/stockify/config/FinnhubProperties.java`

**Responsibility:** A strongly-typed, immutable holder for all Finnhub-related configuration values.
Bridges `application.properties` keys (with prefix `stockify.finnhub`) to Java types.

#### Input
| Property Key | Java Type | Example Value |
|---|---|---|
| `stockify.finnhub.api-key` | `String` | `d1abc123xyz` |
| `stockify.finnhub.ws-url` | `String` | `wss://ws.finnhub.io` |
| `stockify.finnhub.watchlist` | `List<String>` | `AAPL,MSFT,NVDA,TSLA` |

#### Output
| What | Consumer |
|------|----------|
| Immutable config record injected as a Spring bean | `FinnhubWebSocketClient` |

#### Design Principles

**Immutability** — Declared as a Java `record`, all fields are `final`. Configuration cannot be
mutated after startup, preventing accidental runtime modification.

**Separation of Concerns** — Configuration is isolated in its own class rather than scattered as
`@Value` fields across business classes. A single change to the property prefix is all that is
needed to rename keys.

**Type Safety** — The `List<String>` watchlist is bound and validated at startup. A missing or
malformed key fails fast with a clear error rather than a `NullPointerException` at runtime.

```
application.properties
  stockify.finnhub.api-key=...        ─┐
  stockify.finnhub.ws-url=...          ├──► FinnhubProperties (record)
  stockify.finnhub.watchlist=...      ─┘         │
                                                  │ injected into
                                                  ▼
                                      FinnhubWebSocketClient
```

---

### 3.3 `KafkaConfig`

**File:** `src/main/java/com/example/stockify/config/KafkaConfig.java`

**Responsibility:** Declares all Kafka infrastructure beans — the producer factory (with
serialization configuration), the `KafkaTemplate` used to send messages, and the
`stock.prices.raw` topic definition that Spring Kafka's admin will auto-create on startup.

#### Input
| Source | What |
|--------|------|
| `@Value("${spring.kafka.bootstrap-servers}")` | Comma-separated list of Kafka broker addresses injected from properties |

#### Output (Beans produced)
| Bean | Type | Consumed By |
|------|------|-------------|
| `producerFactory` | `ProducerFactory<String, StockQuote>` | `kafkaTemplate` bean |
| `kafkaTemplate` | `KafkaTemplate<String, StockQuote>` | `StockQuoteProducer` |
| `stockPricesRawTopic` | `NewTopic` | Spring Kafka admin → Kafka broker |

#### Serialization Configuration
| Key | Serializer | Role |
|-----|-----------|------|
| Kafka message key | `StringSerializer` | Symbol string (e.g. `"AAPL"`) used as partition key |
| Kafka message value | `StockQuoteSerializer` | Custom Jackson 3 JSON serializer for `StockQuote` records |

#### Design Principles

**Factory Pattern** — `DefaultKafkaProducerFactory` encapsulates the construction logic for Kafka
producers. `KafkaConfig` acts as the factory for all Kafka-related beans; callers receive
ready-to-use objects without knowing how they were assembled.

**Dependency Injection** — `KafkaTemplate` is injected into `StockQuoteProducer` via constructor
injection. `KafkaConfig` wires these dependencies; no class creates its own Kafka infrastructure.

**Open/Closed Principle** — The serializer is configured by class reference
(`StockQuoteSerializer.class`). To change serialization (e.g. swap to Avro), only `KafkaConfig`
needs updating — `StockQuoteProducer` and `FinnhubWebSocketClient` are unaffected.

```
KafkaConfig (@Configuration)
  │
  ├─ producerFactory()  ──────────────────────────► ProducerFactory<String, StockQuote>
  │     └─ StockQuoteSerializer.class (value)              │
  │     └─ StringSerializer.class (key)                    │
  │                                                         ▼
  ├─ kafkaTemplate()  ◄─ producerFactory  ────────► KafkaTemplate<String, StockQuote>
  │                                                         │
  │                                               injected into StockQuoteProducer
  │
  └─ stockPricesRawTopic() ─────────────────────► NewTopic "stock.prices.raw"
                                                   (auto-created by Spring Kafka Admin)
```

---

### 3.4 `StockQuoteSerializer`

**File:** `src/main/java/com/example/stockify/config/StockQuoteSerializer.java`

**Responsibility:** Converts a `StockQuote` Java record into a UTF-8 JSON `byte[]` so Kafka can
transmit it over the wire. Implements Kafka's `Serializer<T>` interface.

#### Input
| Parameter | Type | Description |
|-----------|------|-------------|
| `topic` | `String` | Kafka topic name (available but not used — serialization is topic-agnostic) |
| `data` | `StockQuote` | The domain record to serialise. `null` is handled gracefully. |

#### Output
| Type | Description |
|------|-------------|
| `byte[]` | UTF-8 JSON representation of the `StockQuote`, e.g. `{"symbol":"AAPL","price":189.5,...}` |
| `null` | Returned when `data` is `null` (Kafka convention for tombstone messages) |

#### Example JSON produced
```json
{
  "symbol": "AAPL",
  "price": 189.53,
  "volume": 100.0,
  "timestamp": "2026-06-01T14:32:11.123Z",
  "source": "finnhub"
}
```

#### Design Principles

**Single Responsibility** — This class does one thing: serialise a `StockQuote` to bytes.
No Kafka producer logic, no domain logic.

**Interface Segregation** — Implements only `Serializer<StockQuote>` (one method: `serialize`).
The unused `configure()` and `close()` lifecycle methods from the interface carry default no-op
implementations and are intentionally not overridden.

**Defensive Programming** — Explicit `null` check before serialisation prevents a
`NullPointerException` from propagating into the Kafka producer pipeline.

**Jackson 3 compatibility note** — Spring Boot 4 ships Jackson 3.x (`tools.jackson`), which
changed the top-level package from `com.fasterxml.jackson`. Java Time (`Instant`) support is
built-in to Jackson 3 and does not require registering `JavaTimeModule`.

---

### 3.5 `StockQuote`

**File:** `src/main/java/com/example/stockify/domain/StockQuote.java`

**Responsibility:** The canonical domain object representing a single real-time price tick.
It is the unit of data that flows from ingestion through to storage, enrichment, and prediction
in later phases.

#### Fields
| Field | Type | Description |
|-------|------|-------------|
| `symbol` | `String` | Exchange ticker, e.g. `"AAPL"`, `"NVDA"` |
| `price` | `double` | Last traded price |
| `volume` | `double` | Volume of the trade tick |
| `timestamp` | `Instant` | UTC instant of the tick (derived from Finnhub epoch-milliseconds) |
| `source` | `String` | Origin of the data, e.g. `"finnhub"` — allows multi-provider support later |

#### Input
Created by `FinnhubWebSocketClient.processMessage()` from a `FinnhubMessage.Trade`.

#### Output
Consumed by `StockQuoteProducer` (→ Kafka), and in future phases by the TimescaleDB writer and the
prediction feature pipeline.

#### Design Principles

**Value Object (Domain-Driven Design)** — `StockQuote` has no identity of its own; two quotes
with identical field values are equal. Using a Java `record` gives structural equality (`equals`,
`hashCode`, `toString`) automatically.

**Immutability** — All fields are `final` (enforced by `record`). Once created, a quote cannot
be altered. This makes it safe to pass across thread boundaries (reactive pipeline → Kafka producer)
without defensive copying.

**Ubiquitous Language** — Field names (`symbol`, `price`, `volume`, `timestamp`, `source`) match
the language used by both the market data domain and downstream consumers such as the ML feature
pipeline.

```
FinnhubMessage.Trade          StockQuote (domain record)
  p (double)          ──►  price     (double)
  s (String)          ──►  symbol    (String)
  t (long, epoch ms)  ──►  timestamp (Instant)
  v (double)          ──►  volume    (double)
  [implicit]          ──►  source    ("finnhub")
```

---

### 3.6 `FinnhubMessage`

**File:** `src/main/java/com/example/stockify/ingestion/FinnhubMessage.java`

**Responsibility:** Models the raw JSON payload that Finnhub sends over the WebSocket. Acts as
a pure deserialization target — it maps the Finnhub wire format (single-letter field names) into
readable Java field names.

#### Finnhub wire format (input)
```json
{
  "type": "trade",
  "data": [
    { "p": 189.53, "s": "AAPL", "t": 1688995215000, "v": 100.0 }
  ]
}
```
Non-trade messages (e.g. `{"type":"ping"}`) have no `data` field; `data` will be `null`.

#### Record structure
```
FinnhubMessage
  ├── type   : String          "trade" | "ping" | "error"
  └── data   : List<Trade>     null when type != "trade"
        └── Trade (nested record)
              ├── price     : double   ← @JsonProperty("p")
              ├── symbol    : String   ← @JsonProperty("s")
              ├── timestamp : long     ← @JsonProperty("t")  (epoch ms)
              └── volume    : double   ← @JsonProperty("v")
```

#### Output
Consumed exclusively by `FinnhubWebSocketClient.processMessage()`, which reads `data` and maps
each `Trade` into a `StockQuote`.

#### Design Principles

**Data Transfer Object (DTO) Pattern** — `FinnhubMessage` is a pure data holder with no behaviour.
It exists only to bridge the external Finnhub JSON format to the internal Java type system.

**Anti-Corruption Layer** — The `@JsonProperty` annotations on `Trade` translate cryptic
single-letter Finnhub field names (`p`, `s`, `t`, `v`) into meaningful names (`price`, `symbol`,
`timestamp`, `volume`). Downstream code never sees or depends on the Finnhub wire format.

**Defensive Deserialization** — `@JsonIgnoreProperties(ignoreUnknown = true)` on both records
ensures that new fields added by Finnhub in future API versions do not break deserialization.

---

### 3.7 `FinnhubWebSocketClient`

**File:** `src/main/java/com/example/stockify/ingestion/FinnhubWebSocketClient.java`

**Responsibility:** The core ingestion engine. Manages the lifecycle of a persistent WebSocket
connection to Finnhub, subscribes to the configured watchlist, continuously receives trade events,
parses them, and forwards each tick to Kafka via `StockQuoteProducer`. Reconnects automatically
on any disconnection.

#### Input
| Source | What |
|--------|------|
| `FinnhubProperties` | WS URL, API key, watchlist (injected via constructor) |
| `FinnhubWebSocket stream` | Real-time JSON trade events and ping frames from Finnhub |

#### Output
| What | Where |
|------|-------|
| `StockQuote` records | Forwarded to `StockQuoteProducer` (and from there to Kafka) |
| Log lines | `INFO` on connect/subscribe; `WARN` on reconnect; `INFO` on each tick |

#### Lifecycle (`SmartLifecycle`)
```
Spring context starts
       │
       ▼
   start()  ─────────────────────────────────────────────────────►  connection Disposable
       │                                                              (background, non-blocking)
       │     connect() ──► ReactorNettyWebSocketClient.execute()
       │           │
       │           ├─ session.send(subscriptions)    Subscribe to each watchlist symbol
       │           └─ session.receive()              Infinite reactive stream of ticks
       │                     │
       │                     ├─ filter: TEXT frames only
       │                     ├─ filter: "type":"trade" messages only
       │                     └─ flatMap: processMessage()
       │                                     │
       │                                     └─ parse JSON → StockQuote → producer.send()
       │
       │     On any error/disconnect:
       │           retryWhen(fixedDelay 5s) ─────────► reconnect automatically
       │
Spring context stops
       │
       ▼
   stop()   ─────────────────────────────────────────────────────►  connection.dispose()
```

#### Constructor Dependencies (Injected)
| Dependency | Role |
|------------|------|
| `FinnhubProperties` | Supplies API key, WS URL, and watchlist |
| `StockQuoteProducer` | Receives mapped `StockQuote` for Kafka publishing |
| `ObjectMapper` | Spring Boot auto-configured Jackson 3 mapper for JSON parsing |

#### Design Principles

**Reactive Programming (Project Reactor)** — The entire data pipeline from WebSocket receive to
Kafka send is expressed as a non-blocking `Flux` chain. No threads are blocked waiting for network
I/O; the Netty event loop handles all I/O, freeing JVM threads for other work.

**Dependency Injection** — All collaborators (`FinnhubProperties`, `StockQuoteProducer`,
`ObjectMapper`) are injected through the constructor. The class declares what it needs; Spring
resolves and provides them. This makes the class testable in isolation (swap any collaborator with
a mock).

**Lifecycle Management (`SmartLifecycle`)** — Implements `start()`, `stop()`, and `isRunning()`
so Spring controls the connection lifetime. The WS client starts when the context is ready and
stops cleanly when the application shuts down — no resource leaks.

**Resilience / Retry Pattern** — `Retry.fixedDelay(Long.MAX_VALUE, 5s)` wraps the entire
connection `Mono`. Any disconnection, network error, or Finnhub downtime triggers automatic
reconnection after a 5-second backoff, indefinitely. The calling code is unaware of failures.

**Single Responsibility** — `FinnhubWebSocketClient` is responsible only for the connection and
the mapping of raw messages to `StockQuote`. It delegates Kafka publishing entirely to
`StockQuoteProducer`.

**Anti-Corruption Layer** — The `processMessage()` method is the seam between the external Finnhub
format and the internal domain. `StockQuote` (the domain object) never leaks Finnhub-specific
concepts like epoch-millisecond timestamps or single-letter field names.

```
┌─────────────────────────────── FinnhubWebSocketClient ──────────────────────────────┐
│                                                                                      │
│  SmartLifecycle                                                                      │
│  ┌─────────┐   start()                                                              │
│  │ Spring  │ ──────────► connect() ──► ReactorNettyWebSocketClient                  │
│  │ Context │             [Mono<Void>]         │                                     │
│  │         │   stop()         │       WebSocket session                             │
│  │         │ ──────────► dispose()    │                                             │
│  └─────────┘                          ├─ send(subscribe msgs)                       │
│                                        └─ receive() [Flux<WebSocketMessage>]        │
│                                              │                                      │
│                                         filter & parse                              │
│                                              │                                      │
│                                         FinnhubMessage                              │
│                                              │                                      │
│                                         map to StockQuote                           │
│                                              │                                      │
│                                         StockQuoteProducer.send()                   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
          ▲                                                        │
          │  injects                                               │  delegates to
          │                                                        ▼
   FinnhubProperties                                       StockQuoteProducer
```

---

### 3.8 `StockQuoteProducer`

**File:** `src/main/java/com/example/stockify/ingestion/StockQuoteProducer.java`

**Responsibility:** A thin, focused wrapper around `KafkaTemplate` that publishes `StockQuote`
records to the `stock.prices.raw` Kafka topic. Keeps Kafka publishing mechanics (topic name,
key strategy, async callback) out of the ingestion logic.

#### Input
| Parameter | Type | Description |
|-----------|------|-------------|
| `quote` | `StockQuote` | A populated domain record to publish |

#### Output
| What | Where | Notes |
|------|-------|-------|
| Kafka message | Topic `stock.prices.raw` | Key = `quote.symbol()` (enables per-symbol partition affinity) |
| Log `DEBUG` | Console / log sink | On successful delivery |
| Log `ERROR` | Console / log sink | On broker-reported failure |

#### Key strategy
The Kafka message key is set to `quote.symbol()` (e.g. `"AAPL"`). This ensures all ticks for the
same symbol land in the same Kafka partition, preserving arrival order per symbol for downstream
consumers.

#### Design Principles

**Dependency Injection** — `KafkaTemplate<String, StockQuote>` is injected via the constructor.
`StockQuoteProducer` has no knowledge of how the template was built or what broker it connects to.

**Single Responsibility** — The sole concern of this class is: "given a `StockQuote`, put it
on the right Kafka topic with the right key." Configuration, serialization, and retry are
delegated to the Kafka infrastructure layer.

**Asynchronous / Non-blocking** — `kafkaTemplate.send()` returns a `CompletableFuture`.
`whenComplete()` handles success and failure callbacks asynchronously — the calling thread
(Reactor event loop) is never blocked waiting for broker acknowledgment.

**Facade Pattern** — `StockQuoteProducer` acts as a simplified facade over `KafkaTemplate`.
Callers invoke `producer.send(quote)` and know nothing about Kafka topics, key strategies, or
async futures. If the Kafka topic name changes, only this class needs updating.

```
FinnhubWebSocketClient
        │
        │  producer.send(StockQuote)
        ▼
  StockQuoteProducer
        │
        │  kafkaTemplate.send("stock.prices.raw", symbol, quote)
        ▼
  KafkaTemplate<String, StockQuote>
        │
        │  StockQuoteSerializer ──► byte[] JSON
        ▼
  Kafka broker  ──►  topic: stock.prices.raw
                        partition 0  ──  AAPL, JPM, ...
                        partition 1  ──  MSFT, AMZN, ...
                        partition 2  ──  NVDA, TSLA, ...
```

---

### 3.9 `StockifyApplicationTests`

**File:** `src/test/java/com/example/stockify/StockifyApplicationTests.java`

**Responsibility:** Spring Boot context integration test. Verifies that the full application
context loads successfully — all beans wire up, all configuration is valid, and no startup errors
occur — without making any real external connections.

#### What it tests
- All `@Bean` definitions in `KafkaConfig`, `FinnhubProperties`, and `StockQuoteProducer` are
  valid and injectable.
- `application.properties` (and `src/test/resources/application.properties` overrides) are
  correctly bound to `FinnhubProperties`.
- The Spring context lifecycle completes without errors.

#### Test infrastructure
| Annotation / Tool | Purpose |
|---|---|
| `@SpringBootTest` | Loads the full application context (all beans, auto-configuration) |
| `@EmbeddedKafka` | Spins up an in-process Kafka broker for the test. Injects broker address via `${spring.embedded.kafka.brokers}`, overriding `localhost:9092` |
| `@MockitoBean FinnhubWebSocketClient` | Replaces the real WS client bean with a Mockito mock so no WebSocket connection to Finnhub is attempted. Spring Boot 4 replacement for the removed `@MockBean` |
| `src/test/resources/application.properties` | Overrides `api-key`, `ws-url`, and `watchlist` with test values; points Kafka to the embedded broker |

#### Design Principles

**Test Isolation** — `@MockitoBean` ensures the test does not depend on an external Finnhub
WebSocket endpoint. The test is hermetic: it passes whether or not the network is available.

**Test Doubles (Mock)** — `FinnhubWebSocketClient` is a `SmartLifecycle` bean that attempts a
real WS connection on `start()`. Replacing it with a mock prevents side effects during context
startup while still verifying that the rest of the context wires correctly around it.

**Fail-Fast Validation** — A context-loads test catches mis-configured beans, missing required
properties, and circular dependencies immediately, before any logic is tested — acting as a
canary for the entire configuration layer.

---

## 4. Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  EXTERNAL                                                                    │
│                                                                              │
│  Finnhub WebSocket (wss://ws.finnhub.io?token=...)                          │
│  {"type":"trade","data":[{"p":189.53,"s":"AAPL","t":...,"v":100}]}          │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │  WSS text frame
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                             │
│                                                                              │
│  FinnhubWebSocketClient                                                      │
│    │                                                                         │
│    ├─ filter: TEXT frames only                                               │
│    ├─ filter: contains "type":"trade"                                        │
│    ├─ ObjectMapper.readValue(text, FinnhubMessage.class)                     │
│    │       └── FinnhubMessage { type="trade", data=[Trade{p,s,t,v}] }       │
│    │                                                                         │
│    └─ for each Trade:                                                        │
│           new StockQuote(symbol, price, volume, Instant, "finnhub")         │
│                                 │                                           │
│                                 ▼                                           │
│                       StockQuoteProducer.send(quote)                        │
│                                 │                                           │
│                                 ▼                                           │
│                       KafkaTemplate.send(topic, key, quote)                 │
│                                 │                                           │
│                       StockQuoteSerializer                                  │
│                         writeValueAsBytes(quote) → byte[]                  │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │  Kafka produce (async)
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  KAFKA                                                                       │
│                                                                              │
│  Topic: stock.prices.raw  (3 partitions, key = symbol)                       │
│                                                                              │
│    partition 0: AAPL, JPM, BAC, ...                                          │
│    partition 1: MSFT, AMZN, GOOGL, ...                                       │
│    partition 2: NVDA, TSLA, META, ...                                        │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
         Phase 2         Phase 3          Phase 4
      TimescaleDB      Sentiment        Prediction
        Writer         Enricher          Engine
     (persistence)  (news + social)    (LightGBM)
```

---

## 5. Dependency Graph

```
StockifyApplication
  └─ @ConfigurationPropertiesScan ──► FinnhubProperties

FinnhubWebSocketClient (@Component, SmartLifecycle)
  ├─ FinnhubProperties          (config binding)
  ├─ StockQuoteProducer         (kafka publishing)
  └─ ObjectMapper               (auto-configured by Spring Boot)

StockQuoteProducer (@Component)
  └─ KafkaTemplate<String, StockQuote>    (from KafkaConfig)

KafkaConfig (@Configuration)
  ├─ ProducerFactory<String, StockQuote>
  │     └─ StockQuoteSerializer           (value serializer)
  ├─ KafkaTemplate<String, StockQuote>
  │     └─ ProducerFactory                (above)
  └─ NewTopic "stock.prices.raw"

StockQuote          (domain record — no dependencies)
FinnhubMessage      (DTO record — no Spring dependencies)
StockQuoteSerializer (Kafka Serializer — no Spring dependencies)
FinnhubProperties   (config record — no runtime dependencies)
```

---

## 6. Design Principles Summary

| Principle | Where Applied |
|---|---|
| **Dependency Injection (Constructor)** | `FinnhubWebSocketClient`, `StockQuoteProducer` — all collaborators injected, never `new`-ed |
| **Single Responsibility** | Every class has one reason to change: `StockQuoteSerializer` serializes, `StockQuoteProducer` publishes, `FinnhubWebSocketClient` connects and maps |
| **Immutability / Value Objects** | `StockQuote`, `FinnhubProperties`, `FinnhubMessage` are Java `record`s — structurally equal, thread-safe, no mutation |
| **Factory Pattern** | `KafkaConfig` acts as a factory for all Kafka beans via `@Bean` methods |
| **Facade Pattern** | `StockQuoteProducer` hides Kafka API complexity behind a single `send(StockQuote)` method |
| **Anti-Corruption Layer** | `FinnhubMessage` isolates the Finnhub wire format; `processMessage()` is the translation seam |
| **Reactive / Non-blocking** | `FinnhubWebSocketClient` uses Project Reactor (`Flux`, `Mono`) — no threads blocked on I/O |
| **Retry / Resilience** | `Retry.fixedDelay` in `FinnhubWebSocketClient` ensures automatic reconnection on any failure |
| **Lifecycle Management** | `SmartLifecycle` on `FinnhubWebSocketClient` ties WS connection lifetime to Spring context |
| **Convention over Configuration** | `@SpringBootApplication` + `@ConfigurationPropertiesScan` eliminate boilerplate setup |
| **Defensive Programming** | `null` check in `StockQuoteSerializer`; `@JsonIgnoreProperties` in `FinnhubMessage` |
| **Test Isolation** | `@MockitoBean` + `@EmbeddedKafka` make tests hermetic — no external dependencies |
