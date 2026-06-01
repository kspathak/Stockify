# Stockify — Project Plan

## Overview

A real-time stock monitoring platform that ingests live price data, stores it for historical analysis, enriches it with social media and news sentiment, and produces short-horizon price predictions.

---

## Architecture at a Glance

```
[Data Sources]         [Ingestion Layer]      [Storage]          [Analysis / ML]
  Stock APIs      ──▶   Spring WebFlux   ──▶   TimescaleDB   ──▶  Sentiment Engine
  News APIs       ──▶   Kafka Topics     ──▶   Redis Cache   ──▶  Prediction Model
  Social Media    ──▶   Spring Batch     ──▶   (raw archive)  ──▶  REST/WS API out
```

The existing `build.gradle` already includes WebFlux, Kafka, Spring Batch, REST Client, Actuator, and OpenTelemetry — the foundation is in place.

---

## Phase 1 — Real-Time Price Ingestion

**Goal:** Pull live quotes and push them onto a Kafka topic.

### Data Sources (pick one to start)
| Provider       | Free Tier       | WebSocket? | Notes                            |
|----------------|-----------------|------------|----------------------------------|
| Polygon.io     | 15-min delay    | Yes        | Best WS support, generous free   |
| Alpha Vantage  | 25 req/day      | No         | Simple REST, good for prototyping|
| Yahoo Finance  | Unofficial      | No         | Fragile, no SLA                  |
| Finnhub        | 60 req/min      | Yes        | Good free WS for real-time       |

**Recommended starting point:** Finnhub (free WebSocket) or Polygon.io.

### Components to Build
1. `StockWebSocketClient` — WebFlux `WebClient` / reactive WS client connecting to provider.
2. `PriceIngestorService` — maps raw JSON to a `StockQuote` domain object, publishes to Kafka topic `stock.prices.raw`.
3. `StockQuote` record — `(symbol, price, volume, timestamp)`.

### Kafka Topics
```
stock.prices.raw       # raw tick data from provider
stock.prices.enriched  # after sentiment score is attached
stock.predictions      # model output
```

---

## Phase 2 — Storage

**Decision needed:** time-series vs. general-purpose DB.

### Options

| Option                  | Pros                                    | Cons                              |
|-------------------------|-----------------------------------------|-----------------------------------|
| **TimescaleDB**         | SQL interface, hypertables auto-partition by time, free | Requires PostgreSQL setup    |
| **InfluxDB**            | Purpose-built for time-series, good query language | Different query paradigm    |
| **PostgreSQL (plain)**  | Familiar, Spring JDBC already wired     | Manual partitioning needed        |
| **MongoDB (time-series coll.)** | Flexible schema for news/social | Less efficient for pure OHLCV |
| **Apache Cassandra**    | High write throughput                   | Operationally heavy               |

**Recommendation:** Start with **TimescaleDB** (PostgreSQL extension). It gives you familiar SQL, Spring JDBC works out of the box, and the `hypertable` handles time-partitioning automatically. Add **Redis** as a hot cache for the latest quote per symbol.

### Schema (TimescaleDB)
```sql
-- core price table (becomes a hypertable on `time`)
CREATE TABLE stock_prices (
  time        TIMESTAMPTZ NOT NULL,
  symbol      TEXT        NOT NULL,
  price       NUMERIC(12,4),
  volume      BIGINT,
  source      TEXT
);
SELECT create_hypertable('stock_prices', 'time');

-- sentiment signals
CREATE TABLE sentiment_signals (
  time        TIMESTAMPTZ NOT NULL,
  symbol      TEXT,
  source      TEXT,   -- 'news' | 'reddit' | 'twitter'
  score       FLOAT,
  raw_text    TEXT
);
```

### Spring Batch Integration
- Use the existing Spring Batch setup for **daily OHLCV backfill** from a provider's historical API.
- One job per symbol range; uses `JdbcBatchItemWriter` for bulk inserts into TimescaleDB.

---

## Phase 3 — Sentiment Ingestion (News + Social Media)

**Goal:** Produce a numeric sentiment score per symbol per time window.

### News
| Source           | API              | Free?  |
|------------------|------------------|--------|
| NewsAPI.org      | REST             | Yes (100 req/day dev) |
| Finnhub News     | REST             | Yes (bundled with price API) |
| RSS feeds        | No API needed    | Always free |

### Social Media
| Source     | API                     | Free?  | Notes                        |
|------------|-------------------------|--------|------------------------------|
| Reddit     | Reddit API / `r/stocks` `r/wallstreetbets` | Yes | Official API, OAuth required |
| Twitter/X  | X API v2                | Limited free tier | Basic tier: 1M tweet reads/mo |
| StockTwits | REST API                | Yes    | Finance-specific, easy       |

**Recommendation:** Start with **Finnhub News** (already in your API plan) + **StockTwits** (no auth friction) + **Reddit** API for `r/stocks`.

### Sentiment Analysis Options
1. **Pre-built model via API** — OpenAI / Claude API: send headlines, get scores. Simple but has per-call cost.
2. **FinBERT** (Hugging Face) — finance-tuned BERT. Best accuracy, run as a sidecar Python service.
3. **VADER** — rule-based, runs in JVM via Jython or a tiny Python microservice. Good baseline, zero cost.

**Recommended approach:** Start with **VADER** via a small Python FastAPI sidecar, call it from Spring via `WebClient`. Swap to FinBERT when accuracy matters.

### Components to Build
1. `NewsPollingJob` — Spring Batch job that polls news APIs every 15 min, writes raw articles to DB.
2. `SocialMediaStreamService` — WebFlux polling for StockTwits / Reddit, publishes to `stock.sentiment.raw` Kafka topic.
3. `SentimentEnricherProcessor` — Kafka Streams processor: reads raw sentiment, calls scoring sidecar, writes scored signal to `stock.prices.enriched`.

---

## Phase 4 — Prediction Engine

**Goal:** Generate a short-horizon (next 1h / EOD) directional signal per symbol.

### Feature Inputs
- Rolling OHLCV (1m, 5m, 1h candles)
- Sentiment score moving average (15m, 1h window)
- Volume anomaly score
- Momentum indicators (RSI, MACD) computed via Kafka Streams or a Batch job

### Model Options

| Approach                  | Complexity | Notes                                      |
|---------------------------|------------|--------------------------------------------|
| Linear regression baseline| Low        | Good sanity check, interpretable           |
| ARIMA / SARIMA            | Medium     | Classic time-series, no ML infra needed    |
| XGBoost / LightGBM        | Medium     | Strong on tabular features, fast to train  |
| LSTM / Transformer        | High       | Best for sequential patterns, needs GPU    |

**Recommended path:**
1. Start with a **linear regression baseline** (implemented in Python, served via FastAPI).
2. Graduate to **LightGBM** once features are validated.
3. Consider an LSTM only after the data pipeline is stable (need months of history).

### Training Pipeline
- Spring Batch job exports feature vectors to a flat file / object store (S3 / local) nightly.
- Python training script (separate repo or `ml/` subdirectory) reads features, trains, exports model artifact.
- Prediction sidecar loads the artifact and serves a `/predict` endpoint consumed by Spring.

### Output
- Kafka topic `stock.predictions` → consumed by the API layer.
- Store predictions in TimescaleDB for backtesting / accuracy tracking.

---

## Phase 5 — API & Observability

### REST / WebSocket API (Spring WebMVC + WebFlux)
```
GET  /api/quotes/{symbol}          # latest price from Redis
GET  /api/quotes/{symbol}/history  # time-range query from TimescaleDB
GET  /api/sentiment/{symbol}       # latest sentiment scores
GET  /api/predictions/{symbol}     # latest model prediction
WS   /ws/quotes/{symbol}           # live price stream via WebSocket
```

### Observability (already wired)
- **OpenTelemetry** — traces across ingestion → Kafka → DB write
- **Micrometer + Prometheus** — custom metrics: `stockify.quote.ingestion.rate`, `stockify.sentiment.score`
- **Datadog** micrometer registry already in `build.gradle` — point to a Datadog agent or use the free tier
- **Spring Actuator** `/health`, `/metrics`, `/info` endpoints

---

## Suggested Build Order

```
Week 1-2   Phase 1  Price ingestion → Kafka → log to console, confirm data flows
Week 3     Phase 2  TimescaleDB setup, Kafka consumer writes prices to DB
Week 4     Phase 3  News + StockTwits polling, VADER sidecar, sentiment in DB
Week 5-6   Phase 4  Feature engineering, baseline model, /predict endpoint
Week 7+    Phase 5  Public API, WebSocket feed, dashboards
```

---

## Open Decisions

- [ ] **Storage**: TimescaleDB vs InfluxDB vs plain PostgreSQL
- [ ] **Social media**: StockTwits only vs. add Reddit / Twitter
- [ ] **Sentiment model**: VADER (fast/free) vs FinBERT (accurate/heavier)
- [ ] **Hosting**: local/Docker Compose vs. AWS (RDS + MSK + ECS) vs. Railway/Render
- [ ] **Symbols to track**: start with a fixed watchlist (e.g. S&P 500 top 50) or user-defined

---

## Tech Stack Summary

| Layer            | Technology                               |
|------------------|------------------------------------------|
| Backend          | Spring Boot 4, Java 25                   |
| Reactive streams | Spring WebFlux                           |
| Messaging        | Apache Kafka + Kafka Streams             |
| Batch            | Spring Batch                             |
| Primary DB       | TimescaleDB (PostgreSQL extension)       |
| Cache            | Redis                                    |
| ML sidecar       | Python + FastAPI + VADER / FinBERT       |
| Observability    | OpenTelemetry + Micrometer + Prometheus  |
| Deployment       | Docker Compose (local) → TBD (cloud)     |
