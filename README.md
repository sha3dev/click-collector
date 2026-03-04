# @sha3/click-collector

Real-time collector that stores crypto and Polymarket ticks in ClickHouse, then exposes a TypeScript query API for model training datasets.

## TL;DR

```bash
npm install
npm run check
CLICKHOUSE_HOST=http://localhost:8123 npm run start
```

Then consume from code:

```ts
import { MarketEventsQueryService } from "@sha3/click-collector";

const query = MarketEventsQueryService.createDefault();
const slugs = await query.listMarkets("5m", "btc");
const events = await query.getMarketEvents(slugs[0]);
```

## What It Does

- Ingests real-time ticks from `@sha3/crypto`:
  - exchanges: `binance`, `coinbase`, `kraken`, `okx`
  - `chainlink`
- Ingests real-time ticks from `@sha3/polymarket`:
  - token price changes (`up`/`down`)
  - token orderbook changes (`up`/`down`)
  - market discovery for `5m` and `15m` windows
- Persists normalized events in ClickHouse.
- Lets you query:
  - all stored slugs by (`window`, `asset`)
  - all events related to one slug.

## Why It Exists

Model training pipelines need deterministic historical arrays of heterogeneous events (exchange, oracle, Polymarket). This service centralizes ingestion + normalization + retrieval.

## Compatibility

- Node.js 20+
- ESM (`"type": "module"`)
- TypeScript strict mode
- ClickHouse reachable from runtime

## Data Model (ClickHouse)

Query-shape assumption used in physical design:

- reads are single-asset (`btc` or `eth` or `sol` or `xrp`)
- reads are single-market per request (one `slug` at a time)

### `market_registry`

Catalog of Polymarket markets.

Important columns:

- `slug`
- `asset` (`btc|eth|sol|xrp`)
- `window` (`5m|15m`)
- `market_start_ts`, `market_end_ts`
- `up_asset_id`, `down_asset_id`
- `is_test` (`0|1`, marks records inserted by tests)

Engine:

- `ReplacingMergeTree(updated_at)`
- `PARTITION BY asset`
- `ORDER BY (slug)`

### `ticks`

Unified event storage.

Important columns:

- `event_id`
- `event_ts`
- `source_category` (`exchange|chainlink|polymarket`)
- `source_name`
- `event_type` (`price|orderbook`)
- `asset`
- `window`
- `market_slug`
- `token_side` (`up|down`)
- `price`
- `orderbook` (JSON string with full `asks` + `bids` in one tick)
- `payload_json`
- `is_test` (`0|1`, marks records inserted by tests)

Engine:

- `MergeTree`
- `PARTITION BY (asset, toYYYYMM(event_ts))`
- `TTL event_ts + INTERVAL 90 DAY` (default; configurable)
- `ORDER BY (asset, event_ts, source_category, source_name, event_type, event_id)`

## Public API Reference

### `MarketEventsQueryService`

- `static create(options): MarketEventsQueryService`
- `static createDefault(): MarketEventsQueryService`
- `listMarkets(window, asset): Promise<string[]>`
- `getMarketEvents(slug): Promise<MarketEvent[]>`

`getMarketEvents(slug)` correlation rule:

- includes all Polymarket events with `market_slug = slug`
- includes exchange + chainlink events for same `asset` inside the market time range.

### Exported types

- `AssetSymbol = "btc" | "eth" | "sol" | "xrp"`
- `MarketWindow = "5m" | "15m"`
- `EventType = "price" | "orderbook"`
- `MarketEvent`
  - includes `isTest: boolean` (`true` when row comes from test data)

## Delete Test Data (ClickHouse)

If you need to clean only test rows, run:

```sql
ALTER TABLE default.market_registry DELETE WHERE is_test = 1;
ALTER TABLE default.ticks DELETE WHERE is_test = 1;
```

Check mutation status:

```sql
SELECT database, table, mutation_id, is_done, latest_failed_part, latest_fail_reason
FROM system.mutations
WHERE database = 'default' AND table IN ('market_registry', 'ticks')
ORDER BY create_time DESC;
```

Optional optimization after mutation completion:

```sql
OPTIMIZE TABLE default.market_registry FINAL;
OPTIMIZE TABLE default.ticks FINAL;
```

## Reset Database to Initial State

Stop the collector before reset to avoid new inserts during cleanup.

### Fast reset (keep table definitions)

```sql
TRUNCATE TABLE default.market_registry;
TRUNCATE TABLE default.ticks;
```

### Full reset (drop and recreate tables with current schema)

```sql
DROP TABLE IF EXISTS default.market_registry;
DROP TABLE IF EXISTS default.ticks;

CREATE TABLE default.market_registry (
  slug String,
  asset LowCardinality(String),
  window LowCardinality(String),
  market_start_ts DateTime64(3, 'UTC'),
  market_end_ts DateTime64(3, 'UTC'),
  up_asset_id String,
  down_asset_id String,
  created_at DateTime64(3, 'UTC'),
  updated_at DateTime64(3, 'UTC'),
  is_test UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY asset
ORDER BY (slug);

CREATE TABLE default.ticks (
  event_id String,
  event_ts DateTime64(3, 'UTC'),
  ingested_at DateTime64(3, 'UTC'),
  source_category LowCardinality(String),
  source_name LowCardinality(String),
  event_type LowCardinality(String),
  asset LowCardinality(String),
  window Nullable(String),
  market_slug Nullable(String),
  token_side Nullable(String),
  price Nullable(Float64),
  orderbook Nullable(String),
  payload_json String,
  is_test UInt8 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY (asset, toYYYYMM(event_ts))
TTL event_ts + INTERVAL 90 DAY
ORDER BY (asset, event_ts, source_category, source_name, event_type, event_id);
```

## Integration Guide (External Projects)

### 1) Install

```bash
npm install @sha3/click-collector
```

### 2) Run collector process

```bash
CLICKHOUSE_HOST=http://localhost:8123 node --import tsx src/index.ts
```

### 3) Query training data

```ts
import { MarketEventsQueryService } from "@sha3/click-collector";

const query = MarketEventsQueryService.createDefault();
const slugs = await query.listMarkets("5m", "btc");

for (const slug of slugs) {
  const events = await query.getMarketEvents(slug);
  console.log(slug, events.length);
}
```

## Configuration (`src/config.ts`)

All config is centralized in the default export `CONFIG`.

- `CLICKHOUSE_HOST`: ClickHouse URL
- `CLICKHOUSE_DATABASE`: database name
- `CLICKHOUSE_USER`: username
- `CLICKHOUSE_PASSWORD`: password
- `CLICKHOUSE_TICKS_TABLE`: ticks table name
- `CLICKHOUSE_MARKET_REGISTRY_TABLE`: market registry table name
- `INGEST_BATCH_SIZE`: buffer size before insert flush
- `INGEST_FLUSH_INTERVAL_MS`: periodic flush interval
- `INGEST_COALESCE_WINDOW_MS`: max write frequency per coalesce key (`source + type + asset + market context`); `0` disables coalesce
- `INGEST_COALESCE_CLEANUP_INTERVAL_MS`: periodic cleanup interval for coalesce in-memory keys
- `INGEST_COALESCE_KEY_TTL_MS`: inactivity TTL for coalesce keys in memory
- `ORDERBOOK_MAX_LEVELS`: max asks/bids levels persisted per orderbook tick (default `5`)
- `TICKS_TTL_DAYS`: retention window in days for `ticks` table (`0` disables TTL modification)
- `CLICKHOUSE_ASYNC_INSERT`: ClickHouse async insert mode (`1` enabled)
- `CLICKHOUSE_WAIT_FOR_ASYNC_INSERT`: wait behavior for async insert (`0` fire-and-forget, `1` wait)
- `POLYMARKET_DISCOVERY_INTERVAL_MS`: market discovery refresh interval
- `SUPPORTED_ASSETS`: tracked symbols (`btc|eth|sol|xrp`)
- `SUPPORTED_WINDOWS`: tracked windows (`5m|15m`)
- `CRYPTO_PROVIDERS`: tracked providers

Example:

```dotenv
CLICKHOUSE_HOST=http://localhost:8123
CLICKHOUSE_DATABASE=default
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_TICKS_TABLE=ticks
CLICKHOUSE_MARKET_REGISTRY_TABLE=market_registry
INGEST_BATCH_SIZE=200
INGEST_FLUSH_INTERVAL_MS=1000
INGEST_COALESCE_WINDOW_MS=250
INGEST_COALESCE_CLEANUP_INTERVAL_MS=60000
INGEST_COALESCE_KEY_TTL_MS=3600000
ORDERBOOK_MAX_LEVELS=5
TICKS_TTL_DAYS=90
CLICKHOUSE_ASYNC_INSERT=1
CLICKHOUSE_WAIT_FOR_ASYNC_INSERT=0
POLYMARKET_DISCOVERY_INTERVAL_MS=30000
```

## Scripts

- `npm run start`: starts autonomous collector runtime
- `npm run check`: lint + format check + typecheck + tests
- `npm run fix`: lint/prettier auto-fix
- `npm run test`: node test runner via `scripts/run-tests.mjs`

## Testing

Current suite covers:

- crypto event normalization (`price`, `orderbook`)
- polymarket market discovery + stream mapping
- query service behavior (`listMarkets`, `getMarketEvents`)
- `MarketNotFoundError` path

Run:

```bash
npm run test
```

## AI Usage

If you use coding assistants in this repo:

- treat `AGENTS.md` as blocking contract
- keep class-first design and constructor injection
- keep single-return policy and explicit braces
- keep feature-folder architecture
- always add/update tests for behavior changes
- run `npm run check` before finalizing
