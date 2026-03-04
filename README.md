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
- `PARTITION BY toYYYYMM(event_ts)`
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
