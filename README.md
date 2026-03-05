# @sha3/click-collector

Collects real-time crypto + Polymarket ticks into ClickHouse and exposes a TypeScript query API for training datasets.

## Quick Start (60s)

### 1) Install

```bash
npm install
```

### 2) Set minimum env

```bash
export CLICKHOUSE_HOST=http://localhost:8123
```

### 3) Start collector runtime

```bash
npm run start
```

### 4) Read data from code

```ts
import { MarketEventsQueryService } from "@sha3/click-collector";

const query = MarketEventsQueryService.createDefault();
const markets = await query.listMarkets("5m", "btc");

if (markets.length > 0) {
  const slug = markets[0].slug;
  const events = await query.getMarketEvents(slug);
  const snapshots = await query.getMarketSnapshots(slug);
  console.log({ slug, events: events.length, snapshots: snapshots.length });
}
```

## Why This Exists

Training pipelines need deterministic time-ordered arrays that merge heterogeneous sources:

- exchange ticks
- chainlink ticks
- Polymarket token ticks (`up`/`down`)

`@sha3/click-collector` centralizes ingestion, normalization, storage, and query semantics.

## Core Concepts

- `market_registry` stores market metadata (`slug`, asset, window, market bounds) and derived prices (`price_to_beat`, `final_price`).
- `ticks` stores normalized events from all sources.
- Reads are market-centric (`slug`) and return:
  - raw correlated events (`getMarketEvents`)
  - aggregated state points per event (`getMarketSnapshots`)

## Compatibility

- Node.js `20+`
- ESM package (`"type": "module"`)
- TypeScript strict mode
- ClickHouse must be reachable from runtime

## Public API

## `MarketEventsQueryService`

### Factory

- `static create(options): MarketEventsQueryService`
- `static createDefault(): MarketEventsQueryService`

### Methods

- `listMarkets(window, asset): Promise<MarketRecord[]>`
- `getMarketEvents(slug): Promise<MarketEvent[]>`
- `getMarketSnapshots(slug): Promise<MarketSnapshot[]>`

### Behavior Contract

### `listMarkets(window, asset)`

Returns market records from `market_registry` for the given pair.

### `getMarketEvents(slug)`

Returns all events correlated to that market:

- all `polymarket` events where `market_slug = slug`
- all `exchange` + `chainlink` events for the same market asset inside `[market_start_ts, market_end_ts]`

Sorted by:

- `event_ts ASC`
- `source_category ASC`
- `source_name ASC`
- `event_id ASC`

### `getMarketSnapshots(slug)`

Returns aggregated state over time with **same cardinality and order** as `getMarketEvents(slug)`.

Each result is a `MarketSnapshot`:

- `triggerEvent`: event that defines this state point
- `snapshotTs`: equals `triggerEvent.eventTs`
- `asset`: market asset (`btc|eth|sol|xrp`)
- `window`: market window (`5m|15m`)
- `marketStartTs`: market window start timestamp (ms)
- `marketEndTs`: market window end timestamp (ms)
- `priceToBeat`: opening price of the market window (`market_registry.price_to_beat`)
- `crypto`: latest known values for all assets (`btc|eth|sol|xrp`) and providers (`binance|coinbase|kraken|okx|chainlink`)
- `polymarket`: latest known values for both sides (`up`, `down`) of the requested market

Null semantics:

- if a slot has never received a matching event by that time, it is `null`

## Exported Types

- `AssetSymbol = "btc" | "eth" | "sol" | "xrp"`
- `MarketWindow = "5m" | "15m"`
- `EventType = "price" | "orderbook"`
- `MarketEvent`
- `MarketSnapshot`
- `MarketRecord`

### `MarketRecord` price semantics

- `priceToBeat`: opening price for that market window (fetched from Polymarket crypto price endpoint).
- `finalPrice`: equals `priceToBeat` of the immediate next market for the same (`asset`, `window`).
- enrichment is asynchronous and retried every `PRICE_TO_BEAT_POLL_INTERVAL_MS` when the endpoint still does not expose `openPrice`.

### `MarketEvent` shape

```ts
type MarketEvent = {
  eventId: string;
  eventTs: number;
  sourceCategory: "exchange" | "chainlink" | "polymarket";
  sourceName: string;
  eventType: "price" | "orderbook";
  asset: "btc" | "eth" | "sol" | "xrp";
  window: "5m" | "15m" | null;
  marketSlug: string | null;
  tokenSide: "up" | "down" | null;
  price: number | null;
  orderbook: string | null;
  payloadJson: string;
  isTest: boolean;
};
```

### `MarketSnapshot` shape (simplified)

```ts
type MarketSnapshot = {
  triggerEvent: MarketEvent;
  snapshotTs: number;
  asset: AssetSymbol;
  window: MarketWindow;
  marketStartTs: number;
  marketEndTs: number;
  priceToBeat: number | null;
  crypto: { btc: SnapshotAssetState; eth: SnapshotAssetState; sol: SnapshotAssetState; xrp: SnapshotAssetState };
  polymarket: { up: SnapshotEventState; down: SnapshotEventState };
};

type SnapshotAssetState = {
  binance: SnapshotEventState;
  coinbase: SnapshotEventState;
  kraken: SnapshotEventState;
  okx: SnapshotEventState;
  chainlink: SnapshotEventState;
};

type SnapshotEventState = { price: MarketEvent | null; orderbook: MarketEvent | null };
```

## Integration Guide

## Use as a library in another project

### Install

```bash
npm install @sha3/click-collector
```

### Query usage

```ts
import { MarketEventsQueryService, type MarketSnapshot } from "@sha3/click-collector";

const query = MarketEventsQueryService.createDefault();
const markets = await query.listMarkets("15m", "eth");

for (const market of markets) {
  const slug = market.slug;
  const snapshots: MarketSnapshot[] = await query.getMarketSnapshots(slug);
  if (snapshots.length > 0) {
    const last = snapshots[snapshots.length - 1];
    const ethBinancePrice = last.crypto.eth.binance.price?.price ?? null;
    console.log({ slug, snapshotTs: last.snapshotTs, ethBinancePrice });
  }
}
```

## Use as runtime service

```bash
CLICKHOUSE_HOST=http://localhost:8123 node --import tsx src/index.ts
```

## Configuration Reference (`src/config.ts`)

Import style inside project code is fixed:

```ts
import CONFIG from "../config.ts";
```

### ClickHouse

- `CLICKHOUSE_HOST`: ClickHouse URL
- `CLICKHOUSE_DATABASE`: target database name
- `CLICKHOUSE_USER`: username
- `CLICKHOUSE_PASSWORD`: password
- `CLICKHOUSE_TICKS_TABLE`: ticks table name
- `CLICKHOUSE_MARKET_REGISTRY_TABLE`: market registry table name

### Insert behavior

- `CLICKHOUSE_ASYNC_INSERT`: `1` enables async insert mode
- `CLICKHOUSE_WAIT_FOR_ASYNC_INSERT`: `0` fire-and-forget, `1` wait for completion

### Ingestion buffering/coalescing

- `INGEST_BATCH_SIZE`: max buffered events before flush
- `INGEST_FLUSH_INTERVAL_MS`: periodic flush interval
- `INGEST_COALESCE_WINDOW_MS`: dedupe/coalesce window per key (`0` disables)
- `INGEST_COALESCE_CLEANUP_INTERVAL_MS`: in-memory coalesce cleanup interval
- `INGEST_COALESCE_KEY_TTL_MS`: idle TTL for coalesce keys

### Data shape / retention

- `ORDERBOOK_MAX_LEVELS`: stored asks/bids levels per orderbook event
- `TICKS_TTL_DAYS`: ticks retention in days (`0` disables TTL modification)

### Market discovery / coverage

- `POLYMARKET_DISCOVERY_INTERVAL_MS`: Polymarket discovery refresh interval
- `SUPPORTED_ASSETS`: supported symbols (`btc|eth|sol|xrp`)
- `SUPPORTED_WINDOWS`: supported windows (`5m|15m`)
- `CRYPTO_PROVIDERS`: enabled crypto providers

### Price-to-beat enrichment

- `PRICE_TO_BEAT_POLL_INTERVAL_MS`: retry interval for `price_to_beat` lookup on started markets (default `10000`)
- `PRICE_TO_BEAT_STARTUP_BACKFILL_ENABLED`: run a one-shot massive backfill at startup (`1` enabled, `0` disabled)
- `PRICE_TO_BEAT_STARTUP_BACKFILL_LIMIT`: maximum number of pending markets scanned at startup
- `PRICE_TO_BEAT_STARTUP_BACKFILL_DELAY_MS`: delay between startup backfill endpoint requests (default `2000`)
- `PRICE_TO_BEAT_API_BASE_URL`: endpoint used to fetch window opening prices

### Example `.env`

```dotenv
CLICKHOUSE_HOST=http://localhost:8123
CLICKHOUSE_DATABASE=default
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_TICKS_TABLE=ticks
CLICKHOUSE_MARKET_REGISTRY_TABLE=market_registry
CLICKHOUSE_ASYNC_INSERT=1
CLICKHOUSE_WAIT_FOR_ASYNC_INSERT=0
INGEST_BATCH_SIZE=1000
INGEST_FLUSH_INTERVAL_MS=2000
INGEST_COALESCE_WINDOW_MS=500
INGEST_COALESCE_CLEANUP_INTERVAL_MS=60000
INGEST_COALESCE_KEY_TTL_MS=3600000
ORDERBOOK_MAX_LEVELS=5
TICKS_TTL_DAYS=90
POLYMARKET_DISCOVERY_INTERVAL_MS=30000
PRICE_TO_BEAT_POLL_INTERVAL_MS=10000
PRICE_TO_BEAT_STARTUP_BACKFILL_ENABLED=1
PRICE_TO_BEAT_STARTUP_BACKFILL_LIMIT=500
PRICE_TO_BEAT_STARTUP_BACKFILL_DELAY_MS=2000
PRICE_TO_BEAT_API_BASE_URL=https://polymarket.com/api/crypto/crypto-price
```

## Data Model (ClickHouse)

### `market_registry`

Stores discovered Polymarket markets.

Important columns:

- `slug`
- `asset`
- `window`
- `market_start_ts`
- `market_end_ts`
- `up_asset_id`
- `down_asset_id`
- `price_to_beat` (`Nullable(Float64)`)
- `final_price` (`Nullable(Float64)`)
- `is_test`

Engine:

- `ReplacingMergeTree(updated_at)`
- `PARTITION BY asset`
- `ORDER BY (slug)`

### `ticks`

Stores normalized events across all sources.

Important columns:

- `event_id`
- `event_ts`
- `source_category`
- `source_name`
- `event_type`
- `asset`
- `window`
- `market_slug`
- `token_side`
- `payload_json`
- `is_test`

Engine:

- `MergeTree`
- `PARTITION BY (asset, toYYYYMM(event_ts))`
- `TTL event_ts + INTERVAL 90 DAY` (default)
- `ORDER BY (asset, event_ts, source_category, source_name, event_type, event_id)`

## Price Enrichment Lifecycle

1. Polymarket discovery inserts/updates `market_registry` rows with `price_to_beat = NULL` and `final_price = NULL`.
2. `MarketRegistryPriceEnrichmentService` scans started markets missing `price_to_beat`.
3. For each pending market, it calls:
   `GET {PRICE_TO_BEAT_API_BASE_URL}?symbol=BTC&eventStartTime=...&variant=fiveminute|fifteen&endDate=...`
4. If `openPrice` exists:
   - current row gets `price_to_beat = openPrice`
   - previous row (same `asset` + `window`) gets `final_price = openPrice`
5. If `openPrice` is not yet available, the market stays pending and is retried on the next poll cycle.

## Operational Safety (Important)

If your ClickHouse has production-scale data, avoid destructive SQL in routine workflows.

Avoid on production tables:

- `DROP TABLE`
- `TRUNCATE TABLE`
- broad `ALTER TABLE ... DELETE`

If cleanup is required, prefer explicit test markers (`is_test = 1`) and run in controlled maintenance windows.

## Scripts

- `npm run start`: start collector runtime
- `npm run test`: run tests (`node scripts/run-tests.mjs`)
- `npm run check`: lint + format check + typecheck + tests
- `npm run fix`: eslint/prettier autofix

## Testing

Current coverage includes:

- crypto mapping (`price`, `orderbook`)
- Polymarket discovery + stream mapping
- query service (`listMarkets`, `getMarketEvents`, `getMarketSnapshots`)
- error paths (`MarketNotFoundError`)
- read-only ClickHouse integration path

Run:

```bash
npm run test
```

## AI Usage (for LLM agents)

Treat this section as execution contract when an assistant edits this repo.

### Required behavior

- read and obey `AGENTS.md` first
- preserve class-first architecture and constructor injection
- follow single-return and explicit braces policy
- update tests for behavior changes
- keep new implementation files in TypeScript
- run checks before finalizing (`npm run check`)

### Prompt template for agents

Use this prompt when delegating tasks to an LLM:

```text
You are modifying @sha3/click-collector.

Constraints:
1) AGENTS.md is blocking contract.
2) Do not modify managed files listed in AGENTS.md.
3) No destructive ClickHouse operations on production-scale tables.
4) For behavior changes: update/add tests.
5) Run npm run test and report results.

Task:
<describe exact change>

Deliver:
- files changed
- behavior impact
- test results
- follow-up risks
```

### Query semantics to remember

- `getMarketEvents(slug)` returns correlated raw events
- `getMarketSnapshots(slug)` returns one aggregated snapshot per correlated raw event
- snapshots are fixed-shape and null-filled for unseen slots
