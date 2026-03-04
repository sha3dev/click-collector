import { strict as assert } from "node:assert";
import { test } from "node:test";

import { ClickHouseTickSink } from "../src/collector/clickhouse-tick-sink.ts";
import type { TickRepository } from "../src/clickhouse/tick-repository.ts";
import type { MarketEvent } from "../src/markets/market-events-types.ts";

test("tick sink coalesces events by type and source within configured window", async () => {
  const insertedBatches: MarketEvent[][] = [];
  const repository = {
    async insertTicks(events: MarketEvent[]) {
      insertedBatches.push(events);
    }
  } as unknown as TickRepository;

  const sink = ClickHouseTickSink.create({ repository, coalesceWindowMs: 100 });

  const baseTs = 1_700_000_000_000;
  const events: MarketEvent[] = [
    {
      eventId: "e-1",
      eventTs: baseTs,
      sourceCategory: "exchange",
      sourceName: "binance",
      eventType: "price",
      asset: "btc",
      window: null,
      marketSlug: null,
      tokenSide: null,
      price: 1,
      orderbook: null,
      payloadJson: "{}",
      isTest: true
    },
    {
      eventId: "e-2",
      eventTs: baseTs + 50,
      sourceCategory: "exchange",
      sourceName: "binance",
      eventType: "price",
      asset: "btc",
      window: null,
      marketSlug: null,
      tokenSide: null,
      price: 2,
      orderbook: null,
      payloadJson: "{}",
      isTest: true
    },
    {
      eventId: "e-3",
      eventTs: baseTs + 150,
      sourceCategory: "exchange",
      sourceName: "binance",
      eventType: "price",
      asset: "btc",
      window: null,
      marketSlug: null,
      tokenSide: null,
      price: 3,
      orderbook: null,
      payloadJson: "{}",
      isTest: true
    },
    {
      eventId: "e-4",
      eventTs: baseTs + 60,
      sourceCategory: "exchange",
      sourceName: "binance",
      eventType: "orderbook",
      asset: "btc",
      window: null,
      marketSlug: null,
      tokenSide: null,
      price: null,
      orderbook: JSON.stringify({ asks: [], bids: [] }),
      payloadJson: "{}",
      isTest: true
    }
  ];

  await sink.writeTicks(events);
  await sink.flush();

  const inserted = insertedBatches.flat();
  const insertedIds = inserted.map((event) => {
    return event.eventId;
  });

  assert.deepEqual(insertedIds, ["e-1", "e-3", "e-4"]);
});
