import { strict as assert } from "node:assert";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { createClient } from "@clickhouse/client";

import { MarketRegistryRepository } from "../src/clickhouse/market-registry-repository.ts";
import { TickRepository } from "../src/clickhouse/tick-repository.ts";
import { MarketEventsQueryService } from "../src/markets/market-events-query-service.ts";
import type { MarketEvent, MarketRecord } from "../src/markets/market-events-types.ts";

const CLICKHOUSE_URL = "http://192.168.1.2:8123";
const CLICKHOUSE_USER = "default";
const CLICKHOUSE_PASSWORD = "default";

test("clickhouse integration inserts and consumes stored market events", async (t) => {
  const client = createClient({ url: CLICKHOUSE_URL, username: CLICKHOUSE_USER, password: CLICKHOUSE_PASSWORD, database: "default" });

  t.after(async () => {
    await client.close();
  });

  const marketRegistryRepository = MarketRegistryRepository.create({ client });
  const tickRepository = TickRepository.create({ client });
  const queryService = MarketEventsQueryService.create({ marketRegistryRepository, tickRepository });

  await marketRegistryRepository.ensureSchema();
  await tickRepository.ensureSchema();

  const baseTimestamp = Date.now();
  const marketSlug = `btc-updown-5m-${baseTimestamp}`;
  const marketRecord: MarketRecord = {
    slug: marketSlug,
    asset: "btc",
    window: "5m",
    marketStartTs: baseTimestamp,
    marketEndTs: baseTimestamp + 300_000,
    upAssetId: `up-${randomUUID()}`,
    downAssetId: `down-${randomUUID()}`,
    isTest: true
  };

  await marketRegistryRepository.upsertMarkets([marketRecord]);

  const insertedEvents: MarketEvent[] = [
    {
      eventId: `exchange-${randomUUID()}`,
      eventTs: baseTimestamp + 1_000,
      sourceCategory: "exchange",
      sourceName: "binance",
      eventType: "price",
      asset: "btc",
      window: null,
      marketSlug: null,
      tokenSide: null,
      price: 90_000,
      orderbook: null,
      payloadJson: JSON.stringify({ type: "price", provider: "binance" }),
      isTest: true
    },
    {
      eventId: `chainlink-${randomUUID()}`,
      eventTs: baseTimestamp + 2_000,
      sourceCategory: "chainlink",
      sourceName: "chainlink",
      eventType: "price",
      asset: "btc",
      window: null,
      marketSlug: null,
      tokenSide: null,
      price: 90_005,
      orderbook: null,
      payloadJson: JSON.stringify({ type: "price", provider: "chainlink" }),
      isTest: true
    },
    {
      eventId: `polymarket-${randomUUID()}`,
      eventTs: baseTimestamp + 3_000,
      sourceCategory: "polymarket",
      sourceName: "polymarket",
      eventType: "orderbook",
      asset: "btc",
      window: "5m",
      marketSlug,
      tokenSide: "up",
      price: null,
      orderbook: JSON.stringify({ asks: [{ price: 0.53, size: 11 }], bids: [{ price: 0.52, size: 13 }] }),
      payloadJson: JSON.stringify({ type: "book", source: "polymarket" }),
      isTest: true
    }
  ];

  await tickRepository.insertTicks(insertedEvents);

  const listedSlugs = await queryService.listMarkets("5m", "btc");
  const relatedEvents = await queryService.getMarketEvents(marketSlug);
  const relatedEventIds = new Set<string>(
    relatedEvents.map((event) => {
      return event.eventId;
    })
  );

  assert.equal(listedSlugs.includes(marketSlug), true);

  for (const event of insertedEvents) {
    assert.equal(relatedEventIds.has(event.eventId), true);
  }

  const everyEventMarkedAsTest = relatedEvents.every((event) => {
    return event.isTest;
  });
  assert.equal(everyEventMarkedAsTest, true);
});
