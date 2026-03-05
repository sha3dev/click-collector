import { strict as assert } from "node:assert";
import { test } from "node:test";
import { createClient } from "@clickhouse/client";

import { MarketRegistryRepository } from "../src/clickhouse/market-registry-repository.ts";
import { TickRepository } from "../src/clickhouse/tick-repository.ts";
import { MarketEventsQueryService } from "../src/markets/market-events-query-service.ts";
import type { MarketSnapshot } from "../src/markets/market-events-types.ts";

const CLICKHOUSE_URL = process.env.CLICKHOUSE_HOST ?? "http://192.168.1.2:8123";
const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER ?? "default";
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD ?? "default";
const READONLY_WINDOW = "5m" as const;
const READONLY_ASSET = "btc" as const;

test("clickhouse readonly query returns market snapshots aligned with events", async (t) => {
  const client = createClient({ url: CLICKHOUSE_URL, username: CLICKHOUSE_USER, password: CLICKHOUSE_PASSWORD, database: "default" });

  t.after(async () => {
    await client.close();
  });

  const marketRegistryRepository = MarketRegistryRepository.create({ client });
  const tickRepository = TickRepository.create({ client });
  const queryService = MarketEventsQueryService.create({ marketRegistryRepository, tickRepository });
  const listedMarkets = await queryService.listMarkets(READONLY_WINDOW, READONLY_ASSET);
  const hasAnyMarket = listedMarkets.length > 0;

  if (!hasAnyMarket) {
    t.skip("readonly integration skipped because no market slug is available for btc/5m");
  }

  if (hasAnyMarket) {
    const targetSlug = listedMarkets[0]?.slug ?? "";
    const events = await queryService.getMarketEvents(targetSlug);
    const snapshots = await queryService.getMarketSnapshots(targetSlug);
    const hasSnapshots = snapshots.length > 0;
    const hasEvents = events.length > 0;

    assert.equal(hasEvents, true);
    assert.equal(hasSnapshots, true);

    if (hasSnapshots) {
      const firstSnapshot: MarketSnapshot | null = snapshots[0] ?? null;
      assert.equal(firstSnapshot === null, false);

      if (firstSnapshot) {
        const firstEvent = firstSnapshot.triggerEvent;

        assert.equal(firstSnapshot.triggerEvent.eventId, firstEvent.eventId);
        assert.equal(firstSnapshot.snapshotTs, firstSnapshot.triggerEvent.eventTs);
        assert.equal(firstSnapshot.asset, "btc");
        assert.equal(firstSnapshot.window, READONLY_WINDOW);
        assert.equal(typeof firstSnapshot.marketStartTs, "number");
        assert.equal(typeof firstSnapshot.marketEndTs, "number");
        assert.equal(typeof firstSnapshot.priceToBeat === "number" || firstSnapshot.priceToBeat === null, true);
        assert.equal(Object.hasOwn(firstSnapshot.crypto, "btc"), true);
        assert.equal(Object.hasOwn(firstSnapshot.crypto, "eth"), true);
        assert.equal(Object.hasOwn(firstSnapshot.crypto, "sol"), true);
        assert.equal(Object.hasOwn(firstSnapshot.crypto, "xrp"), true);
        assert.equal(Object.hasOwn(firstSnapshot.polymarket, "up"), true);
        assert.equal(Object.hasOwn(firstSnapshot.polymarket, "down"), true);
      }
    }
  }
});
