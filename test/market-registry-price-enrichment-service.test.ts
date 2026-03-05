import { strict as assert } from "node:assert";
import { test } from "node:test";

import { MarketRegistryPriceEnrichmentService } from "../src/collector/market-registry-price-enrichment-service.ts";
import type { MarketRecord } from "../src/markets/market-events-types.ts";

type FetchCall = { url: string; method: string };

function createMarket(overrides: Partial<MarketRecord>): MarketRecord {
  const market: MarketRecord = {
    slug: "btc-updown-5m-1",
    asset: "btc",
    window: "5m",
    marketStartTs: Date.parse("2026-01-01T00:00:00.000Z"),
    marketEndTs: Date.parse("2026-01-01T00:05:00.000Z"),
    upAssetId: "up-1",
    downAssetId: "down-1",
    priceToBeat: null,
    finalPrice: null,
    isTest: true,
    ...overrides
  };

  return market;
}

function sleep(delayMs: number): Promise<void> {
  const sleepPromise = new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, delayMs);
  });
  return sleepPromise;
}

test("price enrichment updates market price_to_beat and previous final_price", async () => {
  const pendingMarket = createMarket({
    slug: "btc-updown-5m-2",
    marketStartTs: Date.parse("2026-01-01T00:05:00.000Z"),
    marketEndTs: Date.parse("2026-01-01T00:10:00.000Z")
  });
  const previousMarket = createMarket({
    slug: "btc-updown-5m-1",
    marketStartTs: Date.parse("2026-01-01T00:00:00.000Z"),
    marketEndTs: Date.parse("2026-01-01T00:05:00.000Z"),
    priceToBeat: 90_000
  });
  const upsertCalls: MarketRecord[][] = [];
  const fetchCalls: FetchCall[] = [];

  const service = MarketRegistryPriceEnrichmentService.create({
    marketRegistryRepository: {
      async listPendingPriceToBeatMarkets() {
        return [pendingMarket];
      },
      async getPreviousMarketForFinalPrice() {
        return previousMarket;
      },
      async listPendingFinalPriceMarkets() {
        return [];
      },
      async getNextMarketWithPriceToBeat() {
        return null;
      },
      async upsertMarkets(markets) {
        upsertCalls.push(markets);
      }
    },
    fetchFn: async (url, options) => {
      fetchCalls.push({ url, method: options.method });
      return {
        ok: true,
        status: 200,
        async json() {
          return { openPrice: 90_123.45 };
        },
        async text() {
          return "";
        }
      };
    },
    nowFactory: () => new Date("2026-01-01T00:05:10.000Z"),
    pollIntervalMs: 30_000,
    startupBackfillEnabled: false
  });

  await service.start();
  await service.stop();

  assert.equal(fetchCalls.length >= 1, true);
  assert.equal(fetchCalls[0]?.method, "GET");
  assert.equal(fetchCalls[0]?.url.includes("symbol=BTC"), true);
  assert.equal(fetchCalls[0]?.url.includes("variant=fiveminute"), true);
  assert.equal(upsertCalls.length, 1);
  const firstUpdate = upsertCalls[0]?.[0] ?? null;
  const secondUpdate = upsertCalls[0]?.[1] ?? null;
  assert.ok(firstUpdate);
  assert.ok(secondUpdate);

  if (firstUpdate && secondUpdate) {
    assert.equal(firstUpdate.slug, "btc-updown-5m-2");
    assert.equal(firstUpdate.priceToBeat, 90_123.45);
    assert.equal(secondUpdate.slug, "btc-updown-5m-1");
    assert.equal(secondUpdate.finalPrice, 90_123.45);
  }
});

test("price enrichment retries later when price_to_beat is temporarily unavailable", async () => {
  const pendingMarket = createMarket({ slug: "eth-updown-15m-1", asset: "eth", window: "15m", marketEndTs: Date.parse("2026-01-01T00:15:00.000Z") });
  const upsertCalls: MarketRecord[][] = [];
  const fetchCalls: FetchCall[] = [];
  let fetchAttempt = 0;

  const service = MarketRegistryPriceEnrichmentService.create({
    marketRegistryRepository: {
      async listPendingPriceToBeatMarkets() {
        return [pendingMarket];
      },
      async getPreviousMarketForFinalPrice() {
        return null;
      },
      async listPendingFinalPriceMarkets() {
        return [];
      },
      async getNextMarketWithPriceToBeat() {
        return null;
      },
      async upsertMarkets(markets) {
        upsertCalls.push(markets);
      }
    },
    fetchFn: async (url, options) => {
      fetchCalls.push({ url, method: options.method });
      fetchAttempt += 1;
      return {
        ok: true,
        status: 200,
        async json() {
          const payload = fetchAttempt === 1 ? { openPrice: null } : { openPrice: 4_321.01 };
          return payload;
        },
        async text() {
          return "";
        }
      };
    },
    nowFactory: () => new Date("2026-01-01T00:00:10.000Z"),
    pollIntervalMs: 10,
    startupBackfillEnabled: false
  });

  await service.start();
  await sleep(35);
  await service.stop();

  assert.equal(fetchCalls.length >= 2, true);
  assert.equal(fetchCalls[0]?.url.includes("variant=fifteen"), true);
  assert.equal(upsertCalls.length >= 1, true);
  const update = upsertCalls.at(-1)?.[0] ?? null;
  assert.ok(update);

  if (update) {
    assert.equal(update.slug, "eth-updown-15m-1");
    assert.equal(update.priceToBeat, 4_321.01);
  }
});

test("startup backfill fills missing final_price from next market price_to_beat", async () => {
  const currentMarket = createMarket({
    slug: "sol-updown-5m-1",
    asset: "sol",
    window: "5m",
    marketStartTs: Date.parse("2026-01-01T00:00:00.000Z"),
    marketEndTs: Date.parse("2026-01-01T00:05:00.000Z"),
    priceToBeat: 100
  });
  const nextMarket = createMarket({
    slug: "sol-updown-5m-2",
    asset: "sol",
    window: "5m",
    marketStartTs: Date.parse("2026-01-01T00:05:00.000Z"),
    marketEndTs: Date.parse("2026-01-01T00:10:00.000Z"),
    priceToBeat: 101
  });
  const upsertCalls: MarketRecord[][] = [];
  let pendingPriceToBeatCallCount = 0;

  const service = MarketRegistryPriceEnrichmentService.create({
    marketRegistryRepository: {
      async listPendingPriceToBeatMarkets() {
        pendingPriceToBeatCallCount += 1;
        return [];
      },
      async getPreviousMarketForFinalPrice() {
        return null;
      },
      async listPendingFinalPriceMarkets() {
        return [currentMarket];
      },
      async getNextMarketWithPriceToBeat() {
        return nextMarket;
      },
      async upsertMarkets(markets) {
        upsertCalls.push(markets);
      }
    },
    fetchFn: async () => {
      return {
        ok: true,
        status: 200,
        async json() {
          return { openPrice: 0 };
        },
        async text() {
          return "";
        }
      };
    },
    nowFactory: () => new Date("2026-01-01T00:10:00.000Z"),
    pollIntervalMs: 30_000,
    startupBackfillEnabled: true,
    startupBackfillLimit: 100,
    startupBackfillDelayMs: 1
  });

  await service.start();
  await service.stop();

  assert.equal(pendingPriceToBeatCallCount >= 1, true);
  assert.equal(upsertCalls.length >= 1, true);
  const firstUpsert = upsertCalls.at(0)?.at(0) ?? null;
  assert.ok(firstUpsert);

  if (firstUpsert) {
    assert.equal(firstUpsert.slug, "sol-updown-5m-1");
    assert.equal(firstUpsert.finalPrice, 101);
  }
});
