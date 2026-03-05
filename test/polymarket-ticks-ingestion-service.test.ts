import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { AddMarketListenerOptions, LoadCryptoWindowMarketsOptions, MarketEvent as PolymarketEvent, SubscribeMarketAssetsOptions } from "@sha3/polymarket";

import { PolymarketTicksIngestionService } from "../src/collector/polymarket-ticks-ingestion-service.ts";
import type { PolymarketClientContract } from "../src/collector/collector-types.ts";
import type { MarketEvent, MarketRecord } from "../src/markets/market-events-types.ts";

test("polymarket ingestion persists market registry and maps stream events", async () => {
  const persistedMarkets: MarketRecord[][] = [];
  const tickWrites: MarketEvent[][] = [];
  const subscribedAssetIds: string[][] = [];
  let listener: (event: PolymarketEvent) => void = () => {
    return;
  };

  const fakeMarket = {
    id: "m1",
    slug: "btc-updown-5m-1767225900",
    question: "btc updown",
    symbol: "btc",
    conditionId: "c1",
    outcomes: ["up", "down"],
    clobTokenIds: ["up-1", "down-1"],
    upTokenId: "up-1",
    downTokenId: "down-1",
    orderMinSize: 1,
    orderPriceMinTickSize: "0.01",
    eventStartTime: "2026-01-01T00:00:00.000Z",
    endDate: "2026-01-01T00:05:00.000Z",
    start: new Date("2026-01-01T00:00:00.000Z"),
    end: new Date("2026-01-01T00:05:00.000Z"),
    raw: {}
  };

  const service = PolymarketTicksIngestionService.create({
    tickSink: {
      async writeTicks(events) {
        tickWrites.push(events);
      }
    },
    marketRegistrySink: {
      async upsert(markets) {
        persistedMarkets.push(markets);
      }
    },
    client: {
      async connect() {
        return;
      },
      async disconnect() {
        return;
      },
      markets: {
        async loadCryptoWindowMarkets(options: LoadCryptoWindowMarketsOptions) {
          const result = options.window === "5m" ? [fakeMarket] : [];
          return result;
        }
      },
      stream: {
        subscribe(options: SubscribeMarketAssetsOptions) {
          subscribedAssetIds.push(options.assetIds);
        },
        addListener(options: AddMarketListenerOptions) {
          listener = options.listener;
          return () => {
            listener = () => {
              return;
            };
          };
        }
      }
    } as unknown as PolymarketClientContract,
    nowFactory: () => new Date("2026-01-01T00:01:00.000Z")
  });

  await service.start();

  listener({ source: "polymarket", assetId: "up-1", index: 1, date: new Date("2026-01-01T00:01:10.000Z"), type: "price", price: 0.63 });
  listener({
    source: "polymarket",
    assetId: "down-1",
    index: 2,
    date: new Date("2026-01-01T00:01:20.000Z"),
    type: "book",
    asks: [
      { price: 0.38, size: 100 },
      { price: 0.39, size: 101 },
      { price: 0.4, size: 102 },
      { price: 0.41, size: 103 },
      { price: 0.42, size: 104 },
      { price: 0.43, size: 105 }
    ],
    bids: [
      { price: 0.37, size: 120 },
      { price: 0.36, size: 121 },
      { price: 0.35, size: 122 },
      { price: 0.34, size: 123 },
      { price: 0.33, size: 124 },
      { price: 0.32, size: 125 }
    ]
  });

  await service.stop();
  const firstPersisted = persistedMarkets.at(0)?.at(0);
  const firstSubscribed = subscribedAssetIds.at(0);
  const firstTickEvent = tickWrites.at(0)?.at(0);
  const secondTickEvent = tickWrites.at(1)?.at(0);

  assert.equal(persistedMarkets.length >= 1, true);
  assert.ok(firstPersisted);
  assert.equal(firstPersisted.slug, "btc-updown-5m-1767225900");
  assert.equal(firstPersisted.priceToBeat, null);
  assert.equal(firstPersisted.finalPrice, null);
  assert.equal(subscribedAssetIds.length >= 1, true);
  assert.ok(firstSubscribed);
  assert.deepEqual(firstSubscribed.sort(), ["down-1", "up-1"]);

  assert.equal(tickWrites.length, 2);
  assert.ok(firstTickEvent);
  assert.equal(firstTickEvent.eventType, "price");
  assert.equal(firstTickEvent.marketSlug, "btc-updown-5m-1767225900");
  assert.equal(firstTickEvent.tokenSide, "up");

  assert.ok(secondTickEvent);
  assert.equal(secondTickEvent.eventType, "orderbook");
  assert.equal(secondTickEvent.tokenSide, "down");
  const parsedOrderbook = JSON.parse(secondTickEvent.orderbook ?? "{}") as {
    asks: Array<{ price: number; size: number }>;
    bids: Array<{ price: number; size: number }>;
  };
  assert.equal(parsedOrderbook.asks.length, 5);
  assert.equal(parsedOrderbook.bids.length, 5);
});
