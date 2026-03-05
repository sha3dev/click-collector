import { strict as assert } from "node:assert";
import { test } from "node:test";

import { MarketEventsQueryService } from "../src/markets/market-events-query-service.ts";
import { MarketNotFoundError } from "../src/markets/market-not-found-error.ts";
import type { MarketEvent, MarketRecord, MarketSnapshot } from "../src/markets/market-events-types.ts";

test("listMarkets returns market records from registry repository", async () => {
  const marketOne: MarketRecord = {
    slug: "btc-updown-5m-1",
    asset: "btc",
    window: "5m",
    marketStartTs: 1_000,
    marketEndTs: 1_300,
    upAssetId: "up-1",
    downAssetId: "down-1",
    priceToBeat: 90_000,
    finalPrice: 90_010,
    isTest: false
  };
  const marketTwo: MarketRecord = {
    slug: "btc-updown-5m-2",
    asset: "btc",
    window: "5m",
    marketStartTs: 1_300,
    marketEndTs: 1_600,
    upAssetId: "up-2",
    downAssetId: "down-2",
    priceToBeat: 90_010,
    finalPrice: null,
    isTest: false
  };

  const service = MarketEventsQueryService.create({
    marketRegistryRepository: {
      async listMarkets() {
        return [marketOne, marketTwo];
      },
      async getMarketBoundsBySlug() {
        return null;
      }
    },
    tickRepository: {
      async getRelatedEventsByMarketRange() {
        return [];
      },
      async getAllAssetsEventsByMarketRange() {
        return [];
      }
    }
  });

  const markets = await service.listMarkets("5m", "btc");
  assert.deepEqual(markets, [marketOne, marketTwo]);
});

test("getMarketEvents composes related events query using market bounds", async () => {
  let capturedSlug = "";
  let capturedAsset = "";
  let capturedFromTs = 0;
  let capturedToTs = 0;
  const expectedEvent: MarketEvent = {
    eventId: "id-1",
    eventTs: 1,
    sourceCategory: "exchange",
    sourceName: "binance",
    eventType: "price",
    asset: "btc",
    window: null,
    marketSlug: null,
    tokenSide: null,
    price: 100,
    orderbook: null,
    payloadJson: "{}",
    isTest: false
  };

  const service = MarketEventsQueryService.create({
    marketRegistryRepository: {
      async listMarkets() {
        return [];
      },
      async getMarketBoundsBySlug(slug) {
        return { slug, asset: "btc", window: "5m", marketStartTs: 1000, marketEndTs: 2000, priceToBeat: 90_000 };
      }
    },
    tickRepository: {
      async getRelatedEventsByMarketRange(options) {
        capturedSlug = options.slug;
        capturedAsset = options.asset;
        capturedFromTs = options.fromTs;
        capturedToTs = options.toTs;
        return [expectedEvent];
      },
      async getAllAssetsEventsByMarketRange() {
        return [];
      }
    }
  });

  const events = await service.getMarketEvents("btc-updown-5m-123");

  assert.equal(capturedSlug, "btc-updown-5m-123");
  assert.equal(capturedAsset, "btc");
  assert.equal(capturedFromTs, 1000);
  assert.equal(capturedToTs, 2000);
  assert.deepEqual(events, [expectedEvent]);
});

test("getMarketEvents throws MarketNotFoundError when slug is unknown", async () => {
  const service = MarketEventsQueryService.create({
    marketRegistryRepository: {
      async listMarkets() {
        return [];
      },
      async getMarketBoundsBySlug() {
        return null;
      }
    },
    tickRepository: {
      async getRelatedEventsByMarketRange() {
        return [];
      },
      async getAllAssetsEventsByMarketRange() {
        return [];
      }
    }
  });

  await assert.rejects(async () => {
    await service.getMarketEvents("missing-slug");
  }, MarketNotFoundError);
});

test("getMarketSnapshots composes snapshots with carry-forward state and all-assets enrichment", async () => {
  const triggerOne: MarketEvent = {
    eventId: "event-1",
    eventTs: 1_000,
    sourceCategory: "exchange",
    sourceName: "binance",
    eventType: "price",
    asset: "btc",
    window: null,
    marketSlug: null,
    tokenSide: null,
    price: 91_000,
    orderbook: null,
    payloadJson: "{}",
    isTest: false
  };
  const triggerTwo: MarketEvent = {
    eventId: "event-2",
    eventTs: 1_500,
    sourceCategory: "polymarket",
    sourceName: "polymarket",
    eventType: "price",
    asset: "btc",
    window: "5m",
    marketSlug: "btc-updown-5m-123",
    tokenSide: "up",
    price: 0.52,
    orderbook: null,
    payloadJson: "{}",
    isTest: false
  };
  const triggerThree: MarketEvent = {
    eventId: "event-3",
    eventTs: 2_000,
    sourceCategory: "chainlink",
    sourceName: "chainlink",
    eventType: "price",
    asset: "btc",
    window: null,
    marketSlug: null,
    tokenSide: null,
    price: 91_020,
    orderbook: null,
    payloadJson: "{}",
    isTest: false
  };
  const allAssetEvent: MarketEvent = {
    eventId: "event-eth-1",
    eventTs: 1_200,
    sourceCategory: "exchange",
    sourceName: "coinbase",
    eventType: "price",
    asset: "eth",
    window: null,
    marketSlug: null,
    tokenSide: null,
    price: 4_200,
    orderbook: null,
    payloadJson: "{}",
    isTest: false
  };
  const polymarketDownBook: MarketEvent = {
    eventId: "event-down-book",
    eventTs: 1_800,
    sourceCategory: "polymarket",
    sourceName: "polymarket",
    eventType: "orderbook",
    asset: "btc",
    window: "5m",
    marketSlug: "btc-updown-5m-123",
    tokenSide: "down",
    price: null,
    orderbook: '{"asks":[],"bids":[]}',
    payloadJson: "{}",
    isTest: false
  };

  const service = MarketEventsQueryService.create({
    marketRegistryRepository: {
      async listMarkets() {
        return [];
      },
      async getMarketBoundsBySlug(slug) {
        return { slug, asset: "btc", window: "5m", marketStartTs: 1_000, marketEndTs: 2_000, priceToBeat: 91_111 };
      }
    },
    tickRepository: {
      async getRelatedEventsByMarketRange() {
        return [triggerOne, triggerTwo, triggerThree];
      },
      async getAllAssetsEventsByMarketRange() {
        return [triggerOne, allAssetEvent, triggerTwo, polymarketDownBook, triggerThree];
      }
    }
  });

  const snapshots = await service.getMarketSnapshots("btc-updown-5m-123");
  const firstSnapshot: MarketSnapshot | null = snapshots[0] ?? null;
  const secondSnapshot: MarketSnapshot | null = snapshots[1] ?? null;
  const thirdSnapshot: MarketSnapshot | null = snapshots[2] ?? null;

  assert.equal(snapshots.length, 3);
  assert.equal(firstSnapshot === null, false);
  assert.equal(secondSnapshot === null, false);
  assert.equal(thirdSnapshot === null, false);

  if (firstSnapshot && secondSnapshot && thirdSnapshot) {
    assert.deepEqual(firstSnapshot.triggerEvent, triggerOne);
    assert.equal(firstSnapshot.snapshotTs, 1_000);
    assert.equal(firstSnapshot.asset, "btc");
    assert.equal(firstSnapshot.window, "5m");
    assert.equal(firstSnapshot.marketStartTs, 1_000);
    assert.equal(firstSnapshot.marketEndTs, 2_000);
    assert.equal(firstSnapshot.priceToBeat, 91_111);
    assert.deepEqual(firstSnapshot.crypto.btc.binance.price, triggerOne);
    assert.equal(firstSnapshot.crypto.eth.coinbase.price, null);
    assert.equal(firstSnapshot.polymarket.up.price, null);
    assert.deepEqual(secondSnapshot.triggerEvent, triggerTwo);
    assert.equal(secondSnapshot.asset, "btc");
    assert.equal(secondSnapshot.window, "5m");
    assert.equal(secondSnapshot.marketStartTs, 1_000);
    assert.equal(secondSnapshot.marketEndTs, 2_000);
    assert.equal(secondSnapshot.priceToBeat, 91_111);
    assert.deepEqual(secondSnapshot.crypto.btc.binance.price, triggerOne);
    assert.deepEqual(secondSnapshot.crypto.eth.coinbase.price, allAssetEvent);
    assert.deepEqual(secondSnapshot.polymarket.up.price, triggerTwo);
    assert.equal(secondSnapshot.polymarket.down.orderbook, null);
    assert.deepEqual(thirdSnapshot.triggerEvent, triggerThree);
    assert.equal(thirdSnapshot.asset, "btc");
    assert.equal(thirdSnapshot.window, "5m");
    assert.equal(thirdSnapshot.marketStartTs, 1_000);
    assert.equal(thirdSnapshot.marketEndTs, 2_000);
    assert.equal(thirdSnapshot.priceToBeat, 91_111);
    assert.deepEqual(thirdSnapshot.crypto.btc.chainlink.price, triggerThree);
    assert.deepEqual(thirdSnapshot.polymarket.down.orderbook, polymarketDownBook);
    assert.equal(thirdSnapshot.crypto.sol.kraken.price, null);
  }
});

test("getMarketSnapshots throws MarketNotFoundError when slug is unknown", async () => {
  const service = MarketEventsQueryService.create({
    marketRegistryRepository: {
      async listMarkets() {
        return [];
      },
      async getMarketBoundsBySlug() {
        return null;
      }
    },
    tickRepository: {
      async getRelatedEventsByMarketRange() {
        return [];
      },
      async getAllAssetsEventsByMarketRange() {
        return [];
      }
    }
  });

  await assert.rejects(async () => {
    await service.getMarketSnapshots("missing-slug");
  }, MarketNotFoundError);
});
