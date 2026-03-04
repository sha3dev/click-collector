import { strict as assert } from "node:assert";
import { test } from "node:test";

import { MarketEventsQueryService } from "../src/markets/market-events-query-service.ts";
import { MarketNotFoundError } from "../src/markets/market-not-found-error.ts";
import type { MarketEvent } from "../src/markets/market-events-types.ts";

test("listMarkets returns slugs from registry repository", async () => {
  const service = MarketEventsQueryService.create({
    marketRegistryRepository: {
      async listMarketSlugs() {
        return ["btc-updown-5m-1", "btc-updown-5m-2"];
      },
      async getMarketBoundsBySlug() {
        return null;
      }
    },
    tickRepository: {
      async getRelatedEventsByMarketRange() {
        return [];
      }
    }
  });

  const slugs = await service.listMarkets("5m", "btc");
  assert.deepEqual(slugs, ["btc-updown-5m-1", "btc-updown-5m-2"]);
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
      async listMarketSlugs() {
        return [];
      },
      async getMarketBoundsBySlug(slug) {
        return { slug, asset: "btc", window: "5m", marketStartTs: 1000, marketEndTs: 2000 };
      }
    },
    tickRepository: {
      async getRelatedEventsByMarketRange(options) {
        capturedSlug = options.slug;
        capturedAsset = options.asset;
        capturedFromTs = options.fromTs;
        capturedToTs = options.toTs;
        return [expectedEvent];
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
      async listMarketSlugs() {
        return [];
      },
      async getMarketBoundsBySlug() {
        return null;
      }
    },
    tickRepository: {
      async getRelatedEventsByMarketRange() {
        return [];
      }
    }
  });

  await assert.rejects(async () => {
    await service.getMarketEvents("missing-slug");
  }, MarketNotFoundError);
});
