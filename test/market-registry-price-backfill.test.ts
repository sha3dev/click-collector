import { strict as assert } from "node:assert";
import { test } from "node:test";
import { createClient } from "@clickhouse/client";

import CONFIG from "../src/config.ts";
import { MarketRegistryRepository } from "../src/clickhouse/market-registry-repository.ts";
import type { MarketRecord } from "../src/markets/market-events-types.ts";

type PriceToBeatVariant = "fiveminute" | "fifteen";
type PriceToBeatApiResponse = { openPrice?: unknown };

const CLICKHOUSE_URL = process.env.CLICKHOUSE_HOST ?? "http://192.168.1.2:8123";
const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER ?? "default";
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD ?? "default";
const REQUEST_DELAY_MS = 2_000;
const BACKFILL_LIMIT = Number.parseInt(process.env.PRICE_BACKFILL_LIMIT ?? "500", 10);
const RUN_BACKFILL = process.env.RUN_MARKET_PRICE_BACKFILL_TEST === "1";

function toVariant(window: MarketRecord["window"]): PriceToBeatVariant {
  let variant: PriceToBeatVariant = "fiveminute";

  if (window === "15m") {
    variant = "fifteen";
  }

  return variant;
}

function buildPriceToBeatUrl(market: MarketRecord): string {
  const variant = toVariant(market.window);
  const eventStartTime = new Date(market.marketStartTs);
  const endDate = new Date(market.marketEndTs);
  const params = new URLSearchParams({
    symbol: market.asset.toUpperCase(),
    eventStartTime: eventStartTime.toISOString(),
    variant,
    endDate: endDate.toISOString()
  });
  const url = `${CONFIG.PRICE_TO_BEAT_API_BASE_URL}?${params.toString()}`;
  return url;
}

async function sleep(delayMs: number): Promise<void> {
  const sleepPromise = new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, delayMs);
  });
  return sleepPromise;
}

async function fetchOpenPrice(market: MarketRecord): Promise<number | null> {
  const url = buildPriceToBeatUrl(market);
  const response = await fetch(url, { method: "GET" });
  let openPrice: number | null = null;

  if (response.ok) {
    const payload = (await response.json()) as PriceToBeatApiResponse;

    if (typeof payload.openPrice === "number" && Number.isFinite(payload.openPrice)) {
      openPrice = payload.openPrice;
    }
  }

  return openPrice;
}

test("backfill missing price_to_beat and final_price in market_registry", async (t) => {
  if (!RUN_BACKFILL) {
    t.skip("set RUN_MARKET_PRICE_BACKFILL_TEST=1 to enable this backfill test");
  }

  if (RUN_BACKFILL) {
    const client = createClient({ url: CLICKHOUSE_URL, username: CLICKHOUSE_USER, password: CLICKHOUSE_PASSWORD, database: "default" });

    t.after(async () => {
      await client.close();
    });

    const repository = MarketRegistryRepository.create({ client });
    const nowTs = Date.now();
    const missingPriceToBeatMarkets = await repository.listPendingPriceToBeatMarkets({ nowTs, limit: BACKFILL_LIMIT });
    const missingFinalPriceMarkets = await repository.listPendingFinalPriceMarkets({ nowTs, limit: BACKFILL_LIMIT });
    let filledPriceToBeatCount = 0;
    let filledFinalPriceCount = 0;

    console.log(`[backfill] pending price_to_beat: ${missingPriceToBeatMarkets.length}`);
    console.log(`[backfill] pending final_price: ${missingFinalPriceMarkets.length}`);

    for (const market of missingPriceToBeatMarkets) {
      const openPrice = await fetchOpenPrice(market);

      if (openPrice !== null) {
        const updates: MarketRecord[] = [{ ...market, priceToBeat: openPrice }];
        const previous = await repository.getPreviousMarketForFinalPrice({ asset: market.asset, window: market.window, marketStartTs: market.marketStartTs });

        if (previous) {
          updates.push({ ...previous, finalPrice: openPrice });
          filledFinalPriceCount += 1;
        }

        await repository.upsertMarkets(updates);
        filledPriceToBeatCount += 1;
      }

      await sleep(REQUEST_DELAY_MS);
    }

    for (const market of missingFinalPriceMarkets) {
      const nextMarket = await repository.getNextMarketWithPriceToBeat({ asset: market.asset, window: market.window, marketStartTs: market.marketStartTs });
      const nextPriceToBeat = nextMarket?.priceToBeat ?? null;

      if (nextPriceToBeat !== null) {
        await repository.upsertMarkets([{ ...market, finalPrice: nextPriceToBeat }]);
        filledFinalPriceCount += 1;
      }
    }

    console.log(`[backfill] filled price_to_beat: ${filledPriceToBeatCount}`);
    console.log(`[backfill] filled final_price: ${filledFinalPriceCount}`);
    assert.equal(true, true);
  }
});
