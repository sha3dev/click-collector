/**
 * @section imports:externals
 */

import type { CryptoFeedClient, FeedEvent, Subscription } from "@sha3/crypto";
import type { MarketEvent as PolymarketStreamEvent, PolymarketClient, PolymarketMarket } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import type { MarketEvent, MarketRecord } from "../markets/market-events-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type TickSinkContract = { writeTicks(events: MarketEvent[]): Promise<void> };

export type MarketRegistrySinkContract = { upsert(markets: MarketRecord[]): Promise<void> };

export type CryptoClientContract = Pick<CryptoFeedClient, "connect" | "disconnect" | "subscribe">;
export type CryptoFeedEvent = FeedEvent;
export type CryptoFeedSubscription = Subscription;

export type PolymarketClientContract = Pick<PolymarketClient, "connect" | "disconnect" | "markets" | "stream">;
export type PolymarketMarketModel = PolymarketMarket;
export type PolymarketFeedEvent = PolymarketStreamEvent;

export type AssetMarketContext = {
  assetId: string;
  slug: string;
  asset: MarketRecord["asset"];
  window: MarketRecord["window"];
  tokenSide: "up" | "down";
  marketStartTs: number;
  marketEndTs: number;
};
