/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

// empty

/**
 * @section consts
 */

export const ASSET_SYMBOLS = ["btc", "eth", "sol", "xrp"] as const;
export const MARKET_WINDOWS = ["5m", "15m"] as const;
export const CRYPTO_SOURCE_NAMES = ["binance", "coinbase", "kraken", "okx", "chainlink"] as const;

/**
 * @section types
 */

export type AssetSymbol = (typeof ASSET_SYMBOLS)[number];
export type MarketWindow = (typeof MARKET_WINDOWS)[number];
export type CryptoSourceName = (typeof CRYPTO_SOURCE_NAMES)[number];
export type EventType = "price" | "orderbook";
export type SourceCategory = "exchange" | "chainlink" | "polymarket";
export type TokenSide = "up" | "down";

export type MarketEvent = {
  eventId: string;
  eventTs: number;
  sourceCategory: SourceCategory;
  sourceName: string;
  eventType: EventType;
  asset: AssetSymbol;
  window: MarketWindow | null;
  marketSlug: string | null;
  tokenSide: TokenSide | null;
  price: number | null;
  orderbook: string | null;
  payloadJson: string;
  isTest: boolean;
};

export type MarketRecord = {
  slug: string;
  asset: AssetSymbol;
  window: MarketWindow;
  marketStartTs: number;
  marketEndTs: number;
  upAssetId: string;
  downAssetId: string;
  priceToBeat: number | null;
  finalPrice: number | null;
  isTest: boolean;
};

export type SnapshotEventState = { price: MarketEvent | null; orderbook: MarketEvent | null };

export type SnapshotAssetState = {
  binance: SnapshotEventState;
  coinbase: SnapshotEventState;
  kraken: SnapshotEventState;
  okx: SnapshotEventState;
  chainlink: SnapshotEventState;
};

export type MarketSnapshot = {
  triggerEvent: MarketEvent;
  snapshotTs: number;
  asset: AssetSymbol;
  window: MarketWindow;
  marketStartTs: number;
  marketEndTs: number;
  priceToBeat: number | null;
  finalPrice: number | null;
  crypto: { btc: SnapshotAssetState; eth: SnapshotAssetState; sol: SnapshotAssetState; xrp: SnapshotAssetState };
  polymarket: { up: SnapshotEventState; down: SnapshotEventState };
};
