/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { AssetSymbol, EventType, MarketEvent, MarketRecord, MarketWindow, SourceCategory, TokenSide } from "../markets/market-events-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type TickInsertRow = {
  event_id: string;
  event_ts: string;
  ingested_at: string;
  source_category: SourceCategory;
  source_name: string;
  event_type: EventType;
  asset: AssetSymbol;
  window: MarketWindow | null;
  market_slug: string | null;
  token_side: TokenSide | null;
  payload_json: string;
  is_test: number;
};

export type MarketRegistryInsertRow = {
  slug: string;
  asset: AssetSymbol;
  window: MarketWindow;
  market_start_ts: string;
  market_end_ts: string;
  up_asset_id: string;
  down_asset_id: string;
  price_to_beat: number | null;
  final_price: number | null;
  created_at: string;
  updated_at: string;
  is_test: number;
};

export type TickSelectRow = TickInsertRow;
export type MarketRegistrySelectRow = MarketRegistryInsertRow;

export type MarketBounds = {
  slug: string;
  asset: AssetSymbol;
  window: MarketWindow;
  marketStartTs: number;
  marketEndTs: number;
  priceToBeat: number | null;
  finalPrice: number | null;
};

export type ClickHouseQueryResult<T> = { data?: T[] } | T[];

export type ClickHouseClientContract = {
  command(options: { query: string }): Promise<unknown>;
  insert(options: { table: string; values: Record<string, unknown>[]; format: "JSONEachRow" }): Promise<unknown>;
  query(options: { query: string; format: "JSONEachRow" }): Promise<{ json<T>(): Promise<ClickHouseQueryResult<T>> }>;
  close(): Promise<void>;
};

export type TickWriteModel = MarketEvent;
export type MarketRegistryWriteModel = MarketRecord;
