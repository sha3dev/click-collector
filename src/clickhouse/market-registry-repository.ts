/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import CONFIG from "../config.ts";
import type { AssetSymbol, MarketRecord, MarketWindow } from "../markets/market-events-types.ts";
import { ClickHouseInsertError, ClickHouseQueryError } from "./clickhouse-errors.ts";
import type {
  ClickHouseClientContract,
  ClickHouseQueryResult,
  MarketBounds,
  MarketRegistryInsertRow,
  MarketRegistrySelectRow,
  MarketRegistryWriteModel
} from "./clickhouse-types.ts";

/**
 * @section consts
 */

const MARKET_REGISTRY_TABLE = CONFIG.CLICKHOUSE_MARKET_REGISTRY_TABLE;

/**
 * @section types
 */

type MarketRegistryRepositoryOptions = { client: ClickHouseClientContract };
type PendingPriceToBeatQueryOptions = { nowTs: number; limit: number };
type PendingFinalPriceQueryOptions = { nowTs: number; limit: number };
type PreviousMarketQueryOptions = { asset: AssetSymbol; window: MarketWindow; marketStartTs: number };
type NextMarketWithPriceToBeatQueryOptions = { asset: AssetSymbol; window: MarketWindow; marketStartTs: number };

export class MarketRegistryRepository {
  /**
   * @section private:attributes
   */

  // empty

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly client: ClickHouseClientContract;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: MarketRegistryRepositoryOptions) {
    this.client = options.client;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: MarketRegistryRepositoryOptions): MarketRegistryRepository {
    const repository = new MarketRegistryRepository(options);
    return repository;
  }

  /**
   * @section private:methods
   */

  private static toDateTime64Text(timestampMs: number): string {
    const value = new Date(timestampMs).toISOString().replace("T", " ").replace("Z", "");
    return value;
  }

  private static fromRowToBounds(row: MarketRegistrySelectRow): MarketBounds {
    const marketStartTs = Date.parse(row.market_start_ts.replace(" ", "T").concat("Z"));
    const marketEndTs = Date.parse(row.market_end_ts.replace(" ", "T").concat("Z"));
    const bounds: MarketBounds = {
      slug: row.slug,
      asset: row.asset,
      window: row.window,
      marketStartTs,
      marketEndTs,
      priceToBeat: row.price_to_beat,
      finalPrice: row.final_price
    };

    return bounds;
  }

  private static fromRowToMarketRecord(row: MarketRegistrySelectRow): MarketRecord {
    const marketStartTs = Date.parse(row.market_start_ts.replace(" ", "T").concat("Z"));
    const marketEndTs = Date.parse(row.market_end_ts.replace(" ", "T").concat("Z"));
    const record: MarketRecord = {
      slug: row.slug,
      asset: row.asset,
      window: row.window,
      marketStartTs,
      marketEndTs,
      upAssetId: row.up_asset_id,
      downAssetId: row.down_asset_id,
      priceToBeat: row.price_to_beat,
      finalPrice: row.final_price,
      isTest: row.is_test === 1
    };

    return record;
  }

  private static toInsertRow(model: MarketRegistryWriteModel): MarketRegistryInsertRow {
    const nowIsoText = MarketRegistryRepository.toDateTime64Text(Date.now());
    const row: MarketRegistryInsertRow = {
      slug: model.slug,
      asset: model.asset,
      window: model.window,
      market_start_ts: MarketRegistryRepository.toDateTime64Text(model.marketStartTs),
      market_end_ts: MarketRegistryRepository.toDateTime64Text(model.marketEndTs),
      up_asset_id: model.upAssetId,
      down_asset_id: model.downAssetId,
      price_to_beat: model.priceToBeat,
      final_price: model.finalPrice,
      created_at: nowIsoText,
      updated_at: nowIsoText,
      is_test: model.isTest ? 1 : 0
    };

    return row;
  }

  private static toRows(result: ClickHouseQueryResult<MarketRegistrySelectRow>): MarketRegistrySelectRow[] {
    let rows: MarketRegistrySelectRow[];

    if (Array.isArray(result)) {
      rows = result;
    } else {
      rows = result.data ?? [];
    }

    return rows;
  }

  private escapeLiteral(value: string): string {
    const escapedValue = value.replace(/'/g, "''");
    return escapedValue;
  }

  private stringifyError(error: unknown): string {
    const message = error instanceof Error ? error.message : String(error);
    return message;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async ensureSchema(): Promise<void> {
    const query = `
      CREATE TABLE IF NOT EXISTS ${MARKET_REGISTRY_TABLE} (
        slug String,
        asset LowCardinality(String),
        window LowCardinality(String),
        market_start_ts DateTime64(3, 'UTC'),
        market_end_ts DateTime64(3, 'UTC'),
        up_asset_id String,
        down_asset_id String,
        price_to_beat Nullable(Float64),
        final_price Nullable(Float64),
        created_at DateTime64(3, 'UTC'),
        updated_at DateTime64(3, 'UTC'),
        is_test UInt8 DEFAULT 0
      )
      ENGINE = ReplacingMergeTree(updated_at)
      PARTITION BY asset
      ORDER BY (slug)
    `;

    await this.client.command({ query });
    await this.client.command({ query: `ALTER TABLE ${MARKET_REGISTRY_TABLE} ADD COLUMN IF NOT EXISTS is_test UInt8 DEFAULT 0` });
    await this.client.command({ query: `ALTER TABLE ${MARKET_REGISTRY_TABLE} ADD COLUMN IF NOT EXISTS price_to_beat Nullable(Float64)` });
    await this.client.command({ query: `ALTER TABLE ${MARKET_REGISTRY_TABLE} ADD COLUMN IF NOT EXISTS final_price Nullable(Float64)` });
  }

  public async upsertMarkets(markets: MarketRecord[]): Promise<void> {
    const rows = markets.map((market) => {
      const row = MarketRegistryRepository.toInsertRow(market);
      return row;
    });

    if (rows.length > 0) {
      try {
        await this.client.insert({ table: MARKET_REGISTRY_TABLE, values: rows, format: "JSONEachRow" });
      } catch (error) {
        throw ClickHouseInsertError.forTable(MARKET_REGISTRY_TABLE, this.stringifyError(error));
      }
    }
  }

  public async getMarketBoundsBySlug(slug: string): Promise<MarketBounds | null> {
    const escapedSlug = this.escapeLiteral(slug);
    const query = `
      SELECT slug, asset, window, market_start_ts, market_end_ts, up_asset_id, down_asset_id, price_to_beat, final_price, created_at, updated_at, is_test
      FROM ${MARKET_REGISTRY_TABLE}
      WHERE slug = '${escapedSlug}'
      ORDER BY updated_at DESC
      LIMIT 1
    `;

    let marketBounds: MarketBounds | null = null;

    try {
      const resultSet = await this.client.query({ query, format: "JSONEachRow" });
      const result = await resultSet.json<MarketRegistrySelectRow>();
      const rows = MarketRegistryRepository.toRows(result);
      const row = rows.at(0);

      if (row) {
        marketBounds = MarketRegistryRepository.fromRowToBounds(row);
      }
    } catch (error) {
      throw ClickHouseQueryError.forOperation("getMarketBoundsBySlug", this.stringifyError(error));
    }

    return marketBounds;
  }

  public async listMarketSlugs(window: MarketWindow, asset: AssetSymbol): Promise<string[]> {
    const escapedWindow = this.escapeLiteral(window);
    const escapedAsset = this.escapeLiteral(asset);
    const query = `
      SELECT slug, asset, window, market_start_ts, market_end_ts, up_asset_id, down_asset_id, created_at, updated_at, is_test
      FROM ${MARKET_REGISTRY_TABLE}
      WHERE window = '${escapedWindow}' AND asset = '${escapedAsset}'
      ORDER BY market_start_ts DESC
    `;

    let slugs: string[] = [];

    try {
      const resultSet = await this.client.query({ query, format: "JSONEachRow" });
      const result = await resultSet.json<MarketRegistrySelectRow>();
      const rows = MarketRegistryRepository.toRows(result);
      slugs = rows.map((row) => {
        return row.slug;
      });
    } catch (error) {
      throw ClickHouseQueryError.forOperation("listMarketSlugs", this.stringifyError(error));
    }

    return slugs;
  }

  public async listMarkets(window: MarketWindow, asset: AssetSymbol): Promise<MarketRecord[]> {
    const escapedWindow = this.escapeLiteral(window);
    const escapedAsset = this.escapeLiteral(asset);
    const query = `
      SELECT slug, asset, window, market_start_ts, market_end_ts, up_asset_id, down_asset_id, price_to_beat, final_price, created_at, updated_at, is_test
      FROM ${MARKET_REGISTRY_TABLE}
      WHERE window = '${escapedWindow}' AND asset = '${escapedAsset}'
      ORDER BY market_start_ts DESC
    `;

    let markets: MarketRecord[] = [];

    try {
      const resultSet = await this.client.query({ query, format: "JSONEachRow" });
      const result = await resultSet.json<MarketRegistrySelectRow>();
      const rows = MarketRegistryRepository.toRows(result);
      markets = rows.map((row) => {
        const market = MarketRegistryRepository.fromRowToMarketRecord(row);
        return market;
      });
    } catch (error) {
      throw ClickHouseQueryError.forOperation("listMarkets", this.stringifyError(error));
    }

    return markets;
  }

  public async listPendingPriceToBeatMarkets(options: PendingPriceToBeatQueryOptions): Promise<MarketRecord[]> {
    const nowIsoText = MarketRegistryRepository.toDateTime64Text(options.nowTs);
    const query = `
      SELECT slug, asset, window, market_start_ts, market_end_ts, up_asset_id, down_asset_id, price_to_beat, final_price, created_at, updated_at, is_test
      FROM ${MARKET_REGISTRY_TABLE}
      WHERE price_to_beat IS NULL AND market_start_ts <= toDateTime64('${nowIsoText}', 3, 'UTC')
      ORDER BY market_start_ts ASC
      LIMIT ${options.limit}
    `;

    let markets: MarketRecord[] = [];

    try {
      const resultSet = await this.client.query({ query, format: "JSONEachRow" });
      const result = await resultSet.json<MarketRegistrySelectRow>();
      const rows = MarketRegistryRepository.toRows(result);
      markets = rows.map((row) => {
        const market = MarketRegistryRepository.fromRowToMarketRecord(row);
        return market;
      });
    } catch (error) {
      throw ClickHouseQueryError.forOperation("listPendingPriceToBeatMarkets", this.stringifyError(error));
    }

    return markets;
  }

  public async getPreviousMarketForFinalPrice(options: PreviousMarketQueryOptions): Promise<MarketRecord | null> {
    const escapedWindow = this.escapeLiteral(options.window);
    const escapedAsset = this.escapeLiteral(options.asset);
    const startIsoText = MarketRegistryRepository.toDateTime64Text(options.marketStartTs);
    const query = `
      SELECT slug, asset, window, market_start_ts, market_end_ts, up_asset_id, down_asset_id, price_to_beat, final_price, created_at, updated_at, is_test
      FROM ${MARKET_REGISTRY_TABLE}
      WHERE
        window = '${escapedWindow}'
        AND asset = '${escapedAsset}'
        AND market_start_ts < toDateTime64('${startIsoText}', 3, 'UTC')
      ORDER BY market_start_ts DESC, updated_at DESC
      LIMIT 1
    `;

    let market: MarketRecord | null = null;

    try {
      const resultSet = await this.client.query({ query, format: "JSONEachRow" });
      const result = await resultSet.json<MarketRegistrySelectRow>();
      const rows = MarketRegistryRepository.toRows(result);
      const row = rows.at(0);

      if (row) {
        market = MarketRegistryRepository.fromRowToMarketRecord(row);
      }
    } catch (error) {
      throw ClickHouseQueryError.forOperation("getPreviousMarketForFinalPrice", this.stringifyError(error));
    }

    return market;
  }

  public async listPendingFinalPriceMarkets(options: PendingFinalPriceQueryOptions): Promise<MarketRecord[]> {
    const nowIsoText = MarketRegistryRepository.toDateTime64Text(options.nowTs);
    const query = `
      SELECT slug, asset, window, market_start_ts, market_end_ts, up_asset_id, down_asset_id, price_to_beat, final_price, created_at, updated_at, is_test
      FROM ${MARKET_REGISTRY_TABLE}
      WHERE final_price IS NULL AND market_end_ts <= toDateTime64('${nowIsoText}', 3, 'UTC')
      ORDER BY market_start_ts ASC
      LIMIT ${options.limit}
    `;

    let markets: MarketRecord[] = [];

    try {
      const resultSet = await this.client.query({ query, format: "JSONEachRow" });
      const result = await resultSet.json<MarketRegistrySelectRow>();
      const rows = MarketRegistryRepository.toRows(result);
      markets = rows.map((row) => {
        const market = MarketRegistryRepository.fromRowToMarketRecord(row);
        return market;
      });
    } catch (error) {
      throw ClickHouseQueryError.forOperation("listPendingFinalPriceMarkets", this.stringifyError(error));
    }

    return markets;
  }

  public async getNextMarketWithPriceToBeat(options: NextMarketWithPriceToBeatQueryOptions): Promise<MarketRecord | null> {
    const escapedWindow = this.escapeLiteral(options.window);
    const escapedAsset = this.escapeLiteral(options.asset);
    const startIsoText = MarketRegistryRepository.toDateTime64Text(options.marketStartTs);
    const query = `
      SELECT slug, asset, window, market_start_ts, market_end_ts, up_asset_id, down_asset_id, price_to_beat, final_price, created_at, updated_at, is_test
      FROM ${MARKET_REGISTRY_TABLE}
      WHERE
        window = '${escapedWindow}'
        AND asset = '${escapedAsset}'
        AND market_start_ts > toDateTime64('${startIsoText}', 3, 'UTC')
        AND price_to_beat IS NOT NULL
      ORDER BY market_start_ts ASC, updated_at DESC
      LIMIT 1
    `;

    let market: MarketRecord | null = null;

    try {
      const resultSet = await this.client.query({ query, format: "JSONEachRow" });
      const result = await resultSet.json<MarketRegistrySelectRow>();
      const rows = MarketRegistryRepository.toRows(result);
      const row = rows.at(0);

      if (row) {
        market = MarketRegistryRepository.fromRowToMarketRecord(row);
      }
    } catch (error) {
      throw ClickHouseQueryError.forOperation("getNextMarketWithPriceToBeat", this.stringifyError(error));
    }

    return market;
  }

  /**
   * @section static:methods
   */

  // empty
}
