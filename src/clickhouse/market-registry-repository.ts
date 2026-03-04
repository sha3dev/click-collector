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
    const bounds: MarketBounds = { slug: row.slug, asset: row.asset, window: row.window, marketStartTs, marketEndTs };

    return bounds;
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
        created_at DateTime64(3, 'UTC'),
        updated_at DateTime64(3, 'UTC'),
        is_test UInt8 DEFAULT 0
      )
      ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY (slug)
    `;

    await this.client.command({ query });
    await this.client.command({ query: `ALTER TABLE ${MARKET_REGISTRY_TABLE} ADD COLUMN IF NOT EXISTS is_test UInt8 DEFAULT 0` });
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
      SELECT slug, asset, window, market_start_ts, market_end_ts, up_asset_id, down_asset_id, created_at, updated_at, is_test
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

  /**
   * @section static:methods
   */

  // empty
}
