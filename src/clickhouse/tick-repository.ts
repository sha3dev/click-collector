/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import CONFIG from "../config.ts";
import type { MarketEvent } from "../markets/market-events-types.ts";
import { ClickHouseInsertError, ClickHouseQueryError } from "./clickhouse-errors.ts";
import type { ClickHouseClientContract, ClickHouseQueryResult, TickInsertRow, TickSelectRow, TickWriteModel } from "./clickhouse-types.ts";

/**
 * @section consts
 */

const TICKS_TABLE = CONFIG.CLICKHOUSE_TICKS_TABLE;

/**
 * @section types
 */

type TickRepositoryOptions = { client: ClickHouseClientContract };

type TickRangeQueryOptions = { slug: string; asset: string; fromTs: number; toTs: number };

export class TickRepository {
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

  public constructor(options: TickRepositoryOptions) {
    this.client = options.client;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: TickRepositoryOptions): TickRepository {
    const repository = new TickRepository(options);
    return repository;
  }

  /**
   * @section private:methods
   */

  private static toDateTime64Text(timestampMs: number): string {
    const value = new Date(timestampMs).toISOString().replace("T", " ").replace("Z", "");
    return value;
  }

  private static toInsertRow(model: TickWriteModel): TickInsertRow {
    const row: TickInsertRow = {
      event_id: model.eventId,
      event_ts: TickRepository.toDateTime64Text(model.eventTs),
      ingested_at: TickRepository.toDateTime64Text(Date.now()),
      source_category: model.sourceCategory,
      source_name: model.sourceName,
      event_type: model.eventType,
      asset: model.asset,
      window: model.window,
      market_slug: model.marketSlug,
      token_side: model.tokenSide,
      price: model.price,
      orderbook: model.orderbook,
      payload_json: model.payloadJson
    };

    return row;
  }

  private static fromRowToEvent(row: TickSelectRow): MarketEvent {
    const eventTs = Date.parse(row.event_ts.replace(" ", "T").concat("Z"));
    const event: MarketEvent = {
      eventId: row.event_id,
      eventTs,
      sourceCategory: row.source_category,
      sourceName: row.source_name,
      eventType: row.event_type,
      asset: row.asset,
      window: row.window,
      marketSlug: row.market_slug,
      tokenSide: row.token_side,
      price: row.price,
      orderbook: row.orderbook,
      payloadJson: row.payload_json
    };

    return event;
  }

  private static toRows(result: ClickHouseQueryResult<TickSelectRow>): TickSelectRow[] {
    let rows: TickSelectRow[];

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
      CREATE TABLE IF NOT EXISTS ${TICKS_TABLE} (
        event_id String,
        event_ts DateTime64(3, 'UTC'),
        ingested_at DateTime64(3, 'UTC'),
        source_category LowCardinality(String),
        source_name LowCardinality(String),
        event_type LowCardinality(String),
        asset LowCardinality(String),
        window Nullable(String),
        market_slug Nullable(String),
        token_side Nullable(String),
        price Nullable(Float64),
        orderbook Nullable(String),
        payload_json String
      )
      ENGINE = MergeTree
      PARTITION BY toYYYYMM(event_ts)
      ORDER BY (asset, event_ts, source_category, source_name, event_type, event_id)
    `;

    await this.client.command({ query });
  }

  public async insertTicks(events: MarketEvent[]): Promise<void> {
    const rows = events.map((event) => {
      const row = TickRepository.toInsertRow(event);
      return row;
    });

    if (rows.length > 0) {
      try {
        await this.client.insert({ table: TICKS_TABLE, values: rows, format: "JSONEachRow" });
      } catch (error) {
        throw ClickHouseInsertError.forTable(TICKS_TABLE, this.stringifyError(error));
      }
    }
  }

  public async getRelatedEventsByMarketRange(options: TickRangeQueryOptions): Promise<MarketEvent[]> {
    const escapedSlug = this.escapeLiteral(options.slug);
    const escapedAsset = this.escapeLiteral(options.asset);
    const fromIsoText = TickRepository.toDateTime64Text(options.fromTs);
    const toIsoText = TickRepository.toDateTime64Text(options.toTs);
    const query = `
      SELECT event_id, event_ts, ingested_at, source_category, source_name, event_type, asset, window, market_slug, token_side, price, orderbook, payload_json
      FROM ${TICKS_TABLE}
      WHERE
        (source_category = 'polymarket' AND market_slug = '${escapedSlug}')
        OR
        (source_category IN ('exchange', 'chainlink') AND asset = '${escapedAsset}' AND event_ts >= toDateTime64('${fromIsoText}', 3, 'UTC') AND event_ts <= toDateTime64('${toIsoText}', 3, 'UTC'))
      ORDER BY event_ts ASC, source_category ASC, source_name ASC, event_id ASC
    `;

    let events: MarketEvent[] = [];

    try {
      const resultSet = await this.client.query({ query, format: "JSONEachRow" });
      const result = await resultSet.json<TickSelectRow>();
      const rows = TickRepository.toRows(result);
      events = rows.map((row) => {
        const event = TickRepository.fromRowToEvent(row);
        return event;
      });
    } catch (error) {
      throw ClickHouseQueryError.forOperation("getRelatedEventsByMarketRange", this.stringifyError(error));
    }

    return events;
  }

  /**
   * @section static:methods
   */

  // empty
}
