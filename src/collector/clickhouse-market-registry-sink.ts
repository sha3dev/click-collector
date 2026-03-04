/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { MarketRegistryRepository } from "../clickhouse/market-registry-repository.ts";
import type { MarketRecord } from "../markets/market-events-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type ClickHouseMarketRegistrySinkOptions = { repository: MarketRegistryRepository };

export class ClickHouseMarketRegistrySink {
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

  private readonly repository: MarketRegistryRepository;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: ClickHouseMarketRegistrySinkOptions) {
    this.repository = options.repository;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: ClickHouseMarketRegistrySinkOptions): ClickHouseMarketRegistrySink {
    const sink = new ClickHouseMarketRegistrySink(options);
    return sink;
  }

  /**
   * @section private:methods
   */

  // empty

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async upsert(markets: MarketRecord[]): Promise<void> {
    await this.repository.upsertMarkets(markets);
  }

  /**
   * @section static:methods
   */

  // empty
}
