/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import { ClickHouseClientFactory } from "../clickhouse/clickhouse-client-factory.ts";
import type { ClickHouseClientContract } from "../clickhouse/clickhouse-types.ts";
import { MarketRegistryRepository } from "../clickhouse/market-registry-repository.ts";
import { TickRepository } from "../clickhouse/tick-repository.ts";
import { MarketNotFoundError } from "./market-events-errors.ts";
import type { AssetSymbol, MarketEvent, MarketWindow } from "./market-events-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type MarketEventsQueryServiceOptions = {
  marketRegistryRepository: Pick<MarketRegistryRepository, "listMarketSlugs" | "getMarketBoundsBySlug">;
  tickRepository: Pick<TickRepository, "getRelatedEventsByMarketRange">;
};

export class MarketEventsQueryService {
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

  private readonly marketRegistryRepository: Pick<MarketRegistryRepository, "listMarketSlugs" | "getMarketBoundsBySlug">;
  private readonly tickRepository: Pick<TickRepository, "getRelatedEventsByMarketRange">;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: MarketEventsQueryServiceOptions) {
    this.marketRegistryRepository = options.marketRegistryRepository;
    this.tickRepository = options.tickRepository;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: MarketEventsQueryServiceOptions): MarketEventsQueryService {
    const service = new MarketEventsQueryService(options);
    return service;
  }

  public static createDefault(): MarketEventsQueryService {
    const clickHouseClient: ClickHouseClientContract = ClickHouseClientFactory.create();
    const marketRegistryRepository = MarketRegistryRepository.create({ client: clickHouseClient });
    const tickRepository = TickRepository.create({ client: clickHouseClient });
    const service = new MarketEventsQueryService({ marketRegistryRepository, tickRepository });
    return service;
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

  public async listMarkets(window: MarketWindow, asset: AssetSymbol): Promise<string[]> {
    const slugs = await this.marketRegistryRepository.listMarketSlugs(window, asset);
    return slugs;
  }

  public async getMarketEvents(slug: string): Promise<MarketEvent[]> {
    const marketBounds = await this.marketRegistryRepository.getMarketBoundsBySlug(slug);
    let events: MarketEvent[];

    if (marketBounds) {
      events = await this.tickRepository.getRelatedEventsByMarketRange({
        slug: marketBounds.slug,
        asset: marketBounds.asset,
        fromTs: marketBounds.marketStartTs,
        toTs: marketBounds.marketEndTs
      });
    } else {
      throw MarketNotFoundError.forSlug(slug);
    }

    return events;
  }

  /**
   * @section static:methods
   */

  // empty
}
