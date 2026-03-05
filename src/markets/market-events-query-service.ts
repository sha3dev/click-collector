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
import { CRYPTO_SOURCE_NAMES } from "./market-events-types.ts";
import type {
  AssetSymbol,
  CryptoSourceName,
  MarketEvent,
  MarketRecord,
  MarketSnapshot,
  MarketWindow,
  SnapshotAssetState,
  SnapshotEventState
} from "./market-events-types.ts";

/**
 * @section consts
 */

const SOURCE_CATEGORY_ORDER = { chainlink: 0, exchange: 1, polymarket: 2 } as const;

/**
 * @section types
 */

type MarketEventsQueryServiceOptions = {
  marketRegistryRepository: Pick<MarketRegistryRepository, "listMarkets" | "getMarketBoundsBySlug">;
  tickRepository: Pick<TickRepository, "getRelatedEventsByMarketRange" | "getAllAssetsEventsByMarketRange">;
};

type MarketSnapshotState = Pick<MarketSnapshot, "crypto" | "polymarket">;

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

  private readonly marketRegistryRepository: Pick<MarketRegistryRepository, "listMarkets" | "getMarketBoundsBySlug">;
  private readonly tickRepository: Pick<TickRepository, "getRelatedEventsByMarketRange" | "getAllAssetsEventsByMarketRange">;

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

  private static createEmptySnapshotEventState(): SnapshotEventState {
    const state: SnapshotEventState = { price: null, orderbook: null };
    return state;
  }

  private static createEmptySnapshotAssetState(): SnapshotAssetState {
    const state: SnapshotAssetState = {
      binance: MarketEventsQueryService.createEmptySnapshotEventState(),
      coinbase: MarketEventsQueryService.createEmptySnapshotEventState(),
      kraken: MarketEventsQueryService.createEmptySnapshotEventState(),
      okx: MarketEventsQueryService.createEmptySnapshotEventState(),
      chainlink: MarketEventsQueryService.createEmptySnapshotEventState()
    };
    return state;
  }

  private static createEmptySnapshotState(): MarketSnapshotState {
    const state: MarketSnapshotState = {
      crypto: {
        btc: MarketEventsQueryService.createEmptySnapshotAssetState(),
        eth: MarketEventsQueryService.createEmptySnapshotAssetState(),
        sol: MarketEventsQueryService.createEmptySnapshotAssetState(),
        xrp: MarketEventsQueryService.createEmptySnapshotAssetState()
      },
      polymarket: { up: MarketEventsQueryService.createEmptySnapshotEventState(), down: MarketEventsQueryService.createEmptySnapshotEventState() }
    };
    return state;
  }

  private static cloneSnapshotEventState(state: SnapshotEventState): SnapshotEventState {
    const clonedState: SnapshotEventState = { price: state.price, orderbook: state.orderbook };
    return clonedState;
  }

  private static cloneSnapshotAssetState(state: SnapshotAssetState): SnapshotAssetState {
    const clonedState: SnapshotAssetState = {
      binance: MarketEventsQueryService.cloneSnapshotEventState(state.binance),
      coinbase: MarketEventsQueryService.cloneSnapshotEventState(state.coinbase),
      kraken: MarketEventsQueryService.cloneSnapshotEventState(state.kraken),
      okx: MarketEventsQueryService.cloneSnapshotEventState(state.okx),
      chainlink: MarketEventsQueryService.cloneSnapshotEventState(state.chainlink)
    };
    return clonedState;
  }

  private static cloneSnapshotState(state: MarketSnapshotState): MarketSnapshotState {
    const clonedState: MarketSnapshotState = {
      crypto: {
        btc: MarketEventsQueryService.cloneSnapshotAssetState(state.crypto.btc),
        eth: MarketEventsQueryService.cloneSnapshotAssetState(state.crypto.eth),
        sol: MarketEventsQueryService.cloneSnapshotAssetState(state.crypto.sol),
        xrp: MarketEventsQueryService.cloneSnapshotAssetState(state.crypto.xrp)
      },
      polymarket: {
        up: MarketEventsQueryService.cloneSnapshotEventState(state.polymarket.up),
        down: MarketEventsQueryService.cloneSnapshotEventState(state.polymarket.down)
      }
    };
    return clonedState;
  }

  private static isCryptoSourceName(sourceName: string): sourceName is CryptoSourceName {
    const matchedSourceName = CRYPTO_SOURCE_NAMES.find((candidate) => {
      return candidate === sourceName;
    });
    const isKnown = typeof matchedSourceName === "string";
    return isKnown;
  }

  private static compareEvents(left: MarketEvent, right: MarketEvent): number {
    let comparison = left.eventTs - right.eventTs;

    if (comparison === 0) {
      const leftCategoryOrder = SOURCE_CATEGORY_ORDER[left.sourceCategory];
      const rightCategoryOrder = SOURCE_CATEGORY_ORDER[right.sourceCategory];
      comparison = leftCategoryOrder - rightCategoryOrder;
    }

    if (comparison === 0) {
      comparison = left.sourceName.localeCompare(right.sourceName);
    }

    if (comparison === 0) {
      comparison = left.eventId.localeCompare(right.eventId);
    }

    return comparison;
  }

  private static applyEventToSnapshotState(state: MarketSnapshotState, event: MarketEvent): void {
    if (event.sourceCategory === "polymarket") {
      const tokenSide = event.tokenSide;

      if (tokenSide === "up" || tokenSide === "down") {
        state.polymarket[tokenSide][event.eventType] = event;
      }
    }

    if (event.sourceCategory === "exchange" || event.sourceCategory === "chainlink") {
      const sourceName = event.sourceName;

      if (MarketEventsQueryService.isCryptoSourceName(sourceName)) {
        state.crypto[event.asset][sourceName][event.eventType] = event;
      }
    }
  }

  private static buildSnapshot(
    triggerEvent: MarketEvent,
    state: MarketSnapshotState,
    market: { asset: AssetSymbol; window: MarketWindow; marketStartTs: number; marketEndTs: number; priceToBeat: number | null }
  ): MarketSnapshot {
    const snapshotState = MarketEventsQueryService.cloneSnapshotState(state);
    const snapshot: MarketSnapshot = {
      triggerEvent,
      snapshotTs: triggerEvent.eventTs,
      asset: market.asset,
      window: market.window,
      marketStartTs: market.marketStartTs,
      marketEndTs: market.marketEndTs,
      priceToBeat: market.priceToBeat,
      crypto: snapshotState.crypto,
      polymarket: snapshotState.polymarket
    };
    return snapshot;
  }

  private static buildMarketSnapshots(
    triggerEvents: MarketEvent[],
    allEvents: MarketEvent[],
    market: { asset: AssetSymbol; window: MarketWindow; marketStartTs: number; marketEndTs: number; priceToBeat: number | null }
  ): MarketSnapshot[] {
    const snapshots: MarketSnapshot[] = [];
    const state = MarketEventsQueryService.createEmptySnapshotState();
    let allEventsIndex = 0;

    for (const triggerEvent of triggerEvents) {
      let shouldAdvance = allEventsIndex < allEvents.length;

      while (shouldAdvance) {
        const currentEvent = allEvents[allEventsIndex];
        let compareResult = 1;

        if (currentEvent) {
          compareResult = MarketEventsQueryService.compareEvents(currentEvent, triggerEvent);
        }

        if (compareResult <= 0) {
          if (currentEvent) {
            MarketEventsQueryService.applyEventToSnapshotState(state, currentEvent);
            allEventsIndex += 1;
            shouldAdvance = allEventsIndex < allEvents.length;
          } else {
            shouldAdvance = false;
          }
        } else {
          shouldAdvance = false;
        }
      }

      const snapshot = MarketEventsQueryService.buildSnapshot(triggerEvent, state, market);
      snapshots.push(snapshot);
    }

    return snapshots;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async listMarkets(window: MarketWindow, asset: AssetSymbol): Promise<MarketRecord[]> {
    const markets = await this.marketRegistryRepository.listMarkets(window, asset);
    return markets;
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

  public async getMarketSnapshots(slug: string): Promise<MarketSnapshot[]> {
    const marketBounds = await this.marketRegistryRepository.getMarketBoundsBySlug(slug);
    let snapshots: MarketSnapshot[];

    if (marketBounds) {
      const triggerEvents = await this.tickRepository.getRelatedEventsByMarketRange({
        slug: marketBounds.slug,
        asset: marketBounds.asset,
        fromTs: marketBounds.marketStartTs,
        toTs: marketBounds.marketEndTs
      });
      const allEvents = await this.tickRepository.getAllAssetsEventsByMarketRange({
        slug: marketBounds.slug,
        fromTs: marketBounds.marketStartTs,
        toTs: marketBounds.marketEndTs
      });
      snapshots = MarketEventsQueryService.buildMarketSnapshots(triggerEvents, allEvents, {
        asset: marketBounds.asset,
        window: marketBounds.window,
        marketStartTs: marketBounds.marketStartTs,
        marketEndTs: marketBounds.marketEndTs,
        priceToBeat: marketBounds.priceToBeat
      });
    } else {
      throw MarketNotFoundError.forSlug(slug);
    }

    return snapshots;
  }

  /**
   * @section static:methods
   */

  // empty
}
