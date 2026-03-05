/**
 * @section imports:externals
 */

import { randomUUID } from "node:crypto";

/**
 * @section imports:internals
 */

import { ClickHouseClientFactory } from "../clickhouse/clickhouse-client-factory.ts";
import type { ClickHouseClientContract } from "../clickhouse/clickhouse-types.ts";
import LOGGER from "../logger.ts";
import { MarketRegistryRepository } from "../clickhouse/market-registry-repository.ts";
import { TickRepository } from "../clickhouse/tick-repository.ts";
import { MarketEventStream } from "./market-event-stream.ts";
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
type SnapshotListener = (snapshot: MarketSnapshot) => void;
type AddSnapshotListenerOptions = { window: MarketWindow; asset: AssetSymbol; listener: SnapshotListener };
type SnapshotListenerState = {
  id: string;
  window: MarketWindow;
  asset: AssetSymbol;
  listener: SnapshotListener;
  activeSlug: string | null;
  emittedEventIds: Set<string>;
  isSyncing: boolean;
  hasPendingSync: boolean;
};

export class MarketEventsQueryService {
  /**
   * @section private:attributes
   */

  private marketEventStreamListenerId: string | null;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly marketRegistryRepository: Pick<MarketRegistryRepository, "listMarkets" | "getMarketBoundsBySlug">;
  private readonly tickRepository: Pick<TickRepository, "getRelatedEventsByMarketRange" | "getAllAssetsEventsByMarketRange">;
  private readonly snapshotListenersById: Map<string, SnapshotListenerState>;

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
    this.snapshotListenersById = new Map<string, SnapshotListenerState>();
    this.marketEventStreamListenerId = null;
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
    market: { asset: AssetSymbol; window: MarketWindow; marketStartTs: number; marketEndTs: number; priceToBeat: number | null; finalPrice: number | null }
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
      finalPrice: market.finalPrice,
      crypto: snapshotState.crypto,
      polymarket: snapshotState.polymarket
    };
    return snapshot;
  }

  private static buildMarketSnapshots(
    triggerEvents: MarketEvent[],
    allEvents: MarketEvent[],
    market: { asset: AssetSymbol; window: MarketWindow; marketStartTs: number; marketEndTs: number; priceToBeat: number | null; finalPrice: number | null }
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

  private stringifyError(error: unknown): string {
    const message = error instanceof Error ? error.message : String(error);
    return message;
  }

  private ensureMarketEventStreamListener(): void {
    if (!this.marketEventStreamListenerId) {
      const listenerId = MarketEventStream.addListener((event) => {
        const syncPromise = this.onMarketEvent(event);
        void syncPromise;
      });
      this.marketEventStreamListenerId = listenerId;
    }
  }

  private cleanupMarketEventStreamListener(): void {
    if (this.marketEventStreamListenerId && this.snapshotListenersById.size === 0) {
      MarketEventStream.removeListener(this.marketEventStreamListenerId);
      this.marketEventStreamListenerId = null;
    }
  }

  private async findActiveMarket(window: MarketWindow, asset: AssetSymbol): Promise<MarketRecord | null> {
    const nowTs = Date.now();
    const markets = await this.marketRegistryRepository.listMarkets(window, asset);
    let activeMarket: MarketRecord | null = null;

    for (const market of markets) {
      const isActive = market.marketStartTs <= nowTs && market.marketEndTs >= nowTs;

      if (isActive) {
        activeMarket = market;
        break;
      }
    }

    return activeMarket;
  }

  private async emitUnseenSnapshots(listenerState: SnapshotListenerState, slug: string): Promise<void> {
    const snapshots = await this.getMarketSnapshots(slug);

    for (const snapshot of snapshots) {
      const eventId = snapshot.triggerEvent.eventId;

      if (!listenerState.emittedEventIds.has(eventId)) {
        listenerState.listener(snapshot);
        listenerState.emittedEventIds.add(eventId);
      }
    }
  }

  private async syncListenerWithActiveMarket(listenerState: SnapshotListenerState): Promise<void> {
    const activeMarket = await this.findActiveMarket(listenerState.window, listenerState.asset);

    if (activeMarket) {
      const hasMarketChanged = listenerState.activeSlug !== activeMarket.slug;

      if (hasMarketChanged) {
        listenerState.activeSlug = activeMarket.slug;
        listenerState.emittedEventIds.clear();
        const snapshots = await this.getMarketSnapshots(activeMarket.slug);
        for (const snapshot of snapshots) {
          listenerState.emittedEventIds.add(snapshot.triggerEvent.eventId);
        }
      } else {
        await this.emitUnseenSnapshots(listenerState, activeMarket.slug);
      }
    }
  }

  private async initializeListenerBaseline(listenerState: SnapshotListenerState): Promise<void> {
    const activeMarket = await this.findActiveMarket(listenerState.window, listenerState.asset);

    if (activeMarket) {
      listenerState.activeSlug = activeMarket.slug;
      const snapshots = await this.getMarketSnapshots(activeMarket.slug);

      for (const snapshot of snapshots) {
        listenerState.emittedEventIds.add(snapshot.triggerEvent.eventId);
      }
    }
  }

  private async runListenerSync(listenerState: SnapshotListenerState): Promise<void> {
    if (!listenerState.isSyncing) {
      listenerState.isSyncing = true;

      try {
        await this.syncListenerWithActiveMarket(listenerState);
      } catch (error) {
        const reason = this.stringifyError(error);
        LOGGER.error(`snapshot listener sync failed: id=${listenerState.id}; reason=${reason}`);
      } finally {
        listenerState.isSyncing = false;

        if (listenerState.hasPendingSync) {
          listenerState.hasPendingSync = false;
          const rerunPromise = this.runListenerSync(listenerState);
          void rerunPromise;
        }
      }
    } else {
      listenerState.hasPendingSync = true;
    }
  }

  private async onMarketEvent(event: MarketEvent): Promise<void> {
    for (const listenerState of this.snapshotListenersById.values()) {
      if (listenerState.asset === event.asset) {
        const syncPromise = this.runListenerSync(listenerState);
        void syncPromise;
      }
    }
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
        priceToBeat: marketBounds.priceToBeat,
        finalPrice: marketBounds.finalPrice
      });
    } else {
      throw MarketNotFoundError.forSlug(slug);
    }

    return snapshots;
  }

  public async addSnapshotListener(options: AddSnapshotListenerOptions): Promise<string> {
    const listenerId = randomUUID();
    const listenerState: SnapshotListenerState = {
      id: listenerId,
      window: options.window,
      asset: options.asset,
      listener: options.listener,
      activeSlug: null,
      emittedEventIds: new Set<string>(),
      isSyncing: false,
      hasPendingSync: false
    };

    this.snapshotListenersById.set(listenerId, listenerState);
    this.ensureMarketEventStreamListener();
    await this.initializeListenerBaseline(listenerState);

    return listenerId;
  }

  public removeSnapshotListener(listenerId: string): void {
    this.snapshotListenersById.delete(listenerId);
    this.cleanupMarketEventStreamListener();
  }

  /**
   * @section static:methods
   */

  // empty
}
