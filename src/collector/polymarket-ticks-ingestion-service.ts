/**
 * @section imports:externals
 */

import { createHash } from "node:crypto";
import { PolymarketClient } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import CONFIG from "../config.ts";
import type { AssetSymbol, MarketEvent, MarketRecord, MarketWindow } from "../markets/market-events-types.ts";
import type { AssetMarketContext, PolymarketClientContract, PolymarketFeedEvent, PolymarketMarketModel } from "./collector-types.ts";

/**
 * @section consts
 */

const TRACKED_ASSETS = new Set<AssetSymbol>(CONFIG.SUPPORTED_ASSETS);

/**
 * @section types
 */

type PolymarketTicksIngestionServiceOptions = {
  tickSink: { writeTicks(events: MarketEvent[]): Promise<void> };
  marketRegistrySink: { upsert(markets: MarketRecord[]): Promise<void> };
  client?: PolymarketClientContract;
  nowFactory?: () => Date;
};

export class PolymarketTicksIngestionService {
  /**
   * @section private:attributes
   */

  private discoveryTimer: NodeJS.Timeout | null;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly tickSink: { writeTicks(events: MarketEvent[]): Promise<void> };
  private readonly marketRegistrySink: { upsert(markets: MarketRecord[]): Promise<void> };
  private readonly client: PolymarketClientContract;
  private readonly nowFactory: () => Date;
  private readonly contextByAssetId: Map<string, AssetMarketContext>;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: PolymarketTicksIngestionServiceOptions) {
    this.tickSink = options.tickSink;
    this.marketRegistrySink = options.marketRegistrySink;
    this.client = options.client ?? PolymarketClient.create();
    this.nowFactory = options.nowFactory ?? (() => new Date());
    this.contextByAssetId = new Map<string, AssetMarketContext>();
    this.discoveryTimer = null;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: PolymarketTicksIngestionServiceOptions): PolymarketTicksIngestionService {
    const service = new PolymarketTicksIngestionService(options);
    return service;
  }

  /**
   * @section private:methods
   */

  private static createEventId(input: string): string {
    const eventId = createHash("sha256").update(input).digest("hex");
    return eventId;
  }

  private static toAsset(symbol: string | null): AssetSymbol | null {
    const normalized = typeof symbol === "string" ? symbol.toLowerCase() : "";
    let asset: AssetSymbol | null = null;

    if (TRACKED_ASSETS.has(normalized as AssetSymbol)) {
      asset = normalized as AssetSymbol;
    }

    return asset;
  }

  private static toWindowFromSlug(slug: string): MarketWindow | null {
    let window: MarketWindow | null = null;

    if (slug.includes("-5m-")) {
      window = "5m";
    } else if (slug.includes("-15m-")) {
      window = "15m";
    }

    return window;
  }

  private static mapMarketRecord(market: PolymarketMarketModel): MarketRecord | null {
    const asset = PolymarketTicksIngestionService.toAsset(market.symbol);
    const window = PolymarketTicksIngestionService.toWindowFromSlug(market.slug);
    let record: MarketRecord | null = null;

    if (asset && window) {
      record = {
        slug: market.slug,
        asset,
        window,
        marketStartTs: market.start.getTime(),
        marketEndTs: market.end.getTime(),
        upAssetId: market.upTokenId,
        downAssetId: market.downTokenId,
        priceToBeat: null,
        finalPrice: null,
        isTest: false
      };
    }

    return record;
  }

  private static mapAssetContexts(market: MarketRecord): AssetMarketContext[] {
    const contexts: AssetMarketContext[] = [
      {
        assetId: market.upAssetId,
        slug: market.slug,
        asset: market.asset,
        window: market.window,
        tokenSide: "up",
        marketStartTs: market.marketStartTs,
        marketEndTs: market.marketEndTs
      },
      {
        assetId: market.downAssetId,
        slug: market.slug,
        asset: market.asset,
        window: market.window,
        tokenSide: "down",
        marketStartTs: market.marketStartTs,
        marketEndTs: market.marketEndTs
      }
    ];

    return contexts;
  }

  private static serializeOrderBook(asks: { price: number; size: number }[], bids: { price: number; size: number }[]): string {
    const limitedAsks = asks.slice(0, CONFIG.ORDERBOOK_MAX_LEVELS);
    const limitedBids = bids.slice(0, CONFIG.ORDERBOOK_MAX_LEVELS);
    const payload = JSON.stringify({ asks: limitedAsks, bids: limitedBids });
    return payload;
  }

  private mapStreamEvent(event: PolymarketFeedEvent): MarketEvent | null {
    const context = this.contextByAssetId.get(event.assetId) ?? null;
    let mapped: MarketEvent | null = null;

    if (context) {
      const eventType = event.type === "book" ? "orderbook" : "price";
      const orderbook = event.type === "book" ? PolymarketTicksIngestionService.serializeOrderBook(event.asks, event.bids) : null;
      const price = event.type === "price" ? event.price : null;
      const payloadJson = JSON.stringify(event);
      const eventId = PolymarketTicksIngestionService.createEventId(`polymarket:${event.assetId}:${event.index}:${event.date.toISOString()}:${payloadJson}`);

      mapped = {
        eventId,
        eventTs: event.date.getTime(),
        sourceCategory: "polymarket",
        sourceName: "polymarket",
        eventType,
        asset: context.asset,
        window: context.window,
        marketSlug: context.slug,
        tokenSide: context.tokenSide,
        price,
        orderbook,
        payloadJson,
        isTest: false
      };
    }

    return mapped;
  }

  private async discoverAndSubscribe(): Promise<void> {
    const allRecords: MarketRecord[] = [];
    const discoveredAssetIds: string[] = [];

    for (const window of CONFIG.SUPPORTED_WINDOWS) {
      const markets = await this.client.markets.loadCryptoWindowMarkets({ date: this.nowFactory(), window, symbols: [...CONFIG.SUPPORTED_ASSETS] });

      for (const market of markets) {
        const mapped = PolymarketTicksIngestionService.mapMarketRecord(market);

        if (mapped) {
          allRecords.push(mapped);
          const contexts = PolymarketTicksIngestionService.mapAssetContexts(mapped);

          for (const context of contexts) {
            this.contextByAssetId.set(context.assetId, context);
            discoveredAssetIds.push(context.assetId);
          }
        }
      }
    }

    await this.marketRegistrySink.upsert(allRecords);

    if (discoveredAssetIds.length > 0) {
      this.client.stream.subscribe({ assetIds: discoveredAssetIds });
    }
  }

  private scheduleDiscoveryLoop(): void {
    this.discoveryTimer = setInterval(() => {
      const refreshPromise = this.discoverAndSubscribe();
      void refreshPromise;
    }, CONFIG.POLYMARKET_DISCOVERY_INTERVAL_MS);
  }

  private stopDiscoveryLoop(): void {
    if (this.discoveryTimer) {
      clearInterval(this.discoveryTimer);
      this.discoveryTimer = null;
    }
  }

  private bindListener(): void {
    this.client.stream.addListener({
      listener: (event) => {
        const mappedEvent = this.mapStreamEvent(event);

        if (mappedEvent) {
          const writePromise = this.tickSink.writeTicks([mappedEvent]);
          void writePromise;
        }
      }
    });
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async start(): Promise<void> {
    this.bindListener();
    await this.client.connect();
    await this.discoverAndSubscribe();
    this.scheduleDiscoveryLoop();
  }

  public async stop(): Promise<void> {
    this.stopDiscoveryLoop();
    await this.client.disconnect();
  }

  /**
   * @section static:methods
   */

  // empty
}
