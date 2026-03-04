/**
 * @section imports:externals
 */

import { createHash } from "node:crypto";
import { CryptoFeedClient } from "@sha3/crypto";

/**
 * @section imports:internals
 */

import CONFIG from "../config.ts";
import type { AssetSymbol, MarketEvent } from "../markets/market-events-types.ts";
import { CollectorRuntimeError } from "./collector-errors.ts";
import type { CryptoClientContract, CryptoFeedEvent, CryptoFeedSubscription, TickSinkContract } from "./collector-types.ts";

/**
 * @section consts
 */

const TRACKED_ASSETS = new Set<AssetSymbol>(CONFIG.SUPPORTED_ASSETS);

/**
 * @section types
 */

type CryptoTicksIngestionServiceOptions = { tickSink: TickSinkContract; client?: CryptoClientContract };

export class CryptoTicksIngestionService {
  /**
   * @section private:attributes
   */

  private subscription: CryptoFeedSubscription | null;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly tickSink: TickSinkContract;
  private readonly client: CryptoClientContract;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: CryptoTicksIngestionServiceOptions) {
    this.tickSink = options.tickSink;
    this.client = options.client ?? CryptoFeedClient.create({ symbols: [...CONFIG.SUPPORTED_ASSETS], providers: [...CONFIG.CRYPTO_PROVIDERS] });
    this.subscription = null;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: CryptoTicksIngestionServiceOptions): CryptoTicksIngestionService {
    const service = new CryptoTicksIngestionService(options);
    return service;
  }

  /**
   * @section private:methods
   */

  private static createEventId(input: string): string {
    const eventId = createHash("sha256").update(input).digest("hex");
    return eventId;
  }

  private static normalizeAsset(symbol: string): AssetSymbol | null {
    const normalized = symbol.trim().toLowerCase();
    let asset: AssetSymbol | null = null;

    if (TRACKED_ASSETS.has(normalized as AssetSymbol)) {
      asset = normalized as AssetSymbol;
    }

    return asset;
  }

  private static serializeOrderBook(asks: { price: number; size: number }[], bids: { price: number; size: number }[]): string {
    const payload = JSON.stringify({ asks, bids });
    return payload;
  }

  private static toMarketEvent(event: CryptoFeedEvent): MarketEvent | null {
    let mapped: MarketEvent | null = null;

    if (event.type === "price" || event.type === "orderbook") {
      const asset = CryptoTicksIngestionService.normalizeAsset(event.symbol);

      if (asset) {
        const sourceCategory = event.provider === "chainlink" ? "chainlink" : "exchange";
        const orderbook = event.type === "orderbook" ? CryptoTicksIngestionService.serializeOrderBook(event.asks, event.bids) : null;
        const price = event.type === "price" ? event.price : null;
        const payloadJson = JSON.stringify(event);
        const eventId = CryptoTicksIngestionService.createEventId(`${event.provider}:${event.symbol}:${event.ts}:${event.type}:${payloadJson}`);

        mapped = {
          eventId,
          eventTs: event.ts,
          sourceCategory,
          sourceName: event.provider,
          eventType: event.type,
          asset,
          window: null,
          marketSlug: null,
          tokenSide: null,
          price,
          orderbook,
          payloadJson
        };
      }
    }

    return mapped;
  }

  private onFeedEvent(event: CryptoFeedEvent): void {
    const mappedEvent = CryptoTicksIngestionService.toMarketEvent(event);

    if (mappedEvent) {
      const writePromise = this.tickSink.writeTicks([mappedEvent]);

      void writePromise.catch((error: unknown) => {
        const reason = error instanceof Error ? error.message : String(error);
        throw CollectorRuntimeError.forBoundary("crypto-write", reason);
      });
    }
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async start(): Promise<void> {
    this.subscription = this.client.subscribe((event) => {
      this.onFeedEvent(event);
    });

    await this.client.connect();
  }

  public async stop(): Promise<void> {
    if (this.subscription) {
      this.subscription.unsubscribe();
      this.subscription = null;
    }

    await this.client.disconnect();
  }

  /**
   * @section static:methods
   */

  // empty
}
