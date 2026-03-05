/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import CONFIG from "../config.ts";
import LOGGER from "../logger.ts";
import type { MarketRegistryRepository } from "../clickhouse/market-registry-repository.ts";
import type { MarketRecord } from "../markets/market-events-types.ts";
import { CollectorRuntimeError } from "./collector-errors.ts";

/**
 * @section consts
 */

const PRICE_TO_BEAT_BATCH_SIZE = 64;

/**
 * @section types
 */

type PriceToBeatApiResponse = { openPrice?: unknown };
type PriceToBeatVariant = "fiveminute" | "fifteen";
type PriceToBeatFetchResponse = { ok: boolean; status: number; json(): Promise<unknown>; text(): Promise<string> };
type PriceToBeatFetchFn = (url: string, options: { method: "GET" }) => Promise<PriceToBeatFetchResponse>;

type MarketRegistryPriceEnrichmentServiceOptions = {
  marketRegistryRepository: Pick<
    MarketRegistryRepository,
    "listPendingPriceToBeatMarkets" | "getPreviousMarketForFinalPrice" | "listPendingFinalPriceMarkets" | "getNextMarketWithPriceToBeat" | "upsertMarkets"
  >;
  fetchFn?: PriceToBeatFetchFn;
  nowFactory?: () => Date;
  pollIntervalMs?: number;
  startupBackfillEnabled?: boolean;
  startupBackfillLimit?: number;
  startupBackfillDelayMs?: number;
};

export class MarketRegistryPriceEnrichmentService {
  /**
   * @section private:attributes
   */

  private timer: NodeJS.Timeout | null;
  private isProcessing: boolean;
  private startupBackfillPromise: Promise<void> | null;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly marketRegistryRepository: Pick<
    MarketRegistryRepository,
    "listPendingPriceToBeatMarkets" | "getPreviousMarketForFinalPrice" | "listPendingFinalPriceMarkets" | "getNextMarketWithPriceToBeat" | "upsertMarkets"
  >;
  private readonly fetchFn: PriceToBeatFetchFn;
  private readonly nowFactory: () => Date;
  private readonly pollIntervalMs: number;
  private readonly startupBackfillEnabled: boolean;
  private readonly startupBackfillLimit: number;
  private readonly startupBackfillDelayMs: number;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: MarketRegistryPriceEnrichmentServiceOptions) {
    this.marketRegistryRepository = options.marketRegistryRepository;
    this.fetchFn = options.fetchFn ?? fetch;
    this.nowFactory = options.nowFactory ?? (() => new Date());
    this.pollIntervalMs = options.pollIntervalMs ?? CONFIG.PRICE_TO_BEAT_POLL_INTERVAL_MS;
    this.startupBackfillEnabled = options.startupBackfillEnabled ?? CONFIG.PRICE_TO_BEAT_STARTUP_BACKFILL_ENABLED === 1;
    this.startupBackfillLimit = options.startupBackfillLimit ?? CONFIG.PRICE_TO_BEAT_STARTUP_BACKFILL_LIMIT;
    this.startupBackfillDelayMs = options.startupBackfillDelayMs ?? CONFIG.PRICE_TO_BEAT_STARTUP_BACKFILL_DELAY_MS;
    this.timer = null;
    this.isProcessing = false;
    this.startupBackfillPromise = null;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: MarketRegistryPriceEnrichmentServiceOptions): MarketRegistryPriceEnrichmentService {
    const service = new MarketRegistryPriceEnrichmentService(options);
    return service;
  }

  /**
   * @section private:methods
   */

  private static toVariant(window: MarketRecord["window"]): PriceToBeatVariant {
    let variant: PriceToBeatVariant = "fiveminute";

    if (window === "15m") {
      variant = "fifteen";
    }

    return variant;
  }

  private static parseOpenPrice(payload: unknown): number | null {
    let openPrice: number | null = null;

    if (typeof payload === "object" && payload !== null) {
      const model = payload as PriceToBeatApiResponse;

      if (typeof model.openPrice === "number" && Number.isFinite(model.openPrice)) {
        openPrice = model.openPrice;
      }
    }

    return openPrice;
  }

  private buildPriceToBeatUrl(market: MarketRecord): string {
    const eventStartTime = new Date(market.marketStartTs);
    const endDate = new Date(market.marketEndTs);
    const variant = MarketRegistryPriceEnrichmentService.toVariant(market.window);
    const params = new URLSearchParams({
      symbol: market.asset.toUpperCase(),
      eventStartTime: eventStartTime.toISOString(),
      variant,
      endDate: endDate.toISOString()
    });
    const url = `${CONFIG.PRICE_TO_BEAT_API_BASE_URL}?${params.toString()}`;
    return url;
  }

  private async fetchPriceToBeat(market: MarketRecord): Promise<number | null> {
    const url = this.buildPriceToBeatUrl(market);
    const response = await this.fetchFn(url, { method: "GET" });
    let priceToBeat: number | null = null;

    if (response.ok) {
      const payload = await response.json();
      priceToBeat = MarketRegistryPriceEnrichmentService.parseOpenPrice(payload);
    } else {
      const reasonText = await response.text();
      throw CollectorRuntimeError.forBoundary("price-to-beat-fetch", `slug=${market.slug}; status=${response.status}; reason=${reasonText}`);
    }

    return priceToBeat;
  }

  private async sleep(delayMs: number): Promise<void> {
    const sleepPromise = new Promise<void>((resolve) => {
      setTimeout(() => {
        resolve();
      }, delayMs);
    });
    await sleepPromise;
  }

  private async enrichMarket(market: MarketRecord): Promise<void> {
    const priceToBeat = await this.fetchPriceToBeat(market);

    if (priceToBeat !== null) {
      const updates: MarketRecord[] = [{ ...market, priceToBeat }];
      const previousMarket = await this.marketRegistryRepository.getPreviousMarketForFinalPrice({
        asset: market.asset,
        window: market.window,
        marketStartTs: market.marketStartTs
      });

      if (previousMarket) {
        updates.push({ ...previousMarket, finalPrice: priceToBeat });
      }

      await this.marketRegistryRepository.upsertMarkets(updates);
      LOGGER.info(`price_to_beat updated for slug=${market.slug}; value=${priceToBeat}`);
    }
  }

  private async processPendingFinalPriceBackfillMarkets(): Promise<number> {
    const nowTs = this.nowFactory().getTime();
    const markets = await this.marketRegistryRepository.listPendingFinalPriceMarkets({ nowTs, limit: this.startupBackfillLimit });
    let updatedCount = 0;

    for (const market of markets) {
      const nextMarket = await this.marketRegistryRepository.getNextMarketWithPriceToBeat({
        asset: market.asset,
        window: market.window,
        marketStartTs: market.marketStartTs
      });
      const nextPriceToBeat = nextMarket?.priceToBeat ?? null;

      if (nextPriceToBeat !== null) {
        await this.marketRegistryRepository.upsertMarkets([{ ...market, finalPrice: nextPriceToBeat }]);
        updatedCount += 1;
      }
    }

    return updatedCount;
  }

  private async runStartupBackfill(): Promise<void> {
    if (this.startupBackfillEnabled) {
      const nowTs = this.nowFactory().getTime();
      const markets = await this.marketRegistryRepository.listPendingPriceToBeatMarkets({ nowTs, limit: this.startupBackfillLimit });
      let updatedCount = 0;

      for (const market of markets) {
        try {
          await this.enrichMarket(market);
          updatedCount += 1;
        } catch (error) {
          const reason = error instanceof Error ? error.message : String(error);
          LOGGER.error(`startup price enrichment failed for slug=${market.slug}; reason=${reason}`);
        }

        await this.sleep(this.startupBackfillDelayMs);
      }

      const finalPriceUpdatedCount = await this.processPendingFinalPriceBackfillMarkets();
      LOGGER.info(
        `startup market price backfill completed: price_to_beat_candidates=${markets.length}; price_to_beat_attempted=${updatedCount}; final_price_updated=${finalPriceUpdatedCount}`
      );
    }
  }

  private async processPendingMarkets(): Promise<void> {
    const nowTs = this.nowFactory().getTime();
    const markets = await this.marketRegistryRepository.listPendingPriceToBeatMarkets({ nowTs, limit: PRICE_TO_BEAT_BATCH_SIZE });

    for (const market of markets) {
      try {
        await this.enrichMarket(market);
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        LOGGER.error(`price enrichment failed for slug=${market.slug}; reason=${reason}`);
      }
    }
  }

  private async runCycle(): Promise<void> {
    if (!this.isProcessing) {
      this.isProcessing = true;

      try {
        await this.processPendingMarkets();
      } finally {
        this.isProcessing = false;
      }
    }
  }

  private startLoop(): void {
    this.timer = setInterval(() => {
      const cyclePromise = this.runCycle();
      void cyclePromise;
    }, this.pollIntervalMs);
  }

  private stopLoop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
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
    if (!this.timer) {
      this.startLoop();
      await this.runCycle();
      this.startupBackfillPromise = this.runStartupBackfill();
      void this.startupBackfillPromise;
    }
  }

  public async stop(): Promise<void> {
    this.stopLoop();

    if (this.startupBackfillPromise) {
      await this.startupBackfillPromise;
      this.startupBackfillPromise = null;
    }
  }

  /**
   * @section static:methods
   */

  // empty
}
