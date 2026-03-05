/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import CONFIG from "../config.ts";
import { MarketEventStream } from "../markets/market-event-stream.ts";
import type { MarketEvent } from "../markets/market-events-types.ts";
import type { TickRepository } from "../clickhouse/tick-repository.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type CoalesceState = { lastEventTs: number; lastSeenAtMs: number };
type ClickHouseTickSinkOptions = {
  repository: TickRepository;
  coalesceWindowMs?: number;
  coalesceCleanupIntervalMs?: number;
  coalesceKeyTtlMs?: number;
  nowFactory?: () => number;
};

export class ClickHouseTickSink {
  /**
   * @section private:attributes
   */

  private flushTimer: NodeJS.Timeout | null;
  private cleanupTimer: NodeJS.Timeout | null;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly repository: TickRepository;
  private readonly buffer: MarketEvent[];
  private readonly coalesceStateByKey: Map<string, CoalesceState>;
  private readonly coalesceWindowMs: number;
  private readonly coalesceCleanupIntervalMs: number;
  private readonly coalesceKeyTtlMs: number;
  private readonly nowFactory: () => number;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: ClickHouseTickSinkOptions) {
    this.repository = options.repository;
    this.buffer = [];
    this.coalesceStateByKey = new Map<string, CoalesceState>();
    this.coalesceWindowMs = options.coalesceWindowMs ?? CONFIG.INGEST_COALESCE_WINDOW_MS;
    this.coalesceCleanupIntervalMs = options.coalesceCleanupIntervalMs ?? CONFIG.INGEST_COALESCE_CLEANUP_INTERVAL_MS;
    this.coalesceKeyTtlMs = options.coalesceKeyTtlMs ?? CONFIG.INGEST_COALESCE_KEY_TTL_MS;
    this.nowFactory = options.nowFactory ?? (() => Date.now());
    this.flushTimer = null;
    this.cleanupTimer = null;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: ClickHouseTickSinkOptions): ClickHouseTickSink {
    const sink = new ClickHouseTickSink(options);
    return sink;
  }

  /**
   * @section private:methods
   */

  private shouldFlush(): boolean {
    const decision = this.buffer.length >= CONFIG.INGEST_BATCH_SIZE;
    return decision;
  }

  private static toCoalesceKey(event: MarketEvent): string {
    const marketSlug = event.marketSlug ?? "";
    const tokenSide = event.tokenSide ?? "";
    const window = event.window ?? "";
    const key = `${event.sourceCategory}|${event.sourceName}|${event.eventType}|${event.asset}|${window}|${marketSlug}|${tokenSide}`;
    return key;
  }

  private shouldKeepEvent(event: MarketEvent): boolean {
    const key = ClickHouseTickSink.toCoalesceKey(event);
    const state = this.coalesceStateByKey.get(key);
    const previousTs = state?.lastEventTs;
    const eventAgeMs = previousTs === undefined ? Number.POSITIVE_INFINITY : event.eventTs - previousTs;
    const shouldKeep = this.coalesceWindowMs <= 0 || eventAgeMs >= this.coalesceWindowMs;
    const now = this.nowFactory();

    if (shouldKeep) {
      this.coalesceStateByKey.set(key, { lastEventTs: event.eventTs, lastSeenAtMs: now });
    } else if (state) {
      this.coalesceStateByKey.set(key, { lastEventTs: state.lastEventTs, lastSeenAtMs: now });
    }

    return shouldKeep;
  }

  private cleanupCoalesceState(): void {
    const now = this.nowFactory();
    const expirationTs = now - this.coalesceKeyTtlMs;

    for (const [key, state] of this.coalesceStateByKey.entries()) {
      if (state.lastSeenAtMs < expirationTs) {
        this.coalesceStateByKey.delete(key);
      }
    }
  }

  private takeBatch(): MarketEvent[] {
    const batch = this.buffer.splice(0, this.buffer.length);
    return batch;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public start(): void {
    this.flushTimer = setInterval(() => {
      const flushPromise = this.flush();
      void flushPromise;
    }, CONFIG.INGEST_FLUSH_INTERVAL_MS);
    this.cleanupTimer = setInterval(() => {
      this.cleanupCoalesceState();
    }, this.coalesceCleanupIntervalMs);
  }

  public async stop(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }

    await this.flush();
  }

  public async writeTicks(events: MarketEvent[]): Promise<void> {
    for (const event of events) {
      const shouldKeepEvent = this.shouldKeepEvent(event);

      if (shouldKeepEvent) {
        this.buffer.push(event);
        MarketEventStream.publish(event);
      }
    }

    if (this.shouldFlush()) {
      await this.flush();
    }
  }

  public async flush(): Promise<void> {
    const batch = this.takeBatch();

    if (batch.length > 0) {
      await this.repository.insertTicks(batch);
    }
  }

  /**
   * @section static:methods
   */

  // empty
}
