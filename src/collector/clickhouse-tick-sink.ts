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

type CoalesceState = { bucketId: number; pendingEvent: MarketEvent; lastSeenAtMs: number };
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

  private static toBucketId(eventTs: number, bucketWidthMs: number): number {
    const bucketId = Math.floor(eventTs / bucketWidthMs);
    return bucketId;
  }

  private static toCoalesceKey(event: MarketEvent): string {
    const marketSlug = event.marketSlug ?? "";
    const tokenSide = event.tokenSide ?? "";
    const window = event.window ?? "";
    const key = `${event.sourceCategory}|${event.sourceName}|${event.eventType}|${event.asset}|${window}|${marketSlug}|${tokenSide}`;
    return key;
  }

  private bufferEvent(event: MarketEvent): void {
    this.buffer.push(event);
    MarketEventStream.publish(event);
  }

  private flushCompletedCoalescedEvents(includeCurrentBuckets: boolean): void {
    const readyEvents: MarketEvent[] = [];
    const currentBucketId = ClickHouseTickSink.toBucketId(this.nowFactory(), this.coalesceWindowMs);

    for (const [key, state] of this.coalesceStateByKey.entries()) {
      const shouldFlushState = includeCurrentBuckets || state.bucketId < currentBucketId;

      if (shouldFlushState) {
        readyEvents.push(state.pendingEvent);
        this.coalesceStateByKey.delete(key);
      }
    }

    readyEvents.sort((left, right) => {
      return left.eventTs - right.eventTs;
    });

    for (const event of readyEvents) {
      this.bufferEvent(event);
    }
  }

  private addWithoutCoalescing(event: MarketEvent): void {
    this.bufferEvent(event);
  }

  private processCoalescedEvent(event: MarketEvent): void {
    const key = ClickHouseTickSink.toCoalesceKey(event);
    const state = this.coalesceStateByKey.get(key);
    const bucketId = ClickHouseTickSink.toBucketId(event.eventTs, this.coalesceWindowMs);
    const now = this.nowFactory();

    if (state === undefined) {
      this.coalesceStateByKey.set(key, { bucketId, pendingEvent: event, lastSeenAtMs: now });
    } else if (bucketId === state.bucketId) {
      this.coalesceStateByKey.set(key, { bucketId, pendingEvent: event, lastSeenAtMs: now });
    } else if (bucketId > state.bucketId) {
      this.bufferEvent(state.pendingEvent);
      this.coalesceStateByKey.set(key, { bucketId, pendingEvent: event, lastSeenAtMs: now });
    }
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

    this.flushCompletedCoalescedEvents(true);
    await this.flush();
  }

  public async writeTicks(events: MarketEvent[]): Promise<void> {
    if (this.coalesceWindowMs > 0) {
      this.flushCompletedCoalescedEvents(false);
    }

    for (const event of events) {
      if (this.coalesceWindowMs > 0) {
        this.processCoalescedEvent(event);
      } else {
        this.addWithoutCoalescing(event);
      }
    }

    if (this.shouldFlush()) {
      await this.flush();
    }
  }

  public async flush(): Promise<void> {
    if (this.coalesceWindowMs > 0) {
      this.flushCompletedCoalescedEvents(false);
    }

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
