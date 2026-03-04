/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import CONFIG from "../config.ts";
import type { MarketEvent } from "../markets/market-events-types.ts";
import type { TickRepository } from "../clickhouse/tick-repository.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type ClickHouseTickSinkOptions = { repository: TickRepository };

export class ClickHouseTickSink {
  /**
   * @section private:attributes
   */

  private flushTimer: NodeJS.Timeout | null;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly repository: TickRepository;
  private readonly buffer: MarketEvent[];

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
    this.flushTimer = null;
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
  }

  public async stop(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    await this.flush();
  }

  public async writeTicks(events: MarketEvent[]): Promise<void> {
    for (const event of events) {
      this.buffer.push(event);
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
