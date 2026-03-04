/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import LOGGER from "../logger.ts";
import { ClickHouseClientFactory } from "../clickhouse/clickhouse-client-factory.ts";
import type { ClickHouseClientContract } from "../clickhouse/clickhouse-types.ts";
import { MarketRegistryRepository } from "../clickhouse/market-registry-repository.ts";
import { TickRepository } from "../clickhouse/tick-repository.ts";
import { CollectorStartupError } from "./collector-errors.ts";
import { ClickHouseMarketRegistrySink } from "./clickhouse-market-registry-sink.ts";
import { ClickHouseTickSink } from "./clickhouse-tick-sink.ts";
import { CryptoTicksIngestionService } from "./crypto-ticks-ingestion-service.ts";
import { PolymarketTicksIngestionService } from "./polymarket-ticks-ingestion-service.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type AutonomousCollectorServiceOptions = {
  clickHouseClient: ClickHouseClientContract;
  marketRegistryRepository: MarketRegistryRepository;
  tickRepository: TickRepository;
  tickSink: ClickHouseTickSink;
  marketRegistrySink: ClickHouseMarketRegistrySink;
  cryptoIngestionService: CryptoTicksIngestionService;
  polymarketIngestionService: PolymarketTicksIngestionService;
};

export class AutonomousCollectorService {
  /**
   * @section private:attributes
   */

  private isRunning: boolean;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly clickHouseClient: ClickHouseClientContract;
  private readonly marketRegistryRepository: MarketRegistryRepository;
  private readonly tickRepository: TickRepository;
  private readonly tickSink: ClickHouseTickSink;
  private readonly marketRegistrySink: ClickHouseMarketRegistrySink;
  private readonly cryptoIngestionService: CryptoTicksIngestionService;
  private readonly polymarketIngestionService: PolymarketTicksIngestionService;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: AutonomousCollectorServiceOptions) {
    this.clickHouseClient = options.clickHouseClient;
    this.marketRegistryRepository = options.marketRegistryRepository;
    this.tickRepository = options.tickRepository;
    this.tickSink = options.tickSink;
    this.marketRegistrySink = options.marketRegistrySink;
    this.cryptoIngestionService = options.cryptoIngestionService;
    this.polymarketIngestionService = options.polymarketIngestionService;
    this.isRunning = false;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: AutonomousCollectorServiceOptions): AutonomousCollectorService {
    const service = new AutonomousCollectorService(options);
    return service;
  }

  public static createDefault(): AutonomousCollectorService {
    const clickHouseClient = ClickHouseClientFactory.create();
    const marketRegistryRepository = MarketRegistryRepository.create({ client: clickHouseClient });
    const tickRepository = TickRepository.create({ client: clickHouseClient });
    const tickSink = ClickHouseTickSink.create({ repository: tickRepository });
    const marketRegistrySink = ClickHouseMarketRegistrySink.create({ repository: marketRegistryRepository });
    const cryptoIngestionService = CryptoTicksIngestionService.create({ tickSink });
    const polymarketIngestionService = PolymarketTicksIngestionService.create({ tickSink, marketRegistrySink });
    const service = new AutonomousCollectorService({
      clickHouseClient,
      marketRegistryRepository,
      tickRepository,
      tickSink,
      marketRegistrySink,
      cryptoIngestionService,
      polymarketIngestionService
    });

    return service;
  }

  /**
   * @section private:methods
   */

  private stringifyError(error: unknown): string {
    const message = error instanceof Error ? error.message : String(error);
    return message;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async start(): Promise<void> {
    if (!this.isRunning) {
      try {
        await this.marketRegistryRepository.ensureSchema();
        await this.tickRepository.ensureSchema();
        this.tickSink.start();
        await this.cryptoIngestionService.start();
        await this.polymarketIngestionService.start();
        this.isRunning = true;
        LOGGER.info("autonomous collector started");
      } catch (error) {
        throw CollectorStartupError.forOperation("start", this.stringifyError(error));
      }
    }
  }

  public async stop(): Promise<void> {
    if (this.isRunning) {
      await this.polymarketIngestionService.stop();
      await this.cryptoIngestionService.stop();
      await this.tickSink.stop();
      await this.clickHouseClient.close();
      this.isRunning = false;
      LOGGER.info("autonomous collector stopped");
    }
  }

  /**
   * @section static:methods
   */

  // empty
}
