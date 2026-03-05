import LOGGER from "./logger.ts";
import { AutonomousCollectorService } from "./collector/autonomous-collector-service.ts";

export { MarketEventsQueryService } from "./markets/market-events-query-service.ts";
export type { AssetSymbol, EventType, MarketEvent, MarketRecord, MarketSnapshot, MarketWindow } from "./markets/market-events-types.ts";

let collectorService: AutonomousCollectorService | null = null;

async function startRuntime(): Promise<void> {
  collectorService = AutonomousCollectorService.createDefault();
  await collectorService.start();
}

async function stopRuntime(signal: NodeJS.Signals): Promise<void> {
  LOGGER.info(`shutdown signal received: ${signal}`);

  if (collectorService) {
    await collectorService.stop();
    collectorService = null;
  }

  process.exit(0);
}

if (process.env.NODE_ENV !== "test") {
  const bootPromise = startRuntime();

  void bootPromise.catch((error: unknown) => {
    const reason = error instanceof Error ? (error.stack ?? error.message) : String(error);
    LOGGER.error(`collector startup failed: ${reason}`);
    process.exit(1);
  });

  process.on("SIGINT", (signal) => {
    const stopPromise = stopRuntime(signal);
    void stopPromise;
  });

  process.on("SIGTERM", (signal) => {
    const stopPromise = stopRuntime(signal);
    void stopPromise;
  });
}
