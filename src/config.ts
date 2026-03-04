import "dotenv/config";

const DEFAULT_CONFIG = {
  CLICKHOUSE_HOST: "http://localhost:8123",
  CLICKHOUSE_DATABASE: "default",
  CLICKHOUSE_USER: "default",
  CLICKHOUSE_PASSWORD: "default",
  CLICKHOUSE_TICKS_TABLE: "ticks",
  CLICKHOUSE_MARKET_REGISTRY_TABLE: "market_registry",
  INGEST_BATCH_SIZE: 100,
  INGEST_FLUSH_INTERVAL_MS: 1_000,
  POLYMARKET_DISCOVERY_INTERVAL_MS: 30_000,
  SUPPORTED_ASSETS: ["btc", "eth", "sol", "xrp"],
  SUPPORTED_WINDOWS: ["5m", "15m"],
  CRYPTO_PROVIDERS: ["binance", "coinbase", "kraken", "okx", "chainlink"]
} as const;

const TEST_CLICKHOUSE_DEFAULTS = {
  CLICKHOUSE_HOST: "http://192.168.1.2:8123",
  CLICKHOUSE_USER: "default",
  CLICKHOUSE_PASSWORD: "default"
} as const;

function resolveClickHouseHostFallback(): string {
  const fallback = process.env.NODE_ENV === "test" ? TEST_CLICKHOUSE_DEFAULTS.CLICKHOUSE_HOST : DEFAULT_CONFIG.CLICKHOUSE_HOST;
  return fallback;
}

function resolveClickHouseUserFallback(): string {
  const fallback = process.env.NODE_ENV === "test" ? TEST_CLICKHOUSE_DEFAULTS.CLICKHOUSE_USER : DEFAULT_CONFIG.CLICKHOUSE_USER;
  return fallback;
}

function resolveClickHousePasswordFallback(): string {
  const fallback = process.env.NODE_ENV === "test" ? TEST_CLICKHOUSE_DEFAULTS.CLICKHOUSE_PASSWORD : DEFAULT_CONFIG.CLICKHOUSE_PASSWORD;
  return fallback;
}

function readStringEnv(key: string, fallback: string): string {
  const rawValue = process.env[key];
  let resolvedValue = fallback;

  if (typeof rawValue === "string") {
    const trimmedValue = rawValue.trim();

    if (trimmedValue.length > 0) {
      resolvedValue = trimmedValue;
    }
  }

  return resolvedValue;
}

function readIntegerEnv(key: string, fallback: number): number {
  const rawValue = process.env[key];
  let resolvedValue = fallback;

  if (typeof rawValue === "string") {
    const parsedValue = Number.parseInt(rawValue, 10);

    if (Number.isFinite(parsedValue)) {
      resolvedValue = parsedValue;
    }
  }

  return resolvedValue;
}

const CONFIG = {
  CLICKHOUSE_HOST: readStringEnv("CLICKHOUSE_HOST", resolveClickHouseHostFallback()),
  CLICKHOUSE_DATABASE: readStringEnv("CLICKHOUSE_DATABASE", DEFAULT_CONFIG.CLICKHOUSE_DATABASE),
  CLICKHOUSE_USER: readStringEnv("CLICKHOUSE_USER", resolveClickHouseUserFallback()),
  CLICKHOUSE_PASSWORD: readStringEnv("CLICKHOUSE_PASSWORD", resolveClickHousePasswordFallback()),
  CLICKHOUSE_TICKS_TABLE: readStringEnv("CLICKHOUSE_TICKS_TABLE", DEFAULT_CONFIG.CLICKHOUSE_TICKS_TABLE),
  CLICKHOUSE_MARKET_REGISTRY_TABLE: readStringEnv("CLICKHOUSE_MARKET_REGISTRY_TABLE", DEFAULT_CONFIG.CLICKHOUSE_MARKET_REGISTRY_TABLE),
  INGEST_BATCH_SIZE: readIntegerEnv("INGEST_BATCH_SIZE", DEFAULT_CONFIG.INGEST_BATCH_SIZE),
  INGEST_FLUSH_INTERVAL_MS: readIntegerEnv("INGEST_FLUSH_INTERVAL_MS", DEFAULT_CONFIG.INGEST_FLUSH_INTERVAL_MS),
  POLYMARKET_DISCOVERY_INTERVAL_MS: readIntegerEnv("POLYMARKET_DISCOVERY_INTERVAL_MS", DEFAULT_CONFIG.POLYMARKET_DISCOVERY_INTERVAL_MS),
  SUPPORTED_ASSETS: DEFAULT_CONFIG.SUPPORTED_ASSETS,
  SUPPORTED_WINDOWS: DEFAULT_CONFIG.SUPPORTED_WINDOWS,
  CRYPTO_PROVIDERS: DEFAULT_CONFIG.CRYPTO_PROVIDERS
} as const;

export default CONFIG;
