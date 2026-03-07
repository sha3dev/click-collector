import { strict as assert } from "node:assert";
import { test } from "node:test";

import { TickRepository } from "../src/clickhouse/tick-repository.ts";
import type { ClickHouseClientContract } from "../src/clickhouse/clickhouse-types.ts";

test("getRelatedEventsByMarketRange constrains polymarket rows to market time bounds", async () => {
  let capturedQuery = "";
  const client: ClickHouseClientContract = {
    async command() {
      return;
    },
    async insert() {
      return;
    },
    async query(options) {
      capturedQuery = options.query;
      return {
        async json() {
          return [];
        }
      };
    },
    async close() {
      return;
    }
  };
  const repository = TickRepository.create({ client });

  await repository.getRelatedEventsByMarketRange({ slug: "btc-updown-5m-1772621700", asset: "btc", fromTs: 1_772_621_700_000, toTs: 1_772_622_000_000 });

  assert.match(
    capturedQuery,
    /source_category = 'polymarket'[\s\S]*market_slug = 'btc-updown-5m-1772621700'[\s\S]*event_ts >= toDateTime64\('[^']+', 3, 'UTC'\)[\s\S]*event_ts <= toDateTime64\('[^']+', 3, 'UTC'\)/
  );
});

test("getAllAssetsEventsByMarketRange constrains polymarket rows to market time bounds", async () => {
  let capturedQuery = "";
  const client: ClickHouseClientContract = {
    async command() {
      return;
    },
    async insert() {
      return;
    },
    async query(options) {
      capturedQuery = options.query;
      return {
        async json() {
          return [];
        }
      };
    },
    async close() {
      return;
    }
  };
  const repository = TickRepository.create({ client });

  await repository.getAllAssetsEventsByMarketRange({ slug: "btc-updown-15m-1772621100", fromTs: 1_772_621_100_000, toTs: 1_772_622_000_000 });

  assert.match(
    capturedQuery,
    /source_category = 'polymarket'[\s\S]*market_slug = 'btc-updown-15m-1772621100'[\s\S]*event_ts >= toDateTime64\('[^']+', 3, 'UTC'\)[\s\S]*event_ts <= toDateTime64\('[^']+', 3, 'UTC'\)/
  );
});
