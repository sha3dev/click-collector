import { strict as assert } from "node:assert";
import { test } from "node:test";

import { MarketEventsQueryService } from "../src/markets/market-events-query-service.ts";

test("public module exports MarketEventsQueryService", () => {
  assert.equal(typeof MarketEventsQueryService, "function");
});
