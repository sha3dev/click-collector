import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { FeedEvent } from "@sha3/crypto";

import { CryptoTicksIngestionService } from "../src/collector/crypto-ticks-ingestion-service.ts";
import type { MarketEvent } from "../src/markets/market-events-types.ts";

function waitMicrotask(): Promise<void> {
  const promise = new Promise<void>((resolve) => {
    setImmediate(() => {
      resolve();
    });
  });

  return promise;
}

test("crypto ingestion maps price and orderbook events into ticks", async () => {
  const writes: MarketEvent[][] = [];
  let listener: (event: FeedEvent) => void = () => {
    return;
  };

  const service = CryptoTicksIngestionService.create({
    tickSink: {
      async writeTicks(events) {
        writes.push(events);
      }
    },
    client: {
      async connect() {
        return;
      },
      async disconnect() {
        return;
      },
      subscribe(callback: (event: FeedEvent) => void) {
        listener = callback;
        return {
          unsubscribe() {
            return;
          }
        };
      }
    }
  });

  await service.start();

  listener({ type: "price", provider: "binance", symbol: "BTC", ts: 1_700_000_000_000, price: 42_000 });
  listener({
    type: "orderbook",
    provider: "kraken",
    symbol: "btc",
    ts: 1_700_000_000_100,
    asks: [{ price: 42_010, size: 2 }],
    bids: [{ price: 41_990, size: 1 }]
  });
  listener({ type: "trade", provider: "binance", symbol: "btc", ts: 1, price: 1, size: 1, buyerIsMaker: false });

  await waitMicrotask();
  await service.stop();
  const firstWrite = writes.at(0);
  const secondWrite = writes.at(1);
  const firstEvent = firstWrite?.at(0);
  const secondEvent = secondWrite?.at(0);

  assert.equal(writes.length, 2);
  assert.ok(firstEvent);
  assert.equal(firstEvent.eventType, "price");
  assert.equal(firstEvent.price, 42_000);
  assert.equal(firstEvent.orderbook, null);
  assert.equal(firstEvent.sourceCategory, "exchange");

  assert.ok(secondEvent);
  assert.equal(secondEvent.eventType, "orderbook");
  assert.equal(secondEvent.price, null);
  assert.equal(typeof secondEvent.orderbook, "string");
  assert.deepEqual(JSON.parse(secondEvent.orderbook ?? "{}"), { asks: [{ price: 42_010, size: 2 }], bids: [{ price: 41_990, size: 1 }] });
});
