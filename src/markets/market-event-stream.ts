/**
 * @section imports:externals
 */

import { randomUUID } from "node:crypto";

/**
 * @section imports:internals
 */

import type { MarketEvent } from "./market-events-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type MarketEventListener = (event: MarketEvent) => void;

export class MarketEventStream {
  /**
   * @section private:attributes
   */

  // empty

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private static readonly listenersById: Map<string, MarketEventListener> = new Map<string, MarketEventListener>();

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  // empty

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  // empty

  /**
   * @section private:methods
   */

  // empty

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  // empty

  /**
   * @section static:methods
   */

  public static addListener(listener: MarketEventListener): string {
    const listenerId = randomUUID();
    this.listenersById.set(listenerId, listener);
    return listenerId;
  }

  public static removeListener(listenerId: string): void {
    this.listenersById.delete(listenerId);
  }

  public static publish(event: MarketEvent): void {
    for (const listener of this.listenersById.values()) {
      listener(event);
    }
  }
}
