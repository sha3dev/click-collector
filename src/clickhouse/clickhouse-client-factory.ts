/**
 * @section imports:externals
 */

import { createClient } from "@clickhouse/client";

/**
 * @section imports:internals
 */

import CONFIG from "../config.ts";
import type { ClickHouseClientContract } from "./clickhouse-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

// empty

export class ClickHouseClientFactory {
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

  // empty

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

  public static create(): ClickHouseClientContract {
    const client = createClient({
      url: CONFIG.CLICKHOUSE_HOST,
      database: CONFIG.CLICKHOUSE_DATABASE,
      username: CONFIG.CLICKHOUSE_USER,
      password: CONFIG.CLICKHOUSE_PASSWORD
    }) as unknown as ClickHouseClientContract;

    return client;
  }

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

  // empty
}
