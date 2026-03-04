/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

// empty

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

// empty

export class ClickHouseInsertError extends Error {
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

  private readonly tableName: string;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(tableName: string, reason: string) {
    super(`ClickHouse insert failed: table=${tableName}; reason=${reason}`);
    this.name = "ClickHouseInsertError";
    this.tableName = tableName;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static forTable(tableName: string, reason: string): ClickHouseInsertError {
    const error = new ClickHouseInsertError(tableName, reason);
    return error;
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

  public toLogContext(): string {
    const context = `table=${this.tableName}`;
    return context;
  }

  /**
   * @section static:methods
   */

  // empty
}
