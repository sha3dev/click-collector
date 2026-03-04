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

export class CollectorStartupError extends Error {
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

  private readonly operation: string;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(operation: string, reason: string) {
    super(`Collector startup failed: operation=${operation}; reason=${reason}`);
    this.name = "CollectorStartupError";
    this.operation = operation;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static forOperation(operation: string, reason: string): CollectorStartupError {
    const error = new CollectorStartupError(operation, reason);
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
    const context = `operation=${this.operation}`;
    return context;
  }

  /**
   * @section static:methods
   */

  // empty
}
