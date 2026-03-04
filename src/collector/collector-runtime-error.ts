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

export class CollectorRuntimeError extends Error {
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

  private readonly boundary: string;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(boundary: string, reason: string) {
    super(`Collector runtime failure: boundary=${boundary}; reason=${reason}`);
    this.name = "CollectorRuntimeError";
    this.boundary = boundary;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static forBoundary(boundary: string, reason: string): CollectorRuntimeError {
    const error = new CollectorRuntimeError(boundary, reason);
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
    const context = `boundary=${this.boundary}`;
    return context;
  }

  /**
   * @section static:methods
   */

  // empty
}
