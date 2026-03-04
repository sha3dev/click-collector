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

export class InvalidInvoiceCommandError extends Error {
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

  private readonly reason: string;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(reason: string) {
    super(`Invalid invoice command: ${reason}`);
    this.name = "InvalidInvoiceCommandError";
    this.reason = reason;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static forReason(reason: string): InvalidInvoiceCommandError {
    const error = new InvalidInvoiceCommandError(reason);
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

  public getReason(): string {
    const value = this.reason;
    return value;
  }

  /**
   * @section static:methods
   */

  // empty
}
