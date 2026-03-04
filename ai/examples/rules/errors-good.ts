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

export class InvoiceNotFoundError extends Error {
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

  private readonly invoiceId: string;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(invoiceId: string) {
    super(`Invoice not found: ${invoiceId}`);
    this.name = "InvoiceNotFoundError";
    this.invoiceId = invoiceId;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static forId(invoiceId: string): InvoiceNotFoundError {
    const error = new InvoiceNotFoundError(invoiceId);
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
    const context = `invoiceId=${this.invoiceId}`;
    return context;
  }

  /**
   * @section static:methods
   */

  // empty
}
