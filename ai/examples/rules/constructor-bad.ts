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

const FALLBACK_PREFIX = "INV";

/**
 * @section types
 */

type Clock = () => Date;
type IdFactory = () => string;

export class InvoiceIdBuilder {
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

  private readonly clock: Clock;
  private readonly idFactory: IdFactory;
  private readonly prefix: string;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor() {
    // Bad: constructor wires concrete dependencies and environment details directly.
    this.clock = () => new Date();
    this.idFactory = () => Math.random().toString(36).slice(2, 8);
    this.prefix = process.env.INVOICE_PREFIX || FALLBACK_PREFIX;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(): InvoiceIdBuilder {
    const builder = new InvoiceIdBuilder();
    return builder;
  }

  /**
   * @section private:methods
   */

  private currentYear(): number {
    const year = this.clock().getUTCFullYear();
    return year;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public build(): string {
    const rawId = this.idFactory();
    const year = this.currentYear();
    const invoiceId = `${this.prefix}-${year}-${rawId}`;
    return invoiceId;
  }

  /**
   * @section static:methods
   */

  // empty
}
