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

const DEFAULT_INVOICE_PREFIX = "INV";

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

  public constructor(clock: Clock, idFactory: IdFactory, prefix = DEFAULT_INVOICE_PREFIX) {
    this.clock = clock;
    this.idFactory = idFactory;
    this.prefix = prefix;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(clock: Clock, idFactory: IdFactory): InvoiceIdBuilder {
    const builder = new InvoiceIdBuilder(clock, idFactory);
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
