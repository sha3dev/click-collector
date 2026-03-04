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

type CreateInvoiceCommand = { customerId: string; amount: number };
type Invoice = { id: string; customerId: string; amount: number };

export class InvoiceService {
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

  public static create(): InvoiceService {
    const service = new InvoiceService();
    return service;
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

  public async create(command: CreateInvoiceCommand): Promise<Invoice> {
    if (!command.customerId) {
      return Promise.reject(new Error("invalid command"));
    }

    const invoice: Invoice = { id: "1", customerId: command.customerId, amount: command.amount };
    return invoice;
  }

  /**
   * @section static:methods
   */

  // empty
}
