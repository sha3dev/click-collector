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

type InvoiceStatus = "paid" | "void" | "pending";

export class InvoiceStatusPresenter {
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

  public static create(): InvoiceStatusPresenter {
    const presenter = new InvoiceStatusPresenter();
    return presenter;
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

  public toStatusLabel(status: InvoiceStatus): string {
    if (status === "paid") {
      return "Paid";
    }

    if (status === "void") {
      return "Void";
    }

    return "Pending";
  }

  /**
   * @section static:methods
   */

  // empty
}
