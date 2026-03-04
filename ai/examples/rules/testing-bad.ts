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

type Invoice = { issuedAt: Date };

export class InvoiceEscalationPolicy {
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

  public static create(): InvoiceEscalationPolicy {
    const policy = new InvoiceEscalationPolicy();
    return policy;
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

  public evaluateEscalation(invoice: Invoice, now: Date): Promise<string> {
    return Promise.resolve(invoice).then((current: Invoice) => {
      const hasAge = now.getTime() - current.issuedAt.getTime() > 0;
      const decision = hasAge ? "x" : "y";
      return decision;
    });
  }

  /**
   * @section static:methods
   */

  // empty
}
