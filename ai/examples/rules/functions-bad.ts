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

type PaymentInput = { amount?: number; currency?: string; metadata?: Record<string, string> };
type PersistedPaymentInput = PaymentInput & { normalizedAt: number; saved: boolean };

export class PaymentNormalizer {
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

  public static create(): PaymentNormalizer {
    const normalizer = new PaymentNormalizer();
    return normalizer;
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

  public normalize(input: PaymentInput): PersistedPaymentInput {
    // Bad: too many responsibilities in one method.
    const clone = { ...input };

    if (!clone.currency) {
      clone.currency = "usd";
    }

    if (typeof clone.currency === "string") {
      clone.currency = clone.currency.toUpperCase();
    }

    if (!clone.metadata) {
      clone.metadata = {};
    }

    clone.amount = Number(clone.amount || 0);
    const persisted: PersistedPaymentInput = { ...clone, normalizedAt: Date.now(), saved: true };
    return persisted;
  }

  /**
   * @section static:methods
   */

  // empty
}
