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

const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;

/**
 * @section types
 */

type Invoice = { issuedAt: Date };
type EscalationDecision = "manual-review" | "no-escalation";

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

  private daysBetween(from: Date, to: Date): number {
    const diffInMilliseconds = to.getTime() - from.getTime();
    const dayCount = Math.floor(diffInMilliseconds / MILLISECONDS_PER_DAY);
    return dayCount;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  // Business rule: invoices older than 30 days are escalated for manual review.
  public evaluateEscalation(invoice: Invoice, now: Date): EscalationDecision {
    const ageInDays: number = this.daysBetween(invoice.issuedAt, now);
    const decision: EscalationDecision = ageInDays > 30 ? "manual-review" : "no-escalation";
    return decision;
  }

  /**
   * @section static:methods
   */

  // empty
}
