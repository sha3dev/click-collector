/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import CONFIG from "../config.ts";
import type { InvoiceService } from "../invoices/invoice-service.ts";
import type { InvoiceSummary } from "../invoices/invoice-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

export type BillingSnapshot = { customerId: string; invoiceCount: number; totalAmount: number; formattedTotal: string; statusServiceUrl: string };

export class BillingService {
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

  private readonly invoiceService: InvoiceService;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(invoiceService: InvoiceService) {
    this.invoiceService = invoiceService;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(invoiceService: InvoiceService): BillingService {
    const service = new BillingService(invoiceService);
    return service;
  }

  /**
   * @section private:methods
   */

  private formatCurrency(amount: number): string {
    const formattedAmount = `${CONFIG.BILLING_CURRENCY_SYMBOL}${amount.toFixed(2)}`;
    return formattedAmount;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async snapshot(customerId: string): Promise<BillingSnapshot> {
    const summary: InvoiceSummary = await this.invoiceService.summarizeForCustomer(customerId);
    const snapshot: BillingSnapshot = {
      customerId,
      invoiceCount: summary.count,
      totalAmount: summary.totalAmount,
      formattedTotal: this.formatCurrency(summary.totalAmount),
      statusServiceUrl: CONFIG.STATUS_SERVICE_URL
    };
    return snapshot;
  }

  /**
   * @section static:methods
   */

  // empty
}
