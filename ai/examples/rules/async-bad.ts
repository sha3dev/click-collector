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

type SyncInvoicesCommand = { accountId: string };
type Invoice = { id: string; accountId: string };
type SyncResult = { saved: number };
type InvoiceSource = { fetch(accountId: string): Promise<Invoice[]> };
type InvoiceWriter = { persist(invoices: Invoice[]): Promise<SyncResult> };

export class InvoiceSyncService {
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

  private readonly source: InvoiceSource;
  private readonly writer: InvoiceWriter;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(source: InvoiceSource, writer: InvoiceWriter) {
    this.source = source;
    this.writer = writer;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(source: InvoiceSource, writer: InvoiceWriter): InvoiceSyncService {
    const service = new InvoiceSyncService(source, writer);
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

  public execute(command: SyncInvoicesCommand): Promise<SyncResult> {
    return this.source.fetch(command.accountId).then((invoices: Invoice[]) => {
      return this.writer.persist(invoices);
    });
  }

  /**
   * @section static:methods
   */

  // empty
}
