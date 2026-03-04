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

export class MarketNotFoundError extends Error {
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

  private readonly slug: string;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(slug: string) {
    super(`Market slug not found in registry: slug=${slug}`);
    this.name = "MarketNotFoundError";
    this.slug = slug;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static forSlug(slug: string): MarketNotFoundError {
    const error = new MarketNotFoundError(slug);
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
    const context = `slug=${this.slug}`;
    return context;
  }

  /**
   * @section static:methods
   */

  // empty
}
