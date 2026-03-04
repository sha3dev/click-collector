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

type FeatureFlagMap = Record<string, boolean>;

export class FeatureGate {
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

  private readonly flags: FeatureFlagMap;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(flags: FeatureFlagMap) {
    this.flags = flags;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static from(flags: FeatureFlagMap): FeatureGate {
    const gate = new FeatureGate(flags);
    return gate;
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

  public canRunTask(key: string): boolean {
    let enabled = false;

    if (this.flags[key] === true) {
      enabled = true;
    }

    return enabled;
  }

  /**
   * @section static:methods
   */

  // empty
}
