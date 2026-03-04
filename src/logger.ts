import Logger from "@sha3/logger";

const PACKAGE_NAME = "@sha3/click-collector";

function resolveLoggerName(packageName: string): string {
  if (!packageName.startsWith("@")) {
    return packageName;
  }

  const [, unscopedName] = packageName.split("/");
  return unscopedName || packageName;
}

const LOGGER = new Logger({ loggerName: resolveLoggerName(PACKAGE_NAME) });

export default LOGGER;
