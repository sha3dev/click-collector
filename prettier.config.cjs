let config;

try {
  config = require("@sha3/code-standards/prettier");
} catch {
  config = {
    printWidth: 160,
    tabWidth: 2,
    useTabs: false,
    semi: true,
    singleQuote: false,
    trailingComma: "none",
    bracketSpacing: true,
    arrowParens: "always",
    objectWrap: "collapse"
  };
}

module.exports = config;
