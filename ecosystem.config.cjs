module.exports = { apps: [{ name: "@sha3/click-collector", script: "node", args: "--import tsx src/index.ts", env: { NODE_ENV: "production" } }] };
