import { spawnSync } from "node:child_process";
import { rmSync } from "node:fs";
import path from "node:path";
import process from "node:process";

const ROOT_DIR = process.cwd();
const DIST_DIR = path.join(ROOT_DIR, "dist");

function run(command, args) {
  const result = spawnSync(command, args, { cwd: ROOT_DIR, stdio: "inherit", shell: process.platform === "win32" });
  return result.status ?? 1;
}

function main() {
  rmSync(DIST_DIR, { recursive: true, force: true });

  const tscCode = run("npx", [
    "tsc",
    "--outDir",
    "dist",
    "--module",
    "NodeNext",
    "--moduleResolution",
    "NodeNext",
    "--declaration",
    "--sourceMap",
    "false",
    "--allowImportingTsExtensions",
    "true",
    "--rewriteRelativeImportExtensions",
    "true",
    "src/public-api.ts"
  ]);

  if (tscCode !== 0) {
    process.exit(tscCode);
  }
}

main();
