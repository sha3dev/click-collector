# Windsurf Adapter

- Treat `AGENTS.md` as a mandatory contract.
- If repository code conflicts with `AGENTS.md`, `AGENTS.md` and `@sha3/code-standards` conventions MUST win.
- Never modify `@sha3/code-standards` managed files (`AGENTS.md`, `ai/*`, `ai/examples/*`, tooling configs) unless user explicitly requests it.
- Follow class-first and feature-folder conventions.
- Avoid early returns; keep a single return per function.
- Execute local deterministic checks with `npm run check`.
