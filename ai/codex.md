# Codex Adapter

- Read `AGENTS.md` first and treat all rules as blocking.
- If existing code conflicts with `AGENTS.md`, `AGENTS.md` and `@sha3/code-standards` conventions MUST win.
- Never modify `@sha3/code-standards` managed files (`AGENTS.md`, `ai/*`, `ai/examples/*`, tooling configs) unless user explicitly requests it.
- Do not bypass the single-return policy.
- Prefer class-based implementations with constructor injection.
- Always run `npm run check` before finalizing changes.
