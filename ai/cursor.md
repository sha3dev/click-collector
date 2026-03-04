# Cursor Adapter

- Apply `AGENTS.md` as the highest-priority local instruction file.
- If repository code conflicts with `AGENTS.md`, `AGENTS.md` and `@sha3/code-standards` conventions MUST win.
- Never modify `@sha3/code-standards` managed files (`AGENTS.md`, `ai/*`, `ai/examples/*`, tooling configs) unless user explicitly requests it.
- Keep feature-folder organization intact.
- Enforce single-return functions even during quick refactors.
- Run `npm run check` after modifications.
