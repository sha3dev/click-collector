# GitHub Copilot Adapter

- Generate class-first code aligned with `AGENTS.md` rules.
- If existing project code conflicts with `AGENTS.md`, `AGENTS.md` and `@sha3/code-standards` conventions MUST win.
- Never modify `@sha3/code-standards` managed files (`AGENTS.md`, `ai/*`, `ai/examples/*`, tooling configs) unless user explicitly requests it.
- Keep methods short and focused.
- Use `async/await` only and typed errors for failure paths.
- Include test updates for behavior changes.
