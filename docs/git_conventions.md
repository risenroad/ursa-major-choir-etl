# Git conventions for this project

## Language
- We chat in Russian.
- Branch names, commit messages, and PR titles are in English.

## Branch naming (kebab-case, short, no spaces)
Use one of the prefixes:

- `feature/<area>-<action>` — new functionality  
  Examples: `feature/gsheets-read`, `feature/transform-unpivot`, `feature/telegram-alerts`

- `fix/<what>` — bug fixes  
  Examples: `fix/date-parse-ddmmyy`, `fix/duplicate-ids`

- `docs/<topic>` — documentation  
  Examples: `docs/data-contract`, `docs/setup`

- `chore/<what>` — refactor / maintenance / config (no behavior change)  
  Examples: `chore/deps-update`, `chore/project-structure`

## Scope rule
- One task = one branch = one PR.
- Avoid unrelated “by the way” changes in the same PR.
- If scope grows, split into multiple PRs.

## Commit messages (imperative mood)
Use:
- `Add ...`
- `Fix ...`
- `Update ...`
- `Refactor ...`
- `Docs: ...`

Examples:
- `Add ETL project skeleton`
- `Fix date parsing for dd.mm.yy`
- `Docs: describe target tabs`

## Pull Requests
- PR title is in English and matches the main change.
- PR description contains two sections (bullets are enough):
  - **What changed**
  - **How to verify** (1–2 steps)

## Safety
- Never commit secrets (tokens, OAuth credentials, `.env`, `credentials*.json`, `token*.json`).
- Always check `git diff` before committing.

