# Repository Agent Policy

This file is the permanent workflow policy for Codex work in this repository.
Apply it automatically on every task. Do not ask the user to repeat these rules.

## Hard Limits

- Allowed model only: `gpt-5.5`
- Allowed reasoning effort only: `high`
- Forbidden: local models, Ollama, custom providers, or any non-OpenAI model
- Forbidden in this repository: `gpt-5.4-mini`, `gpt-5.3-codex`, or any lower-tier fallback unless the user explicitly changes this repository rule in a later instruction
- Repository override: always use `gpt-5.5` with `high` reasoning for this project unless the user explicitly changes that rule
- Default starting model: `gpt-5.5` with `high` reasoning
- Never switch away from `gpt-5.5` with `high` reasoning during repository work unless the user explicitly changes this rule
- Do not downgrade model tier or reasoning effort for SQL discovery, TMDS mapping, Genisys work, ICD work, workflow extraction, parser work, decoder work, UI architecture, validation, or review
- Never guess missing logic, defaults, mappings, state transitions, or protocol behavior
- Never invent behavior
- If the source of truth is missing, stop, mark it unknown, and surface the blocker
- Never silently ignore errors or ambiguous results
- Never claim `production ready` or `100% correct`

## Source Grounding Requirements

- For every decoder output in this repository, search all applicable local sources before leaving anything unresolved, unnamed, generic, or weakly explained
- If the user says the repository should already contain the needed name, mapping, translation, or context, treat that as a mandatory full-source grounding pass, not as a narrow sample lookup
- This applies to all interpretation work, including but not limited to:
  - assignment names
  - bit names
  - station and component labels
  - host and node identity
  - workflow interpretation
  - state explanations
  - socket/session interpretation
  - protocol behavior claims
  - translation text
  - UI-decoded summaries, labels, and detail text
- Required local source set for this repository:
  - `C:\Users\Ji\Music\...` project source folders that are relevant to the current task
  - `exports/raw/sql_foundation/tmdsDatabaseStatic.*`
  - `exports/raw/sql_foundation/tmdsDatabaseDynamic.*`
  - `exports/normalized/*` only after checking whether the raw static/dynamic exports carry stronger grounding
- Required inspection depth:
  - inspect actual raw data tables and exported rows, not just schema names, manifest files, inventory summaries, table titles, column lists, or normalized labels
  - when searching TMDS static or dynamic sources, go through the relevant exported table data needed to prove or disprove the mapping
  - when searching the Music folder, inspect the relevant real files and extracted content needed to ground the answer; do not stop at folder names or file titles
- Forbidden shortcuts for this repository:
  - do not claim a lookup was exhausted after checking only schema inventory, table names, column names, manifests, headings, or sample labels
  - do not say you searched the static or dynamic TMDS database unless you actually inspected the relevant exported data rows/tables for the current question
  - do not rely on normalized outputs alone when the raw static/dynamic exports or Music-folder materials may still carry stronger evidence
- Do not stop after checking only normalized exports if the raw static/dynamic database exports or Music-folder sources may still carry the needed grounding
- Do not accept a generic, weak, or placeholder explanation if the Music folder or raw static/dynamic TMDS exports may still ground it more precisely
- If any output still cannot be fully grounded after checking the Music folder plus raw static/dynamic TMDS exports, mark exactly which files/tables/row sources were checked and what remained unresolved

## Required Workflow

Every task must follow this sequence:

1. Fill the task frame from [`.codex/prompts/TASK_TEMPLATE.md`](/D:/NCTD%20TMDS%20Decoder/.codex/prompts/TASK_TEMPLATE.md)
2. IMPLEMENTATION
3. AUDIT
4. VALIDATION
5. REBUILD / RUN APP WHEN FEASIBLE
6. USER REVIEW

Rules:

- Implementation may not self-approve
- Audit and validation are separate stages
- A task is not ready for user review until the required stages are completed or explicitly blocked
- If rebuild/run is feasible after meaningful changes, it must be attempted
- Results must clearly distinguish `rebuilt successfully` from `validated against requirements`

## Automatic Model Routing

Use these routing rules without waiting for user input.

### `gpt-5.4-mini` + `medium`

Forbidden in this repository. Do not use unless the user explicitly changes the repository model rule.

### `gpt-5.3-codex` + `medium` or `high`

Forbidden in this repository. Do not use unless the user explicitly changes the repository model rule.

### `gpt-5.5` + `high`

Use for:

- all repository work without exception unless the user explicitly changes the repository model rule
- architecture decisions
- debugging with unclear root cause
- parser logic
- decoder logic
- TMDS or Genisys translation logic
- protocol interpretation
- database mapping
- state or reset behavior
- final audit and approval gate

## Critical Areas

These always require a separate `gpt-5.5` audit and strict validation:

- parser
- decoding
- bit-level logic
- translation engine
- protocol interpretation
- database mapping
- state or reset behavior
- user-visible interpretation output

## Role Separation

### IMPLEMENTER

- Use `gpt-5.5` with `high` reasoning
- Make a short plan first
- Inspect relevant code, docs, tests, and configuration before changing behavior
- Make the smallest safe change that satisfies the request
- List files to change before editing when the task is non-trivial
- Avoid assumptions
- Explicitly state whether rebuild/run is feasible
- Never self-approve

### AUDITOR

- Must be a separate agent or fresh run from the implementer
- Must use `gpt-5.5` with `high` reasoning
- Assume the implementation may be wrong
- Try to falsify the change
- Check requirement mismatch, hidden assumptions, missing edge cases, incorrect defaults, inconsistent file behavior, regression risk, and incomplete state/reset handling

### VALIDATOR

- Must be a separate agent or fresh run from both the implementer and the auditor
- Check observable behavior against explicit expected behavior
- Distinguish passed, failed, unverified, and ambiguous results
- Do not treat compilation, linting, or confidence language as proof

## Rebuild / Run Policy

- After meaningful code changes, attempt to rebuild and run the app when feasible
- Report rebuild/run status separately from validation status
- A successful rebuild or successful app launch is supporting evidence only
- If rebuild/run cannot be attempted because of environment limits, missing dependencies, or task scope, say so explicitly

## Validation Policy

- Validation is required for all meaningful changes
- Validation must compare behavior versus requirements
- Validation must list what was tested
- Validation must list what was not tested
- Validation must identify unknowns
- Implementation cannot self-validate
- The auditor does not replace the validator

## Definition Of Done

A task is done only when all applicable items below are true:

- Implementation is complete
- Requirements were checked against the request
- The smallest safe change was used
- Risks were listed explicitly
- Required audit was completed
- Required validation was completed
- The app was rebuilt/run when feasible, or the limitation was stated explicitly
- Results were reported clearly
- Unknowns were listed explicitly
- Final output follows the required final review format
- Human final review is still required before production use

## Required Final Output

Every task must end with this structure:

Task Result:
- `Model Used`
- `Reasoning Effort`
- `Files Changed`
- `Summary`
- `Risks`
- `Audit Result`
- `Validation Result`
- `Rebuild/Run Status`
- `What Was Verified`
- `What Failed`
- `What Remains Unverified`
- `Recommended User Review Steps`

Use plain, concise language. Be deterministic. No fluff.
