# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/cxl-cli`.
Follow the root instructions first.

## Purpose

Standalone binary package for the `cxl` command. It checks, evaluates, and
formats CXL source by calling into the `cxl` language crate and
`clinker-record` value/context types.

## Responsibilities

- Expose the user-facing `cxl` CLI with `check`, `eval`, and `fmt` subcommands.
- Preserve documented exit codes: `0` success or warnings, `1` CXL parse/resolve/typecheck/eval errors, and `2` I/O/input errors.
- Keep CXL language semantics delegated to `cxl`; do not reimplement parser, resolver, typechecker, or evaluator logic here.
- Convert CLI input data into `clinker_record::Value` for evaluation.
- Print `eval` output as valid pretty JSON.
- Provide a minimal eval context for system variables such as `$pipeline.name`, `$source.*`, and `now`.

## Important public APIs

This is a binary crate, not a Rust library API.

User-facing API:

- Clap command name: `cxl`.
- Subcommands: `check <file>`, `eval [file] [-e <expr>] [--record <json> | --field name=value]`, and `fmt <file>`.
- Exit-code and stdout/stderr behavior.
- Important internal functions: `cmd_check`, `cmd_eval`, and `cmd_fmt`.

## Internal module map

All implementation lives in `src/main.rs`.

- CLI definition: `Cli` and `Command`.
- Command handlers: `cmd_check`, `cmd_eval`, and `cmd_fmt`.
- Eval support: `NullStorage`, `EvalContext` construction, `HashMapResolver`, and `ProgramEvaluator`.
- Input conversion: `parse_field_value` and `json_to_value`.
- Output conversion: `print_record_json` and `value_to_json`.
- Formatting helpers: `format_statement` and `format_expr`.
- Field-reference helpers for `check`: `extract_field_names`, `collect_field_refs_stmt`, and `collect_field_refs_expr`.
- Tests: inline unit tests in `src/main.rs`.

## Dependency rules

### Allowed dependencies

Existing normal dependencies are expected:

- Workspace crates: `cxl` and `clinker-record`.
- CLI/data support: `clap`, `serde_json`, `indexmap`, and `chrono`.
- `miette` is declared in `Cargo.toml`; Hypothesis: keep it only if CLI diagnostics are intended to move toward richer miette rendering, because current source prints diagnostics manually.

### Forbidden or suspicious dependencies

- Do not add `clinker-plan`, `clinker-exec`, `clinker-format`, `clinker-channel`, `clinker-net`, `clinker-schema`, or benchmark crates without architecture review.
- Do not add async runtimes, network clients, native TLS/OpenSSL, C build requirements, or daemon/service assumptions.
- Be suspicious of duplicating CXL syntax, type, or eval behavior here instead of using `cxl`.
- Be suspicious of expanding this into the main pipeline CLI; `clinker` owns pipeline execution.

## Important invariants

- `check` should run parse -> resolve -> typecheck and never evaluate records.
- `eval` should run parse -> resolve -> typecheck -> `ProgramEvaluator::eval_record`.
- `eval` stdout should remain parseable JSON, including skip cases.
- `--record` must remain mutually exclusive with `--field`.
- Keep the minimal eval context stable where documented: `pipeline_name = "cxl-eval"`, zeroed execution/batch UUIDs, row `1`, and source name/file from input or `"<inline>"`.
- `fmt` is a printer over parsed AST, not a language parser replacement.
- Changes to CXL syntax or semantics belong primarily in `crates/cxl`.

## Common mistakes for AI agents to avoid

- Copying CXL language rules into this crate instead of updating `crates/cxl`.
- Treating `cxl-cli` as a library crate with stable Rust APIs.
- Changing documented exit codes without updating user docs.
- Breaking JSON stdout by mixing notes/errors into stdout.
- Adding pipeline execution, YAML config parsing, REST/file format handling, or bounded-memory runtime concerns here.
- Assuming user docs are current without checking source.

## Local commands

- **Inferred:** `cargo check -p cxl-cli --locked --offline`
- **Inferred:** `cargo test -p cxl-cli --locked --offline`
- **Inferred:** `cargo clippy -p cxl-cli --all-targets --locked --offline -- -D warnings`
- **Inferred:** `cargo run --locked --offline -p cxl-cli -- --help`
- **Inferred smoke:** `cargo run --locked --offline -p cxl-cli -- eval -e 'emit result = 1 + 2'`

For language semantics changes, also run the relevant `crates/cxl` checks.

## Documentation updates

- `docs/user/src/cxl/cxl-cli.md` for CLI behavior, examples, exit codes, stdout/stderr, or JSON mapping changes.
- `docs/user/src/cxl/*.md` and `docs/engine/src/cxl-internals.md` for language phase or semantic changes.
- `docs/user/src/getting-started/installation.md` for install/binary behavior.
- `docs/user/src/ops/cli-reference.md` if CLI reference wording changes.
- `docs/ai/20_CRATE_MAP.md`, `docs/ai/50_TESTING_AND_COMMANDS.md`, `docs/ai/70_GLOSSARY.md`, `docs/ai/80_OPEN_QUESTIONS.md`, and possibly `docs/ai/90_CRATE_AGENT_PLAN.md`.

## Unclear / ask human

- `docs/ai/90_CRATE_AGENT_PLAN.md` marked this local file as probably unnecessary because this binary is thin over `cxl`; keep local guidance focused on CLI behavior.
- `docs/user/src/cxl/cxl-cli.md` shows multiple `-e` flags, but `Command::Eval.expr` is `Option<String>`. Decide whether docs are stale or CLI should support repeated `-e`.
- `json_to_value` maps nested JSON objects to `Value::Null`, while the user docs do not clearly document object handling for `--record`.

## Evidence

- `crates/cxl-cli/Cargo.toml`
- `crates/cxl-cli/src/main.rs`
- `crates/cxl/AGENTS.md`
- `docs/user/src/cxl/cxl-cli.md`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/50_TESTING_AND_COMMANDS.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
