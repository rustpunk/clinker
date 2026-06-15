# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/clinker-schema`.
Follow the root instructions first.

## Purpose

`clinker-schema` parses `.schema.yaml` files, discovers schema and pipeline
YAML files in a workspace, builds a `SchemaIndex`, and validates pipeline
source/schema references as authoring-time warnings. Treat it as an
edge/authoring support crate unless the boundary questions in
`docs/ai/80_OPEN_QUESTIONS.md` are resolved differently.

## Responsibilities

- Model `.schema.yaml` metadata, field descriptors, source formats, field types, and schema indexes.
- Parse schema YAML through `clinker_plan::yaml::from_str`.
- Discover `.schema.yaml` files and pipeline YAML files from workspace directories.
- Extract scalar `schema:` references from pipeline YAML and populate `SourceSchema::referencing_pipelines`.
- Validate `PipelineConfig` source formats and transform field references against linked schemas.
- Return advisory warnings; do not block planning or execution from this crate.

## Important public APIs

- `parse_schema`, `parse_schema_file`, and `SchemaParseError`.
- `build_workspace_schema_index`.
- `validate_pipeline`, `SchemaWarning`, and `WarningKind`.
- Model exports: `SourceSchema`, `SchemaMetadata`, `SourceFormat`, `FormatCategory`, `FieldDescriptor`, `FieldType`, `SchemaIndex`.
- Public module APIs including `discovery::{discover_schemas, discover_pipelines, extract_schema_refs, resolve_schema_references}`.
- Helper methods such as `SourceFormat::{label, category}`, `FieldType::badge`, `FieldDescriptor::{is_xml_attribute, field_count, flat_field_names}`, `SourceSchema::{total_field_count, all_field_names}`, and `SchemaIndex` lookup methods.

## Internal module map

- `src/lib.rs`: crate docs, public modules, and re-exports.
- `src/model.rs`: schema data model, recursive field descriptors, format/type enums, and `SchemaIndex`.
- `src/parse.rs`: `.schema.yaml` parser and parse errors.
- `src/discovery.rs`: filesystem discovery, simple glob handling, schema reference extraction, reference resolution, and index building.
- `src/validate.rs`: warning model and schema validation against `clinker_plan::config::PipelineConfig`.

All modules are currently public. Most implementation-only helpers still live inside `discovery.rs` and `validate.rs`.

## Dependency rules

### Allowed dependencies

Existing direct dependencies are the baseline:

- `clinker-plan` for `PipelineConfig`, source config types, `InputFormat`, `SchemaSource`, and YAML parsing.
- `serde` for schema model serialization/deserialization.
- `serde_json`, `serde-saphyr`, and `ahash` are declared today; verify source use before changing dependency guidance.
- Dev-only: `tempfile` for filesystem tests.

### Forbidden or suspicious dependencies

- Do not add `clinker-exec`, `clinker`, `clinker-net`, `clinker-channel`, or benchmark crates without architecture review.
- Do not add async runtimes, network clients, native TLS/OpenSSL, C build steps, or cargo-deny exceptions without approval.
- Do not introduce `serde_yaml`; schema YAML currently flows through `clinker_plan::yaml`.
- Be cautious about adding `cxl` to replace validation heuristics. That may be valid later, but it changes this crate's role and should be reviewed.

## Important invariants

- Schema files are shaped as `_schema:` metadata plus `fields:`.
- Schema parsing must stay on `clinker_plan::yaml::from_str`.
- `SourceSchema::path` and `SourceSchema::referencing_pipelines` are index metadata and are skipped by serde.
- `FieldDescriptor::nullable` defaults to `true`.
- Nested fields are recursive and flatten to dot notation.
- Discovery is non-recursive by default: schemas under `schema_dir`, pipelines in the workspace root.
- Include glob support is simple directory/extension scanning; `exclude_globs` is currently not applied.
- `extract_schema_refs` is line-oriented and only strips simple scalar `schema:` values.
- Validation warnings are advisory. They should not become planner errors from this crate.
- CXL field extraction in `validate.rs` is heuristic, not parser/typechecker-backed.
- Format matching is limited to current `InputFormat` arms; fixed-width and Swift currently do not match a `.schema.yaml` format token.

## Common mistakes for AI agents to avoid

- Describing this as runtime schema binding; planner schema binding lives in `clinker-plan`.
- Claiming discovery performs full YAML-aware pipeline parsing.
- Claiming full glob/exclude support.
- Claiming compiler-grade CXL validation.
- Copying old `inputs:` / `transformations:` pipeline shapes from tests or stale docs into current user guidance.
- Copying stale editor-tooling wording into new docs without checking current source.
- Silently making schema model parsing strict with `deny_unknown_fields`; current structs do not establish that behavior.
- Adding runtime, network, or benchmark dependencies casually.

## Local commands

- **Inferred:** `cargo check -p clinker-schema --locked --offline`
- **Inferred:** `cargo test -p clinker-schema --locked --offline`
- **Verified:** `cargo fmt --all --check`
- **Verified:** `git diff --check`

For Rust source changes, also run the relevant workspace gates from the root `AGENTS.md`.

## Documentation updates

Update relevant docs/examples when this crate changes schema file shape, discovery, validation warnings, public APIs, or crate boundaries:

- `crates/clinker-schema/src/lib.rs`
- `docs/ai/10_ARCHITECTURE.md`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/70_GLOSSARY.md`
- `docs/ai/80_OPEN_QUESTIONS.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
- `docs/user/src/nodes/source.md`
- Relevant `docs/user/src/formats/*.md`
- `docs/engine/src/architecture.md`, after verifying against current source because older engine docs contain stale crate layout.
- `examples/pipelines/retract-demo/*.schema.yaml`

## Unclear / ask human

Track unresolved questions only in `docs/ai/80_OPEN_QUESTIONS.md`. Relevant entries include the `clinker-schema`/`clinker-plan` boundary and schema validation completeness.

## Evidence

- `crates/clinker-schema/Cargo.toml`
- `crates/clinker-schema/src/lib.rs`
- `crates/clinker-schema/src/model.rs`
- `crates/clinker-schema/src/parse.rs`
- `crates/clinker-schema/src/discovery.rs`
- `crates/clinker-schema/src/validate.rs`
- `examples/pipelines/retract-demo/orders.schema.yaml`
- `examples/pipelines/retract-demo/audit_events.schema.yaml`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/80_OPEN_QUESTIONS.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
