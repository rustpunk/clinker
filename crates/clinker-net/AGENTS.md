# AGENTS.md

Root `AGENTS.md` still applies. This file adds local guidance for `clinker-net`.

## Purpose

`clinker-net` provides finite-pull network source integration for Clinker. The implemented transport today is REST: blocking paginated HTTP `GET` responses are decoded as records and adapted into executor `RecordSource` inputs.

## Responsibilities

- Build REST readers from validated `clinker-plan` source config.
- Preserve finite batch behavior with hard `max_pages` and optional `max_records` caps.
- Use synchronous blocking HTTP via `ureq` over rustls on the source ingest thread.
- Decode REST page bodies through `clinker-format` JSON/XML readers.
- Project/coerce page records through the same `CoercingReader` path used by file sources.
- Feed runtime through `clinker_exec::source::RecordSource` / `SourceInput::Records`, not a separate dispatch path.
- Treat connect, HTTP status, and body-read failures as hard source errors.

## Important public APIs

- `build_rest_source(...) -> Result<Box<dyn clinker_exec::source::RecordSource>, FormatError>`
- Related config types from `clinker-plan`: `RestSourceConfig`, `RestPagination`, `RestAuth`, `SourceTransport::Rest`
- Related runtime contract from `clinker-exec`: `RecordSource`, `SourceInput::Records`

## Internal module map

- `src/lib.rs`: crate docs, private `rest` module, public `build_rest_source`, REST error helpers.
- `src/rest.rs`: `RestRecordSource`, pagination cursor state, blocking GET/retry logic, auth headers, body decoding, schema construction, shutdown handling.
- `tests/rest_pagination.rs`: local socket tests for offset, link-header, cursor-token, caps, wrapped bodies, and shutdown.
- `tests/rest_executor_e2e.rs`: executor integration through `SourceInput::Records`, provenance, and DLQ behavior.

## Dependency rules

### Allowed dependencies

Current normal dependencies are intentional: `clinker-exec`, `clinker-plan`, `clinker-format`, `clinker-record`, `serde_json`, `indexmap`, `tracing`, and workspace `ureq` configured with rustls and no default native TLS path. Current dev dependencies are `clinker-exec` with `test-utils` and `clinker-bench-support`.

### Forbidden or suspicious dependencies

- Do not add native TLS/OpenSSL/cmake/C build dependencies without approval.
- Do not add async HTTP clients or Tokio-driven transport/runtime behavior without architecture review.
- Do not add long-lived streaming, websocket, daemon, or polling connectors.
- Do not add benchmark/test helper dependencies to default runtime paths.
- Treat new database/SQL client dependencies as suspicious until SQL cursor support is explicitly approved and bounded.

## Important invariants

- `RecordSource::next_record` must remain finite.
- Every network transport needs explicit bounds equivalent to REST `max_pages` / `max_records`.
- REST is pathless: no filesystem discovery, no file matchers, and synthetic `<source:NAME>` identity in edge code.
- REST bodies currently decode only through JSON/XML reader paths.
- Network readers flow into the same ingest channel as file sources.
- HTTP/connect/body failures are hard source errors, not per-row DLQ records.
- 5xx/transport failures retry only within configured retry limits; 4xx is fatal.
- Page body reads are capped by `MAX_PAGE_BYTES`.
- Hypothesis: SQL cursor wording in docs/manifests is roadmap language, not implemented behavior in this crate.

## Common mistakes for AI agents to avoid

- Inventing implemented SQL cursor support from broad wording in docs or the manifest.
- Making `max_pages` optional for REST.
- Adding async runtime assumptions.
- Faking filesystem paths for network sources.
- Special-casing REST inside executor operator dispatch.
- Turning hard HTTP/body errors into per-row DLQ records.
- Bypassing `CoercingReader` and drifting from file-source row semantics.
- Assuming local socket tests will pass inside restricted sandboxes.

## Local commands

- Inferred: `cargo check -p clinker-net --locked --offline`
- Inferred: `cargo test -p clinker-net --locked --offline`
- Inferred, targeted pagination: `cargo test -p clinker-net --test rest_pagination --locked --offline`
- Inferred, if config validation changes: `cargo test -p clinker-exec --test transport_validation --locked --offline`
- Inferred: if localhost socket binding fails with `Operation not permitted`, rerun outside the restricted sandbox or in CI.

## Documentation updates

Update these when changing related behavior:

- `docs/user/src/formats/source-network.md`
- `docs/user/src/nodes/source.md`
- `docs/user/src/pipelines/envelope-and-doc-context.md` for REST `$doc` / envelope rules
- `docs/user/src/getting-started/concepts.md` and `docs/user/src/README.md` if finite-cursor wording changes
- `docs/ai/10_ARCHITECTURE.md`
- `docs/ai/20_CRATE_MAP.md`
- `docs/ai/30_DESIGN_RULES.md`
- `docs/ai/40_COMMON_PATTERNS.md`
- `docs/ai/50_TESTING_AND_COMMANDS.md` if socket/test behavior changes
- `docs/ai/80_OPEN_QUESTIONS.md`

## Evidence

- `crates/clinker-net/Cargo.toml`
- `crates/clinker-net/src/lib.rs`
- `crates/clinker-net/src/rest.rs`
- `crates/clinker-net/tests/rest_pagination.rs`
- `crates/clinker-net/tests/rest_executor_e2e.rs`
- `crates/clinker-exec/src/source/mod.rs`
- `crates/clinker-plan/src/config/source.rs`
- `crates/clinker/src/main.rs`
- `crates/clinker-exec/tests/transport_validation.rs`
- `docs/user/src/formats/source-network.md`
- `docs/ai/50_TESTING_AND_COMMANDS.md`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
