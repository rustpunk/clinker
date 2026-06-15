# AI Onboarding: Open Questions

Purpose: Track unresolved documentation and architecture questions for human review before AI agents treat them as facts.

## Status

This is an AI-generated initial draft requiring human review.

## Source Evidence

Validate this page against:

- `CLAUDE.md`
- `CHANGELOG.md`
- `CROSS_RECORD_TRANSFORMS_PLAN.md`
- `notebooklm-sources/**/*.md`
- `rg "TODO|FIXME|open question|question|future|planned|TBD" .`
- `rg "TODO|FIXME|todo!|unimplemented!|panic!" crates examples benches`

Existing files under `docs/*` and `notebooklm-sources/*` may be stale. Treat them as secondary context only; any open question found there must be validated against code, manifests, tests, examples, or safe local commands before being copied here.

## Architecture Questions

- **What is the intended expansion of "CXL"?**
  - Evidence: `docs/user/src/getting-started/concepts.md`, `docs/engine/src/cxl-internals.md`, `crates/cxl/Cargo.toml`, and `crates/cxl/src/lib.rs` consistently define CXL as Clinker's per-record expression language, but this pass did not find an authoritative expansion of the acronym.
  - Why it matters: The glossary can safely define what CXL does, but should not invent what the letters stand for.

- **Should stale `clinker-core` architecture references be updated or removed?**
  - Evidence: `docs/ai/20_CRATE_MAP.md` says current workspace layout has `clinker-plan`, `clinker-exec`, and `clinker-core-types`, while older `docs/engine/src/architecture.md` and `CLAUDE.md` still mention an older `clinker-core` layout.
  - Why it matters: Future agents may otherwise copy obsolete crate-boundary language into reviews or docs.

- **Is `clinker-format -> cxl` intended as a permanent layering edge?**
  - Evidence: `docs/ai/20_CRATE_MAP.md` flags this as a suspicious-but-current dependency edge. `clinker-format` owns CXL-aware envelope/extraction behavior today.
  - Why it matters: The glossary names both crates, but architectural guidance should confirm whether format code is meant to stay CXL-aware.

- **Should `cxl::resolve::HashMapResolver` remain public API?**
  - Evidence: `crates/cxl/src/resolve/test_double.rs` re-exports `clinker_record::resolver::HashMapResolver`, and `crates/cxl/src/resolve/mod.rs` publicly re-exports that test-double module. Current direct uses found in this pass are tests and `crates/cxl/benches/eval.rs`.
  - Why it matters: Future agents should not document or extend it as a stable user-facing API if it is retained only for tests or compatibility.

- **Should `cxl` continue to own `typecheck::Row`?**
  - Evidence: `crates/cxl/src/typecheck/row.rs` documents `Row` as the plan-time row shape and says keeping it in `cxl` preserves the `clinker-plan -> cxl` dependency direction; `crates/cxl/src/typecheck/mod.rs` publicly re-exports `Row`.
  - Why it matters: Row-type ownership affects the long-term language/planner boundary and dependency direction.

## Primary Code Evidence

- **Is `reserve/` definitively only a crates.io name-reservation package?**
  - Evidence: `docs/ai/20_CRATE_MAP.md` describes `reserve/` as a separate non-workspace Cargo package named `clinker` that appears to reserve the crate name.
  - Why it matters: The glossary marks this Medium confidence because the role is inferred from local files, not an explicit current-owner statement.

- **Should the public legacy `RouteConfig` / `RouteBranch` surface remain documented?**
  - Evidence: Current unified `PipelineNode::Route` uses `RouteBody { conditions: IndexMap<_, _>, default, mode }` (`crates/clinker-plan/src/config/pipeline_node.rs`), while `crates/clinker-plan/src/config/route.rs` still defines public `RouteConfig` / `RouteBranch` using a `branches` vector and comments call that legacy.
  - Why it matters: The glossary documents user-facing route terms from `RouteBody.conditions`, but a future API-facing glossary might need to explain whether `RouteConfig` is retained compatibility surface or cleanup debt.

## Crate Boundary Questions

- **What is the intended long-term status of Kiln/Klinx and Dioxus references in this repository?**
  - Evidence: `docs/ai/20_CRATE_MAP.md` says Kiln was extracted and renamed to Klinx in another repository, but local files still contain Kiln references such as `_notes` comments, `examples/pipelines/kiln.toml`, release workflow/root dependency references noted by the crate map, and explain JSON comments for Kiln consumers.
  - Why it matters: The glossary can only mark Kiln/Klinx as historical/currently external with Medium confidence until maintainers decide which references are active compatibility surface versus stale cleanup.

- **Should `clinker-exec` continue re-entering compilation from `CompiledPlan::config()` during runs?**
  - Evidence: `PipelineExecutor::run_plan_with_readers_writers` requires `&CompiledPlan`, but `crates/clinker-exec/src/executor/mod.rs` forwards through `plan.config()` into `run_with_readers_writers`; `docs/ai/10_ARCHITECTURE.md` also notes this nuance.
  - Why it matters: Future agents should preserve the public compiled-plan boundary, but should not assume the stored `ExecutionPlanDag` is the only runtime input unless maintainers confirm or the implementation changes.

- **Is the optional `clinker-exec -> clinker-bench-support` `bench-alloc` edge acceptable long term?**
  - Evidence: `crates/clinker-exec/Cargo.toml` has optional `clinker-bench-support` behind `bench-alloc`, and `docs/ai/20_CRATE_MAP.md` flags this as intentional-looking but suspicious coupling.
  - Why it matters: Crate-level guidance can require the edge to stay feature-gated, but broader architecture guidance should confirm whether allocation instrumentation belongs there or elsewhere.

- **Should benchmark helper APIs remain strictly outside production runtime dependency paths?**
  - Evidence: `crates/clinker-bench-support/src/lib.rs` says the crate is used via dev dependencies and is not shipped in release builds, while `crates/clinker-bench-support/src/io.rs` avoids constructing executor `FileSlot` values because importing `clinker-exec` would form a cycle.
  - Why it matters: Future agents should not promote panic/unwrap-heavy test helpers, generator shortcuts, or synthetic I/O helpers into runtime code without maintainer review.

- **Which benchmark identifiers and cache keys are compatibility surfaces?**
  - Evidence: `crates/clinker-bench-support/src/lib.rs` exposes `Scale` labels used in Criterion IDs, `FieldKind` uses stable discriminants, and `crates/clinker-bench-support/src/cache.rs` hashes `DataFormat`, field types, tags, wrappers, generator version, and other generation parameters.
  - Why it matters: Changing labels, discriminants, cache filenames, or hash inputs can invalidate benchmark comparisons and cached data in ways that may be intentional or accidental.

- **How broad should benchmark-generated format support become?**
  - Evidence: `crates/clinker-benchmarks/src/format_mapping.rs` maps CSV, XML, fixed-width, JSON NDJSON, and JSON array to generated data, but explicitly rejects JSON object, EDIFACT, X12, HL7, and SWIFT benchmark generation.
  - Why it matters: Future agents should not add benchmark YAML for unsupported generated formats or claim benchmark coverage without extending data generation and mapping deliberately.

- **Is `clinker-benchmarks` CI JSON a stable compatibility surface?**
  - Evidence: `crates/clinker-benchmarks/src/report.rs` defines `BenchReport`, `BenchResult`, and `StageResult`, and writes summary JSON to `target/bench-results/summary.json`, but this pass found no separate JSON schema or documented compatibility promise.
  - Why it matters: Future agents changing report fields need to know whether downstream CI/report tooling expects the current shape.

- **Should `clinker-schema` remain separate and dependent on `clinker-plan`, or should schema discovery/validation live in planning long term?**
  - Evidence: `docs/ai/20_CRATE_MAP.md` describes `clinker-schema` as parsing `.schema.yaml`, discovering schema files, building `SchemaIndex`, and validating pipeline schema references; `crates/clinker-schema/Cargo.toml` depends on `clinker-plan`.
  - Why it matters: Future agents need to know whether schema discovery/validation is an edge authoring/API concern or part of the compile-time planning boundary.

- **How complete should `clinker-schema` authoring validation be?**
  - Evidence: `crates/clinker-schema/src/discovery.rs` extracts `schema:` references with a line-oriented scan, implements only simple include glob expansion, and currently ignores `exclude_globs`; `crates/clinker-schema/src/validate.rs` uses heuristic CXL field extraction rather than the CXL parser/typechecker, has `WarningKind::{JoinKeyMissing, JoinKeyTypeMismatch}` variants that this pass did not find emitted, and its module comment says missing schemas produce no warnings while linked missing schema files emit `SchemaMissing`.
  - Why it matters: Future agents should not document this crate as full pipeline/CXL validation or broaden it into planner-grade validation without maintainer confirmation.

- **Should stale `clinker-kiln` wording in `clinker-schema` crate docs be replaced?**
  - Evidence: `crates/clinker-schema/src/lib.rs` says the crate is shared between `clinker-kiln` and `clinker`, while `docs/ai/20_CRATE_MAP.md` and other AI docs flag Kiln/Klinx references as stale or external to this workspace.
  - Why it matters: Future docs should not present historical editor integration as current architecture without maintainer confirmation.

- **Are `clinker-schema` manifest dependencies broader than current source use?**
  - Evidence: `crates/clinker-schema/Cargo.toml` declares `serde_json`, `serde-saphyr`, and `ahash`, while this crate-agent pass found weak direct source evidence for those dependencies under `crates/clinker-schema/src`.
  - Why it matters: Future agents should verify before relying on, documenting, or removing those dependencies.

- **How broad should composition resource support become beyond implemented file resources?**
  - Evidence: `clinker-plan` owns `ResourceDecl`, `ResourceKind`, `Resource`, and composition resource parsing, but `docs/ai/40_COMMON_PATTERNS.md` says evidence currently shows only file resources and validation is still partly stubbed.
  - Why it matters: Future agents should not document or implement broader resource support by inference; maintainers should confirm the intended resource model before agents expand it.

- **Are channel files only an edge-layer concern, or can lower crates own channel-adjacent concepts?**
  - Evidence: `clinker-channel` owns binding/overlay/staging-copy code, while `clinker-plan` owns channel identity in `CompiledPlan` and composition/resource declarations.
  - Why it matters: The glossary links channels to several crates, but crate-boundary docs should clarify ownership before future agents move code.

- **Should channel `resources.default` / `resources.fixed` be implemented as overlays or documented as reserved future surface?**
  - Evidence: `clinker-channel::ChannelBinding` parses `resources_default` and `resources_fixed`, but `apply_channel_overlay` currently applies `config_default` and `config_fixed` plus var overrides, not resource overlays.
  - Why it matters: Future crate-level guidance should not tell agents that resource overlays are fully implemented unless maintainers confirm or the implementation lands.

- **Should pipeline-target channel config keys be validated before overlay application?**
  - Evidence: `validate_channel_bindings` validates composition-target config keys against `CompositionSymbolTable`, while `crates/clinker-channel/tests/channel_binding_test.rs` notes pipeline-targeting fixtures cannot be validated against that table and validation is deferred.
  - Why it matters: Future agents changing channel validation need to know whether deferred validation is intentional design or cleanup debt.

- **Should `clinker-net` docs and manifests mention SQL cursors before implementation exists?**
  - Evidence: `crates/clinker-net/Cargo.toml` describes "REST, SQL cursors" and user docs mention finite SQL cursors, but `crates/clinker-net/src/lib.rs`, `src/rest.rs`, and tests currently implement REST only.
  - Why it matters: Agents should not infer SQL cursor support from roadmap wording when updating transport behavior or docs.

- **Should `clinker-net` remain coupled to `clinker-exec::source::RecordSource`?**
  - Evidence: `docs/ai/20_CRATE_MAP.md` and `docs/ai/10_ARCHITECTURE.md` note the current `clinker-net -> clinker-exec` edge; `crates/clinker-net/src/lib.rs` exposes `build_rest_source` as an adapter into `Box<dyn clinker_exec::source::RecordSource>`.
  - Why it matters: This determines whether network transport stays an executor integration crate or whether source contracts should move to a lower layer.

- **Are parsed-but-underwired CLI flags intentional placeholders or documentation drift?**
  - Evidence: `crates/clinker/src/main.rs` parses and tests flags such as `--memory-limit`, `--error-threshold`, `--dry-run-output`, `--quiet`, and `--rules-path`, but this crate-agent pass did not find clear use of those fields in the run path beyond parsing/helper tests.
  - Why it matters: User-facing CLI docs should not promise behavior that the binary does not implement, and future agents should verify wiring before changing related docs.

- **Should `clinker explain --channel` apply channel overlays?**
  - Evidence: `crates/clinker/src/main.rs` defines `ExplainArgs::channel` and help text for `clinker explain ... --channel`, while `run_explain` should be checked against the `run` path where `apply_channel_overlay` is invoked before explain output.
  - Why it matters: Explain output may differ depending on invocation path unless channel handling is intentionally scoped.

- **Should production-path `expect(...)` calls in the CLI be replaced or documented as invariant checks?**
  - Evidence: `crates/clinker/src/main.rs` contains production-path `expect("compile")` and `expect("pipeline has at least one source")` calls in the run flow.
  - Why it matters: Future agents should not copy panic-style invariant handling into user-facing CLI paths without confirming those assumptions.

## Testing Questions

- **Should glossary-only doc changes get a docs renderer check?**
  - Evidence: `docs/ai/50_TESTING_AND_COMMANDS.md` says docs-only edits often do not need cargo tests, but Markdown rendering still matters. This pass produced large Markdown tables that should be checked visually or with a book renderer if that is part of the normal workflow.
  - Why it matters: A rendered-doc command for AI docs is not documented in the current AI onboarding set.

## Documentation Questions

- **Should user-facing node lists be updated from eight to eleven node types?**
  - Evidence: `docs/user/src/getting-started/concepts.md` and `docs/user/src/pipelines/structure.md` list eight node types, while current `PipelineNode` accepts eleven variants: `source`, `transform`, `aggregate`, `route`, `merge`, `combine`, `output`, `reshape`, `cull`, `envelope`, and `composition`.
  - Why it matters: The glossary follows current code, but user docs may under-document active `reshape`, `cull`, and `envelope` nodes.

- **Should source-node docs be updated for REST transport?**
  - Evidence: `docs/user/src/nodes/source.md` says the only transport today is `file`, while `docs/user/src/formats/source-network.md`, `clinker-plan` source config, and `clinker-net` implement finite REST sources.
  - Why it matters: Future agents may copy stale source-node transport guidance into examples or validation docs.

- **Should CLI long help be updated for the unified `nodes:` pipeline shape?**
  - Evidence: `crates/clinker/src/main.rs` long help says pipelines specify "inputs, outputs, field mappings"; `docs/ai/10_ARCHITECTURE.md` and `docs/ai/30_DESIGN_RULES.md` say unified `nodes:` is the active parsed pipeline shape.
  - Why it matters: Help text is user-facing and can otherwise reinforce retired config shapes.

- **Should `ExplainFormat::Json` wording keep mentioning Kiln canvas consumers?**
  - Evidence: `crates/clinker/src/main.rs` documents `ExplainFormat::Json` as structured JSON for Kiln canvas consumption, while `docs/ai/10_ARCHITECTURE.md` and `docs/ai/20_CRATE_MAP.md` flag Kiln/Klinx references as stale or external.
  - Why it matters: Public docs and help text should distinguish current supported consumers from historical or external integrations.

- **Should channel var diagnostic codes `E109`, `E110`, and `E111` get dedicated explain pages?**
  - Evidence: `crates/clinker-channel/src/overlay.rs` emits `E109`, `E110`, and `E111` for channel var overlay failures, and `docs/user/src/pipelines/channels.md` / `docs/user/src/pipelines/variables.md` document those codes. `crates/clinker-plan/src/plan/explain_provenance.rs` maps only selected codes to `docs/explain/<code>.md` pages.
  - Why it matters: Diagnostic-code lookup should be consistent for user-visible channel variable failures.

- **Should broad non-UTF-8 or charset support be documented or expanded?**
  - Evidence: `docs/user/src/formats/x12.md` documents an `encoding` option with UTF-8 / ISO-8859-1 behavior, while other format docs and source files such as `docs/user/src/formats/hl7.md`, `docs/user/src/formats/swift.md`, and `crates/clinker-format/src/hl7/tokenizer.rs` reject non-UTF-8 message text.
  - Why it matters: Format docs should not promise encoding support beyond implemented reader/writer behavior.

- **Which older internal format notes are historical versus current guidance?**
  - Evidence: Older docs under `docs/internal/` and `notebooklm-sources/` may describe historical format behavior, while `docs/ai/00_READ_THIS_FIRST.md` says existing docs are secondary and must be validated against source before reuse.
  - Why it matters: Future agents may otherwise copy stale format behavior into implementation or docs.

- **Which docs should be treated as canonical for envelope/document examples?**
  - Evidence: `docs/user/src/pipelines/envelope-and-doc-context.md` contains older-looking snippets using `sources:` / `transform:` / `project:` shapes, while current pipeline structure and examples use unified `nodes:` with `type:` discriminators.
  - Why it matters: The glossary defines envelope concepts from both current code and docs, but examples may need modernization before future agents copy them.

- **Should `CROSS_RECORD_TRANSFORMS_PLAN.md` be marked historical or active?**
  - Evidence: It references paths such as `crates/clinker-core/src/config/pipeline_node.rs` that do not exist in the current workspace, while related concepts such as Reshape and Cull now have implemented docs and code under `clinker-plan` / `clinker-exec`.
  - Why it matters: Future agents may mistake old planning text for current implementation guidance unless the document status is clarified.

## Human Review Notes

- Review the confidence levels in `docs/ai/70_GLOSSARY.md`, especially entries marked Medium or Low.
- Confirm whether AI onboarding docs should use "Klinx" anywhere beyond noting stale/external Kiln references.
