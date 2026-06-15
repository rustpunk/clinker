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

- **Are parsed-but-underwired CLI flags intentional placeholders or documentation drift?**
  - Evidence: `crates/clinker/src/main.rs` parses and tests flags such as `--memory-limit`, `--error-threshold`, `--dry-run-output`, `--quiet`, and `--rules-path`, but this crate-agent pass did not find clear use of those fields in the run path beyond parsing/helper tests.
  - Why it matters: User-facing CLI docs should not promise behavior that the binary does not implement, and future agents should verify wiring before changing related docs.

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

- **Which docs should be treated as canonical for envelope/document examples?**
  - Evidence: `docs/user/src/pipelines/envelope-and-doc-context.md` contains older-looking snippets using `sources:` / `transform:` / `project:` shapes, while current pipeline structure and examples use unified `nodes:` with `type:` discriminators.
  - Why it matters: The glossary defines envelope concepts from both current code and docs, but examples may need modernization before future agents copy them.

- **Should `CROSS_RECORD_TRANSFORMS_PLAN.md` be marked historical or active?**
  - Evidence: It references paths such as `crates/clinker-core/src/config/pipeline_node.rs` that do not exist in the current workspace, while related concepts such as Reshape and Cull now have implemented docs and code under `clinker-plan` / `clinker-exec`.
  - Why it matters: Future agents may mistake old planning text for current implementation guidance unless the document status is clarified.

## Human Review Notes

- Review the confidence levels in `docs/ai/70_GLOSSARY.md`, especially entries marked Medium or Low.
- Confirm whether AI onboarding docs should use "Klinx" anywhere beyond noting stale/external Kiln references.
