//! Top-level pipeline configuration and the compile pipeline that lowers it to an execution plan.

use super::*;
use crate::yaml::Spanned;
// Default `fNN`/`eNN` ceilings each reader enforces, referenced by the HL7
// split-field reachability check and the `$doc` positional-bound validation
// so the planner's bounds cannot drift from the reader's own constants.
use clinker_format::HL7_DEFAULT_MAX_FIELDS;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level pipeline configuration, deserialized from YAML.
///
/// Only the unified `nodes:` YAML shape parses. Legacy top-level
/// `inputs:`/`outputs:`/`transformations:` sections are rejected by
/// serde at deserialization time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub pipeline: PipelineMeta,
    /// Unified pipeline node taxonomy. Each node carries its
    /// YAML source span via the [`Spanned`] outer wrap.
    #[serde(skip_serializing)]
    pub nodes: Vec<Spanned<PipelineNode>>,
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
    /// External tooling metadata: pipeline-level notes. Ignored by the engine.
    #[serde(default, rename = "_notes", skip_serializing_if = "Option::is_none")]
    pub notes: Option<serde_json::Value>,
    /// BLAKE3 hash of the post-env-var-interpolated source YAML bytes.
    /// Stamped by [`load_config_with_vars`]; zero array for in-memory
    /// configs that did not flow through a file load (e.g. tests).
    /// Threaded onto `CompiledPlan` at compile time so the executor can
    /// expand `{pipeline_hash}` template tokens and stamp provenance
    /// sidecars without needing to re-read the source.
    #[serde(skip)]
    pub source_hash: [u8; 32],
}

/// Pipeline-level metadata and global settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineMeta {
    pub name: String,
    /// Memory-arbitrator tuning. Whole block is optional; when omitted
    /// each field falls back to its default (512 MiB limit, `pause`
    /// backpressure policy).
    #[serde(default, skip_serializing_if = "MemoryConfig::is_default")]
    pub memory: MemoryConfig,
    /// Per-batch event count for streaming inter-stage handoff. Bounds
    /// how many records (plus document-boundary punctuations) a
    /// streaming-eligible stage accumulates before charging and handing
    /// off one batch, so peak inter-stage memory is one batch rather than
    /// the whole stage. `None` (omitted) falls back to the executor's
    /// default batch size; an explicit
    /// `0` is rejected at config validation (a zero batch never flushes).
    /// A per-Transform `batch_size` on `TransformBody` overrides this for
    /// that one stage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,
    /// Static configuration knobs read via `$vars.<key>`. Flat top-level
    /// shape: `vars: { fuzzy_threshold: { type: float, default: 0.85 } }`.
    /// Channel-overridable, frozen at pipeline start. No producer; no
    /// DAG-descendant rule. Producer-written scoped state lives on
    /// individual Transforms via their `declares:` block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vars: Option<IndexMap<String, ScopedVarDecl>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_formats: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rules_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<ConcurrencyConfig>,
    // Spec stubs — processed in later phases
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_locale: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_rules: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_provenance: Option<bool>,
    /// Execution metrics spool configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsConfig>,
}

/// Memory-arbitrator tuning, nested under `pipeline.memory`.
///
/// `limit` accepts a byte-size suffix grammar: `"512M"`, `"2G"`,
/// `"1024K"`, or a raw integer byte count. When omitted, the
/// arbitrator falls back to a 512 MiB hard ceiling, resolved at
/// construction time by the memory subsystem rather than here.
///
/// `backpressure` selects the active arbitration policy. The
/// default `pause` installs `BackPressurePreferred -> Priority`:
/// prefer pausing a producer over forcing anyone to spill, falling
/// back to cheapest-to-spill-first when no consumer can be paused.
/// `spill` installs bare `Priority` for users who want react-only
/// behavior keyed on per-operator spill priority. `both` installs
/// `BackPressurePreferred -> LargestFirst`: pause when possible,
/// otherwise force the largest holder regardless of priority.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryConfig {
    /// Hard memory limit for the arbitrator. `None` (omitted) →
    /// 512 MiB at construction time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<String>,
    /// Arbitration policy selector. Omitted → `pause` (the runtime
    /// default).
    #[serde(skip_serializing_if = "BackpressureKnob::is_default")]
    pub backpressure: BackpressureKnob,
}

impl MemoryConfig {
    /// True when every field matches its default. Used as the
    /// `skip_serializing_if` gate on `PipelineMeta.memory` so a
    /// pipeline with no memory opinions round-trips through serde
    /// without emitting an empty `memory: {}` block.
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

/// Arbitration policy selector for `pipeline.memory.backpressure`.
///
/// Values are user-facing YAML strings: `spill`, `pause`, `both`.
/// `Pause` is the runtime default.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureKnob {
    /// `Priority` only — never pause a producer. Equivalent to the
    /// post-#108 / pre-#157 react-only behavior, but with
    /// deterministic priority-based victim selection rather than
    /// "whichever operator polls next."
    Spill,
    /// `BackPressurePreferred -> Priority` — pause any
    /// back-pressureable consumer before forcing a spill; otherwise
    /// fall back to `Priority`. Default.
    #[default]
    Pause,
    /// `BackPressurePreferred -> LargestFirst` — pause when
    /// possible, otherwise force the largest holder to spill
    /// regardless of priority. Useful when one operator dominates
    /// the budget and a fairness override is wanted.
    Both,
}

impl BackpressureKnob {
    /// True when this knob equals the default (`Pause`). Used as
    /// the `skip_serializing_if` gate inside `MemoryConfig` so a
    /// `backpressure: pause` field is dropped from serde output
    /// rather than re-emitted as redundant noise.
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Pause)
    }

    /// Whether this policy can park a producer on a pause that only a
    /// later `resume()` releases.
    ///
    /// `Pause` and `Both` both install a `BackPressurePreferred` front
    /// that prefers pausing a back-pressureable producer over forcing a
    /// spill; `Spill` (bare `Priority`) never pauses. The distinction is
    /// load-bearing for the unsatisfiable-budget startup check: a budget
    /// below the process baseline RSS deadlocks only under a pausing
    /// policy (the paused Source never resumes because RSS can never drop
    /// under the ceiling), whereas under `Spill` the same budget spills
    /// or aborts immediately and makes forward progress.
    pub fn pauses_producers(&self) -> bool {
        match self {
            Self::Spill => false,
            Self::Pause | Self::Both => true,
        }
    }

    /// Display name of the arbitration policy this knob selects.
    ///
    /// Matches the `policy_name()` the corresponding boxed policy reports
    /// at runtime, so the `--explain` plan-time renderer can label the
    /// policy without constructing the runtime arbitrator.
    pub fn policy_name(&self) -> &'static str {
        match self {
            Self::Spill => "Priority",
            Self::Pause => "BackPressurePreferred -> Priority",
            Self::Both => "BackPressurePreferred -> LargestFirst",
        }
    }
}

/// Execution metrics reporting configuration.
///
/// Clinker writes one JSON file per pipeline run to `spool_dir` using an
/// atomic write-then-rename strategy. A separate `clinker metrics collect`
/// command sweeps the spool and appends records to an NDJSON archive.
///
/// Config precedence (highest → lowest):
/// `--metrics-spool-dir` CLI flag > `CLINKER_METRICS_SPOOL_DIR` env var > this field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Directory where per-execution JSON files are spooled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spool_dir: Option<String>,
}

/// Concurrency settings for parallel chunk processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConcurrencyConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<usize>,
}

impl PipelineConfig {
    /// Public iterator over source nodes.
    pub fn source_configs(&self) -> impl Iterator<Item = &SourceConfig> + '_ {
        self.nodes.iter().filter_map(|n| match &n.value {
            PipelineNode::Source { config: body, .. } => Some(&body.source),
            _ => None,
        })
    }

    /// Public iterator over source bodies. Each item carries the inline
    /// `schema:` declaration and the per-source `correlation_key:` (when
    /// declared) alongside the format-layer [`SourceConfig`].
    pub fn source_bodies(
        &self,
    ) -> impl Iterator<Item = &crate::config::pipeline_node::SourceBody> + '_ {
        self.nodes.iter().filter_map(|n| match &n.value {
            PipelineNode::Source { config: body, .. } => Some(body),
            _ => None,
        })
    }

    /// Whether any source declares a `correlation_key:`. Surfaces the
    /// "is grouped DLQ active anywhere in this pipeline" bit consumed
    /// by planner gates and the runtime correlation-buffer setup.
    pub fn any_source_has_correlation_key(&self) -> bool {
        self.source_bodies().any(|b| b.correlation_key.is_some())
    }

    /// Whether any source declares `dlq_granularity: document`. Drives the
    /// runtime document-DLQ buffer setup and gates the streaming fast-paths
    /// off pipeline-wide, mirroring `any_source_has_correlation_key` —
    /// per-document buffering at the Output arm needs the materialized
    /// `DocumentClose` punctuation the streaming-Output / streaming-ingest
    /// short-circuits would otherwise consume out of band.
    pub fn any_source_has_document_dlq(&self) -> bool {
        self.source_configs()
            .any(|s| s.dlq_granularity == DlqGranularity::Document)
    }

    /// Whether any Output declares `reconstruct_envelope: true`. Gates the
    /// fused streaming-writer fast path off pipeline-wide, mirroring
    /// `any_source_has_document_dlq`: the fused streaming consumer writes each
    /// record straight to disk with no per-document framing, so an
    /// envelope-reconstructing Output must take the materialized arm, which
    /// detects document boundaries from each record's context and fires the
    /// writer's `begin_document` / `end_document`.
    pub fn any_output_reconstructs_envelope(&self) -> bool {
        self.output_configs().any(|o| o.reconstruct_envelope)
    }

    /// Public iterator over output nodes.
    pub fn output_configs(&self) -> impl Iterator<Item = &OutputConfig> + '_ {
        self.nodes.iter().filter_map(|n| match &n.value {
            PipelineNode::Output { config: body, .. } => Some(&body.output),
            _ => None,
        })
    }

    /// Public iterator over transform-like nodes (Transform + Aggregate +
    /// Route), yielding a lightweight [`TransformView`] with the minimum
    /// surface external tooling and schema validation need. Merge nodes are
    /// deliberately excluded — they have no CXL body or description.
    pub fn transform_views(&self) -> impl Iterator<Item = TransformView<'_>> + '_ {
        self.nodes.iter().filter_map(|n| match &n.value {
            PipelineNode::Transform {
                header,
                config: body,
            } => Some(TransformView {
                name: &header.name,
                description: header.description.as_deref(),
                cxl_source: body.cxl.as_ref(),
                notes: header.notes.as_ref(),
                kind: TransformViewKind::Transform,
            }),
            PipelineNode::Aggregate {
                header,
                config: body,
            } => Some(TransformView {
                name: &header.name,
                description: header.description.as_deref(),
                cxl_source: body.cxl.as_ref(),
                notes: header.notes.as_ref(),
                kind: TransformViewKind::Aggregate,
            }),
            PipelineNode::Route { header, .. } => Some(TransformView {
                name: &header.name,
                description: header.description.as_deref(),
                cxl_source: "",
                notes: header.notes.as_ref(),
                kind: TransformViewKind::Route,
            }),
            _ => None,
        })
    }

    /// Look up the `_notes` field for a stage by name, reading from
    /// whichever node variant hosts it. Returns `None` if no node with
    /// that name exists (or the node type has no notes slot).
    pub fn stage_notes(&self, stage_name: &str) -> Option<&serde_json::Value> {
        self.nodes.iter().find_map(|n| match &n.value {
            PipelineNode::Source { config: body, .. } if body.source.name == stage_name => {
                body.source.notes.as_ref()
            }
            PipelineNode::Output { config: body, .. } if body.output.name == stage_name => {
                body.output.notes.as_ref()
            }
            PipelineNode::Transform { header, .. }
            | PipelineNode::Aggregate { header, .. }
            | PipelineNode::Route { header, .. }
            | PipelineNode::Composition { header, .. }
                if header.name == stage_name =>
            {
                header.notes.as_ref()
            }
            PipelineNode::Merge { header, .. } if header.name == stage_name => {
                header.notes.as_ref()
            }
            _ => None,
        })
    }

    /// Set the `_notes` field for a stage by name. No-op if no such
    /// stage exists.
    pub fn set_stage_notes(&mut self, stage_name: &str, notes: Option<serde_json::Value>) {
        for spanned in self.nodes.iter_mut() {
            match &mut spanned.value {
                PipelineNode::Source { config: body, .. } if body.source.name == stage_name => {
                    body.source.notes = notes;
                    return;
                }
                PipelineNode::Output { config: body, .. } if body.output.name == stage_name => {
                    body.output.notes = notes;
                    return;
                }
                PipelineNode::Transform { header, .. }
                | PipelineNode::Aggregate { header, .. }
                | PipelineNode::Route { header, .. }
                | PipelineNode::Composition { header, .. }
                    if header.name == stage_name =>
                {
                    header.notes = notes;
                    return;
                }
                PipelineNode::Merge { header, .. } if header.name == stage_name => {
                    header.notes = notes;
                    return;
                }
                _ => {}
            }
        }
    }

    /// Count of Transform-ish nodes (Transform + Aggregate + Route + Merge).
    pub fn transform_node_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|n| {
                matches!(
                    &n.value,
                    PipelineNode::Transform { .. }
                        | PipelineNode::Aggregate { .. }
                        | PipelineNode::Route { .. }
                        | PipelineNode::Merge { .. }
                )
            })
            .count()
    }

    /// Validation pre-pass over the unified `nodes:` taxonomy. Runs the
    /// four name/topology stages in fixed order, accumulating diagnostics:
    ///
    ///   1. Duplicate names (`E001` exact dup, `W002` case-only dup)
    ///   2. Self-loops (`E002`)
    ///   3. General cycles (`E003` via `tarjan_scc`)
    ///   4. Path validation (delegates to `security::validate_all_config_paths`)
    ///
    /// Stage 5 (per-variant lowering to `PlanNode`) is intentionally
    /// omitted here. This method returns either an empty diagnostics
    /// vector (the unified topology is consistent) or a populated one
    /// (caller decides whether to abort).
    ///
    /// Stages run to completion and append rather than short-circuit,
    /// matching the rustc `Session::has_errors` pattern. Self-loops
    /// are routed to the dedicated E002 check before general cycle
    /// detection so the diagnostic message is more actionable.
    pub fn compile_topology_only(
        &self,
        ctx: &CompileContext,
    ) -> Vec<clinker_core_types::Diagnostic> {
        use clinker_core_types::graph::NameGraph;
        use clinker_core_types::span::Span;
        use clinker_core_types::{Diagnostic, LabeledSpan};
        use std::collections::BTreeMap;

        let mut diags = Vec::new();
        // span_for(spanned) converts a per-node saphyr
        // `Spanned<PipelineNode>` into a `LabeledSpan` carrying a
        // `Span::line_only` synthetic span with the real source line.
        let span_for = |spanned: &Spanned<PipelineNode>| -> LabeledSpan {
            let line = spanned.referenced.line() as u32;
            let s = if line > 0 {
                Span::line_only(line)
            } else {
                // (c) serde-saphyr loses node-header location info
                // through `#[serde(tag)] + #[serde(flatten)]`; no
                // precise span is recoverable at this layer.
                Span::SYNTHETIC
            };
            LabeledSpan::primary(s, String::new())
        };
        // (a) Whole-DAG diagnostic: stage-3 cycle detection emits one
        // diagnostic that covers the entire pipeline graph, with no
        // single node to anchor on.
        let synth = || LabeledSpan::primary(Span::SYNTHETIC, String::new());

        // ── Stage 1: duplicate names ────────────────────────────────
        // Names are case-sensitive (matches Unix FS, Airflow, Beam).
        // Exact duplicates → E001 error. Case-only duplicates → W002.
        let mut seen_exact: BTreeMap<String, ()> = BTreeMap::new();
        let mut by_name_lower: BTreeMap<String, String> = BTreeMap::new();
        for spanned in &self.nodes {
            let node = &spanned.value;
            let name = node.name();
            if seen_exact.contains_key(name) {
                diags.push(Diagnostic::error(
                    "E001",
                    format!("duplicate node name: {name:?}"),
                    span_for(spanned),
                ));
                continue;
            }
            let lower = name.to_ascii_lowercase();
            if let Some(prev_name) = by_name_lower.get(&lower) {
                diags.push(Diagnostic::warning(
                    "W002",
                    format!(
                        "node names differ only in case ({prev_name:?} vs {name:?}); \
                         this is allowed but discouraged"
                    ),
                    span_for(spanned),
                ));
            } else {
                by_name_lower.insert(lower, name.to_string());
            }
            seen_exact.insert(name.to_string(), ());
        }

        // ── Stage 2: self-loops (E002) ──────────────────────────────
        for spanned in &self.nodes {
            let node = &spanned.value;
            let name = node.name();
            if node.direct_input_names().contains(&name) {
                diags.push(Diagnostic::error(
                    "E002",
                    format!("node {name:?} lists itself as an input"),
                    span_for(spanned),
                ));
            }
        }

        // ── Stage 3: general cycles (E003) ──────────────────────────
        let mut graph = NameGraph::new();
        for spanned in &self.nodes {
            graph.add_node(spanned.value.name());
        }
        for spanned in &self.nodes {
            let node = &spanned.value;
            let consumer = node.name();
            for producer in node.direct_input_names() {
                if producer != consumer && graph.index_of(producer).is_some() {
                    graph.add_edge(producer, consumer);
                }
            }
        }
        if let Some(cycle) = graph.detect_cycle() {
            let path = cycle.join(" → ");
            diags.push(Diagnostic::error(
                "E003",
                format!("cycle detected: {path} → {}", cycle[0]),
                synth(),
            ));
        }

        // ── Stage 3.5: unified input-reference resolution (E004) ────
        // A single pass walks every node's declared input(s), looks them
        // up in the unified node-name table, and emits E004 with a
        // structured payload for each undeclared reference (covering
        // both standalone-node and combine-arm references with one code).
        //
        // This pass runs BEFORE bind_schema so undeclared-input
        // diagnostics surface even when a sibling node has a CXL
        // error that would otherwise short-circuit the compile.
        resolve_all_input_references(&self.nodes, &mut diags);

        // ── Stage 4: path validation ────────────────────────────────
        let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        let allow_absolute =
            ctx.allow_absolute_paths || std::env::var("CLINKER_ALLOW_ABSOLUTE_PATHS").is_ok();
        diags.extend(crate::security::validate_all_config_paths(
            self,
            &cwd,
            allow_absolute,
        ));

        // ── Stage 5: D3b — dotted-name check ────────────────────────
        // `.` is reserved for branch references (e.g. "route.high").
        // Enforced structurally here against the nodes: taxonomy.
        for spanned in &self.nodes {
            let name = spanned.value.name();
            if matches!(
                spanned.value,
                PipelineNode::Transform { .. }
                    | PipelineNode::Aggregate { .. }
                    | PipelineNode::Route { .. }
            ) && name.contains('.')
            {
                diags.push(Diagnostic::error(
                    "E010",
                    format!(
                        "transform name {name:?} is invalid: '.' is reserved \
                         for branch references (use underscores or hyphens)"
                    ),
                    span_for(spanned),
                ));
            }
        }

        // ── Stage 6: D3b — log directive sanity ─────────────────────
        // Mirrors the `validate_config` pass but against the nodes:
        // taxonomy directly (so new-shape YAML is covered too).
        for spanned in &self.nodes {
            let (name, log) = match &spanned.value {
                PipelineNode::Transform { header, config } => {
                    (header.name.as_str(), config.log.as_ref())
                }
                _ => continue,
            };
            let Some(directives) = log else { continue };
            for (i, d) in directives.iter().enumerate() {
                if let Some(every) = d.every {
                    if every == 0 {
                        diags.push(Diagnostic::error(
                            "E011",
                            format!(
                                "transform {name:?}: log directive #{}: every must be >= 1",
                                i + 1
                            ),
                            span_for(spanned),
                        ));
                    }
                    if d.when != LogTiming::PerRecord {
                        diags.push(Diagnostic::error(
                            "E011",
                            format!(
                                "transform {name:?}: log directive #{}: 'every' is only valid with when: per_record",
                                i + 1
                            ),
                            span_for(spanned),
                        ));
                    }
                }
            }
        }

        diags
    }

    /// Compile `self.nodes` into a [`crate::plan::CompiledPlan`].
    ///
    /// Walks the unified `nodes:` taxonomy and builds a
    /// [`crate::plan::CompiledPlan`] wrapping an
    /// [`crate::plan::execution::ExecutionPlanDag`] populated with
    /// enriched [`crate::plan::execution::PlanNode`] variants whose
    /// `span` fields point back into the originating YAML document and
    /// whose `resolved` payloads carry the fully-resolved per-variant
    /// configuration.
    ///
    /// On error, returns the accumulated diagnostics from the topology
    /// pre-pass plus any per-variant lowering errors. Composition binding
    /// errors (E102–E109) are non-fatal — the composition node is silently
    /// omitted from the DAG.
    pub fn compile(
        &self,
        ctx: &CompileContext,
    ) -> Result<crate::plan::CompiledPlan, Vec<clinker_core_types::Diagnostic>> {
        let (plan, _warnings) = self.compile_with_diagnostics(ctx)?;
        Ok(plan)
    }

    /// Like [`compile`], but also returns non-fatal diagnostics (warnings)
    /// that were collected during compilation. On error, all diagnostics
    /// (errors + warnings) are in the `Err` variant as before.
    pub fn compile_with_diagnostics(
        &self,
        ctx: &CompileContext,
    ) -> Result<
        (
            crate::plan::CompiledPlan,
            Vec<clinker_core_types::Diagnostic>,
        ),
        Vec<clinker_core_types::Diagnostic>,
    > {
        use crate::config::composition::scan_workspace_signatures;
        use crate::plan::CompiledPlan;
        use crate::plan::execution::{
            DependencyType, ExecutionPlanDag, ParallelismProfile, PlanEdge, PlanNode,
        };
        use clinker_core_types::span::Span;
        use clinker_core_types::{Diagnostic, LabeledSpan};
        use petgraph::graph::{DiGraph, NodeIndex};
        use std::collections::HashMap;

        // Stage 1-4: name/topology/path validation pre-pass.
        let mut diags = self.compile_topology_only(ctx);
        // Hard-error stop: stages 1-4 already collected; stage 5
        // refuses to lower if any error-severity diagnostic is present.
        let has_errors = diags
            .iter()
            .any(|d| matches!(d.severity, clinker_core_types::Severity::Error));
        if has_errors {
            return Err(diags);
        }

        // Stage 4.4: workspace composition scan.
        // If this pipeline has any composition nodes, scan the workspace
        // root resolved in `ctx` for `.comp.yaml` signatures and append
        // any scan-level diagnostics (E101) to the compile diagnostics
        // list. Pipelines without compositions skip the scan entirely so
        // non-composition tests and benches are not coupled to workspace
        // fixture validity.
        //
        // The resulting symbol table is built and dropped here — body
        // resolution carries it forward onto CompiledPlan.
        let has_compositions = self
            .nodes
            .iter()
            .any(|n| matches!(&n.value, PipelineNode::Composition { .. }));
        let symbol_table: crate::config::composition::CompositionSymbolTable = if has_compositions {
            match scan_workspace_signatures(ctx.workspace_root()) {
                Ok(table) => std::sync::Arc::try_unwrap(table).unwrap_or_else(|arc| (*arc).clone()),
                Err(mut scan_diags) => {
                    diags.append(&mut scan_diags);
                    let has_errors = diags
                        .iter()
                        .any(|d| matches!(d.severity, clinker_core_types::Severity::Error));
                    if has_errors {
                        return Err(diags);
                    }
                    indexmap::IndexMap::new()
                }
            }
        } else {
            indexmap::IndexMap::new()
        };

        // Stage 4.5: compile-time CXL typecheck.
        // Walks `self.nodes` in declaration order (topologically sound
        // per stage-3 validation), seeds each source's schema from its
        // author-declared `schema:` block, and typechecks every
        // CXL-bearing node against the propagated upstream schema.
        // E200 diagnostics surface here with per-node spans. Also
        // recurses into composition bodies via bind_composition,
        // populating CompileArtifacts.composition_bodies.
        let scoped_vars_registry =
            build_scoped_vars_registry(self.pipeline.vars.as_ref(), &self.nodes);
        let mut artifacts = crate::plan::bind_schema::bind_schema(
            &self.nodes,
            &mut diags,
            ctx,
            &symbol_table,
            &ctx.pipeline_dir,
            scoped_vars_registry,
        );

        crate::plan::bind_schema::validate_dlq_per_source(
            self.error_handling.dlq.as_ref(),
            &self.nodes,
            &mut diags,
        );
        crate::plan::bind_schema::validate_output_path_collisions(
            &self.nodes,
            self.error_handling.dlq.as_ref(),
            &mut diags,
        );

        // Only abort on non-composition CXL errors (E200/E201) and
        // source-CK validation errors (E153). Composition binding
        // errors (E102–E109) are non-fatal for the rest of the pipeline
        // — the composition node is omitted from the DAG.
        let has_cxl_errors = diags.iter().any(|d| {
            matches!(d.severity, clinker_core_types::Severity::Error)
                && matches!(d.code.as_str(), "E200" | "E201" | "E153" | "E317" | "E318")
        });
        if has_cxl_errors {
            return Err(diags);
        }

        // ── Stage 5: per-variant lowering + enrichment ─────────────
        //
        // The lowering step produces a structurally complete
        // `ExecutionPlanDag` that the executor can run without
        // re-compilation. Per-variant lowering gets its enrichment
        // inputs (analyzer report, window configs, dedup'd indices)
        // from the helpers below, all of which were previously only
        // exercised by the deleted
        // `ExecutionPlanDag::compile_with_runtime_schema` path — the
        // two compile sites have converged onto this one.
        let source_configs: Vec<crate::config::SourceConfig> =
            self.source_configs().cloned().collect();
        let output_configs: Vec<crate::config::OutputConfig> =
            self.output_configs().cloned().collect();
        let primary_source: String = source_configs
            .first()
            .map(|s| s.name.clone())
            .unwrap_or_default();

        // Harvest planner entries (name + analytic_window) directly off
        // Transform/Aggregate/Route nodes in declaration order. The
        // resulting `entries` array is parallel to `window_configs`;
        // its index is reused as the `transform_index` in raw index
        // requests built after the graph topology is known.
        // Source/Output/Merge/Composition/Combine variants do not
        // contribute (they don't carry `analytic_window` or CXL
        // programs the analyzer pass would consume).
        struct PlannerEntry {
            name: String,
            analytic_window: Option<AnalyticWindowSpec>,
        }
        let entries: Vec<PlannerEntry> = self
            .nodes
            .iter()
            .filter_map(|spanned| match &spanned.value {
                PipelineNode::Transform {
                    header,
                    config: body,
                } => Some(PlannerEntry {
                    name: header.name.clone(),
                    analytic_window: body.analytic_window.clone(),
                }),
                PipelineNode::Aggregate { header, .. } => Some(PlannerEntry {
                    name: header.name.clone(),
                    analytic_window: None,
                }),
                PipelineNode::Route { header, .. } => Some(PlannerEntry {
                    name: header.name.clone(),
                    analytic_window: None,
                }),
                PipelineNode::Source { .. }
                | PipelineNode::Output { .. }
                | PipelineNode::Merge { .. }
                | PipelineNode::Reshape { .. }
                | PipelineNode::Cull { .. }
                | PipelineNode::Envelope { .. }
                | PipelineNode::Composition { .. }
                | PipelineNode::Combine { .. } => None,
            })
            .collect();
        let mut entries_by_name: HashMap<String, usize> = HashMap::new();
        for (i, e) in entries.iter().enumerate() {
            entries_by_name.insert(e.name.clone(), i);
        }
        let compiled_refs: Vec<(&str, &cxl::typecheck::pass::TypedProgram)> = entries
            .iter()
            .filter_map(|e| {
                artifacts
                    .typed
                    .get(&e.name)
                    .map(|tp| (e.name.as_str(), tp.as_ref()))
            })
            .collect();
        let report = cxl::analyzer::analyze_all(&compiled_refs);

        // Collect the `$doc` envelope paths every program references, so
        // each lowered Source carries its declared document-path set for
        // the reader. The walk covers EVERY typed program in the compile
        // artifacts — not just the `entries` subset that feeds the
        // analyzer — because `$doc` is valid in Combine predicates /
        // residual filters / emit bodies and in composition-body
        // programs, all of which land in `artifacts.typed` under their
        // node name (body programs are inserted there by the recursive
        // bind_schema pass). Combine `where:` predicates and Route branch
        // conditions are typed into separate side-tables and chained in
        // below. Missing any of them would drop a referenced path from the
        // declared set.
        let doc_refs: Vec<(&str, &cxl::typecheck::pass::TypedProgram)> = artifacts
            .typed
            .iter()
            .map(|(name, tp)| (name.as_str(), tp.as_ref()))
            // A Combine's node-keyed `typed` entry holds only its `cxl:`
            // body; its `where:` predicate program lives in a separate
            // side-table, so include those too — a `$doc` access used only
            // in a predicate would otherwise be dropped. The side-table is
            // keyed by `ScopedNodeId`; the downstream source attribution
            // keys by the bare node name, so project the scoped key to its
            // `name`. A combine named the same in two scopes now contributes
            // BOTH predicate programs (the scope key no longer overwrites
            // one with the other), which is strictly more correct for `$doc`
            // detection.
            .chain(
                artifacts
                    .combine_where_typed
                    .iter()
                    .map(|(scoped, tp)| (scoped.name.as_str(), tp.as_ref())),
            )
            // A Route's node-keyed `typed` entry is an empty body; its
            // branch-condition programs live in a separate side-table, one
            // per branch. Include each, keyed by the Route node name so the
            // per-node source attribution stamps the path onto the Route's
            // source(s) — a `$doc` access used only in a route condition
            // would otherwise be dropped. As with the combine side-table,
            // project the `ScopedNodeId` to its bare `name` for the
            // attribution key.
            .chain(
                artifacts
                    .route_branch_typed
                    .iter()
                    .flat_map(|(scoped, programs)| {
                        programs
                            .iter()
                            .map(move |tp| (scoped.name.as_str(), tp.as_ref()))
                    }),
            )
            .collect();
        let doc_path_set = cxl::analyzer::doc_paths::collect_doc_paths(&doc_refs);
        // Anchor each `$doc` diagnostic at the offending node's source line.
        // Top-level node lines come from the saphyr-captured YAML position;
        // a composition-body node (not present in `self.nodes`) falls back
        // to a synthetic span. Shared by the E340 unresolvable-index pass
        // and the E341 undeclared-path pass below.
        let doc_node_line: HashMap<&str, u32> = self
            .nodes
            .iter()
            .filter_map(|sp| {
                let line = sp.referenced.line();
                (line > 0).then(|| (sp.value.name(), line as u32))
            })
            .collect();
        if !doc_path_set.unresolvable.is_empty() {
            for (node_name, d) in &doc_path_set.unresolvable {
                let primary = match doc_node_line.get(node_name.as_str()) {
                    Some(&line) => Span::line_only(line),
                    None => Span::SYNTHETIC,
                };
                let mut diag = Diagnostic::error(
                    "E340",
                    d.message.clone(),
                    LabeledSpan::primary(primary, String::new()),
                );
                if let Some(help) = &d.help {
                    diag = diag.with_help(help.clone());
                }
                diags.push(diag);
            }
            return Err(diags);
        }
        // Attribute each collected `$doc` path to the source(s) it flows
        // from: trace every referencing node back through the DAG to its
        // upstream Source set and stamp the path onto only those sources.
        // A `$doc` access carries no source qualifier, so a multi-source
        // run must not stamp the pipeline-wide union onto every source —
        // a source would otherwise be told to extract envelope sections
        // its own document never declares.
        let node_sources = build_node_source_sets(&self.nodes, &artifacts);
        let mut doc_paths_by_source: HashMap<String, std::collections::BTreeSet<_>> =
            HashMap::new();
        for (doc_path, nodes) in &doc_path_set.by_node {
            for node_name in nodes {
                let Some(sources) = node_sources.get(node_name) else {
                    continue;
                };
                for source in sources {
                    doc_paths_by_source
                        .entry(source.clone())
                        .or_default()
                        .insert(doc_path.clone());
                }
            }
        }
        let mut declared_doc_paths: HashMap<String, Vec<cxl::analyzer::doc_paths::DocPath>> =
            doc_paths_by_source
                .into_iter()
                .map(|(source, paths)| (source, paths.into_iter().collect()))
                .collect();
        // Output-envelope reconstruction is a SECOND consumer of `$doc`
        // sections, beyond CXL `$doc.*` references: an Output's
        // `header_from_doc` / `footer_from_doc` echoes a whole section, but it
        // appears in no program, so the readers' path-pruned pre-scan would
        // not extract it. Register each enveloped section's declared fields as
        // doc paths against the source(s) feeding that Output, so the reader
        // retains the section. The reader extracts the whole declared section
        // once any of its fields is wanted, so one path per declared field is
        // sufficient.
        //
        // These registered paths drive the reader's path-pruned extraction
        // (consumed at the `declared_doc_paths` stamp below); they do NOT
        // flow through the `doc_schema_for` / E341/E348 validation pass,
        // which iterates `doc_path_set.by_node` (the CXL `$doc.*` refs) and
        // never sees this register map. A header/footer naming an undeclared
        // section is rejected separately by E346 below, so an absent section
        // here is fine.
        //
        // `source_by_name` is shared by this register block and the
        // E341/E348/E349 validation pass below — one index over the source
        // configs serves both.
        let source_by_name: HashMap<&str, &crate::config::SourceConfig> = source_configs
            .iter()
            .map(|s| (s.name.as_str(), s))
            .collect();
        for output in self.output_configs() {
            if !output.reconstruct_envelope {
                continue;
            }
            let Some((env_cfg, _)) = generic_output_envelope(&output.format) else {
                continue;
            };
            let sections: Vec<&str> = [
                env_cfg.header_from_doc.as_deref(),
                env_cfg.footer_from_doc.as_deref(),
            ]
            .into_iter()
            .flatten()
            .collect();
            if sections.is_empty() {
                continue;
            }
            let Some(feeding) = node_sources.get(&output.name) else {
                continue;
            };
            for source_name in feeding {
                let Some(source) = source_by_name.get(source_name.as_str()) else {
                    continue;
                };
                let Some(envelope) = source.envelope.as_ref() else {
                    continue;
                };
                let paths = declared_doc_paths.entry(source_name.clone()).or_default();
                for section_name in &sections {
                    let Some(section) = envelope.sections.get(*section_name) else {
                        continue;
                    };
                    for field in section.fields.keys() {
                        let path = cxl::analyzer::doc_paths::DocPath {
                            section: Box::from(*section_name),
                            field: Box::from(field.as_str()),
                            indices: Vec::new(),
                        };
                        if !paths.contains(&path) {
                            paths.push(path);
                        }
                    }
                }
            }
        }
        // Cross-check every referenced `$doc.<section>.<field>` against the
        // schema the feeding source's reader will actually serve. Without
        // this a typo (`$doc.Summry.total` against a declared `Summary`)
        // compiles silently and resolves to `Value::Null` at run time; the
        // cxl typechecker cannot catch it because `$doc.*` types as `Any`
        // with no envelope context in scope. The schema differs by source:
        //
        // - REST transport (E349): a `rest` source buffers no document, so
        //   any `$doc` access against it can never resolve — rejected
        //   outright regardless of declared format.
        // - Closed (E341): XML/JSON readers serve exactly the declared
        //   `envelope:` sections/fields. The declaration is the
        //   authoritative closed schema, so an omitted path is a genuine
        //   typo.
        // - Segment/positional (E348): X12/EDIFACT/HL7 synthesize nested
        //   levels (`functional_group`/`transaction_set`/`batch`) keyed by
        //   positional `eNN`/`fNN` elements beyond the declared header. A
        //   path is checked against that known synthesized vocabulary plus
        //   any user-declared section/field, so a typo or out-of-range
        //   positional element is caught without false-positiving on a
        //   legitimate wire path.
        // - Unvalidated: CSV, fixed-width, SWIFT (see `doc_schema_for`).
        //
        // `doc_schema_for` is keyed once per source here, not rebuilt on
        // every (path × node × source) iteration of the loop below — a
        // `SegmentPositional` schema allocates its synthesized-section
        // vector, so recomputing it per reference would be wasteful.
        let doc_schema_by_source: HashMap<&str, DocSchema> = source_configs
            .iter()
            .map(|s| (s.name.as_str(), doc_schema_for(s)))
            .collect();
        // E349 — a `rest` source that declares an `envelope:` block. The
        // declaration is always inert (a REST pull buffers no document), so
        // reject it even when no downstream node reads `$doc` from the
        // source. The per-`$doc`-access E349 below covers the complementary
        // case: a `$doc` read against a REST source that declared no
        // envelope.
        for source in &source_configs {
            if let SourceTransport::Rest(_) = source.transport
                && source.envelope.is_some()
            {
                let primary = doc_node_line
                    .get(source.name.as_str())
                    .map(|&line| Span::line_only(line))
                    .unwrap_or(Span::SYNTHETIC);
                diags.push(
                    Diagnostic::error(
                        "E349",
                        format!(
                            "the rest source '{}' declares an `envelope:` block, but a rest \
                             source pulls records page by page over HTTP and buffers no \
                             document — the declared sections are inert and every \
                             `$doc.<section>.<field>` against this source resolves to null",
                            source.name
                        ),
                        LabeledSpan::primary(primary, String::new()),
                    )
                    .with_help(
                        "remove the `envelope:` block from this rest source; envelope \
                         sections are a file-document concept. Pull document-level metadata \
                         into record fields through the API's response shape (`record_path`, \
                         `array_paths`) instead"
                            .to_string(),
                    ),
                );
            }
        }
        for (doc_path, ref_nodes) in &doc_path_set.by_node {
            for node_name in ref_nodes {
                let Some(sources) = node_sources.get(node_name) else {
                    continue;
                };
                for source_name in sources {
                    let Some(source) = source_by_name.get(source_name.as_str()) else {
                        continue;
                    };
                    let primary = match doc_node_line.get(node_name.as_str()) {
                        Some(&line) => Span::line_only(line),
                        None => Span::SYNTHETIC,
                    };
                    // A REST source has no buffered document; a `$doc`
                    // access against it can never resolve. Reject before
                    // the format-shaped schema check, which does not model
                    // the transport.
                    if let SourceTransport::Rest(_) = source.transport {
                        diags.push(rest_doc_access_diagnostic(&source.name, doc_path, primary));
                        continue;
                    }
                    let problem = match doc_schema_by_source.get(source_name.as_str()) {
                        Some(DocSchema::Closed) => closed_doc_path_problem(source, doc_path),
                        Some(DocSchema::SegmentPositional(schema)) => {
                            segment_positional_doc_path_problem(source, schema, doc_path)
                        }
                        Some(DocSchema::Unvalidated) | None => None,
                    };
                    let Some(problem) = problem else {
                        continue;
                    };
                    diags.push(
                        Diagnostic::error(
                            problem.code,
                            problem.message,
                            LabeledSpan::primary(primary, String::new()),
                        )
                        .with_help(problem.help),
                    );
                }
            }
        }
        if diags
            .iter()
            .any(|d| d.code == "E341" || d.code == "E348" || d.code == "E349")
        {
            return Err(diags);
        }
        // Keep an analysis-by-name index so we can surface the analysis
        // alongside its matching entry regardless of filter order.
        let mut analysis_by_name: HashMap<String, &cxl::analyzer::TransformAnalysis> =
            HashMap::new();
        for a in &report.transforms {
            analysis_by_name.insert(a.name.clone(), a);
        }
        let window_configs: Vec<Option<AnalyticWindowSpec>> =
            entries.iter().map(|e| e.analytic_window.clone()).collect();
        // Validate: if a transform uses window.* but has no analytic_window, error.
        for (i, analysis) in report.transforms.iter().enumerate() {
            if !analysis.window_calls.is_empty() {
                let entry_idx = entries_by_name.get(&analysis.name).copied().unwrap_or(i);
                if window_configs
                    .get(entry_idx)
                    .map(|w| w.is_none())
                    .unwrap_or(true)
                {
                    diags.push(Diagnostic::error(
                        "E003",
                        format!(
                            "transform '{}' uses window.* functions but declares no analytic_window",
                            analysis.name
                        ),
                        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                    ));
                    return Err(diags);
                }
            }
        }
        // E003 — every cross-source `wc.source: <name>` must name a
        // declared source. The full `RawIndexRequest` set is built later
        // (after the DAG topology is known so node-rooted windows can
        // pin their `PlanIndexRoot::Node { upstream, .. }` to a real
        // NodeIndex), but the unknown-source diagnostic does not depend
        // on graph topology and runs first.
        for (i, wc_opt) in window_configs.iter().enumerate() {
            if let Some(wc) = wc_opt {
                let source = wc.source.clone().unwrap_or_else(|| primary_source.clone());
                if !source_configs.iter().any(|inp| inp.name == source) {
                    diags.push(Diagnostic::error(
                        "E003",
                        format!(
                            "transform '{}' references unknown source '{}' in analytic_window",
                            entries[i].name, source
                        ),
                        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                    ));
                    return Err(diags);
                }
            }
        }
        // Build source DAG + output projections.
        let source_dag = crate::plan::execution::build_source_dag(
            &source_configs,
            &window_configs,
            &primary_source,
        )
        .map_err(|e| {
            vec![Diagnostic::error(
                "E003",
                format!("source DAG construction failed: {e}"),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            )]
        })?;
        let output_projections: Vec<crate::plan::execution::OutputSpec> = output_configs
            .iter()
            .map(|o| crate::plan::execution::OutputSpec {
                name: o.name.clone(),
                mapping: o.mapping.clone().unwrap_or_default(),
                exclude: o.exclude.clone().unwrap_or_default(),
                include_unmapped: o.include_unmapped,
            })
            .collect();

        let mut graph = DiGraph::<PlanNode, PlanEdge>::new();
        let mut name_to_idx: HashMap<String, NodeIndex> = HashMap::new();

        // Phase 1: insert one PlanNode per spanned-PipelineNode.
        // Transform + Aggregate variants draw their enrichment from
        // the analyzer report / window configs; other variants ignore
        // the LoweringCtx fields. Window-bearing Transforms are
        // emitted with `window_index = None`; a post-edge-wiring pass
        // populates the deduplicated index list and backfills the
        // field once the upstream `NodeIndex` is known.
        for spanned in &self.nodes {
            // Thread the real source line number off the saphyr
            // `Spanned<PipelineNode>::referenced` Location.
            // (c) If saphyr did not capture a line (the documented
            // tagged-enum + flatten edge case), fall back to
            // `Span::SYNTHETIC`.
            let saphyr_line = spanned.referenced.line();
            let span = if saphyr_line > 0 {
                Span::line_only(saphyr_line as u32)
            } else {
                Span::SYNTHETIC
            };
            let node = &spanned.value;
            let name = node.name().to_string();
            let entry_idx = entries_by_name.get(&name).copied();
            let lowering_ctx = LoweringCtx {
                analysis: analysis_by_name.get(name.as_str()).copied(),
                window_config: entry_idx.and_then(|i| window_configs[i].as_ref()),
                primary_source: primary_source.as_str(),
            };
            let plan_node =
                lower_node_to_plan_node(node, &name, span, &artifacts, &lowering_ctx, &mut diags);
            if let Some(pn) = plan_node {
                let idx = graph.add_node(pn);
                name_to_idx.insert(name, idx);
            }
        }

        // Phase 2: wire edges from each consumer's input(s) to itself.
        // Undeclared producer references were already diagnosed by the
        // unified `resolve_all_input_references` pass at stage 3.5.
        // This loop only adds graph edges; missing producers are
        // silently skipped here because the diagnostic has already fired.
        fn strip_port_for_edge(r: &str) -> &str {
            r.split('.').next().unwrap_or(r)
        }
        for spanned in &self.nodes {
            let node = &spanned.value;
            let consumer_name = node.name().to_string();
            let Some(&consumer_idx) = name_to_idx.get(consumer_name.as_str()) else {
                continue;
            };
            let mut wire = |producer_full: &str, port: Option<String>| {
                let producer_key = strip_port_for_edge(producer_full);
                if let Some(&producer_idx) = name_to_idx.get(producer_key) {
                    // Tag the edge with the producer output port it draws
                    // from so the dispatcher routes Route branches off the
                    // live graph; `None` for single-output producers.
                    let producer_port = resolve_producer_port(
                        &graph[producer_idx],
                        producer_full,
                        producer_key,
                        &consumer_name,
                    );
                    graph.add_edge(
                        producer_idx,
                        consumer_idx,
                        PlanEdge {
                            dependency_type: DependencyType::Data,
                            port,
                            producer_port,
                        },
                    );
                }
            };
            match node {
                PipelineNode::Source { .. } => {}
                PipelineNode::Transform { header, .. }
                | PipelineNode::Aggregate { header, .. }
                | PipelineNode::Route { header, .. }
                | PipelineNode::Reshape { header, .. }
                | PipelineNode::Cull { header, .. }
                | PipelineNode::Output { header, .. } => {
                    wire(&input_full_reference(&header.input.value), None);
                }
                PipelineNode::Composition {
                    inputs: call_inputs,
                    ..
                } => {
                    // Composition's named-port `inputs:` map is the
                    // authoritative call-site binding. Each entry
                    // produces one port-tagged incoming edge — the
                    // dispatcher walks live incoming edges and reads
                    // the tag at composition entry to harvest
                    // per-port records. `header.input:` is YAML-shape
                    // obligation on the shared `NodeHeader` struct
                    // and adds no information beyond what `inputs:`
                    // already covers (every required port is
                    // validated to be present in `inputs:` per E104),
                    // so it does not produce its own edge.
                    for (port_name, upstream) in call_inputs {
                        wire(upstream, Some(port_name.clone()));
                    }
                }
                PipelineNode::Merge { header, .. } => {
                    for inp in &header.inputs {
                        wire(&input_full_reference(&inp.value), None);
                    }
                }
                PipelineNode::Combine { header, .. } => {
                    for node_input in header.input.values() {
                        wire(&input_full_reference(&node_input.value), None);
                    }
                }
                PipelineNode::Envelope { header, .. } => {
                    // Wire the body spine plus any wired header / trailer port
                    // (a wired value is rejected at validation this release,
                    // but the edge is built unconditionally so the rejection
                    // diagnostic still resolves the reference).
                    wire(&input_full_reference(&header.body.value), None);
                    for inp in [&header.header, &header.trailer].into_iter().flatten() {
                        wire(&input_full_reference(&inp.value), None);
                    }
                }
            }
        }

        // Build index requests with full graph context. A window-bearing
        // transform's `IndexSpec.root` resolves to a real `NodeIndex`
        // for the upstream operator (after walking past pass-through
        // Sort/Route nodes), or to a declared source name for the
        // degenerate source-rooted case. Source-rooted is only
        // selected when the immediate predecessor is a `PlanNode::Source`
        // AND the user did not request a different source via
        // `wc.source: <other>`; the cross-source `wc.source` form
        // continues to lower to `PlanIndexRoot::Source(<name>)`.
        let mut raw_index_requests: Vec<crate::plan::index::RawIndexRequest> = Vec::new();
        let primary_source_str = primary_source.as_str();
        for (i, wc_opt) in window_configs.iter().enumerate() {
            let Some(wc) = wc_opt else { continue };
            let transform_name = entries[i].name.as_str();
            let Some(&transform_idx) = name_to_idx.get(transform_name) else {
                // Lowering produced no node for this transform (e.g. a
                // typecheck failure already surfaced its diagnostic);
                // skip — the missing-program diagnostic has already fired.
                continue;
            };

            let mut arena_fields: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for gb in &wc.group_by {
                arena_fields.insert(gb.clone());
            }
            for sf in &wc.sort_by {
                arena_fields.insert(sf.field.clone());
            }
            for f in &report.transforms[i].accessed_fields {
                arena_fields.insert(f.clone());
            }
            // Sort arena_fields for deterministic order — the source
            // collection is a HashSet whose iteration order randomizes
            // run-to-run, which leaks into `--explain` output and into
            // any snapshot test that captures the explain block.
            let mut arena_fields_vec: Vec<String> = arena_fields.into_iter().collect();
            arena_fields_vec.sort();

            // Cross-source `wc.source: <other>` always roots at that
            // declared source. `wc.source: None` (or matching the
            // primary) defers to predecessor inspection: if the
            // immediate non-pass-through ancestor is a `PlanNode::Source`,
            // it is source-rooted; otherwise it is node-rooted on the
            // ancestor.
            let cross_source = wc
                .source
                .as_ref()
                .filter(|s| s.as_str() != primary_source_str)
                .cloned();

            // E150c — when a window declares a cross-source reference
            // (`wc.source: <other>`), the referenced source's ingestion
            // tier MUST be earlier than (or equal to) the tier of the
            // window-bearing transform's primary input source.
            // `build_source_dag` orders cross-source-referenced sources
            // into earlier tiers so their indices are populated before
            // any consumer reads them; an inverted-tier reference means
            // the engine would attempt to project against a not-yet-
            // ingested source.
            if let Some(other) = cross_source.as_deref() {
                let primary_for_transform =
                    crate::plan::execution::primary_input_source_for_transform(
                        &graph,
                        transform_idx,
                    )
                    .unwrap_or_else(|| primary_source_str.to_string());
                let other_tier = crate::plan::execution::source_tier_index(&source_dag, other);
                let primary_tier = crate::plan::execution::source_tier_index(
                    &source_dag,
                    primary_for_transform.as_str(),
                );
                if let (Some(other_tier), Some(primary_tier)) = (other_tier, primary_tier)
                    && other_tier > primary_tier
                {
                    diags.push(Diagnostic::error(
                        "E150c",
                        format!(
                            "cross-source window references source '{other}' whose \
                             ingestion tier is downstream of the window-bearing \
                             transform '{transform_name}' primary input source \
                             '{primary_for_transform}'; promote '{other}' to an \
                             earlier tier or remove the cross-source reference"
                        ),
                        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                    ));
                    return Err(diags);
                }
            }

            let root = if let Some(other) = cross_source {
                // Cross-source window: resolve the referenced source's
                // `NodeIndex` in the same DAG. E150c (above) has already
                // guaranteed the referenced source is at an earlier
                // ingestion tier, so by topo order its node-rooted
                // arena is populated by its Source dispatch arm before
                // this window-bearing transform runs.
                let Some(&other_idx) = name_to_idx.get(other.as_str()) else {
                    diags.push(Diagnostic::error(
                        "E003",
                        format!(
                            "windowed transform '{}' references cross-source '{}' \
                             which is not a known node in the plan",
                            transform_name, other
                        ),
                        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                    ));
                    return Err(diags);
                };
                let Some(anchor_schema) = graph[other_idx].stored_output_schema().cloned() else {
                    diags.push(Diagnostic::error(
                        "E003",
                        format!(
                            "windowed transform '{}' references cross-source '{}' \
                             whose Source node has no output schema",
                            transform_name, other
                        ),
                        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                    ));
                    return Err(diags);
                };
                crate::plan::index::PlanIndexRoot::Node {
                    upstream: other_idx,
                    anchor_schema,
                }
            } else {
                let pred_idx = match graph
                    .neighbors_directed(transform_idx, petgraph::Direction::Incoming)
                    .next()
                {
                    Some(p) => p,
                    None => {
                        diags.push(Diagnostic::error(
                            "E003",
                            format!(
                                "windowed transform '{}' has no upstream input; \
                                 analytic_window requires a predecessor in the DAG",
                                transform_name
                            ),
                            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                        ));
                        return Err(diags);
                    }
                };
                let rooted_idx =
                    crate::plan::execution::first_non_passthrough_ancestor(&graph, pred_idx);
                match &graph[rooted_idx] {
                    crate::plan::execution::PlanNode::Merge { .. } => {
                        diags.push(Diagnostic::error(
                            "E150d",
                            format!(
                                "windowed transform '{}' is rooted at a Merge node; \
                                 Merge concatenates streams without a single producer \
                                 identity, so a window cannot anchor to it",
                                transform_name
                            ),
                            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                        ));
                        return Err(diags);
                    }
                    other => {
                        let Some(anchor_schema) = other.stored_output_schema().cloned() else {
                            diags.push(Diagnostic::error(
                                "E003",
                                format!(
                                    "windowed transform '{}' rooted at upstream node \
                                     '{}' which has no output schema",
                                    transform_name,
                                    other.name()
                                ),
                                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                            ));
                            return Err(diags);
                        };
                        // E150b — every arena field must be present in
                        // the upstream's output schema (group_by,
                        // sort_by, and any field the window builtins
                        // accessed). Schema membership is checked by
                        // name only at this stage.
                        for f in &arena_fields_vec {
                            if !anchor_schema.contains(f.as_str()) {
                                diags.push(Diagnostic::error(
                                    "E150b",
                                    format!(
                                        "windowed transform '{}' references field '{}' \
                                         that the upstream operator '{}' does not emit; \
                                         a node-rooted window can only see columns \
                                         produced by its rooted operator",
                                        transform_name,
                                        f,
                                        other.name()
                                    ),
                                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                                ));
                                return Err(diags);
                            }
                        }
                        // E150e — windows over Combine emit columns
                        // cannot reference an array-typed field. The
                        // typed `output_row` lives in `artifacts.typed`
                        // keyed by the combine's name and carries the
                        // CXL `Type` per emitted column; a
                        // `match: collect` body emits `Type::Array` for
                        // every collected build-row column. Window
                        // builtins (sum/avg/min/max/lag/lead/...) do
                        // not handle `Value::Array`, so they would
                        // silently see Null at runtime.
                        if let crate::plan::execution::PlanNode::Combine { .. } = other {
                            let combine_name = other.name();
                            if let Some(typed) = artifacts.typed.get(combine_name) {
                                for f in &report.transforms[i].accessed_fields {
                                    let is_array = typed
                                        .output_row
                                        .fields()
                                        .find(|(qf, _)| qf.name.as_ref() == f.as_str())
                                        .map(|(_, ty)| {
                                            matches!(
                                                ty.unwrap_nullable(),
                                                cxl::typecheck::Type::Array
                                            )
                                        })
                                        .unwrap_or(false);
                                    if is_array {
                                        diags.push(Diagnostic::error(
                                            "E150e",
                                            format!(
                                                "windowed transform '{transform_name}' \
                                                 references field '{f}' typed as Array; \
                                                 window builtin does not support \
                                                 array-typed field '{f}' from \
                                                 `match: collect` combine '{combine_name}'; \
                                                 flatten the array upstream or use \
                                                 `match: first | all`"
                                            ),
                                            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                                        ));
                                        return Err(diags);
                                    }
                                }
                            }
                        }
                        crate::plan::index::PlanIndexRoot::Node {
                            upstream: rooted_idx,
                            anchor_schema,
                        }
                    }
                }
            };

            // Node-rooted / parent-node-rooted arenas have no
            // declared source ordering — partitions are sorted
            // post-build at the upstream-arm exit. The executor's
            // per-partition `sort_partition` call normalizes the
            // slice before window evaluation; declared
            // `sort_order:` on a Source no longer skips it because
            // Source-rooted arenas no longer exist as a runtime
            // category.
            let already_sorted = false;

            raw_index_requests.push(crate::plan::index::RawIndexRequest {
                root,
                group_by: wc.group_by.clone(),
                sort_by: wc.sort_by.clone(),
                arena_fields: arena_fields_vec,
                already_sorted,
                transform_index: i,
                // Default false here; the buffer-recompute derivation
                // walks the DAG after lowering and overwrites the
                // flag on the resulting IndexSpec when a relaxed-CK
                // upstream aggregate's dropped CK fields overlap
                // this window's partition_by.
                requires_buffer_recompute: false,
            });
        }
        let indices = crate::plan::index::deduplicate_indices(raw_index_requests);

        // Backfill `window_index` and `partition_lookup` on each
        // window-bearing Transform node now that `indices` exists. The
        // initial lowering pass deferred these because `PlanIndexRoot`
        // for node-rooted windows requires the post-graph NodeIndex.
        for (i, wc_opt) in window_configs.iter().enumerate() {
            let Some(wc) = wc_opt else { continue };
            let transform_name = entries[i].name.as_str();
            let Some(&transform_idx) = name_to_idx.get(transform_name) else {
                continue;
            };
            // Recompute the same root used above. The duplicated walk is
            // intentional — sharing a side table would couple the
            // diagnostic-emitting and update passes via a structure
            // that adds no clarity over re-walking a small graph.
            let cross_source = wc
                .source
                .as_ref()
                .filter(|s| s.as_str() != primary_source_str)
                .cloned();
            let root = if let Some(other) = cross_source {
                let Some(&other_idx) = name_to_idx.get(other.as_str()) else {
                    continue;
                };
                let Some(anchor_schema) = graph[other_idx].stored_output_schema().cloned() else {
                    continue;
                };
                crate::plan::index::PlanIndexRoot::Node {
                    upstream: other_idx,
                    anchor_schema,
                }
            } else {
                let pred_idx = match graph
                    .neighbors_directed(transform_idx, petgraph::Direction::Incoming)
                    .next()
                {
                    Some(p) => p,
                    None => continue,
                };
                let rooted_idx =
                    crate::plan::execution::first_non_passthrough_ancestor(&graph, pred_idx);
                let Some(anchor_schema) = graph[rooted_idx].stored_output_schema().cloned() else {
                    continue;
                };
                crate::plan::index::PlanIndexRoot::Node {
                    upstream: rooted_idx,
                    anchor_schema,
                }
            };
            let new_window_index =
                crate::plan::index::find_index_for(&indices, &root, &wc.group_by, &wc.sort_by);
            if let crate::plan::execution::PlanNode::Transform {
                window_index,
                partition_lookup,
                ..
            } = &mut graph[transform_idx]
            {
                *window_index = new_window_index;
                // `partition_lookup` mirrors the cross-source vs
                // same-source distinction. Re-derive from `wc.source`
                // and `primary_source`, matching the lowering arm in
                // `lower_node_to_plan_node`.
                use crate::plan::execution::PartitionLookupKind;
                let source = wc
                    .source
                    .clone()
                    .unwrap_or_else(|| primary_source_str.to_string());
                *partition_lookup = if source == primary_source_str && wc.on.is_none() {
                    Some(PartitionLookupKind::SameSource)
                } else {
                    Some(PartitionLookupKind::CrossSource {
                        on_expr: wc.on.clone(),
                    })
                };
            }
        }

        // Topo sort. Cycles were already caught by stage 3 (E003) so
        // toposort here is expected to succeed; if it doesn't, surface
        // a generic E003 fallback.
        let topo_order = match petgraph::algo::toposort(&graph, None) {
            Ok(order) => order,
            Err(_) => {
                // (a) Whole-DAG fallback: stage-5 toposort failure
                // covers the whole graph; stage 3 should already have
                // caught cycles upstream with node-level spans.
                diags.push(Diagnostic::error(
                    "E003",
                    "cycle detected during stage-5 lowering (post-validate)".to_string(),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
                return Err(diags);
            }
        };

        // Per-transform parallelism profile. Derived by walking the
        // topo order; Transform nodes contribute their `parallelism_class`,
        // everything else is skipped.
        let parallelism = ParallelismProfile {
            per_transform: topo_order
                .iter()
                .filter_map(|&idx| match &graph[idx] {
                    PlanNode::Transform {
                        parallelism_class, ..
                    } => Some(*parallelism_class),
                    _ => None,
                })
                .collect(),
            worker_threads: self
                .pipeline
                .concurrency
                .as_ref()
                .and_then(|c| c.threads)
                .unwrap_or(4),
        };

        let mut dag = ExecutionPlanDag::from_parts(
            graph,
            topo_order,
            source_dag,
            indices,
            output_projections,
            parallelism,
        );

        // ── Enrichment pipeline ─────────────────────────────────────
        let source_bodies: Vec<&crate::config::pipeline_node::SourceBody> =
            self.source_bodies().collect();
        let inputs_map: HashMap<String, &crate::config::pipeline_node::SourceBody> = source_bodies
            .iter()
            .map(|b| (b.source.name.clone(), *b))
            .collect();
        let format_inputs_map: HashMap<String, crate::config::SourceConfig> = source_configs
            .iter()
            .map(|i| (i.name.clone(), i.clone()))
            .collect();
        // Per-source correlation-sort injection runs before the
        // operator-level enforcer pass so the latter sees every
        // CK-driven sort already in place. No-op for sources that
        // declared no `correlation_key:`.
        if let Err(e) = dag.inject_correlation_sort(&source_bodies) {
            diags.push(Diagnostic::error(
                "E003",
                format!("correlation-sort injection failed: {e}"),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diags);
        }
        if let Err(e) = dag.insert_enforcer_sorts(&format_inputs_map) {
            diags.push(Diagnostic::error(
                "E003",
                format!("enforcer-sort insertion failed: {e}"),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diags);
        }
        // Enforcer insertion may have grown the graph; re-derive topo
        // + tiers before property derivation walks it.
        dag.topo_order = match petgraph::algo::toposort(&dag.graph, None) {
            Ok(order) => order,
            Err(cycle) => {
                let cycle_path =
                    crate::plan::execution::extract_cycle_path(&dag.graph, cycle.node_id());
                diags.push(Diagnostic::error(
                    "E003",
                    format!("cycle detected post-enforcer-insertion: {cycle_path}"),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
                return Err(diags);
            }
        };
        crate::plan::execution::assign_tiers(&mut dag.graph, &dag.topo_order);
        if let Err(e) = dag.compute_node_properties(&inputs_map) {
            diags.push(Diagnostic::error(
                "E003",
                format!("node property derivation failed: {e}"),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diags);
        }

        // Aggregate retraction-mode flags are derived once
        // `compute_node_properties` has stamped the per-node `ck_set`.
        // Walks every aggregate (top-level + body mini-DAG) and flips
        // the strict default to relaxed when its parent ck_set carries
        // a field the aggregate's `group_by` does not cover.
        crate::plan::execution::apply_retraction_flags(&mut dag);
        for body in artifacts.composition_bodies.values_mut() {
            crate::plan::execution::apply_retraction_flags_in_body(body);
        }

        // Window buffer-recompute derivation. Reads `node_properties.ck_set`
        // populated above; flags every IndexSpec whose downstream
        // window-bearing Transform sits under a relaxed-CK aggregate
        // that dropped CK fields the window's `partition_by`
        // references. The executor's window arm reads the flag at
        // runtime to choose between streaming-emit and buffered emit;
        // pipelines without a relaxed-CK aggregate keep every flag
        // false and the executor stays on its existing path.
        dag.derive_window_buffer_recompute_flags();

        // Composition body windows. `bind_composition` cannot stamp
        // `window_index` on body Transforms because body lowering
        // runs before the parent DAG's NodeIndex space is allocated.
        // This post-pass walks each body's mini-DAG, classifies each
        // window's rooting (Source / Node / ParentNode), constructs
        // the body's IndexSpec list, and backfills `window_index` on
        // each body Transform. Bodies whose windows resolve through
        // an `input:` port emit `PlanIndexRoot::ParentNode` pointing
        // at the parent-DAG operator feeding the port — the body
        // executor inherits the parent's WindowRuntime via
        // `Arc::clone` at recursion entry. The pass only short-
        // circuits on errors it itself emits (E003 / E150d at body
        // root); existing E102-E109 from bind_composition stay
        // non-fatal here, mirroring the post-bind-schema gate above
        // that lets composition-binding errors land as soft
        // diagnostics while CXL errors hard-stop.
        let pre_pass_diag_count = diags.len();
        crate::plan::execution::resolve_composition_body_windows(&dag, &mut artifacts, &mut diags);
        if diags[pre_pass_diag_count..]
            .iter()
            .any(|d| matches!(d.severity, clinker_core_types::Severity::Error))
        {
            return Err(diags);
        }

        // Per-body buffer-recompute flag derivation. Mirrors the top-
        // level `derive_window_buffer_recompute_flags` walk above —
        // body-internal relaxed-CK aggregates engage the retraction
        // protocol the same way top-level relaxed-CK aggregates do, so
        // any body window whose `partition_by` does not cover the
        // visible CK set must flip to buffered emit. Without this
        // pass, body-window retraction would silently bypass the
        // commit-phase recompute path.
        for body in artifacts.composition_bodies.values_mut() {
            crate::plan::execution::derive_window_buffer_recompute_flags_in_body(body);
        }

        // Deferred-region detection. Runs after retraction flags and
        // window-buffer-recompute flags so each relaxed-CK Aggregate is
        // already classified. The walk returns top-level regions
        // (producer in `dag.graph`) separately from body-internal
        // regions (producer in some `BoundBody.graph`); we flatten each
        // bucket into a NodeIndex-keyed map at the right scope so
        // dispatcher arms can do O(1) lookup at every participating
        // node (producer, members, outputs). The two index spaces are
        // disjoint by scope — body-local NodeIndex values can
        // numerically collide with parent-graph indices, so they live
        // on `BoundBody.deferred_regions`, not on the parent map.
        let analysis = crate::plan::deferred_region::detect_deferred_regions(
            &dag.graph,
            &dag.node_properties,
            &artifacts,
            &dag.indices_to_build,
        );
        let mut region_map: HashMap<
            petgraph::graph::NodeIndex,
            crate::plan::deferred_region::DeferredRegion,
        > = HashMap::new();
        for region in analysis.top_level {
            let producer = region.producer;
            let members = region.members.clone();
            let outputs = region.outputs.clone();
            for k in std::iter::once(producer).chain(members).chain(outputs) {
                region_map.insert(k, region.clone());
            }
        }
        dag.deferred_regions = region_map;
        dag.parent_continuations = analysis.top_continuations;

        for (body_id, regions) in analysis.body_regions {
            let Some(body) = artifacts.composition_bodies.get_mut(&body_id) else {
                continue;
            };
            for region in regions {
                let producer = region.producer;
                let members = region.members.clone();
                let outputs = region.outputs.clone();
                for k in std::iter::once(producer).chain(members).chain(outputs) {
                    body.deferred_regions.insert(k, region.clone());
                }
            }
        }

        for (body_id, conts) in analysis.body_continuations {
            let Some(body) = artifacts.composition_bodies.get_mut(&body_id) else {
                continue;
            };
            for (idx, cont) in conts {
                body.parent_continuations.insert(idx, cont);
            }
        }

        // E15Y: an aggregate whose `group_by` omits any correlation-key
        // field cannot also use `strategy: streaming`. Streaming
        // aggregates emit at group-boundary close, before the terminal
        // CorrelationCommit, which defeats the rollback window the
        // retraction protocol needs. Runs before
        // `select_aggregation_strategies` so the post-pass's generic
        // "explicit Streaming on ineligible input" diagnostic does not
        // preempt the more specific E15Y message.
        let mut e15y_present = false;
        for idx in dag.graph.node_indices() {
            let crate::plan::execution::PlanNode::Aggregation { name, config, .. } =
                &dag.graph[idx]
            else {
                continue;
            };
            if !matches!(
                config.strategy,
                crate::config::AggregateStrategyHint::Streaming
            ) {
                continue;
            }
            let parent_ck = dag
                .graph
                .neighbors_directed(idx, petgraph::Direction::Incoming)
                .next()
                .and_then(|p| dag.node_properties.get(&p))
                .map(|p| p.ck_set.clone())
                .unwrap_or_default();
            if !crate::plan::execution::group_by_omits_any_ck_field(&config.group_by, &parent_ck) {
                continue;
            }
            diags.push(Diagnostic::error(
                "E15Y",
                format!(
                    "E15Y aggregate '{}' has `strategy: streaming` but its \
                     `group_by` omits at least one correlation-key field \
                     visible upstream, which routes it through the retraction \
                     protocol. Streaming aggregates emit per group-boundary \
                     close, before correlation-commit, which defeats the \
                     rollback window. Use `strategy: hash` (the default), or \
                     include every correlation-key field in `group_by` so the \
                     aggregate stays on the strict-collateral path.",
                    name
                ),
                LabeledSpan::primary(dag.graph[idx].span(), String::new()),
            ));
            e15y_present = true;
        }
        if e15y_present {
            return Err(diags);
        }

        // Aggregation-strategy post-pass: resolves the user `strategy`
        // hint on each `PlanNode::Aggregation` against upstream
        // `OrderingProvenance` and rewrites the node's stored ordering
        // provenance accordingly. DataFusion `PhysicalOptimizerRule`
        // pattern: a frozen-plan walk that mutates only strategy +
        // side-table ordering, never the graph topology.
        if let Err(e) = dag.select_aggregation_strategies() {
            diags.push(Diagnostic::error(
                "E003",
                format!("aggregation strategy selection failed: {e}"),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diags);
        }

        // N-ary combine decomposition. Rewrites every `PlanNode::Combine`
        // with input count > 2 into a left-deep chain of binary combines
        // so the strategy pass below sees only N=2 nodes. Runs against
        // the fully-enriched DAG; emits E300 (input cap) and E305
        // (disconnected join graph). Mutates `artifacts` in place to
        // add per-step `combine_inputs` / `combine_predicates` /
        // `combine_driving` entries, and grows the graph with
        // (N-2) synthetic chain nodes per N-ary combine.
        crate::plan::combine::decompose_nary_combines(&mut dag, &mut artifacts, &mut diags);

        // Synthetic chain nodes pushed by `decompose_nary_combines`
        // are not in `dag.topo_order` — that vector was built from the
        // pre-decomposition graph. Re-derive the topological order so
        // the executor walks every chain step in dependency order.
        // The graph remains acyclic by construction (each step has
        // exactly two distinct upstream edges chosen from previously
        // placed nodes).
        dag.topo_order = match petgraph::algo::toposort(&dag.graph, None) {
            Ok(order) => order,
            Err(cycle) => {
                let cycle_path =
                    crate::plan::execution::extract_cycle_path(&dag.graph, cycle.node_id());
                diags.push(Diagnostic::error(
                    "E003",
                    format!("cycle detected post-combine-decomposition: {cycle_path}"),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
                return Err(diags);
            }
        };

        // Seed the statistics catalog's Plane A row counts from source
        // file metadata before the combine post-pass reads them. Uses the
        // same on-disk byte read the byte-volume estimates derive from,
        // divided by the shared average-record-bytes divisor — no second
        // data-reading pass. A build side's metadata-derived row count is
        // what lets the planner choose grace-hash over an in-memory hash
        // before a single record is read.
        dag.seed_statistics_row_counts(ctx, &mut artifacts.statistics);

        // Combine strategy + driving-input post-pass. Runs after the
        // DAG is fully enriched (so every PlanNode::Combine is present
        // and property derivation has stamped ordering provenance) and
        // after N-ary decomposition (so this pass only sees binary
        // nodes). The pass mutates PlanNode::Combine in place,
        // replacing construction-time placeholders for `strategy` and
        // `driving_input`.
        crate::plan::combine::select_combine_strategies(
            &mut dag,
            &artifacts,
            &mut diags,
            self.pipeline.memory.limit.as_deref(),
        );
        // Companion sweep over composition body mini-DAGs. Body
        // graphs hold their own `PlanNode::Combine` nodes that the
        // top-level pass above cannot reach, so without this call
        // body-context combines never get their strategy + driving
        // qualifier stamped and short-circuit at dispatch.
        crate::plan::combine::select_combine_strategies_in_bodies(
            &mut artifacts,
            &mut diags,
            self.pipeline.memory.limit.as_deref(),
        );

        // Envelope header-port resolution post-pass. Runs after every DAG edge
        // is built and correlation Sorts are spliced, so a wired `header:` port
        // resolves to its predecessor `NodeIndex` (the executor reads it to
        // drain the header stream separately from the body). Composition body
        // mini-DAGs hold their own Envelope nodes, so sweep them too.
        crate::plan::execution::resolve_envelope_header_upstreams(&mut dag);
        for body in artifacts.composition_bodies.values_mut() {
            crate::plan::execution::resolve_envelope_header_upstreams_in_graph(&mut body.graph);
        }

        // Per-node input-volume byte estimates. Runs last among the
        // property passes so the resolved aggregation/combine strategies
        // are visible (blocking-ness is read from the same arbitration
        // classifier `--explain` uses) and no later overwrite of an
        // Aggregation node's `NodeProperties` can clobber the volume
        // fields. Each Source reads its own resolved `SourceConfig` off the
        // plan node, so the seed is keyed by node identity rather than by a
        // separate name map; sizes resolve against the pipeline file's
        // directory and propagate forward in topo order. Pipelines whose
        // sources are unsized or multi-file keep every estimate at the `0`
        // "unknown" sentinel.
        dag.derive_volume_estimates(ctx);

        // Correlation-key planner passes. Run AFTER the DAG is fully
        // enriched so we see every Transform's `window_index` and every
        // Aggregate's resolved `group_by`.
        //
        // Auto-extension: for every Aggregate, transparently append
        // `$ck.<field>` shadow columns to the runtime `group_by`
        // whenever the user-declared CK field is already listed AND
        // the parent's `ck_set` carries that CK field. The user never
        // types the engine-internal namespace in YAML; the engine
        // routes frozen identity through the aggregation key by
        // construction. An aggregate whose `group_by` omits a CK field
        // activates the retraction protocol at runtime — it is not a
        // compile-time error.
        extend_aggregate_group_by_with_shadow(&mut dag);
        for body in artifacts.composition_bodies.values_mut() {
            extend_aggregate_group_by_with_shadow_in_body(body);
        }
        let pipeline_has_any_ck = self.any_source_has_correlation_key();
        if pipeline_has_any_ck {
            for node in dag.graph.node_weights() {
                if let crate::plan::execution::PlanNode::Transform {
                    name,
                    window_index: Some(idx_num),
                    ..
                } = node
                {
                    // Two disjoint reasons E150 lifts:
                    //
                    // 1. Window anchored at a non-Source upstream
                    //    (Aggregate, Combine, Transform, etc.) — the
                    //    arena materializes from that operator's emit
                    //    buffer, not from per-CK-group source rows,
                    //    so the per-group arena concern does not
                    //    apply. CK-aligned partitions fall here.
                    // 2. Window anchored at a Source in buffer-
                    //    recompute mode — the orchestrator's commit
                    //    phase reruns the window over surviving
                    //    partition rows after a CK group is
                    //    retracted, so anchoring at a CK-bearing
                    //    Source is still safe.
                    //
                    // The unsafe case is a window whose
                    // `PlanIndexRoot::Node { upstream, .. }` points
                    // at a Source whose `correlation_key:` is
                    // declared, without buffer-recompute.
                    let safe = dag
                        .indices_to_build
                        .get(*idx_num)
                        .map(|s| {
                            let upstream_is_correlated_source = match &s.root {
                                crate::plan::index::PlanIndexRoot::Node { upstream, .. }
                                | crate::plan::index::PlanIndexRoot::ParentNode {
                                    upstream, ..
                                } => {
                                    let upstream_node = &dag.graph[*upstream];
                                    if let crate::plan::execution::PlanNode::Source {
                                        name: source_name,
                                        ..
                                    } = upstream_node
                                    {
                                        self.nodes.iter().any(|spanned| {
                                            matches!(
                                                &spanned.value,
                                                PipelineNode::Source { header, config }
                                                if header.name == *source_name
                                                    && config.correlation_key.is_some()
                                            )
                                        })
                                    } else {
                                        false
                                    }
                                }
                            };
                            !upstream_is_correlated_source || s.requires_buffer_recompute
                        })
                        .unwrap_or(false);
                    if safe {
                        continue;
                    }
                    let err = crate::plan::execution::PlanError::CorrelationKeyWithArena {
                        transform: name.clone(),
                    };
                    diags.push(Diagnostic::error(
                        "E150",
                        err.to_string(),
                        LabeledSpan::primary(node.span(), String::new()),
                    ));
                }
            }
        }

        // Inject the terminal correlation-commit node once the DAG is
        // otherwise frozen. Re-derive topo afterward because the
        // commit node and its incoming edges change the order. No-op
        // when no source declares a correlation key.
        let max_group_buffer = self.error_handling.max_group_buffer.unwrap_or(100_000);
        if let Err(e) = dag.inject_correlation_commit(&source_bodies, max_group_buffer) {
            diags.push(Diagnostic::error(
                "E003",
                format!("correlation-commit injection failed: {e}"),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diags);
        }
        if dag.graph.node_weights().any(|n| {
            matches!(
                n,
                crate::plan::execution::PlanNode::CorrelationCommit { .. }
            )
        }) {
            dag.topo_order = match petgraph::algo::toposort(&dag.graph, None) {
                Ok(order) => order,
                Err(cycle) => {
                    let cycle_path =
                        crate::plan::execution::extract_cycle_path(&dag.graph, cycle.node_id());
                    diags.push(Diagnostic::error(
                        "E003",
                        format!("cycle detected post-correlation-commit: {cycle_path}"),
                        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                    ));
                    return Err(diags);
                }
            };
        }

        // E152 — every PlanNode::Composition's incoming edges must carry
        // a `PlanEdge.port` tag. Compile-time guard for the dispatcher's
        // collect_port_records invariant: a planner pass that splices an
        // intermediate node between a producer and a composition without
        // preserving the port tag would silently drop records at
        // dispatch. Runs after every edge-rewriting pass so the final
        // plan state is what's verified.
        diags.extend(crate::plan::execution::diagnose_untagged_composition_edges(
            &dag, &artifacts,
        ));

        // If lowering accumulated any non-composition error-severity
        // diagnostics, return them. Composition binding errors
        // (E102–E109) are non-fatal — the composition node is silently
        // omitted from the DAG. Warnings do not block.
        let has_fatal_errors = diags.iter().any(|d| {
            matches!(d.severity, clinker_core_types::Severity::Error) && !d.code.starts_with("E10")
        });
        if has_fatal_errors {
            return Err(diags);
        }

        // Stamp the runtime config's source bodies with their declared
        // `$doc.*` path set. The executor's ingest path reads source configs
        // from `PipelineConfig::source_configs` (the config nodes), so the
        // envelope pre-scan only learns which sections a downstream program
        // reads from this stamp. The attribution map is keyed by node
        // identity (`header.name`), which can differ from the nested
        // `config.name:`, so the lookup keys on `header.name` and writes onto
        // that node's source body. A source no program reads a `$doc` path
        // through keeps an empty set, which the reader treats as "skip the
        // pre-scan entirely".
        let mut runtime_config = self.clone();
        for spanned in runtime_config.nodes.iter_mut() {
            if let PipelineNode::Source {
                header,
                config: body,
            } = &mut spanned.value
                && let Some(paths) = declared_doc_paths.get(&header.name)
            {
                body.source.declared_doc_paths = paths.clone();
            }
        }

        let plan = CompiledPlan::new(dag, runtime_config, artifacts);
        Ok((plan, diags))
    }
}

/// Render a [`NodeInput`] back into a human-readable reference string.
/// `Single("foo")` → `"foo"`; `Port { node: "route", port: "high" }` →
/// `"route.high"`. Used in diagnostic messages so the user sees the
/// reference exactly as they wrote it.
fn input_full_reference(input: &node_header::NodeInput) -> String {
    match input {
        node_header::NodeInput::Single(s) => s.clone(),
        node_header::NodeInput::Port { node, port } => format!("{node}.{port}"),
    }
}

/// Resolve the producer output port a consumer's edge draws from, given
/// the producer node, the consumer's full input reference, the bare
/// producer name, and the consumer's own name.
///
/// Two reference shapes select a port on a multi-output producer (today
/// only `PlanNode::Route`, whose ports are its branches plus the default):
///
/// - Port form: `input: <route>.<branch>` — the suffix names the port.
/// - Bare form: `input: <route>` on a consumer whose own *name* equals a
///   branch (or the default). This shorthand reads as "this node IS the
///   `<name>` branch": each branch has one consumer named for it.
///
/// Returns `None` for single-output producers, and for references that
/// match neither shape (a suffix or consumer name that is not a declared
/// port). Shared by the top-level and composition-body edge wiring so
/// both scopes route Route branches off the live edge graph identically.
pub(crate) fn resolve_producer_port(
    producer: &crate::plan::execution::PlanNode,
    producer_full: &str,
    producer_key: &str,
    consumer_name: &str,
) -> Option<String> {
    let ports = producer.output_ports()?;
    if let Some(suffix) = producer_full
        .strip_prefix(producer_key)
        .and_then(|rest| rest.strip_prefix('.'))
    {
        return ports.contains(&suffix).then(|| suffix.to_string());
    }
    // Bare reference: the consumer's name selects the port.
    ports
        .contains(&consumer_name)
        .then(|| consumer_name.to_string())
}

/// Unified input-reference resolution pass. Walks every node's
/// declared input(s) and emits [`Diagnostic`] code `E004` with a
/// structured [`clinker_core_types::DiagnosticPayload::InputRefUndeclared`]
/// payload for each reference that doesn't resolve to a declared node
/// name — covering both standalone-node `input:` references and
/// combine-arm per-port references with a single code.
///
/// Runs BEFORE `bind_schema` so undeclared-input diagnostics surface
/// independently of CXL errors. Per-input spans
/// (`Spanned<NodeInput>::referenced.line()`) are preserved on the
/// emitted diagnostic so span-level assertions can verify placement.
fn resolve_all_input_references(
    nodes: &[Spanned<PipelineNode>],
    diags: &mut Vec<clinker_core_types::Diagnostic>,
) {
    use clinker_core_types::span::Span;
    use clinker_core_types::{Diagnostic, DiagnosticPayload, LabeledSpan};

    let declared_names: std::collections::HashSet<String> =
        nodes.iter().map(|s| s.value.name().to_string()).collect();

    fn strip_port(r: &str) -> &str {
        r.split('.').next().unwrap_or(r)
    }

    let mut emit = |consumer_name: &str,
                    qualifier: Option<&str>,
                    input_node: &Spanned<node_header::NodeInput>| {
        let reference_full = input_full_reference(&input_node.value);
        let producer_key = strip_port(&reference_full);
        if declared_names.contains(producer_key) {
            return;
        }
        let line = input_node.referenced.line() as u32;
        let span = if line > 0 {
            Span::line_only(line)
        } else {
            Span::SYNTHETIC
        };
        let message = match qualifier {
            Some(q) => format!(
                "at line {line}: combine '{consumer_name}' input '{q}' references undeclared upstream '{reference_full}'"
            ),
            None => format!(
                "node {consumer_name:?} input {reference_full:?} references an undeclared node"
            ),
        };
        diags.push(
            Diagnostic::error("E004", message, LabeledSpan::primary(span, String::new()))
                .with_payload(DiagnosticPayload::InputRefUndeclared {
                    consumer: consumer_name.to_string(),
                    qualifier: qualifier.map(str::to_string),
                    reference: reference_full,
                }),
        );
    };

    for spanned in nodes {
        let node = &spanned.value;
        let consumer_name = node.name();
        match node {
            PipelineNode::Source { .. } | PipelineNode::Composition { .. } => {}
            PipelineNode::Transform { header, .. }
            | PipelineNode::Aggregate { header, .. }
            | PipelineNode::Route { header, .. }
            | PipelineNode::Reshape { header, .. }
            | PipelineNode::Cull { header, .. }
            | PipelineNode::Output { header, .. } => {
                emit(consumer_name, None, &header.input);
            }
            PipelineNode::Merge { header, .. } => {
                for inp in &header.inputs {
                    emit(consumer_name, None, inp);
                }
            }
            PipelineNode::Combine { header, .. } => {
                for (qualifier, node_input) in &header.input {
                    emit(consumer_name, Some(qualifier.as_str()), node_input);
                }
            }
            PipelineNode::Envelope { header, .. } => {
                emit(consumer_name, None, &header.body);
                for inp in [&header.header, &header.trailer].into_iter().flatten() {
                    emit(consumer_name, None, inp);
                }
            }
        }
    }
}

/// How a source's `$doc.<section>.<field>` references are validated at
/// compile time, derived from the source's transport and format.
///
/// A `$doc` access carries no envelope context the cxl typechecker can see,
/// so the planner cross-checks each referenced path against the schema the
/// feeding source's reader will actually serve. That schema differs by
/// format family:
///
/// - **Closed** (XML, JSON): the reader serves exactly the sections and
///   fields the `envelope:` block declares — the declaration is the
///   complete schema, so a path naming anything it omits is a genuine typo
///   (`E341`).
/// - **Segment/positional** (X12, EDIFACT, HL7): the file-level header
///   (`ISA`/`UNB`/`FHS`) is declared through `envelope:` and is closed, but
///   the reader *also* synthesizes nested envelope levels the config never
///   names — X12's `functional_group`/`transaction_set`, HL7's
///   `batch`/`transaction_set` — keyed by positional `eNN`/`fNN` elements
///   bounded by the format's `max_elements`/`max_fields`. A `$doc` path is
///   validated against that known synthesized vocabulary plus any
///   user-declared section/field, so a typo (`$doc.functonal_group.e06`)
///   or an out-of-range positional element (`$doc.transaction_set.e99`) is
///   caught (`E348`) without false-positiving on a legitimate wire path.
/// - **Unvalidated** (CSV, fixed-width, SWIFT): not statically checked. A
///   plain flat file synthesizes no `$doc` sections (a `$doc` read against
///   one is inert but harmless), a multi-record flat file's `record_type`
///   sections are author-declared but the section vocabulary the reader
///   serves is not statically reconstructible here without resolving the
///   `format_schema`, and SWIFT serves declared sections under user-chosen
///   *or* stable default block labels — none fits the closed or positional
///   model cleanly, so each is left unchecked rather than risk a false
///   positive.
///
/// REST sources are handled before this classification: a `rest` transport
/// buffers no document, so any `$doc` access against one can never resolve
/// and is rejected outright (`E349`), regardless of the declared format.
enum DocSchema {
    /// Validate against the declared `envelope:` block alone.
    Closed,
    /// Validate against the format's synthesized section/positional-element
    /// vocabulary plus any user-declared section/field.
    SegmentPositional(SegmentPositionalSchema),
    /// Not statically validated (CSV, fixed-width, SWIFT).
    Unvalidated,
}

/// The output-envelope config of a generic-format Output (CSV / JSON / XML /
/// fixed-width) paired with whether that format can carry a computed footer
/// field, or `None` when the format declares no envelope config (or is an EDI
/// format, which has its own per-format reconstruction vocabulary, not this
/// generic one).
///
/// Used by the E346 check: a non-`None` result means the Output reconstructs a
/// generic output envelope whose section names and computed footer must be
/// validated. Fixed-width returns `false` for the computed-footer flag — its
/// positional lines have no field to inject a count into.
fn generic_output_envelope(
    format: &OutputFormat,
) -> Option<(&crate::config::OutputEnvelopeConfig, bool)> {
    match format {
        OutputFormat::Csv(opts) => opts.as_ref()?.envelope.as_ref().map(|e| (e, true)),
        OutputFormat::Json(opts) => opts.as_ref()?.envelope.as_ref().map(|e| (e, true)),
        OutputFormat::Xml(opts) => opts.as_ref()?.envelope.as_ref().map(|e| (e, true)),
        OutputFormat::FixedWidth(opts) => opts.as_ref()?.envelope.as_ref().map(|e| (e, false)),
        OutputFormat::Edifact(_)
        | OutputFormat::X12(_)
        | OutputFormat::Hl7(_)
        | OutputFormat::Swift(_) => None,
    }
}

/// The upstream lineage of an Output, as it bears on envelope reconstruction:
/// the set of Source node names that feed it, and the name of the first
/// document-lineage-stripping node found on any path to it (if any).
///
/// A node strips document lineage when it emits records with a synthetic /
/// merged [`clinker_record::DocumentContext`] rather than carrying their
/// originating document's context forward:
///
/// - `Aggregate` finalizes group rows through the merged eval context (even
///   per-document aggregation finalizes through it), so every emitted row is
///   synthetic-grained.
/// - `Combine` fans several inputs into merged-lineage rows.
/// - `Composition` is opaque at config-validation time (its body lives in a
///   separate artifact, not in `config.nodes`), so its lineage cannot be
///   proven preserved — treated conservatively as a stripper.
///
/// Such records reach an enveloped Output with a non-concrete `source_file`,
/// which the envelope arm streams UNFRAMED. The plan-time guard (E347) uses
/// `lineage_stripper` to reject the combination before it can emit malformed /
/// misattributed framing; `feeding_sources` scopes the E346 section-name check
/// to the sources whose documents this Output actually frames.
struct OutputLineage {
    feeding_sources: std::collections::BTreeSet<String>,
    lineage_stripper: Option<String>,
    /// Whether the body feeding this Output can statically carry >= 2 document
    /// grains with no consolidating node on the path — the signal the E355
    /// cardinality gate keys off. MULTI is the conservative default: a false
    /// MULTI is a fixable compile error, a false SINGLE would let a
    /// multi-document body silently merge into one single-document envelope.
    body_cardinality: Cardinality,
}

/// Walk upstream from `output_name` over the config node graph, collecting the
/// feeding sources and the first lineage-stripping node on any path. Cycle-safe
/// via a visited set (cycles are rejected separately by the DAG-edge pass, so
/// this only guards against re-traversal, never relies on acyclicity).
fn trace_output_lineage(config: &PipelineConfig, output_name: &str) -> OutputLineage {
    let by_name: HashMap<&str, &PipelineNode> = config
        .nodes
        .iter()
        .map(|n| (n.value.name(), &n.value))
        .collect();
    let mut feeding_sources = std::collections::BTreeSet::new();
    let mut lineage_stripper: Option<String> = None;
    let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut stack: Vec<&str> = vec![output_name];
    while let Some(node_name) = stack.pop() {
        if !visited.insert(node_name) {
            continue;
        }
        let Some(node) = by_name.get(node_name) else {
            continue;
        };
        match node {
            PipelineNode::Source { .. } => {
                feeding_sources.insert(node_name.to_string());
            }
            PipelineNode::Aggregate { .. }
            | PipelineNode::Combine { .. }
            | PipelineNode::Composition { .. } => {
                // Record the first stripper found (deterministic by the
                // declaration-order traversal seed), but keep walking so the
                // feeding-source set stays complete for the E346 scoping.
                if lineage_stripper.is_none() {
                    lineage_stripper = Some(node_name.to_string());
                }
                for up in node.direct_input_names() {
                    stack.push(up);
                }
            }
            _ => {
                for up in node.direct_input_names() {
                    stack.push(up);
                }
            }
        }
    }
    // Document cardinality is a separate, path-sensitive fold (a consolidating
    // node collapses only its own upstream subtree), so compute it with a fresh
    // recursion over the same node map rather than contorting the flat walk above.
    let body_cardinality =
        stream_cardinality(&by_name, output_name, &mut std::collections::HashSet::new());
    OutputLineage {
        feeding_sources,
        lineage_stripper,
        body_cardinality,
    }
}

/// Whether a node's output stream can statically carry more than one document.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cardinality {
    /// Provably collapses to at most one document grain at this point.
    Single,
    /// Can statically carry two or more document grains.
    Multi,
}

impl Cardinality {
    fn join(self, other: Cardinality) -> Cardinality {
        if self == Cardinality::Multi || other == Cardinality::Multi {
            Cardinality::Multi
        } else {
            Cardinality::Single
        }
    }
}

/// Conservatively decide whether the stream produced by `node_name` can carry
/// two or more document grains. Consolidating nodes (`Combine` / `Aggregate` /
/// `Composition`, or an `Envelope` with `strategy: concat`) collapse everything
/// upstream of them to a single grain; an `Envelope` `preserve` and the
/// per-record operators pass cardinality through; a `Merge` of >= 2 inputs is
/// itself a multiplier; a `Source` is MULTI when it discovers multiple files or
/// its reader yields multiple documents per file. Unknowns bias to MULTI (a
/// false MULTI is a loud, fixable compile error; a false SINGLE would let silent
/// document-merging through). Cycle-safe via the shared `visited` set.
fn stream_cardinality<'a>(
    by_name: &HashMap<&'a str, &'a PipelineNode>,
    node_name: &'a str,
    visited: &mut std::collections::HashSet<&'a str>,
) -> Cardinality {
    if !visited.insert(node_name) {
        // Already folded on another path; the first visit returned its true
        // value and `join` is monotonic toward MULTI, so re-counting it as
        // SINGLE here cannot lower an already-MULTI result.
        return Cardinality::Single;
    }
    let Some(node) = by_name.get(node_name) else {
        return Cardinality::Single;
    };
    match node {
        PipelineNode::Source { config, .. } => source_cardinality(&config.source),
        // Consolidators: collapse the whole upstream subtree to one document.
        PipelineNode::Combine { .. }
        | PipelineNode::Aggregate { .. }
        | PipelineNode::Composition { .. } => Cardinality::Single,
        PipelineNode::Envelope { config, .. } => {
            use crate::config::pipeline_node::EnvelopeStrategy;
            match config.strategy {
                EnvelopeStrategy::Concat => Cardinality::Single,
                // `preserve` keeps per-document grains, so the output document
                // cardinality follows the BODY only — `direct_input_names`
                // returns the body first, then the optional header/trailer
                // framing ports, which carry framing records, not body
                // documents, and must not inflate the body's cardinality.
                EnvelopeStrategy::Preserve => node
                    .direct_input_names()
                    .first()
                    .map(|body| stream_cardinality(by_name, body, visited))
                    .unwrap_or(Cardinality::Single),
            }
        }
        // A merge of >= 2 document-carrying inputs is itself multi-document:
        // each predecessor contributes its own grains.
        PipelineNode::Merge { .. } => {
            if node.direct_input_names().len() >= 2 {
                Cardinality::Multi
            } else {
                join_input_cardinality(by_name, node, visited)
            }
        }
        // Per-record / pass-through operators: cardinality of their inputs.
        PipelineNode::Transform { .. }
        | PipelineNode::Route { .. }
        | PipelineNode::Cull { .. }
        | PipelineNode::Reshape { .. }
        | PipelineNode::Output { .. } => join_input_cardinality(by_name, node, visited),
    }
}

fn join_input_cardinality<'a>(
    by_name: &HashMap<&'a str, &'a PipelineNode>,
    node: &'a PipelineNode,
    visited: &mut std::collections::HashSet<&'a str>,
) -> Cardinality {
    node.direct_input_names()
        .into_iter()
        .map(|up| stream_cardinality(by_name, up, visited))
        .fold(Cardinality::Single, Cardinality::join)
}

/// A `Source` is statically MULTI only when its multiplicity is provable from
/// the pipeline shape alone: an explicit `paths:` list of length >= 2 is
/// definitely >= 2 documents. A `glob:`/`regex:` may match a single file, and a
/// structured single file (X12/EDIFACT/HL7/SWIFT) may hold a single
/// interchange/message — both are content-dependent, not shape-provable, so
/// they are left to the runtime guard (which rejects precisely when a second
/// concrete document grain materializes). This keeps single-document
/// round-trips such as `x12 -> x12` legal at plan time.
fn source_cardinality(source: &SourceConfig) -> Cardinality {
    if source.paths.as_ref().is_some_and(|p| p.len() >= 2) {
        Cardinality::Multi
    } else {
        Cardinality::Single
    }
}

/// A node that strips document lineage — `Combine` / `Aggregate` / `Composition`
/// emit records with a synthetic (`<merged>`) document context (see
/// [`trace_output_lineage`]).
fn is_lineage_stripper(node: &PipelineNode) -> bool {
    matches!(
        node,
        PipelineNode::Combine { .. }
            | PipelineNode::Aggregate { .. }
            | PipelineNode::Composition { .. }
    )
}

/// Find a lineage stripper whose synthetic `<merged>` records can reach
/// `output_name` WITHOUT being re-consolidated by a `concat` `Envelope`, or
/// `None` if every stripper on the way is shielded. A `concat` Envelope
/// re-stamps its whole body onto one document grain, so it shields a
/// `reconstruct_envelope` Output from synthetic grains; a `preserve` Envelope
/// passes the synthetic grain through unchanged and so does NOT shield. This is
/// the E347 relaxation predicate: re-enveloping downstream of a stripper is
/// legal only through a *consolidating* Envelope, and only for the strippers it
/// actually covers (a `concat` Envelope on a sibling merge branch does not
/// shield a stripper on another branch).
fn unconsolidated_stripper(config: &PipelineConfig, output_name: &str) -> Option<String> {
    use crate::config::pipeline_node::EnvelopeStrategy;
    let by_name: HashMap<&str, &PipelineNode> = config
        .nodes
        .iter()
        .map(|n| (n.value.name(), &n.value))
        .collect();
    let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut stack: Vec<&str> = vec![output_name];
    while let Some(name) = stack.pop() {
        if !visited.insert(name) {
            continue;
        }
        let Some(node) = by_name.get(name) else {
            continue;
        };
        // A `concat` Envelope consolidates everything on its body path into one
        // grain — stop descending, the strippers above it are shielded.
        if let PipelineNode::Envelope { config, .. } = node
            && config.strategy == EnvelopeStrategy::Concat
        {
            continue;
        }
        if is_lineage_stripper(node) {
            return Some(name.to_string());
        }
        for up in node.direct_input_names() {
            stack.push(up);
        }
    }
    None
}

/// The synthesized `$doc` vocabulary of a segment/positional format: the
/// nested sections its reader produces beyond the declared `envelope:`
/// block, the positional-element prefix, and the per-element bound.
struct SegmentPositionalSchema {
    /// Nested sections the reader synthesizes for this source, in addition
    /// to any the user declares under `envelope.sections`. For X12 these are
    /// the (possibly user-renamed) functional-group / transaction-set
    /// levels; for HL7 the fixed `batch` / `transaction_set`; EDIFACT
    /// synthesizes none beyond the declared `UNB` interchange. Each carries
    /// how its fields are keyed (positional `eNN`/`fNN`, or a user-declared
    /// typed nested schema).
    synthesized_sections: Vec<SynthesizedSection>,
    /// Positional field-name prefix: `e` for X12/EDIFACT element columns
    /// (`e01`…), `f` for HL7 field columns (`f01`…).
    positional_prefix: char,
    /// 1-based upper bound on a positional element/field, set to the
    /// reader's body-segment ceiling (`max_elements` / `max_fields`).
    ///
    /// This is a LOOSE bound, not a tight per-segment guarantee. The
    /// synthesized nested levels are keyed by the *header* segment's actual
    /// arity (an X12 `ST` carries ~3 elements, a `GS` ~8; an HL7 `MSH`/`BHS`
    /// likewise has a fixed-ish field count), but that precise per-segment
    /// arity is not known at plan time — and the readers themselves do not
    /// enforce it (they cap only the body-segment element/field count). So
    /// positional validation catches an unknown section name and a
    /// grossly-out-of-range element (past the body max), but a mid-range
    /// positional typo on a short header segment — e.g. `$doc.transaction_set.e20`
    /// on a 3-element `ST` — passes here and still resolves null at run
    /// time. Tightening to per-segment arity is tracked as a follow-up
    /// (see `doc_schema_for`).
    max_positional: usize,
}

/// One synthesized nested section and the set of fields its reader serves.
struct SynthesizedSection {
    /// Section name `$doc.<name>.<field>` resolves under.
    name: String,
    /// How the section's fields are keyed. A default-keyed level exposes
    /// every positional element up to the bound; a level given a typed
    /// nested schema (X12 `group_section` / `set_section`) exposes ONLY its
    /// declared fields, exactly as a declared file-level section does.
    fields: SynthesizedFields,
}

/// How a synthesized nested section's fields are keyed.
enum SynthesizedFields {
    /// Default positional keying: every `<prefix>NN`, `1 <= N <= bound`.
    Positional,
    /// A user-declared typed schema (X12 `group_section.fields` /
    /// `set_section.fields`). The reader coerces and serves ONLY these
    /// fields, so the section is closed to them — an undeclared name (even
    /// an in-range positional one) resolves null.
    ///
    /// Owned `Vec<String>` rather than a borrow of the source's `IndexMap`:
    /// the `DocSchema` is cached once per source and a borrow would thread a
    /// lifetime through the cache and every helper signature, for a linear
    /// scan over a handful of declared field names. Membership is checked
    /// with `.iter().any(..)` — O(declared fields), trivially small.
    Declared(Vec<String>),
}

/// Build a synthesized nested section from an optional user-declared nested
/// schema: a declared schema closes the section to its declared field
/// names, an absent one keeps the default positional keying.
fn synthesized_section(
    default_name: &str,
    declared: Option<&clinker_format::NestedEnvelopeSection>,
) -> SynthesizedSection {
    match declared {
        Some(s) => SynthesizedSection {
            name: s.name.clone(),
            fields: SynthesizedFields::Declared(s.fields.keys().cloned().collect()),
        },
        None => SynthesizedSection {
            name: default_name.to_string(),
            fields: SynthesizedFields::Positional,
        },
    }
}

/// Classify how a source's `$doc` references are validated, from its
/// format. The REST transport is checked by the caller before this is
/// consulted, so this maps purely on `InputFormat`.
///
/// The positional bound is the reader's body-segment ceiling, not the
/// header segment's precise arity (see [`SegmentPositionalSchema`]). A
/// tighter per-segment-arity bound — to catch a mid-range positional typo
/// on a short header segment — is tracked at
/// <https://github.com/rustpunk/clinker/issues/558>.
fn doc_schema_for(source: &crate::config::SourceConfig) -> DocSchema {
    match &source.format {
        InputFormat::Xml(_) | InputFormat::Json(_) => DocSchema::Closed,
        InputFormat::X12(opts) => {
            let opts = opts.as_ref();
            DocSchema::SegmentPositional(SegmentPositionalSchema {
                synthesized_sections: vec![
                    synthesized_section(
                        "functional_group",
                        opts.and_then(|o| o.group_section.as_ref()),
                    ),
                    synthesized_section(
                        "transaction_set",
                        opts.and_then(|o| o.set_section.as_ref()),
                    ),
                ],
                positional_prefix: 'e',
                max_positional: opts
                    .and_then(|o| o.max_elements)
                    .unwrap_or(clinker_format::X12_DEFAULT_MAX_ELEMENTS),
            })
        }
        InputFormat::Edifact(opts) => DocSchema::SegmentPositional(SegmentPositionalSchema {
            // A single UNB..UNZ interchange synthesizes no nested level; the
            // declared `UNB` section is the only one, so the synthesized set
            // is empty and a non-`UNB`-declared section name is a typo.
            synthesized_sections: Vec::new(),
            positional_prefix: 'e',
            max_positional: opts
                .as_ref()
                .and_then(|o| o.max_elements)
                .unwrap_or(clinker_format::EDIFACT_DEFAULT_MAX_ELEMENTS),
        }),
        InputFormat::Hl7(opts) => DocSchema::SegmentPositional(SegmentPositionalSchema {
            // HL7's `batch` (BHS) and `transaction_set` (MSH) levels always
            // surface their whole header verbatim as positional `fNN`
            // columns — they carry no user-declared typed schema.
            synthesized_sections: vec![
                SynthesizedSection {
                    name: "batch".to_string(),
                    fields: SynthesizedFields::Positional,
                },
                SynthesizedSection {
                    name: "transaction_set".to_string(),
                    fields: SynthesizedFields::Positional,
                },
            ],
            positional_prefix: 'f',
            max_positional: opts
                .as_ref()
                .and_then(|o| o.max_fields)
                .unwrap_or(HL7_DEFAULT_MAX_FIELDS),
        }),
        InputFormat::Csv(_) | InputFormat::FixedWidth(_) | InputFormat::Swift(_) => {
            DocSchema::Unvalidated
        }
    }
}

/// A rejected `$doc` path: the diagnostic code, formatted message, and
/// fix-it help text. The code distinguishes a closed-schema rejection
/// (`E341`) from a segment/positional one (`E348`).
struct DocPathProblem {
    code: &'static str,
    message: String,
    help: String,
}

/// Check one `$doc.<section>.<field>` path against a source's declared
/// closed envelope, returning the diagnostic content when the path names an
/// undeclared section or an undeclared field within a declared section.
///
/// Returns `None` when the path is fully declared. The caller has already
/// confirmed the source's format carries a closed envelope schema, so an
/// absent `envelope:` block means every `$doc` path is undeclared.
fn closed_doc_path_problem(
    source: &crate::config::SourceConfig,
    doc_path: &cxl::analyzer::doc_paths::DocPath,
) -> Option<DocPathProblem> {
    let section_name = &*doc_path.section;
    let field_name = &*doc_path.field;
    let source_name = &source.name;
    let section = source
        .envelope
        .as_ref()
        .and_then(|env| env.sections.get(section_name));
    match section {
        None => Some(DocPathProblem {
            code: "E341",
            message: format!(
                "`$doc.{section_name}.{field_name}` references envelope section \
                 '{section_name}', but source '{source_name}' declares no such section"
            ),
            help: format!(
                "declare the section under the source's `envelope.sections`, or correct \
                 the name to a declared section. Section names are user-chosen and \
                 case-sensitive — '{section_name}' must match a key under \
                 `envelope.sections` exactly"
            ),
        }),
        Some(section) if !section.fields.contains_key(field_name) => Some(DocPathProblem {
            code: "E341",
            message: format!(
                "`$doc.{section_name}.{field_name}` references field '{field_name}' in \
                 envelope section '{section_name}', but source '{source_name}' declares no \
                 such field in that section"
            ),
            help: format!(
                "add '{field_name}' to `envelope.sections.{section_name}.fields`, or correct \
                 the name to a declared field. Field names are case-sensitive"
            ),
        }),
        Some(_) => None,
    }
}

/// Check one `$doc.<section>.<field>` path against a segment/positional
/// format's synthesized vocabulary plus any user-declared section/field.
///
/// A path's section must be either a user-declared `envelope.sections` key
/// (the closed file-level header — `ISA`/`UNB`/`FHS`) or one the reader
/// synthesizes for this format (X12/HL7 nested levels). The field check
/// then depends on the section kind:
///
/// - a **declared file-level section** is closed to its declared fields
///   (the reader serves only those);
/// - a **synthesized positional level** serves every `<prefix>NN`,
///   `1 <= N <= bound`;
/// - a **synthesized level given a typed nested schema** (X12
///   `group_section` / `set_section`) is closed to its declared fields.
///
/// Returns the `E348` diagnostic content for a section outside the known
/// set, or a field that section's reader will not serve.
fn segment_positional_doc_path_problem(
    source: &crate::config::SourceConfig,
    schema: &SegmentPositionalSchema,
    doc_path: &cxl::analyzer::doc_paths::DocPath,
) -> Option<DocPathProblem> {
    let section_name = &*doc_path.section;
    let field_name = &*doc_path.field;
    let source_name = &source.name;
    let format = source.format.format_name();

    let declared_section = source
        .envelope
        .as_ref()
        .and_then(|env| env.sections.get(section_name));
    let synthesized = schema
        .synthesized_sections
        .iter()
        .find(|s| s.name == section_name);

    // Unknown section — neither a declared file-level section nor one the
    // reader synthesizes.
    if declared_section.is_none() && synthesized.is_none() {
        let mut known: Vec<String> = source
            .envelope
            .as_ref()
            .map(|env| env.sections.keys().cloned().collect())
            .unwrap_or_default();
        known.extend(schema.synthesized_sections.iter().map(|s| s.name.clone()));
        let known_list = if known.is_empty() {
            String::from("(none)")
        } else {
            known.join(", ")
        };
        return Some(DocPathProblem {
            code: "E348",
            message: format!(
                "`$doc.{section_name}.{field_name}` references envelope section \
                 '{section_name}', but the {format} source '{source_name}' synthesizes no \
                 such section"
            ),
            help: format!(
                "name a section this {format} source exposes ({known_list}), declare \
                 '{section_name}' under the source's `envelope.sections`, or correct the \
                 spelling. Section names are case-sensitive"
            ),
        });
    }

    // A declared file-level section (`ISA`/`UNB`/`FHS`) is closed: the
    // reader serves only its declared fields.
    if let Some(section) = declared_section {
        if section.fields.contains_key(field_name) {
            return None;
        }
        return Some(undeclared_field_in_closed_section(
            source_name,
            format,
            section_name,
            field_name,
            "`envelope.sections`",
        ));
    }

    // A synthesized nested level: closed to its declared schema if it has
    // one, else keyed by positional elements up to the bound. `synthesized`
    // is `Some` here — the both-`None` case returned above, and the
    // `declared_section` case returned just above — but match it rather than
    // unwrap so the exhaustive path is explicit.
    match synthesized.map(|s| &s.fields) {
        Some(SynthesizedFields::Declared(fields)) => {
            if fields.iter().any(|f| f == field_name) {
                return None;
            }
            Some(undeclared_field_in_closed_section(
                source_name,
                format,
                section_name,
                field_name,
                "the level's `fields` schema",
            ))
        }
        Some(SynthesizedFields::Positional) => positional_field_problem(
            source_name,
            format,
            section_name,
            field_name,
            schema.positional_prefix,
            schema.max_positional,
        ),
        None => None,
    }
}

/// `E348` for a field a closed section (a declared file-level section, or a
/// synthesized level given a typed schema) does not declare.
fn undeclared_field_in_closed_section(
    source_name: &str,
    format: &str,
    section_name: &str,
    field_name: &str,
    declared_under: &str,
) -> DocPathProblem {
    DocPathProblem {
        code: "E348",
        message: format!(
            "`$doc.{section_name}.{field_name}` references field '{field_name}' in section \
             '{section_name}', but the {format} source '{source_name}' declares no such field \
             in that section"
        ),
        help: format!(
            "add '{field_name}' to that section's field schema ({declared_under}), or correct \
             the name to a declared field. Field names are case-sensitive"
        ),
    }
}

/// Validate a field against a positionally-keyed synthesized level: it must
/// be `<prefix>NN` with `1 <= N <= max_positional`. Returns `None` when in
/// range, else the `E348` content for an out-of-range or non-positional
/// field.
fn positional_field_problem(
    source_name: &str,
    format: &str,
    section_name: &str,
    field_name: &str,
    prefix: char,
    max_positional: usize,
) -> Option<DocPathProblem> {
    match positional_element_index(field_name, prefix) {
        Some(n) if n >= 1 && n <= max_positional => None,
        Some(_) => Some(DocPathProblem {
            code: "E348",
            message: format!(
                "`$doc.{section_name}.{field_name}` references positional element \
                 '{field_name}', but the {format} source '{source_name}' exposes only \
                 '{prefix}01'..'{prefix}{max:02}' on section '{section_name}' (its \
                 configured {bound_opt} is {max})",
                max = max_positional,
                bound_opt = positional_bound_option(prefix),
            ),
            help: format!(
                "reference a positional element within range, or raise the source's \
                 `{bound_opt}` if the wire segment genuinely carries that many",
                bound_opt = positional_bound_option(prefix),
            ),
        }),
        None => Some(DocPathProblem {
            code: "E348",
            message: format!(
                "`$doc.{section_name}.{field_name}` references field '{field_name}' on section \
                 '{section_name}', but the {format} source '{source_name}' exposes that \
                 section's wire data only as positional elements \
                 ('{prefix}01'..'{prefix}{max:02}')",
                max = max_positional,
            ),
            help: format!(
                "reference a positional element ('{prefix}NN'), or declare '{field_name}' under \
                 a typed nested-section schema for this level. Field names are case-sensitive"
            ),
        }),
    }
}

/// Parse a positional element/field name into its 1-based index, or `None`
/// when it does not match the EXACT column name the reader emits.
///
/// The readers key positional columns as `format!("{prefix}{:02}", i + 1)`
/// — zero-padded to a minimum of two digits (`e01`, `f09`, `e32`, `e100`).
/// A `$doc` access resolves by the literal field identifier, so only that
/// canonical spelling resolves at run time: `$doc.s.e7` looks up a `e7`
/// column that does not exist (the reader emits `e07`) and would silently
/// yield null. We therefore accept a name only when it round-trips to the
/// canonical key — rejecting `e7` (→ `e07`) and `e007` (→ `e07`) so the
/// near-miss is caught rather than passing compile and resolving null.
fn positional_element_index(field: &str, prefix: char) -> Option<usize> {
    let rest = field.strip_prefix(prefix)?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: usize = rest.parse().ok()?;
    (field == format!("{prefix}{n:02}")).then_some(n)
}

/// The source-option name that bounds the positional element/field count
/// for a given prefix, used in `E348` help text.
fn positional_bound_option(prefix: char) -> &'static str {
    match prefix {
        'f' => "max_fields",
        _ => "max_elements",
    }
}

/// Build the `E349` diagnostic for a `$doc` access attributed to a REST
/// source. A `rest` transport pulls records page by page over HTTP and
/// buffers no single document, so it carries no envelope context — the
/// access would resolve to `Value::Null` at run time with no signal.
fn rest_doc_access_diagnostic(
    source_name: &str,
    doc_path: &cxl::analyzer::doc_paths::DocPath,
    primary: clinker_core_types::span::Span,
) -> clinker_core_types::Diagnostic {
    use clinker_core_types::{Diagnostic, LabeledSpan};
    let section = &*doc_path.section;
    let field = &*doc_path.field;
    Diagnostic::error(
        "E349",
        format!(
            "`$doc.{section}.{field}` reads document-envelope context from the rest source \
             '{source_name}', but a rest source pulls records page by page over HTTP and \
             buffers no document — the access can never resolve and would silently yield null"
        ),
        LabeledSpan::primary(primary, String::new()),
    )
    .with_help(
        "remove the `$doc` access; a rest source has no document envelope. Pull \
         document-level metadata into record fields through the API's response shape \
         (`record_path`, `array_paths`) instead"
            .to_string(),
    )
}

/// Map every CXL-bearing node name — top-level and composition-body — to
/// the set of upstream `Source` names that feed it. The `$doc` path
/// attributor unions these source sets across a path's referencing nodes
/// to stamp each source with only the paths it actually delivers.
///
/// This is a parallel walk to `bind_schema`'s `upstream_sources` map
/// (which feeds the E156 time-window watermark guard). The two
/// intentionally diverge: that walk unions every input unconditionally,
/// whereas this one narrows a `Combine` to its driving input and resolves
/// a `Composition` through its full `inputs:` port set. The semantics here
/// model where a record's document context actually flows; the watermark
/// walk models which sources can deliver records at all. Keep them in sync
/// on the shared shape (Source-seeds-self, declaration-order topo walk,
/// composition-body recursion) when either changes.
///
/// The top-level walk runs in declaration order, which Stage-3 validation
/// proved topologically sound, so each node's source set is the union of
/// its direct inputs' sets (Sources seed themselves).
///
/// A `Combine` is the one node whose source set is NOT the union of its
/// inputs: a joined record carries the DRIVING input's document context
/// forward (the same per-driver-document rule a downstream Aggregate
/// follows), and the predicate's `$doc` accesses likewise bind to the
/// driving record's envelope. So a Combine's source set is the driving
/// input's source set alone — using the union would tell the probe-side
/// source to extract envelope sections only the driver's document
/// declares. Both the Combine's `cxl:` body and its `where:` predicate
/// program are keyed by the combine node's own name, so they share this
/// driving-input source set with no extra handling.
///
/// A `Composition` forwards the union of EVERY bound `inputs:` port's
/// source set, not just its primary port. `direct_input_names` reports
/// only `header.input` (the single primary port) for a composition, so a
/// multi-port call (`inputs: { primary: a, reference: b }`) would drop the
/// non-primary ports' sources — a downstream `$doc` read off the
/// composition output must still see them. Composition bodies are isolated
/// mini-DAGs whose nodes draw their records from the same call-site port
/// bindings; every body node (recursively, through nested compositions)
/// inherits the same port-union set. A body `$doc` access carries no port
/// qualifier, so it could read any incoming port's source envelope — the
/// union is the correct attribution.
fn build_node_source_sets(
    nodes: &[Spanned<PipelineNode>],
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
) -> HashMap<String, std::collections::BTreeSet<String>> {
    use crate::plan::composition_body::CompositionBodyId;

    let mut node_sources: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();

    // Attribute every node in a composition body (recursively through
    // nested bodies) to the call-site's source set.
    fn attribute_body(
        body_id: CompositionBodyId,
        call_site_sources: &std::collections::BTreeSet<String>,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        node_sources: &mut HashMap<String, std::collections::BTreeSet<String>>,
    ) {
        let Some(body) = artifacts.body_of(body_id) else {
            return;
        };
        for body_node_name in body.name_to_idx.keys() {
            node_sources
                .entry(body_node_name.clone())
                .or_default()
                .extend(call_site_sources.iter().cloned());
        }
        for nested in &body.nested_body_ids {
            attribute_body(*nested, call_site_sources, artifacts, node_sources);
        }
    }

    // The driving input's upstream node name for a combine, if the
    // planner picked one (combines that fail driver selection are absent).
    let combine_driver_upstream = |combine_name: &str| -> Option<String> {
        let qualifier = artifacts.combine_driving.get(combine_name)?;
        let inputs = artifacts.combine_inputs.get(combine_name)?;
        inputs
            .get(qualifier)
            .map(|ci| ci.upstream_name.as_ref().to_string())
    };

    // Union of the source sets feeding the named producers, resolved
    // against the sets recorded for already-walked nodes.
    let union_of = |producers: &[&str],
                    sources: &HashMap<String, std::collections::BTreeSet<String>>|
     -> std::collections::BTreeSet<String> {
        producers
            .iter()
            .filter_map(|inp| sources.get(*inp))
            .flat_map(|s| s.iter().cloned())
            .collect()
    };

    // Union of the source sets feeding a composition's `inputs:` port
    // bindings — the authoritative call-site connectivity. `header.input`
    // (what `direct_input_names` reports for a Composition) names only the
    // single primary port, so it must NOT be used for a multi-port
    // composition: a downstream `$doc` read off the composition output, or
    // a body `$doc` access, can resolve against any bound port's source.
    let composition_call_site_sources =
        |inputs: &IndexMap<String, String>,
         sources: &HashMap<String, std::collections::BTreeSet<String>>|
         -> std::collections::BTreeSet<String> {
            let producers: Vec<&str> = inputs
                .values()
                .map(|upstream| upstream.split('.').next().unwrap_or(upstream))
                .collect();
            union_of(&producers, sources)
        };

    for spanned in nodes {
        let name = spanned.value.name().to_string();
        let sources: std::collections::BTreeSet<String> = match &spanned.value {
            PipelineNode::Source { .. } => std::iter::once(name.clone()).collect(),
            // A joined record carries only the driving input's document
            // context, so a Combine takes the driving input's source set —
            // not the union of every input. Falls back to the input union
            // when the planner could not pick a driver (the combine is then
            // omitted from the DAG, so the set is unused either way).
            PipelineNode::Combine { .. } => combine_driver_upstream(&name)
                .and_then(|driver| node_sources.get(&driver).cloned())
                .unwrap_or_else(|| union_of(&spanned.value.direct_input_names(), &node_sources)),
            // A composition forwards the union of EVERY bound input port's
            // source set, not just its primary port — a downstream node
            // reading `$doc` off the composition output can see any port's
            // document context. This matches the body-node attribution
            // below, which unions the same port sources.
            PipelineNode::Composition { inputs, .. } => {
                composition_call_site_sources(inputs, &node_sources)
            }
            // Every other non-Source variant inherits the union of its
            // direct inputs' source sets.
            other => union_of(&other.direct_input_names(), &node_sources),
        };

        // A composition's body nodes feed from the call-site `inputs:`
        // port bindings — union every bound port's source set so a body
        // `$doc` access is attributed to whichever input could supply its
        // envelope. That union is exactly the source set the Composition
        // arm above computed into `sources`, so reuse it.
        if let PipelineNode::Composition { .. } = &spanned.value
            && let Some(&body_id) = artifacts.composition_body_assignments.get(&name)
        {
            attribute_body(body_id, &sources, artifacts, &mut node_sources);
        }

        node_sources.insert(name, sources);
    }

    node_sources
}

/// Cross-cutting inputs threaded into [`lower_node_to_plan_node`] for
/// variants that need derived fields (Transform, Aggregate).
///
/// Top-level callers in `compile_with_diagnostics` populate every field
/// from the already-computed analyzer report / window configs;
/// body-node callers in `bind_composition` use
/// [`LoweringCtx::default`] (all fields `None`/empty), which falls back
/// to minimal placeholder lowering suitable for drill-in
/// inspection. Body nodes are not executed directly — the top-level
/// DAG produced by `compile_with_diagnostics` is the single source of
/// truth for runtime planning.
///
/// `window_index` on Transform nodes is intentionally NOT derived here —
/// it requires the post-graph upstream `NodeIndex` for node-rooted
/// windows. The Stage-5 lowering pass mutates the field in place
/// after edges are wired and indices are deduplicated.
#[derive(Default)]
pub(crate) struct LoweringCtx<'a> {
    pub analysis: Option<&'a cxl::analyzer::TransformAnalysis>,
    pub window_config: Option<&'a AnalyticWindowSpec>,
    pub primary_source: &'a str,
}

/// Lower a single `PipelineNode` into its `PlanNode` counterpart.
///
/// Returns `None` for compositions whose binding failed (no body
/// assignment in `artifacts`) or Transforms whose typechecking failed
/// (no typed program in `artifacts.typed`). Called from
/// `PipelineConfig::compile_with_diagnostics` Stage 5 with a populated
/// `LoweringCtx` for top-level nodes, and from `bind_composition` with
/// `LoweringCtx::default()` for body nodes.
pub(crate) fn lower_node_to_plan_node(
    node: &PipelineNode,
    name: &str,
    span: clinker_core_types::span::Span,
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
    ctx: &LoweringCtx<'_>,
    diags: &mut Vec<clinker_core_types::Diagnostic>,
) -> Option<crate::plan::execution::PlanNode> {
    use crate::plan::composition_body::CompositionBodyId;
    use crate::plan::execution::{
        NodeExecutionReqs, ParallelismClass, PartitionLookupKind, PlanNode, PlanOutputPayload,
        PlanSourcePayload, PlanTransformPayload, derive_parallelism_class, extract_has_distinct,
        extract_write_set,
    };
    use crate::plan::types::AggregateStrategy;
    use clinker_core_types::{Diagnostic, LabeledSpan};
    use clinker_record::{FieldMetadata, SchemaBuilder};
    use std::sync::Arc;

    // Build an `Arc<Schema>` from the bound output row for this node.
    // Returns an empty sentinel if bind_schema didn't record one — the
    // caller skips lowering in every such case, so the sentinel never
    // reaches the executor.
    //
    // Columns whose name has the `$ck.` prefix carry engine-stamp
    // metadata. Two prefix shapes are recognized in priority order:
    //
    // - `$ck.aggregate.<aggregate_name>` — synthetic column emitted by
    //   a relaxed aggregate, stamped `AggregateGroupIndex`.
    // - `$ck.<field>` — source-CK shadow column, stamped
    //   `SourceCorrelation`.
    //
    // The aggregate prefix is checked first because `$ck.aggregate.x`
    // also matches the generic `$ck.` prefix; misordering would mis-
    // classify aggregate columns as source-CK shadows. The marker
    // travels with the column through the DAG: when a Transform /
    // Aggregate / Combine output row inherits a `$ck.*` column, its
    // own `Arc<Schema>` recovers the same metadata here. The reserved
    // `$` prefix guarantees no user-declared column collides.
    let schema_from_bound = |node_name: &str| -> Arc<clinker_record::Schema> {
        match artifacts.typed.get(node_name) {
            Some(tp) => {
                let mut builder = SchemaBuilder::with_capacity(tp.output_row.field_count());
                for (qf, _) in tp.output_row.fields() {
                    let col = qf.name.as_ref();
                    builder = if let Some(aggregate_name) = col.strip_prefix("$ck.aggregate.") {
                        builder.with_field_meta(
                            col,
                            FieldMetadata::aggregate_group_index(aggregate_name),
                        )
                    } else if let Some(field) = col.strip_prefix("$ck.") {
                        builder.with_field_meta(col, FieldMetadata::source_correlation(field))
                    } else if col == crate::config::pipeline_node::WIDENED_SIDECAR_COLUMN {
                        builder.with_field_meta(col, FieldMetadata::widened_sidecar())
                    } else if col == crate::config::pipeline_node::SOURCE_FILE_COLUMN {
                        builder.with_field_meta(col, FieldMetadata::source_file())
                    } else if col == crate::config::pipeline_node::SOURCE_NAME_COLUMN {
                        builder.with_field_meta(col, FieldMetadata::source_name())
                    } else if col == crate::config::pipeline_node::SOURCE_EVENT_TIME_COLUMN {
                        builder.with_field_meta(col, FieldMetadata::source_event_time())
                    } else if col == crate::config::pipeline_node::RESHAPE_SYNTHETIC_COLUMN
                        || col == crate::config::pipeline_node::RESHAPE_SYNTHESIZED_BY_COLUMN
                        || col == crate::config::pipeline_node::RESHAPE_MUTATED_BY_COLUMN
                    {
                        builder.with_field_meta(col, FieldMetadata::reshape_audit())
                    } else {
                        builder.with_field(col)
                    };
                }
                builder.build()
            }
            None => SchemaBuilder::new().build(),
        }
    };

    match node {
        PipelineNode::Source { config, .. } => {
            // The `$doc` path set the reader extracts is stamped on the
            // runtime `PipelineConfig` source body (see `compile_with_diagnostics`),
            // which is what the executor's ingest path reads; the DAG payload
            // does not carry it.
            let source = config.source.clone();
            Some(PlanNode::Source {
                name: name.to_string(),
                span,
                resolved: Some(Box::new(PlanSourcePayload {
                    source,
                    validated_path: None,
                })),
                output_schema: schema_from_bound(name),
            })
        }
        PipelineNode::Transform { config, .. } => {
            // Missing typed program means bind_schema hit a CXL error
            // (E108, E200, etc.) on this node — skip lowering.
            let typed = match artifacts.typed.get(name) {
                Some(t) => t.clone(),
                None => return None,
            };
            // When the caller supplied a populated `LoweringCtx` (top-level
            // compile path), derive every enrichment field from the
            // analyzer report + window config + dedup'd indices. Body-node
            // callers (`bind_composition`) pass the default ctx, which
            // collapses all of the below to the unified-diagnostic placeholder
            // shape — this is fine for drill-in inspection; body
            // nodes never execute through this DAG.
            let (parallelism_class, execution_reqs, window_index, partition_lookup) =
                if let Some(analysis) = ctx.analysis {
                    let pc = derive_parallelism_class(
                        analysis,
                        &ctx.window_config.cloned(),
                        ctx.primary_source,
                    );
                    let reqs = if ctx.window_config.is_some() {
                        NodeExecutionReqs::RequiresArena
                    } else {
                        NodeExecutionReqs::Streaming
                    };
                    // `window_index` is computed after the graph topology
                    // is known — `PlanIndexRoot::Node` for post-aggregate
                    // / post-combine windows requires the upstream
                    // operator's `NodeIndex`, which only exists once the
                    // graph is built. The Stage-5 lowering pass in
                    // `compile_with_diagnostics` mutates this field in
                    // place after edges are wired and indices are
                    // deduplicated. Lowering callers from
                    // `bind_composition` always pass `LoweringCtx::default()`
                    // (so `ctx.window_config` is `None`); body-internal
                    // windows are not yet rooted through this path.
                    let wi = None;
                    let pl = ctx.window_config.map(|wc| {
                        let source = wc
                            .source
                            .clone()
                            .unwrap_or_else(|| ctx.primary_source.to_string());
                        if source == ctx.primary_source && wc.on.is_none() {
                            PartitionLookupKind::SameSource
                        } else {
                            PartitionLookupKind::CrossSource {
                                on_expr: wc.on.clone(),
                            }
                        }
                    });
                    (pc, reqs, wi, pl)
                } else {
                    (
                        ParallelismClass::Stateless,
                        NodeExecutionReqs::Streaming,
                        None,
                        None,
                    )
                };
            let write_set = extract_write_set(&typed);
            let has_distinct = extract_has_distinct(&typed);
            Some(PlanNode::Transform {
                name: name.to_string(),
                span,
                resolved: Some(Box::new(PlanTransformPayload {
                    analytic_window: config.analytic_window.clone(),
                    log: config.log.clone().unwrap_or_default(),
                    validations: config.validations.clone().unwrap_or_default(),
                    dlq_node: None,
                    typed,
                    declares: config.declares.clone(),
                    phase: config.phase,
                })),
                parallelism_class,
                tier: 0,
                execution_reqs,
                window_index,
                partition_lookup,
                write_set,
                has_distinct,
                output_schema: schema_from_bound(name),
            })
        }
        PipelineNode::Output { config, .. } => Some(PlanNode::Output {
            name: name.to_string(),
            span,
            resolved: Some(Box::new(PlanOutputPayload {
                output: config.output.clone(),
                validated_path: None,
                fan_out_per_source_file: false,
            })),
        }),
        PipelineNode::Route { config, .. } => Some(PlanNode::Route {
            name: name.to_string(),
            span,
            mode: config.mode,
            branches: config.conditions.keys().cloned().collect(),
            default: config.default.clone(),
        }),
        PipelineNode::Merge { .. } => Some(PlanNode::Merge {
            name: name.to_string(),
            span,
            output_schema: schema_from_bound(name),
        }),
        PipelineNode::Composition { .. } => {
            // Look up the body assigned by bind_composition. If binding
            // failed (E102–E109), there's no entry — silently omit the
            // node (the binding errors already surfaced in Stage 4.5).
            let body_id = artifacts
                .composition_body_assignments
                .get(name)
                .copied()
                .unwrap_or(CompositionBodyId::SENTINEL);
            if body_id == CompositionBodyId::SENTINEL {
                return None;
            }
            // Composition's output schema is the first declared
            // output port's row, not the call-site node name (which
            // has no entry in `artifacts.typed` — compositions don't
            // carry their own CXL body). The downstream
            // `expected_input_schema_in` walk uses this Arc to
            // schema-check records flowing out of the composition.
            let comp_output_schema = artifacts
                .composition_bodies
                .get(&body_id)
                .and_then(|body| body.output_port_rows.values().next())
                .map(|row| {
                    row.fields()
                        .map(|(qf, _)| qf.name.as_ref())
                        .collect::<SchemaBuilder>()
                        .build()
                })
                .unwrap_or_else(|| SchemaBuilder::new().build());
            Some(PlanNode::Composition {
                name: name.to_string(),
                span,
                body: body_id,
                output_schema: comp_output_schema,
            })
        }
        PipelineNode::Aggregate {
            config: agg_body, ..
        } => {
            // Skip if bind_schema produced no typed program (CXL error).
            let typed = match artifacts.typed.get(name) {
                Some(t) => t.clone(),
                None => return None,
            };
            let agg_cfg = crate::config::AggregateConfig {
                group_by: agg_body.group_by.clone(),
                cxl: agg_body.cxl.source.as_str().to_string(),
                strategy: agg_body.strategy,
                time_window: agg_body.time_window.clone(),
                allowed_lateness: agg_body.allowed_lateness,
            };
            // `typed.field_types` is keyed and ordered by `bind_schema`'s
            // upstream `Row`, so iterating its keys yields the live
            // column layout the aggregator will project against — no
            // separate runtime-schema thread is needed.
            let input_schema: Vec<String> = typed
                .field_types
                .keys()
                .map(|qf| qf.name.to_string())
                .collect();
            let mut compiled_agg =
                match cxl::plan::extract_aggregates(&typed, &agg_cfg.group_by, &input_schema) {
                    Ok(c) => c,
                    Err(errs) => {
                        for e in errs {
                            diags.push(Diagnostic::error(
                                "E210",
                                format!("aggregate extraction failed for {name:?}: {}", e.message),
                                LabeledSpan::primary(span, String::new()),
                            ));
                        }
                        return None;
                    }
                };
            // Default to strict-collateral. The actual retraction-mode
            // flags are derived after `compute_node_properties` runs on
            // the full DAG: a downstream post-pass walks every
            // aggregate, compares its `group_by` against the parent's
            // `ck_set` lattice, and rewrites the flags via
            // `set_retraction_flags(true)` when the aggregate omits
            // any visible CK field. The strict default below is the
            // identity for `set_retraction_flags(false)` so a body
            // mini-DAG that the post-pass also walks stays consistent.
            compiled_agg.set_retraction_flags(false);
            // `schema_from_bound` reads the typed `output_row` produced
            // by `propagate_aggregate` (group-by columns first, then
            // emits) and stamps engine-stamp metadata on the
            // `$ck.<field>` shadow columns that propagate through. The
            // emit-only path used previously omitted any group-by
            // column the user could not cover with an explicit emit
            // (the CXL parser rejects `emit $ck.* = ...`), so engine-
            // stamped group-by columns silently dropped from the
            // aggregate's `output_schema` and the runtime
            // `finalize_group_inner` had no slot to populate from the
            // group key.
            let output_schema = schema_from_bound(name);
            Some(PlanNode::Aggregation {
                name: name.to_string(),
                span,
                config: agg_cfg,
                compiled: Arc::new(compiled_agg),
                strategy: AggregateStrategy::Hash,
                output_schema,
                fallback_reason: None,
                skipped_streaming_available: false,
                qualified_sort_order: None,
            })
        }
        // Combine lowers to PlanNode::Combine. Inline fields here are
        // the ones the `ExecutionPlanDag` serializer (which does not see
        // `CompileArtifacts`) must emit for `--explain`:
        //   - `strategy` — planner default is `HashBuildProbe`; the
        //     `select_combine_strategies` post-pass may rewrite it.
        //   - `driving_input` / `build_inputs` — empty until that same
        //     post-pass selects the driver.
        //   - `predicate_summary` — filled here from
        //     `CompileArtifacts.combine_predicates[name]` (populated by
        //     `bind_schema` before lowering runs). Zero-valued when the
        //     combine failed predicate decomposition; the E3xx diagnostic
        //     is already emitted elsewhere in that case.
        //   - `decomposed_from` — non-`None` only on synthetic binary
        //     combines produced by N-ary decomposition; user-authored
        //     nodes lower with `None`.
        // The heavy decomposed programs and per-input schema rows stay
        // in `CompileArtifacts` — no duplication.
        PipelineNode::Combine { config, .. } => {
            use crate::plan::combine::{CombinePredicateSummary, CombineStrategy};
            let predicate_summary = artifacts
                .combine_predicates
                .get(name)
                .map(CombinePredicateSummary::from_decomposed)
                .unwrap_or_default();
            let resolved_column_map = artifacts
                .combine_resolved_columns
                .get(name)
                .cloned()
                .unwrap_or_else(|| Arc::new(std::collections::HashMap::new()));
            // Carry the typed `cxl:` body program on the node, mirroring the
            // Transform arm's `artifacts.typed.get(name)` read into
            // `PlanTransformPayload.typed`. Body and top-level combines both
            // flow through this arm; the bare-name lookup into the
            // `artifacts.typed` body-program table is collision-exposed exactly
            // like the Transform arm's — cross-scope node-name reuse is
            // last-writer-wins in that bare-keyed table. `None` is legitimate
            // here — a body-less combine (e.g. `match: collect`) carries no
            // program.
            let typed = artifacts.typed.get(name).cloned();
            Some(crate::plan::execution::PlanNode::Combine {
                name: name.to_string(),
                span,
                strategy: CombineStrategy::HashBuildProbe,
                driving_input: String::new(),
                build_inputs: Vec::new(),
                driving_upstream: None,
                predicate_summary,
                match_mode: config.match_mode,
                on_miss: config.on_miss,
                propagate_ck: config.propagate_ck.clone(),
                decomposed_from: None,
                output_schema: schema_from_bound(name),
                resolved_column_map,
                typed,
            })
        }
        // Reshape lowers to PlanNode::Reshape carrying the parsed config
        // and the audit-widened output schema. The executor compiles each
        // rule's `when` / `set` / `overrides` CXL against the live input
        // schema at dispatch time (the Route condition-compile seam), so no
        // per-rule typed program is threaded through CompileArtifacts.
        PipelineNode::Reshape { config, .. } => {
            // A missing typed entry means bind_schema rejected the node
            // (E200 on a rule predicate / assignment) — skip lowering.
            artifacts.typed.get(name)?;
            Some(crate::plan::execution::PlanNode::Reshape {
                name: name.to_string(),
                span,
                config: config.clone(),
                output_schema: schema_from_bound(name),
            })
        }
        // Cull lowers to PlanNode::Cull carrying the parsed config and the
        // (unchanged) upstream output schema. The executor recompiles each
        // rule's `drop_group_when` predicate against the live input schema
        // at dispatch time, so no per-rule typed program is threaded
        // through CompileArtifacts.
        PipelineNode::Cull { config, .. } => {
            // A missing typed entry means bind_schema rejected the node
            // (E200 on a rule predicate / removed_to) — skip lowering.
            artifacts.typed.get(name)?;
            Some(crate::plan::execution::PlanNode::Cull {
                name: name.to_string(),
                span,
                config: config.clone(),
                output_schema: schema_from_bound(name),
            })
        }
        // Envelope lowers carrying the framing strategy, the body's (unchanged)
        // output schema, and — when a `header:` port is wired — the producer
        // name of that header stream. The resolved `header_upstream` NodeIndex
        // stays `None` here because the DAG edges (and any spliced correlation
        // Sort) do not exist until after lowering; the
        // `resolve_envelope_header_upstreams` post-pass fills it. A missing
        // typed entry means bind_schema could not resolve the body input — skip.
        PipelineNode::Envelope { header, config } => {
            artifacts.typed.get(name)?;
            let header_input = header
                .header
                .as_ref()
                .map(|h| match &h.value {
                    crate::config::node_header::NodeInput::Single(s) => s.clone(),
                    crate::config::node_header::NodeInput::Port { node, port } => {
                        format!("{node}.{port}")
                    }
                })
                .unwrap_or_default();
            // The compiled header/footer synthesis (when the node declared a
            // `config.header:` / `config.footer:` map) was built during binding
            // against the body input row and stashed in `CompileArtifacts`;
            // attach it here. Absent → the node synthesizes nothing.
            let synthesis = artifacts.envelope_synthesis.get(name).map(Arc::clone);
            Some(crate::plan::execution::PlanNode::Envelope {
                name: name.to_string(),
                span,
                strategy: config.strategy,
                header_input,
                header_upstream: None,
                output_schema: schema_from_bound(name),
                synthesis,
            })
        }
    }
}

fn default_max_group_buffer() -> Option<u64> {
    Some(100_000)
}

/// Error handling strategy and DLQ configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorHandlingConfig {
    #[serde(default = "default_strategy")]
    pub strategy: ErrorStrategy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dlq: Option<DlqConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_error_threshold: Option<f64>,
    /// Maximum buffered records per correlation group. Once a group reaches
    /// this cap, the group is DLQ'd with a `group_size_exceeded` root-cause
    /// entry plus collateral entries for every other buffered record of the
    /// group. Default: 100_000.
    #[serde(
        default = "default_max_group_buffer",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_group_buffer: Option<u64>,
    /// Pipeline-level default for collateral fan-out at correlation commit.
    /// Defaults to `Any` when any source has a correlation key; pipelines
    /// without any correlation key never observe this field. Per-Combine
    /// / per-Output overrides win against this default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_fanout_policy: Option<CorrelationFanoutPolicy>,
}

impl Default for ErrorHandlingConfig {
    fn default() -> Self {
        Self {
            strategy: default_strategy(),
            dlq: None,
            type_error_threshold: None,
            max_group_buffer: None,
            correlation_fanout_policy: None,
        }
    }
}

/// Error handling strategy variants.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorStrategy {
    FailFast,
    Continue,
    BestEffort,
}

fn default_strategy() -> ErrorStrategy {
    ErrorStrategy::FailFast
}

/// Dead-letter queue configuration.
///
/// `max_rate` is a cumulative fraction in `[0.0, 1.0]`. When set, the
/// pipeline halts once `dlq_count / total_count >= max_rate` AND
/// `total_count >= min_records`. `min_records` defaults to 100 to avoid
/// 1/1 = 100% false positives on the first failure.
///
/// `per_source` overrides keyed by Source-node name win against the
/// pipeline-wide `max_rate` / `min_records`. A per-source `path`, if
/// set, reroutes that source's DLQ entries to a separate sidecar file —
/// those entries do not appear in the pipeline-wide file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DlqConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_reason: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_source_row: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_records: Option<u64>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub per_source: std::collections::BTreeMap<String, DlqPerSourceConfig>,
}

/// Per-source overrides for [`DlqConfig`].
///
/// Operators consulting `path` should grep both the pipeline-wide DLQ
/// file and every per-source sidecar — entries with a `per_source` path
/// override do not duplicate into the pipeline-wide file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DlqPerSourceConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_records: Option<u64>,
}

/// Default `min_records` floor used when neither pipeline-wide nor per-source
/// override is supplied.
pub const DEFAULT_DLQ_MIN_RECORDS: u64 = 100;

/// Reserved `$pipeline.*` member names that cannot be used as
/// `declares:` entry names with `scope: pipeline`. Mirrors
/// `crates/cxl/src/resolve/pass.rs::PIPELINE_MEMBERS`.
pub const RESERVED_PIPELINE_NAMES: &[&str] = &[
    "start_time",
    "name",
    "execution_id",
    "batch_id",
    "total_count",
    "ok_count",
    "dlq_count",
    "filtered_count",
    "distinct_count",
];

/// Reserved `$source.*` member names. Mirrors
/// `crates/cxl/src/resolve/pass.rs::SOURCE_MEMBERS`.
pub const RESERVED_SOURCE_NAMES: &[&str] = &[
    "file",
    "row",
    "path",
    "count",
    "batch",
    "ingestion_timestamp",
];

/// Reserved `$record.*` member names. Empty today — issue #44 will add
/// `index` (record's positional index in its source) and `source` (a
/// back-reference to the originating source-node name) as builtin
/// members.
pub const RESERVED_RECORD_NAMES: &[&str] = &[];

/// Single source of truth for the reserved-name lookup by scope.
/// Used by `declares:` validation and channel-overlay validation.
pub fn reserved_names_for(scope: pipeline_node::VarScope) -> &'static [&'static str] {
    match scope {
        pipeline_node::VarScope::Pipeline => RESERVED_PIPELINE_NAMES,
        pipeline_node::VarScope::Source => RESERVED_SOURCE_NAMES,
        pipeline_node::VarScope::Record => RESERVED_RECORD_NAMES,
    }
}

/// Post-deserialization validation.
pub(crate) fn validate_config(config: &PipelineConfig) -> Result<(), ConfigError> {
    for input in config.source_configs() {
        // Fail-fast: inline schema + schema_overrides is a conflict.
        // Overrides only apply to externally referenced schemas.
        if let Some(SchemaSource::Inline(_)) = &input.schema
            && input.schema_overrides.is_some()
        {
            return Err(ConfigError::Validation(format!(
                "input '{}': cannot use both inline 'schema' and 'schema_overrides' — \
                     overrides only apply to externally referenced schemas",
                input.name
            )));
        }

        // Matcher-exclusivity, gated on transport. A file transport
        // resolves its file set through the discovery layer's
        // `path`/`glob`/`regex`/`paths` matchers, so exactly one must be
        // set — surfaced here at config time (E210/E211) rather than only
        // when `discover` runs at CLI time, so a misconfigured file source
        // fails on `--explain` and in-process executor callers too. The
        // discovery layer keeps its own `pick_matcher` guard for the
        // direct-`discover` path; this is the parse/validate-time mirror.
        if input.transport.is_file() {
            let matcher_count = [
                input.path.is_some(),
                input.glob.is_some(),
                input.regex.is_some(),
                input.paths.is_some(),
            ]
            .into_iter()
            .filter(|&set| set)
            .count();
            match matcher_count {
                1 => {}
                0 => {
                    return Err(ConfigError::Validation(format!(
                        "[E211] source '{}': file transport declares no matcher; set exactly \
                         one of `path`, `glob`, `regex`, `paths`",
                        input.name
                    )));
                }
                _ => {
                    let which: Vec<&str> = [
                        ("path", input.path.is_some()),
                        ("glob", input.glob.is_some()),
                        ("regex", input.regex.is_some()),
                        ("paths", input.paths.is_some()),
                    ]
                    .into_iter()
                    .filter_map(|(name, set)| set.then_some(name))
                    .collect();
                    return Err(ConfigError::Validation(format!(
                        "[E210] source '{}': file transport declares more than one matcher \
                         ({}); set exactly one of `path`, `glob`, `regex`, `paths`",
                        input.name,
                        which.join(", ")
                    )));
                }
            }
        } else {
            // The rest transport never goes through fs discovery, so a
            // file matcher on it is dead config that would mislead a
            // reader into thinking the source reads from disk. Reject it
            // so the transport's intent is unambiguous.
            let matchers: Vec<&str> = [
                ("path", input.path.is_some()),
                ("glob", input.glob.is_some()),
                ("regex", input.regex.is_some()),
                ("paths", input.paths.is_some()),
            ]
            .into_iter()
            .filter_map(|(name, set)| set.then_some(name))
            .collect();
            if !matchers.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "[E219] source '{}': {} transport declares file matcher(s) ({}); the rest \
                     transport reads from its endpoint, not the filesystem — remove the \
                     matcher key(s)",
                    input.name,
                    input.transport.transport_name(),
                    matchers.join(", ")
                )));
            }

            // REST decodes each response body through the declared on-disk
            // format, so only the byte-stream formats with multi-record
            // bodies (`json`/`xml`) are meaningful.
            if !matches!(input.format, InputFormat::Json(_) | InputFormat::Xml(_)) {
                return Err(ConfigError::Validation(format!(
                    "[E220] source '{}': rest transport decodes response bodies through the \
                     declared format, which must be `json` or `xml` (got `{}`)",
                    input.name,
                    input.format.format_name()
                )));
            }
        }

        if let InputFormat::Hl7(Some(opts)) = &input.format {
            validate_hl7_split_fields(&input.name, opts)?;
        }
    }

    // Single-envelope output formats wrap every record in one frame whose
    // trailer counts are recomputed and written only at end of stream:
    // EDIFACT's UNB..UNZ interchange, X12's ISA..IEA interchange, and HL7
    // v2's FHS..FTS batch/file structure. Byte-limit file splitting flushes
    // (and therefore finalizes) the writer mid-stream, which seals the
    // envelope after the first split and emits later records — with their
    // fresh headers — after the trailer. There is no meaningful way to
    // split one envelope across files, so reject the combination up front
    // rather than emit a structurally corrupt envelope that still reports
    // run success. One pass over the outputs keeps these per-format
    // rejections in lockstep; a fourth single-envelope format is one match
    // arm, not a fourth copied loop.
    for output in config.output_configs() {
        // Each indivisible-envelope format maps to its diagnostic code, the
        // `format:` token a user wrote, and the structural-envelope phrase
        // naming why it can't be split; splittable formats yield `None`.
        let envelope = match output.format {
            OutputFormat::Edifact(_) => Some((
                "E323",
                "edifact",
                "an EDIFACT interchange is a single UNB..UNZ envelope",
            )),
            OutputFormat::X12(_) => Some((
                "E338",
                "x12",
                "an X12 interchange is a single ISA..IEA envelope",
            )),
            OutputFormat::Hl7(_) => Some((
                "E339",
                "hl7",
                "an HL7 v2 batch/file envelope is a single FHS..FTS structure",
            )),
            OutputFormat::Swift(_) => Some((
                "E342",
                "swift",
                "a SWIFT MT message is a single brace-balanced {1:..}..{5:..} envelope",
            )),
            OutputFormat::Csv(_)
            | OutputFormat::Json(_)
            | OutputFormat::Xml(_)
            | OutputFormat::FixedWidth(_) => None,
        };
        if let Some((code, format_token, envelope_phrase)) = envelope
            && output.split.is_some()
        {
            return Err(ConfigError::Validation(format!(
                "[{code}] output '{name}': `{format_token}` output cannot be combined \
                 with `split` — {envelope_phrase} and cannot be divided across files; \
                 remove the `split` block or choose a splittable format",
                name = output.name,
            )));
        }
    }

    // [E355] A single-document output format frames ONE top-level envelope for
    // its entire record stream. If the body feeding it is SHAPE-PROVABLY
    // multi-document — a >=2-input Merge, or an explicit `paths:` list of >= 2
    // files — with no consolidating node on the path (a
    // Combine/Aggregate/Composition, or an Envelope with `strategy: concat`),
    // every input document's records collapse into that one envelope, silently
    // merging distinct interchanges/messages. This plan-time gate rejects the
    // shape-provable case at `compile()` so the failure is a `clinker explain`
    // diagnostic, not a mid-run abort. Content-dependent multiplicity (a
    // `glob:`/`regex:` source that may match one file, a structured single file
    // that may hold one interchange) is left to the runtime guard (#584), which
    // rejects precisely when a second concrete grain materializes — so a
    // single-document round-trip like `x12 -> x12` stays legal here.
    // Multi-document formats (CSV/JSON/XML/fixed-width) frame a valid document
    // sequence and are unaffected. Independent of `reconstruct_envelope` so it
    // survives the later removal of that flag.
    for output in config.output_configs() {
        if !output.format.is_single_document() {
            continue;
        }
        // Per-file fan-out (`split:`, or a `{source_file}` / `{source_path}`
        // path template) routes each document to its own output file, so every
        // document lands in its own valid single-document envelope rather than
        // being merged into one — exactly the remedy this gate's message names.
        let per_file_fanout = output.split.is_some()
            || crate::config::path_template::PathTemplate::parse(&output.path)
                .map(|t| t.has_per_record_tokens())
                .unwrap_or(false);
        if per_file_fanout {
            continue;
        }
        if trace_output_lineage(config, &output.name).body_cardinality == Cardinality::Multi {
            return Err(ConfigError::Validation(format!(
                "[E355] output '{name}': `{format}` output frames a single top-level \
                 document envelope, but its body can carry more than one document with no \
                 consolidating node on the path — every input document's records would \
                 collapse into one envelope, silently merging distinct messages/interchanges. \
                 Insert an `envelope` node with `strategy: concat` to consolidate the body \
                 into one document, route each document to its own output via a per-document \
                 `split:` / `{{source_file}}` path template, or choose a document-sequence \
                 format (csv / json / xml / fixed_width)",
                name = output.name,
                format = output.format.format_name(),
            )));
        }
    }

    // Document-level DLQ cannot coexist with per-source-file fan-out output.
    // Under `dlq_granularity: document` the engine buffers each document and
    // decides flush-vs-reject at its boundary, opening ONE writer for the
    // Output; a per-source-file fan-out (`{source_file}` / `{source_path}`
    // in the path template over a multi-file source) instead routes each
    // record to a writer keyed by its own file. The two writer models are
    // mutually exclusive — a buffered-then-flushed document cannot also be
    // streamed per-record to a file-keyed writer — so reject the
    // combination up front rather than silently dropping every clean
    // document (the fan-out writer registry is keyed differently than the
    // single writer the document buffer flushes to).
    if config.any_source_has_document_dlq() {
        for output in config.output_configs() {
            let Ok(template) = crate::config::path_template::PathTemplate::parse(&output.path)
            else {
                continue;
            };
            if template.has_per_record_tokens() {
                return Err(ConfigError::Validation(format!(
                    "[E343] output '{name}': a per-source-file output template \
                     (`{{source_file}}` / `{{source_path}}`) cannot be combined with a \
                     source declaring `dlq_granularity: document` — document-level \
                     dead-lettering buffers each document and flushes it to a single \
                     writer, which is incompatible with per-record file fan-out; use a \
                     single output path, or set `dlq_granularity: record`",
                    name = output.name,
                )));
            }
        }
    }

    // Document-level dead-lettering is in direct tension with `fail_fast`:
    // `dlq_granularity: document` says "do not abort the run on a bad
    // document — dead-letter it and keep processing", while `fail_fast` says
    // "abort on the first error". Under `fail_fast` a per-record failure
    // aborts before any document-DLQ disposition, AND a malformed-envelope
    // structural-count failure would dead-letter-and-continue, so the two
    // settings give contradictory dispositions for the same run. Reject the
    // combination up front rather than silently no-op'ing a user-set field.
    if config.any_source_has_document_dlq()
        && config.error_handling.strategy == ErrorStrategy::FailFast
    {
        let source = config
            .source_configs()
            .find(|s| s.dlq_granularity == DlqGranularity::Document)
            .map(|s| s.name.as_str())
            .unwrap_or("");
        return Err(ConfigError::Validation(format!(
            "[E344] source '{source}': `dlq_granularity: document` cannot be combined \
             with `error_handling.strategy: fail_fast` — document-level dead-lettering \
             keeps the run going past a bad document, which contradicts fail-fast's \
             abort-on-first-error. Use `strategy: continue` (or `best_effort`) with \
             `dlq_granularity: document`, or set `dlq_granularity: record` to keep \
             fail-fast"
        )));
    }

    // `reconstruct_envelope` drives a single per-document writer through the
    // punctuation-unaware Output arm, which streams each record straight
    // through one writer with per-document header/footer framing. Three
    // pipeline shapes silently misbehave when combined with it, so each is
    // rejected up front rather than producing wrong output that reports
    // success:
    //
    //  * per-file `split` / fan-out: the envelope arm consults only the
    //    single-writer registry, never the per-file fan-out registry, so a
    //    file-keyed Output finds no writer and silently drops EVERY record
    //    while the run counters still report them written — total silent data
    //    loss.
    //  * `dlq_granularity: document`: the document-DLQ Output arm takes
    //    precedence over the envelope arm, so the framing hooks never fire and
    //    envelope reconstruction silently does nothing.
    //  * `correlation_key`: correlation buffering owns the writes through its
    //    own commit terminal, which the envelope arm bypasses — dirty groups
    //    would leak into the framed output and inflate `ok_count`.
    if config.any_output_reconstructs_envelope() {
        // Source-level conflicts are pipeline-wide (any envelope Output is
        // incompatible with them), so resolve them once. The per-output loop
        // below names the offending output directly — no re-derivation.
        let has_document_dlq = config.any_source_has_document_dlq();
        let has_correlation_key = config.any_source_has_correlation_key();
        // E346 scopes its section-name check to the sources that actually FEED
        // each Output (not the pipeline-wide union), so a header/footer naming
        // a section declared only on a non-feeding source is rejected — that
        // would otherwise emit a silently-empty header/footer.
        let source_envelope_by_name: HashMap<&str, &crate::config::SourceConfig> = config
            .source_configs()
            .map(|s| (s.name.as_str(), s))
            .collect();
        for output in config.output_configs() {
            if !output.reconstruct_envelope {
                continue;
            }
            let out = &output.name;
            let per_file_fanout = output.split.is_some()
                || crate::config::path_template::PathTemplate::parse(&output.path)
                    .map(|t| t.has_per_record_tokens())
                    .unwrap_or(false);
            if per_file_fanout {
                return Err(ConfigError::Validation(format!(
                    "[E347] output '{out}': `reconstruct_envelope` cannot be combined \
                     with per-file output splitting (`split:`) or a per-source-file path \
                     template (`{{source_file}}` / `{{source_path}}`) — envelope \
                     reconstruction frames one document stream into a single writer, \
                     which is incompatible with routing each record to a file-keyed \
                     writer. Use a single output path without `split:`, or drop \
                     `reconstruct_envelope`",
                )));
            }
            if has_document_dlq {
                return Err(ConfigError::Validation(format!(
                    "[E347] output '{out}': `reconstruct_envelope` cannot be combined with a \
                     source declaring `dlq_granularity: document` — document-level \
                     dead-lettering routes the Output through its own per-document buffer, \
                     which takes precedence over envelope framing and silently leaves the \
                     header/footer unwritten. Use `dlq_granularity: record`, or drop \
                     `reconstruct_envelope`"
                )));
            }
            if has_correlation_key {
                return Err(ConfigError::Validation(format!(
                    "[E347] output '{out}': `reconstruct_envelope` cannot be combined with a \
                     source declaring `correlation_key` — correlation buffering owns the \
                     writes through its own commit stage, which envelope framing bypasses, so \
                     dirty correlation groups would leak into the framed output and inflate \
                     the success counts. Drop `correlation_key`, or drop `reconstruct_envelope`"
                )));
            }

            // Trace this Output's upstream lineage once: the sources feeding it
            // (for the E346 section-name scoping) and any document-lineage
            // stripper on the path (for the guard just below).
            let lineage = trace_output_lineage(config, out);

            // A cross-document `Combine` / global-or-per-document `Aggregate` /
            // opaque `Composition` upstream emits records with a synthetic
            // (`<merged>`) document context, which the envelope arm streams
            // UNFRAMED. For JSON that means body bytes outside any open
            // document object — malformed JSON — and for every format it means
            // silently dropped framing / a miscounted footer. This is legal,
            // though, when an `Envelope` node sits between the stripper and the
            // Output: it re-frames the merged stream into a consolidated
            // document, so the per-document framing has a grain to attach to.
            // Reject only when no such consolidation is present; the E355
            // cardinality gate above then enforces that a single-document output
            // actually gets a consolidating (`concat`) Envelope.
            if lineage.lineage_stripper.is_some()
                && let Some(stripper) = unconsolidated_stripper(config, out)
            {
                return Err(ConfigError::Validation(format!(
                    "[E347] output '{out}': `reconstruct_envelope` cannot be combined with an \
                     upstream node ('{stripper}') that strips document lineage unless an \
                     `envelope` node with `strategy: concat` consolidates the merged stream \
                     first — a Combine, Aggregate, or Composition emits records with no \
                     originating document, so the per-document envelope cannot be framed around \
                     them (the framing arm would stream them unframed, e.g. producing malformed \
                     JSON). Insert an `envelope` node with `strategy: concat` between '{stripper}' \
                     and this output to consolidate the merged stream into one document, or drop \
                     `reconstruct_envelope`"
                )));
            }

            // E346: the output-envelope config (header/footer sections + a
            // possible computed footer) must reference sections the FEEDING
            // sources actually declare, and a computed footer count is
            // unsupported on a format whose lines are positional (fixed-width).
            // Only checked for an Output that reconstructs the envelope AND
            // carries an envelope config; a flag-on Output with no envelope
            // config frames nothing and is fine.
            let Some((envelope_cfg, supports_computed_footer)) =
                generic_output_envelope(&output.format)
            else {
                continue;
            };
            // Sections declared by THIS output's feeding sources only — a
            // header/footer naming a section that exists solely on a
            // non-feeding source would emit an empty header/footer silently,
            // the exact failure E346 exists to catch.
            let feeding_sections: std::collections::BTreeSet<&str> = lineage
                .feeding_sources
                .iter()
                .filter_map(|s| source_envelope_by_name.get(s.as_str()))
                .filter_map(|s| s.envelope.as_ref())
                .flat_map(|e| e.sections.keys().map(|k| k.as_str()))
                .collect();
            for (role, section) in [
                ("header_from_doc", envelope_cfg.header_from_doc.as_deref()),
                ("footer_from_doc", envelope_cfg.footer_from_doc.as_deref()),
            ] {
                let Some(section) = section else { continue };
                if !feeding_sections.contains(section) {
                    let declared: Vec<&str> = feeding_sections.iter().copied().collect();
                    return Err(ConfigError::Validation(format!(
                        "[E346] output '{out}': envelope {role} references section \
                         '{section}' which no source feeding this output declares an \
                         `envelope:` section for. Sections declared by the feeding \
                         source(s): {declared:?}. Name a declared section, or add the \
                         section to a feeding source's `envelope:` config"
                    )));
                }
            }
            if envelope_cfg.footer_record_count_field.is_some() {
                // A computed count is injected INTO the footer section, so it
                // requires a `footer_from_doc` to attach to — a count-only
                // footer has no section to ride and would otherwise be silently
                // dropped (the writer emits a footer only when its section is
                // present on the document).
                if envelope_cfg.footer_from_doc.is_none() {
                    return Err(ConfigError::Validation(format!(
                        "[E346] output '{out}': envelope `footer_record_count_field` requires \
                         `footer_from_doc` — the computed record count is injected into the \
                         footer section, so a footer section must be named for it to attach to. \
                         Add `footer_from_doc`, or drop `footer_record_count_field`"
                    )));
                }
                if !supports_computed_footer {
                    return Err(ConfigError::Validation(format!(
                        "[E346] output '{out}': envelope `footer_record_count_field` is not \
                         supported for {} output — a fixed-width line has no field to inject a \
                         computed count into without a width declaration. Drop \
                         `footer_record_count_field`, or use a CSV / JSON / XML output",
                        output.format.format_name()
                    )));
                }
            }
        }
    }

    // Validate the flat `vars:` block (`$vars.<key>` static config)
    if let Some(ref vars) = config.pipeline.vars {
        for (name, decl) in vars {
            if let Some(default) = &decl.default {
                check_scoped_var_default("vars", name, decl.var_type, default)?;
            }
        }
    }

    // Reject a wired `trailer:` port on an Envelope node. A wired `header:` is
    // now a real port (it replaces each body grain's ambient envelope with a
    // grain-matched header record), but `trailer:` has no draining path yet, so
    // a wired value would silently do nothing. The undeclared-producer case for
    // a wired `header:` is caught earlier by the unified input-reference pass
    // (E004), exactly as for any other consumer input.
    for spanned in &config.nodes {
        if let PipelineNode::Envelope { header, .. } = &spanned.value
            && header.trailer.is_some()
        {
            return Err(ConfigError::Validation(format!(
                "envelope node '{}': explicit `trailer` input wiring is not yet supported — \
                 omit it to frame with the body's own envelope",
                header.name
            )));
        }
    }

    // Validate per-Transform `declares:` entries: reserved-name
    // collisions per-scope, and default-type matches.
    for spanned in &config.nodes {
        let PipelineNode::Transform {
            header,
            config: body,
        } = &spanned.value
        else {
            continue;
        };
        for entry in &body.declares {
            let scope_label = match entry.scope {
                pipeline_node::VarScope::Pipeline => "pipeline",
                pipeline_node::VarScope::Source => "source",
                pipeline_node::VarScope::Record => "record",
            };
            if reserved_names_for(entry.scope).contains(&entry.name.as_str()) {
                return Err(ConfigError::Validation(format!(
                    "transform '{}': declares: '{}' is a reserved ${} member name and cannot be used as a variable",
                    header.name, entry.name, scope_label,
                )));
            }
            if let Some(default) = &entry.default {
                check_scoped_var_default(
                    &format!("transform '{}' declares", header.name),
                    &entry.name,
                    entry.var_type,
                    default,
                )?;
            }
        }
    }

    validate_unique_scoped_declarations(config)?;

    // Validate log directives on Transform nodes.
    for spanned in &config.nodes {
        if let PipelineNode::Transform {
            header,
            config: body,
        } = &spanned.value
            && let Some(directives) = &body.log
        {
            for (i, d) in directives.iter().enumerate() {
                if let Some(every) = d.every {
                    if every == 0 {
                        return Err(ConfigError::Validation(format!(
                            "transform '{}': log directive #{}: every must be >= 1",
                            header.name,
                            i + 1,
                        )));
                    }
                    if d.when != LogTiming::PerRecord {
                        return Err(ConfigError::Validation(format!(
                            "transform '{}': log directive #{}: 'every' is only valid with when: per_record",
                            header.name,
                            i + 1,
                        )));
                    }
                }
            }
        }
    }

    // Reject a zero `batch_size` at both the pipeline level and any
    // per-Transform override: a zero-event batch never flushes, so it
    // would accumulate a whole stage in memory — the inverse of the
    // streaming handoff's purpose. Omitting the knob (`None`) inherits
    // the built-in default and is the common case.
    if config.pipeline.batch_size == Some(0) {
        return Err(ConfigError::Validation(
            "pipeline.batch_size must be >= 1 (omit it to use the default)".to_string(),
        ));
    }
    for spanned in &config.nodes {
        if let PipelineNode::Transform {
            header,
            config: body,
        } = &spanned.value
            && body.batch_size == Some(0)
        {
            return Err(ConfigError::Validation(format!(
                "transform '{}': batch_size must be >= 1 (omit it to inherit pipeline.batch_size)",
                header.name,
            )));
        }
    }

    Ok(())
}

/// Validate an HL7 source's `split_fields` declarations: each names a
/// reachable positional field (`fNN`, `1 <= N <= max_fields`) with at least
/// one column on every structural axis, and no two splits target the same
/// field.
///
/// # Errors
///
/// Returns [`ConfigError::Validation`] naming the offending split when a
/// `field` is not a `fNN` name, names position 0 or a position past
/// `max_fields`, declares a zero axis width, or duplicates another split's
/// target field.
fn validate_hl7_split_fields(source_name: &str, opts: &Hl7InputOptions) -> Result<(), ConfigError> {
    let Some(splits) = &opts.split_fields else {
        return Ok(());
    };
    let max_fields = opts.max_fields.unwrap_or(HL7_DEFAULT_MAX_FIELDS);
    let mut seen: Vec<usize> = Vec::with_capacity(splits.len());
    for split in splits {
        let position = split.field_position().ok_or_else(|| {
            ConfigError::Validation(format!(
                "source '{source_name}': split field {:?} is not a positional `fNN` column name \
                 (e.g. `f08`)",
                split.field
            ))
        })?;
        if position > max_fields {
            return Err(ConfigError::Validation(format!(
                "source '{source_name}': split field {:?} names position {position}, past the \
                 configured max_fields of {max_fields}; raise `max_fields` or remove the split",
                split.field
            )));
        }
        for (axis, count) in [
            ("components", split.components),
            ("subcomponents", split.subcomponents),
            ("repetitions", split.repetitions),
        ] {
            if count == 0 {
                return Err(ConfigError::Validation(format!(
                    "source '{source_name}': split field {:?} declares `{axis}: 0`; every axis \
                     width must be at least 1",
                    split.field
                )));
            }
        }
        if seen.contains(&position) {
            return Err(ConfigError::Validation(format!(
                "source '{source_name}': field {:?} is split more than once; declare each split \
                 field at most once",
                split.field
            )));
        }
        seen.push(position);
    }
    Ok(())
}

/// Auto-extend every strict `PlanNode::Aggregation.group_by` with the
/// `$ck.<field>` shadow column when the user-declared correlation-key
/// field is already listed AND the parent's lattice carries that CK
/// field. Engine-internal namespace stays out of user YAML; the
/// engine routes frozen identity through the aggregation key by
/// construction.
///
/// Walks the top-level DAG once, mutating each Aggregation in place:
/// - `config.group_by` gains the shadow column at the tail.
/// - `compiled.group_by_fields` and `compiled.group_by_indices`
///   gain the corresponding upstream-schema position.
/// - `output_schema` is rebuilt to carry the shadow column with
///   engine-stamp metadata so writers, projection, and downstream
///   consumers can identify it as engine-stamped.
///
/// Relaxed aggregates (whose `group_by` omits a parent CK field) get
/// their synthetic `$ck.aggregate.<name>` column added by
/// `propagate_aggregate` at bind-schema time and lowered onto the
/// `PlanNode::Aggregation.output_schema` via `schema_from_bound`. This
/// pass therefore only handles the strict source-CK shadow shape;
/// touching the relaxed path here would double-append the synthetic
/// column.
///
/// Body mini-DAGs go through
/// [`extend_aggregate_group_by_with_shadow_in_body`] which derives
/// each aggregate's parent CK set inline because composition bodies
/// don't carry a `node_properties` side table.
fn extend_aggregate_group_by_with_shadow(dag: &mut crate::plan::execution::ExecutionPlanDag) {
    use petgraph::graph::NodeIndex;

    // Precompute parent ck_set per aggregate so the in-place mutation
    // below does not need to peek into `node_properties` while holding
    // a mutable borrow on `graph`.
    let parent_ck_set: HashMap<NodeIndex, std::collections::BTreeSet<String>> = dag
        .graph
        .node_indices()
        .filter_map(|idx| {
            if !matches!(
                &dag.graph[idx],
                crate::plan::execution::PlanNode::Aggregation { .. }
            ) {
                return None;
            }
            let ck_set = dag
                .graph
                .neighbors_directed(idx, petgraph::Direction::Incoming)
                .next()
                .and_then(|p| dag.node_properties.get(&p))
                .map(|p| p.ck_set.clone())
                .unwrap_or_default();
            Some((idx, ck_set))
        })
        .collect();
    extend_aggregate_group_by_with_shadow_for_graph(&mut dag.graph, &parent_ck_set);
}

fn extend_aggregate_group_by_with_shadow_for_graph(
    graph: &mut petgraph::graph::DiGraph<
        crate::plan::execution::PlanNode,
        crate::plan::execution::PlanEdge,
    >,
    parent_ck_set: &HashMap<petgraph::graph::NodeIndex, std::collections::BTreeSet<String>>,
) {
    use clinker_record::{FieldMetadata, Schema, SchemaBuilder};
    use petgraph::Direction;
    use petgraph::graph::NodeIndex;
    use std::sync::Arc;

    struct ShadowAppend {
        shadow_name: String,
        source_field: String,
        upstream_pos: u32,
    }

    fn upstream_schema(
        graph: &petgraph::graph::DiGraph<
            crate::plan::execution::PlanNode,
            crate::plan::execution::PlanEdge,
        >,
        mut idx: NodeIndex,
    ) -> Option<Arc<Schema>> {
        loop {
            let upstream = graph.neighbors_directed(idx, Direction::Incoming).next()?;
            if let Some(s) = graph[upstream].stored_output_schema() {
                return Some(Arc::clone(s));
            }
            idx = upstream;
        }
    }

    // The relaxed aggregate's synthetic `$ck.aggregate.<name>` column
    // is added by `propagate_aggregate` at bind_schema time, then
    // travels into the lowered `output_schema` through
    // `lower_node_to_plan_node`'s `schema_from_bound`. This pass only
    // needs to handle the strict source-CK shadow shape — appending
    // `$ck.<field>` to `output_schema`, `config.group_by`, and the
    // compiled group-key projection so the runtime stamps each row
    // with the upstream source's CK field at the aggregator hot loop.
    let mut work: Vec<(NodeIndex, Vec<ShadowAppend>)> = Vec::new();
    for idx in graph.node_indices() {
        let group_by = match &graph[idx] {
            crate::plan::execution::PlanNode::Aggregation { config, .. } => config.group_by.clone(),
            _ => continue,
        };
        let Some(ck_fields) = parent_ck_set.get(&idx) else {
            continue;
        };
        let mut to_append: Vec<ShadowAppend> = Vec::new();
        for ck_field in ck_fields {
            let shadow = format!("$ck.{ck_field}");
            let user_present = group_by.iter().any(|f| f == ck_field);
            let shadow_present = group_by.iter().any(|f| f == &shadow);
            if !user_present || shadow_present {
                continue;
            }
            let Some(input_schema) = upstream_schema(graph, idx) else {
                continue;
            };
            let Some(upstream_idx) = input_schema.index(&shadow) else {
                continue;
            };
            to_append.push(ShadowAppend {
                shadow_name: shadow,
                source_field: ck_field.clone(),
                upstream_pos: upstream_idx as u32,
            });
        }
        if !to_append.is_empty() {
            work.push((idx, to_append));
        }
    }

    for (idx, to_append) in work {
        let crate::plan::execution::PlanNode::Aggregation {
            config,
            compiled,
            output_schema,
            ..
        } = &mut graph[idx]
        else {
            continue;
        };

        let compiled_mut = Arc::make_mut(compiled);
        for entry in &to_append {
            config.group_by.push(entry.shadow_name.clone());
            compiled_mut.group_by_fields.push(entry.shadow_name.clone());
            compiled_mut.group_by_indices.push(entry.upstream_pos);
        }

        let mut builder =
            SchemaBuilder::with_capacity(output_schema.column_count() + to_append.len());
        for (i, col) in output_schema.columns().iter().enumerate() {
            match output_schema.field_metadata(i) {
                Some(meta) => builder = builder.with_field_meta(col.clone(), meta.clone()),
                None => builder = builder.with_field(col.clone()),
            }
        }
        for entry in &to_append {
            builder = builder.with_field_meta(
                entry.shadow_name.clone(),
                FieldMetadata::source_correlation(entry.source_field.as_str()),
            );
        }
        *output_schema = builder.build();
    }
}

/// Body-graph variant. Walks the body mini-DAG and derives each
/// aggregate's parent ck_set by inspecting the upstream node's
/// `output_schema` for `$ck.<field>` columns, since body mini-DAGs do
/// not maintain a `node_properties` side table.
fn extend_aggregate_group_by_with_shadow_in_body(
    body: &mut crate::plan::composition_body::BoundBody,
) {
    use clinker_record::FieldMetadata;
    use petgraph::Direction;
    use petgraph::graph::NodeIndex;

    let mut parent_ck_set: HashMap<NodeIndex, std::collections::BTreeSet<String>> = HashMap::new();
    for idx in body.graph.node_indices() {
        if !matches!(
            &body.graph[idx],
            crate::plan::execution::PlanNode::Aggregation { .. }
        ) {
            continue;
        }
        let mut ck: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut cursor = idx;
        while let Some(upstream) = body
            .graph
            .neighbors_directed(cursor, Direction::Incoming)
            .next()
        {
            if let Some(schema) = body.graph[upstream].stored_output_schema() {
                for (i, col) in schema.columns().iter().enumerate() {
                    if matches!(
                        schema.field_metadata(i),
                        Some(FieldMetadata::SourceCorrelation { .. }),
                    ) && let Some(field) = col.strip_prefix("$ck.")
                    {
                        ck.insert(field.to_string());
                    }
                }
                break;
            }
            cursor = upstream;
        }
        parent_ck_set.insert(idx, ck);
    }
    extend_aggregate_group_by_with_shadow_for_graph(&mut body.graph, &parent_ck_set);
}
