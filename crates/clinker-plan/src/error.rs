//! Top-level pipeline error type.
//!
//! The structured compile-time diagnostic vocabulary (`Diagnostic`,
//! `Severity`, `LabeledSpan`, `DiagnosticPayload`) lives in
//! [`clinker_core_types`]; this module owns only [`PipelineError`], the
//! runtime error enum that aggregates every subsystem failure with the
//! `From` conversions that thread them together.

use std::fmt;

/// Top-level pipeline error enum with From impls for subsystem errors.
#[derive(Debug)]
pub enum PipelineError {
    Config(crate::config::ConfigError),
    Schema(crate::schema::SchemaError),
    Format(clinker_format::FormatError),
    Eval(cxl::eval::EvalError),
    Compilation {
        transform_name: String,
        messages: Vec<String>,
    },
    Io(std::io::Error),
    /// Spill-format I/O or decode failure surfaced by the execution
    /// layer's spill reader. Distinct from `Io` so the rendered
    /// diagnostic preserves postcard and JSON-schema decode context that
    /// bare `std::io::Error` would lose.
    Spill(crate::runtime_error::SpillError),
    ThreadPool(String),
    /// Multiple errors collected from parallel writer threads.
    /// DataFusion `Collection` pattern (PR #14439).
    Multiple(Vec<PipelineError>),
    /// Plan-time invariant violated at runtime — Clinker bug, not a data
    /// error. ALWAYS aborts the run regardless of `ErrorStrategy::Continue`.
    /// Mirrors DataFusion's `internal_err!` macro and PR #9241 / #12086
    /// post-mortem on unreachable-arm panics in long-lived executors.
    Internal {
        op: &'static str,
        node: String,
        detail: String,
    },
    /// Finalize-time accumulator failure (overflow, type mismatch, etc.).
    /// Wraps `AccumulatorError` with the failing transform + binding for
    /// diagnostics. Routed to the DLQ under `Continue`, propagated under
    /// `FailFast`.
    Accumulator {
        transform: String,
        binding: String,
        source: clinker_record::accumulator::AccumulatorError,
    },
    /// Streaming aggregation detected an out-of-order group key on the
    /// USER-INPUT path (`StreamingAggregator<AddRaw>`). ALWAYS hard-aborts
    /// regardless of error strategy — the user's declared sort order was
    /// wrong. Surfaced to the user as a DLQ-styled error.
    SortOrderViolation {
        message: String,
    },
    /// Spill-merge produced an out-of-order group key. This is a
    /// **Clinker bug** because the LoserTree should never produce
    /// out-of-order keys; the only way to reach this variant is an
    /// internal invariant violation in the spill-recovery path. ALWAYS
    /// hard-aborts.
    MergeSortOrderViolation {
        message: String,
    },
    /// E314 — a record arriving at an operator carried a schema whose
    /// column list does not match the operator's compile-time input
    /// schema. Surfaces at the start of every executor arm that reads
    /// from an upstream channel (Transform, Aggregate, Combine probe/
    /// build, Output, Route, Merge, Composition). Owns the two `Arc`s
    /// so the diagnostic can be rendered lazily without lifetime
    /// gymnastics.
    SchemaMismatch {
        expected: std::sync::Arc<clinker_record::Schema>,
        actual: std::sync::Arc<clinker_record::Schema>,
        operator_name: String,
        operator_kind: &'static str,
        upstream_name: String,
    },
    /// E112 — a composition node's runtime body recursion exceeded
    /// `MAX_COMPOSITION_DEPTH`. Distinct from E107 (compile-time depth
    /// guard) so log greppers can pinpoint the runtime emission site
    /// without ambiguity. Always aborts the run.
    CompositionDepthExceeded {
        composition_name: String,
        depth: u32,
    },
    /// Internal invariant violation: `PlanNode::Composition.body`
    /// resolves to no entry in `CompileArtifacts.composition_bodies`.
    /// Should never happen post-bind. Always aborts the run.
    CompositionBodyMissing {
        composition_name: String,
    },
    /// Internal invariant violation: a body input port has no
    /// `BoundBody.port_name_to_node_idx` entry, so no body node
    /// consumes the parent's records. Should never happen post-bind.
    /// Always aborts the run.
    CompositionUnknownPort {
        composition_name: String,
        port_name: String,
    },
    /// Wraps an error that surfaced inside a composition body's
    /// recursive walk so the rendered diagnostic carries the
    /// composition's name. Lets users see "in composition '<name>'"
    /// in failure messages instead of an opaque inner error.
    ///
    /// # Two-path model for composition-involved errors
    ///
    /// This wrapper is applied **only** to errors that bubble up
    /// from a body operator whose `node` field names a body-internal
    /// identifier the user never wrote (e.g. a body-internal Transform
    /// named `stage_split`). The outer `composition_name` field
    /// supplies the user-visible call-site name so the rendered
    /// diagnostic can attribute the failure to the operator the
    /// user actually authored.
    ///
    /// Errors emitted at the **composition boundary** — when records
    /// are cloned into a body input port, or when the body's output
    /// is harvested back into the parent's `node_buffers` slot — are
    /// **not** wrapped. Their `node` field already carries the
    /// call-site composition name, so an outer wrapper would just
    /// duplicate the same identifier in two places. Consumers that
    /// want to catch every composition-involved failure must match
    /// both this variant and the bare inner-variant form.
    ///
    /// The wrapper is purely diagnostic attribution, not a separate
    /// enforcement path: body operators share the same `MemoryArbitrator`
    /// instance as the parent pipeline and admit through the same
    /// site-level primitives.
    CompositionBodyError {
        composition_name: String,
        inner: Box<PipelineError>,
    },
    /// Diagnostic carrier for a correlation-key group that exceeded
    /// `error_handling.max_group_buffer`. The actual flow-control
    /// signal lives in the DLQ entries the `CorrelationCommit` arm
    /// emits — this variant is rendered into the trigger entry's
    /// `error_message` and never propagated up the call stack.
    CorrelationGroupOverflow {
        group_key: String,
        count: u64,
    },
    /// E310 — a memory-budget surface (arena state, `node_buffers`,
    /// or accumulated disk-spill bytes) exceeded the configured RSS
    /// hard limit. `node` names the producing operator; `source`
    /// distinguishes which surface tripped. `detail` carries any
    /// site-specific context the rendered message previously inlined
    /// (e.g. partition id, distinct-count estimate, disk-spill quota
    /// figures). Always aborts the run.
    ///
    /// # Composition involvement
    ///
    /// When a composition is in play, this variant can reach the
    /// user in two shapes — see [`CompositionBodyError`] for the
    /// full two-path model. In summary: budget exceedances at a
    /// composition boundary (records flowing into a body input port
    /// or back out of the body) surface as a **bare**
    /// `MemoryBudgetExceeded` with `node` set to the call-site
    /// composition name; exceedances inside the body surface
    /// **wrapped** in `CompositionBodyError`, with `node` then
    /// pointing at a body-internal operator. The budget itself is
    /// shared across the whole run — body operators charge the same
    /// `MemoryArbitrator` instance the parent pipeline uses.
    ///
    /// [`CompositionBodyError`]: PipelineError::CompositionBodyError
    MemoryBudgetExceeded {
        node: String,
        used: u64,
        limit: u64,
        source: crate::runtime_error::BudgetCategory,
        detail: Option<String>,
    },
    /// E312 — the configured `memory.limit` is below the process's
    /// baseline resident memory (RSS), measured at startup before any
    /// pipeline data loads. Such a budget is unsatisfiable: the whole
    /// process — including any host memory when clinker runs embedded — sits
    /// above the ceiling with an empty pipeline, and none of that is
    /// clinker's to spill, so no amount of spilling or back-pressure can
    /// bring RSS under it. Under a producer-pausing policy the run would
    /// otherwise churn — pausing producers against a ceiling it can never get
    /// under — so rejecting at startup fails fast on the impossible budget
    /// instead. `limit` is the configured ceiling in bytes; `baseline_rss` is
    /// the measured startup RSS. Always aborts the run before any
    /// source-ingest thread spawns.
    UnsatisfiableMemoryBudget {
        limit: u64,
        baseline_rss: u64,
    },
    /// E319 — a combine with `on_miss: error` saw a driver row that
    /// matched no build row. Semantically distinct from E310 (which
    /// is a memory-budget overflow). Always aborts the run.
    CombineMissingMatch {
        combine: String,
        driver_row: u64,
    },
    /// E325 — a combine's emitted-row count would exceed the opt-in
    /// `max_output_rows` cap configured on the node. A result-size runaway guard,
    /// deliberately distinct from E310 (`MemoryBudgetExceeded`): a permissive
    /// high-fan-out join can produce a correct but explosively large result while
    /// sitting well inside its memory envelope (the output sort spills), so the
    /// cap fires on row COUNT, not bytes. Fails loud rather than truncating — a
    /// capped/partial result would silently corrupt downstream data — so it always
    /// aborts the run. `combine` names the node; `cap` is the configured ceiling.
    CombineOutputCapExceeded {
        combine: String,
        cap: u64,
    },
    /// E350 — an Envelope node with the `concat` strategy collapsed a
    /// multi-document body into one framed document, but the body carried
    /// two or more distinct non-empty envelope headers. One consolidated
    /// document can frame only one header, so emitting it would silently
    /// drop every header but one. Rather than shadow, concat rejects: the
    /// run needs a header-folding strategy (e.g. choose one, regenerate, or
    /// merge) to declare which header wins. `envelope` names the node.
    /// Always aborts the run.
    EnvelopeMultiHeaderConflict {
        envelope: String,
        header_count: usize,
    },
    /// E351 — an Envelope node with a wired `header:` port received a header
    /// record whose document grain matches no in-flight body grain (or is the
    /// synthetic / ungrounded grain a Transform stamps onto an in-pipeline-
    /// synthesized record). The node attaches a header to a body strictly by
    /// grain, so it cannot place a header that grounds to no body document —
    /// doing so would either drop the header or mis-frame an unrelated document.
    /// The fix is to carry the body's grain forward onto the header record: a
    /// grain-preserving Transform of the source's promoted header keeps it, and
    /// a replacement from a different source establishes it via a business-key
    /// join against the body. `envelope` names the node; `grain` is the
    /// offending header record's grain rendered for the diagnostic. Always
    /// aborts the run.
    EnvelopeHeaderGrainUnmatched {
        envelope: String,
        grain: String,
    },
    /// E352 — an Envelope node with a wired `header:` port received two or more
    /// header records carrying the SAME body document grain. The node attaches
    /// exactly one header per document grain, so a second header on a grain is
    /// ambiguous: keeping last-write-wins would silently drop the earlier header
    /// (data loss), and the node has no fold rule to reconcile them. The fix is
    /// to deduplicate the header stream to one record per grain upstream — an
    /// Aggregate or distinct keyed on the grain's business key, or a Transform
    /// that emits a single rewritten header per source document. `envelope`
    /// names the node; `grain` is the offending duplicated grain rendered for
    /// the diagnostic. Always aborts the run.
    EnvelopeHeaderMultipleForGrain {
        envelope: String,
        grain: String,
    },
    /// E320 — the cumulative on-disk size of the run's spill files crossed
    /// the configured `storage.spill.disk_cap_bytes` quota. Deliberately
    /// distinct from E310 (`MemoryBudgetExceeded`): a run can sit well
    /// inside its RSS envelope yet still exhaust local disk through an
    /// unbounded stream of spill files, and conflating the two reproduces
    /// the duckdb/duckdb#14142 trap where a disk-cap hit rendered as an
    /// out-of-memory message and operators wrongly concluded the engine was
    /// broken. Also distinct from E321 (`SpillDiskFull`): the cap is a
    /// configured policy ceiling the run chose to stop at, not the physical
    /// volume running out of space. `node` names the spilling operator;
    /// `cap` is the configured quota; `attempted` is the byte count of the
    /// flush that would have crossed it; `current` is the cumulative total
    /// after that flush. Always aborts the run.
    SpillCapExceeded {
        node: String,
        cap: u64,
        attempted: u64,
        current: u64,
    },
    /// E315 (pipeline-wide, `source: None`) or E316 (per-source,
    /// `source: Some(name)`) — the cumulative DLQ fraction crossed
    /// the configured `max_rate` after at least `min_records` rows
    /// from the relevant scope had been observed. ALWAYS aborts the
    /// run regardless of `ErrorStrategy::Continue`; this is a halt
    /// directive, not a per-record error.
    DlqRateExceeded {
        source: Option<std::sync::Arc<str>>,
        observed_rate: f64,
        max_rate: f64,
        observed_count: u64,
        total_count: u64,
    },
    /// A chunk-boundary shutdown poll tripped (SIGINT/SIGTERM or a
    /// programmatic shutdown request from the execution layer).
    /// The dispatch unwinds so the executor can drop senders, join worker
    /// threads, and exit gracefully (the CLI maps this to exit code 130).
    Interrupted,
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(e) => write!(f, "config error: {e}"),
            Self::Schema(e) => write!(f, "schema error: {e}"),
            Self::Format(e) => write!(f, "format error: {e}"),
            Self::Eval(e) => write!(f, "evaluation error: {e}"),
            Self::Compilation {
                transform_name,
                messages,
            } => {
                write!(
                    f,
                    "CXL compilation failed for transform '{transform_name}': "
                )?;
                for msg in messages {
                    write!(f, "\n  {msg}")?;
                }
                Ok(())
            }
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Spill(e) => write!(f, "{e}"),
            Self::ThreadPool(e) => write!(f, "thread pool error: {e}"),
            Self::Multiple(errors) => {
                write!(f, "{} errors:", errors.len())?;
                for e in errors {
                    write!(f, "\n  - {e}")?;
                }
                Ok(())
            }
            Self::Internal { op, node, detail } => {
                write!(f, "internal error in {op} '{node}': {detail}")
            }
            Self::Accumulator {
                transform,
                binding,
                source,
            } => write!(
                f,
                "accumulator finalize failed for {transform}.{binding}: {source:?}"
            ),
            Self::SortOrderViolation { message } => {
                write!(f, "sort-order violation: {message}")
            }
            Self::MergeSortOrderViolation { message } => {
                write!(f, "spill-merge sort-order violation: {message}")
            }
            Self::SchemaMismatch {
                expected,
                actual,
                operator_name,
                operator_kind,
                upstream_name,
            } => {
                let exp_list = expected
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("{c}@{i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let act_list = actual
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("{c}@{i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "E314 Schema mismatch at operator '{operator_name}' (kind={operator_kind}, \
                     input from '{upstream_name}'): expected {} columns: [{exp_list}] \
                     record has {} columns: [{act_list}]",
                    expected.column_count(),
                    actual.column_count(),
                )
            }
            Self::CompositionDepthExceeded {
                composition_name,
                depth,
            } => write!(
                f,
                "E112 runtime composition recursion depth exceeded ({depth}) \
                 in composition '{composition_name}'"
            ),
            Self::CompositionBodyMissing { composition_name } => write!(
                f,
                "internal error in composition '{composition_name}': \
                 body handle resolves to no entry in composition_bodies"
            ),
            Self::CompositionUnknownPort {
                composition_name,
                port_name,
            } => write!(
                f,
                "internal error in composition '{composition_name}': \
                 input port '{port_name}' has no body-side consumer"
            ),
            Self::CompositionBodyError {
                composition_name,
                inner,
            } => write!(f, "in composition '{composition_name}': {inner}"),
            Self::CorrelationGroupOverflow { group_key, count } => write!(
                f,
                "correlation-key group {group_key:?} exceeded max_group_buffer \
                 after {count} records — remaining records of the group are \
                 DLQ'd as collateral"
            ),
            Self::MemoryBudgetExceeded {
                node,
                used,
                limit,
                source,
                detail,
            } => match detail {
                Some(d) => write!(
                    f,
                    "E310 {node}: {source} exceeded budget ({used}/{limit}) [{d}]"
                ),
                None => write!(f, "E310 {node}: {source} exceeded budget ({used}/{limit})"),
            },
            Self::UnsatisfiableMemoryBudget {
                limit,
                baseline_rss,
            } => write!(
                f,
                "E312 memory.limit of {limit} bytes is below this process's \
                 whole-process baseline resident memory ({baseline_rss} bytes), \
                 measured before any data loads — a figure that, when clinker runs \
                 embedded in a larger host, includes the host's own memory, which \
                 clinker cannot spill. Raise memory.limit above the baseline \
                 (realistic budgets are hundreds of MiB), or partition the input \
                 across smaller invocations when a single host cannot hold the \
                 working set under a satisfiable budget. Setting \
                 memory.backpressure: spill only opts out of this producer-pausing \
                 startup check; it does not let an undersized budget hold a \
                 working set that does not fit. \
                 See: clinker explain --code E312"
            ),
            Self::CombineMissingMatch {
                combine,
                driver_row,
            } => write!(
                f,
                "E319 combine '{combine}': on_miss: error — no matching build row \
                 for driver row {driver_row}"
            ),
            Self::CombineOutputCapExceeded { combine, cap } => write!(
                f,
                "E325 combine '{combine}': output exceeded max_output_rows of {cap} \
                 rows. This is a result-size runaway guard, not an out-of-memory \
                 condition: the combine would emit more than the configured cap, so \
                 the run stops rather than truncating to a partial result. Raise \
                 max_output_rows if the large result is expected, or tighten the \
                 combine predicate."
            ),
            Self::EnvelopeMultiHeaderConflict {
                envelope,
                header_count,
            } => write!(
                f,
                "E350 envelope '{envelope}': concat collapses the body into one \
                 framed document, but the body carried {header_count} distinct \
                 non-empty envelope headers — one document can frame only one \
                 header, so concat will not silently drop the rest. Make the \
                 headers identical upstream, or add a header-folding strategy \
                 that declares which header the consolidated document keeps. \
                 See: clinker explain --code E350"
            ),
            Self::EnvelopeHeaderGrainUnmatched { envelope, grain } => write!(
                f,
                "E351 envelope '{envelope}': a wired header record carries document \
                 grain {grain}, which matches no in-flight body grain (or is a \
                 synthetic / ungrounded grain). The node attaches a header to a body \
                 strictly by grain, so it cannot place a header that grounds to no \
                 body document. Carry the body's grain onto the header record — a \
                 grain-preserving transform of the source's promoted header keeps it, \
                 or a business-key join against the body establishes it. \
                 See: clinker explain --code E351"
            ),
            Self::EnvelopeHeaderMultipleForGrain { envelope, grain } => write!(
                f,
                "E352 envelope '{envelope}': the wired header input carries two or \
                 more records for document grain {grain} — exactly one header \
                 record per document grain is required. The node attaches one \
                 header per grain and has no rule to fold a second, so it will not \
                 silently drop one. Deduplicate the header stream to one record \
                 per grain upstream (an aggregate or distinct on the grain's \
                 business key, or a transform that emits a single rewritten header \
                 per source document). See: clinker explain --code E352"
            ),
            Self::SpillCapExceeded {
                node,
                cap,
                attempted,
                current,
            } => write!(
                f,
                "E320 {node}: spill disk cap exceeded — the configured \
                 storage.spill.disk_cap_bytes of {cap} bytes was crossed by a \
                 {attempted}-byte flush ({current} bytes spilled in total). This \
                 is a disk-cap stop, not an out-of-memory condition and not a \
                 full volume; raise storage.spill.disk_cap_bytes, reduce input \
                 size, or point storage.spill.dir at a larger volume."
            ),
            Self::DlqRateExceeded {
                source,
                observed_rate,
                max_rate,
                observed_count,
                total_count,
            } => match source {
                Some(name) => write!(
                    f,
                    "E316 source {name:?} DLQ rate {observed_rate:.4} exceeds \
                     per_source.{name}.max_rate {max_rate:.4} \
                     ({observed_count}/{total_count} records)"
                ),
                None => write!(
                    f,
                    "E315 pipeline DLQ rate {observed_rate:.4} exceeds \
                     max_rate {max_rate:.4} ({observed_count}/{total_count} records)"
                ),
            },
            Self::Interrupted => write!(f, "pipeline interrupted by shutdown signal"),
        }
    }
}

impl PipelineError {
    /// E112 — runtime recursion depth guard fired in a composition
    /// body executor. Distinct from E107 (compile-time depth check)
    /// so log greppers can pinpoint the runtime emission site.
    pub fn compose_depth_exceeded(composition_name: String, depth: u32) -> Self {
        Self::CompositionDepthExceeded {
            composition_name,
            depth,
        }
    }

    /// Invariant violation: `PlanNode::Composition.body` resolved to
    /// no body in `CompileArtifacts.composition_bodies`. Should never
    /// happen post-bind; constructed only from a debug-asserted path.
    pub fn compose_body_missing(composition_name: String) -> Self {
        Self::CompositionBodyMissing { composition_name }
    }

    /// Invariant violation: a body input port has no
    /// `port_name_to_node_idx` entry, so no body node consumes the
    /// parent's records. Should never happen post-bind.
    pub fn compose_unknown_port(composition_name: &str, port_name: &str) -> Self {
        Self::CompositionUnknownPort {
            composition_name: composition_name.to_string(),
            port_name: port_name.to_string(),
        }
    }

    /// Wrap an inner error from a composition body walk so the
    /// rendered diagnostic carries the composition's name. Apply
    /// only to body-interior errors whose inner `node` would
    /// otherwise be opaque to the user; boundary admits already
    /// carry the call-site composition name and should stay
    /// unwrapped — see [`PipelineError::CompositionBodyError`].
    pub fn compose_body_error(composition_name: String, inner: Box<PipelineError>) -> Self {
        Self::CompositionBodyError {
            composition_name,
            inner,
        }
    }

    /// E320 — the run crossed its configured `storage.spill.disk_cap_bytes`
    /// quota. Built at every spill site the moment a flush would push the
    /// cumulative spilled total past the cap, so the cap-exceeded surface
    /// stays uniform across the node-buffer, streaming-handoff, grace-hash,
    /// and sort-merge spill paths. Kept distinct from
    /// [`PipelineError::MemoryBudgetExceeded`] (E310) on purpose — see the
    /// variant docs.
    pub fn spill_cap_exceeded(
        node: impl Into<String>,
        cap: u64,
        attempted: u64,
        current: u64,
    ) -> Self {
        Self::SpillCapExceeded {
            node: node.into(),
            cap,
            attempted,
            current,
        }
    }
}

impl std::error::Error for PipelineError {}

impl From<crate::config::ConfigError> for PipelineError {
    fn from(e: crate::config::ConfigError) -> Self {
        Self::Config(e)
    }
}

impl From<clinker_format::FormatError> for PipelineError {
    fn from(e: clinker_format::FormatError) -> Self {
        Self::Format(e)
    }
}

impl From<cxl::eval::EvalError> for PipelineError {
    fn from(e: cxl::eval::EvalError) -> Self {
        Self::Eval(e)
    }
}

impl From<crate::schema::SchemaError> for PipelineError {
    fn from(e: crate::schema::SchemaError) -> Self {
        Self::Schema(e)
    }
}

impl From<std::io::Error> for PipelineError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<crate::runtime_error::SpillError> for PipelineError {
    fn from(e: crate::runtime_error::SpillError) -> Self {
        Self::Spill(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_cap_exceeded_renders_e320_distinct_from_oom() {
        let e =
            PipelineError::spill_cap_exceeded("sort_orders", 104_857_600, 8_388_608, 109_051_904);
        let rendered = e.to_string();
        assert!(rendered.contains("E320"), "{rendered}");
        assert!(rendered.contains("spill disk cap exceeded"), "{rendered}");
        assert!(rendered.contains("104857600"), "{rendered}");
        assert!(rendered.contains("109051904"), "{rendered}");
        // The message must steer the reader away from reading this as an OOM.
        assert!(
            rendered.contains("not an out-of-memory condition"),
            "{rendered}"
        );
    }

    #[test]
    fn spill_cap_exceeded_carries_structured_fields() {
        match PipelineError::spill_cap_exceeded("n", 100, 200, 300) {
            PipelineError::SpillCapExceeded {
                node,
                cap,
                attempted,
                current,
            } => {
                assert_eq!(node, "n");
                assert_eq!(cap, 100);
                assert_eq!(attempted, 200);
                assert_eq!(current, 300);
            }
            other => panic!("constructor must build SpillCapExceeded; got {other:?}"),
        }
    }
}
