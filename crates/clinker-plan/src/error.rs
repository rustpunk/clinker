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
    /// E319 — a combine with `on_miss: error` saw a driver row that
    /// matched no build row. Semantically distinct from E310 (which
    /// is a memory-budget overflow). Always aborts the run.
    CombineMissingMatch {
        combine: String,
        driver_row: u64,
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
            Self::CombineMissingMatch {
                combine,
                driver_row,
            } => write!(
                f,
                "E319 combine '{combine}': on_miss: error — no matching build row \
                 for driver row {driver_row}"
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
