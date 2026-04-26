//! Diagnostic and error types.
//!
//! # Diagnostic code registry
//!
//! Every `Diagnostic::error` / `Diagnostic::warning` call site in the
//! workspace MUST use one of the codes listed here. Adding a new code
//! requires a new entry in this block. No orphan literals.
//!
//! | Code        | Severity | Meaning                                              |
//! |-------------|----------|------------------------------------------------------|
//! | `E000`      | error    | serde-saphyr parse error (from `from_serde_saphyr_error`) |
//! | `E001`      | error    | Duplicate node name (exact match)                    |
//! | `E002`      | error    | Self-referential node input                          |
//! | `E003`      | error    | Cycle detected between nodes                         |
//! | `E004`      | error    | Node input references undeclared producer (unified pass; payload `InputRefUndeclared`) |
//! | `E010`      | error    | Dotted-name check (`.` reserved for branch refs)     |
//! | `E011`      | error    | Log directive sanity (`every` must be valid)         |
//! | `E101`      | error    | Composition signature parse error (malformed `.comp.yaml`) |
//! | `E102`      | error    | Composition body references undeclared port          |
//! | `E103`      | error    | Call site or channel binds undeclared input/config/resource |
//! | `E104`      | error    | Call site or channel missing required input/config   |
//! | `E105`      | error    | Channel binding references undeclared config key     |
//! | `E106`      | error    | Name collision after composition expansion           |
//! | `E107`      | error    | Cycle detected in flat post-expansion graph          |
//! | `E108`      | error    | Composition body references enclosing scope (IsolatedFromAbove) |
//! | `E109`      | error    | Ambiguous column reference (declared vs pass-through in open row) |
//! | `E111`      | error    | Composition body has zero nodes (rejected at bind time) |
//! | `E112`      | error    | Runtime composition recursion depth exceeded |
//! | `E200`      | error    | CXL type error (compile-time typecheck failure)      |
//! | `E201`      | error    | Source declaration missing required `schema:` field  |
//! | `E-SEC-001` | error    | Path security violation (escape, symlink, etc.)      |
//! | `W002`      | warning  | Node names differ only in case                       |
//! | `W100`      | warning  | Aggregate lowering deferred (stub)                   |
//! | `W101`      | warning  | Pass-through column shadowed by composition body column |
//! | `W102`      | warning  | Composition signature validation (required+default contradiction, suspicious port) |
//!
//! Combine node diagnostics:
//!
//! | Code        | Severity | Meaning                                              |
//! |-------------|----------|------------------------------------------------------|
//! | `E300`      | error    | Combine input count is out of bounds (must be 2..=8) |
//! | `E301`      | error    | Combine input qualifier collides with reserved namespace |
//! | `E303`      | error    | Combine where-clause is not boolean                  |
//! | `E304`      | error    | Field not in combine merged row                      |
//! | `E305`      | error    | Combine where-clause has no cross-input comparisons OR forms a disconnected join graph |
//! | `E306`      | error    | Combine drive hint references unknown input          |
//! | `E307`      | error    | Combine input references undeclared upstream         |
//! | `E308`      | error    | Combine cxl body references unknown field            |
//! | `E309`      | error    | Combine output schema is empty                       |
//! | `E310`      | error    | Combine runtime exceeded hard memory limit           |
//! | `E311`      | error    | Combine `match: collect` has a non-empty `cxl:` body |
//! | `E313`      | error    | Combine has no equality conjuncts (HashBuildProbe needs ≥1) |
//! | `E314`      | error    | Schema mismatch at operator entry (column list divergence) |
//! | `W302`      | warning  | Pure-equi combine with all small inputs — consider InMemoryHash |
//! | `W305`      | warning  | Combine where-clause has no equality conjuncts       |
//! | `W306`      | warning  | Combine planner cannot determine optimal driving input |

use std::fmt;

use crate::span::Span;

/// Severity level for a [`Diagnostic`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Severity {
    Error,
    Warning,
    Note,
}

/// A span plus an optional human-readable label. Analogous to
/// `miette::LabeledSpan` but tied to our own [`Span`] type.
#[derive(Clone, Debug)]
pub struct LabeledSpan {
    pub span: Span,
    pub label: Option<String>,
}

impl LabeledSpan {
    pub fn new(span: Span, label: impl Into<Option<String>>) -> Self {
        Self {
            span,
            label: label.into(),
        }
    }

    pub fn primary(span: Span, label: impl Into<String>) -> Self {
        Self {
            span,
            label: Some(label.into()),
        }
    }
}

/// Typed structured payload for diagnostics, addressable by code.
///
/// Tests and downstream consumers destructure this enum directly to
/// assert on logical structure (field values) rather than display
/// strings. Display rendering of the diagnostic remains the
/// `Diagnostic.message` field — `payload` is the machine-readable
/// sibling.
///
/// Add a new variant when a diagnostic code carries identifying data
/// that callers may want to assert on without parsing the human message.
/// Variants are append-only — removing one is a breaking change to any
/// test or downstream consumer that destructures it.
///
/// Backs the unified input-reference resolution diagnostic so every
/// undeclared-reference case (standalone node or combine arm) emits a
/// single error code with machine-readable structure.
#[derive(Clone, Debug)]
pub enum DiagnosticPayload {
    /// E004 — a node's declared `input` field references a producer
    /// name that does not exist in the unified node-name table.
    ///
    /// `qualifier` is `Some` only for combine-arm references (the
    /// per-input port name in `combine.from { products: ... }`). All
    /// other node variants (Transform/Aggregate/Route/Output/Merge)
    /// have a single `input:` field and produce `qualifier: None`.
    InputRefUndeclared {
        /// The downstream node whose `input` is broken.
        consumer: String,
        /// For combine, the qualifier (port name) on the broken
        /// reference — e.g. `"products"` for
        /// `combine x { from { products: ... } }`. `None` for
        /// single-input nodes.
        qualifier: Option<String>,
        /// The undeclared producer name being referenced (the typo).
        reference: String,
    },
}

/// A structured compile-time diagnostic.
///
/// Diagnostics carry a machine-readable `code` (e.g. `"E001"`), a severity,
/// a short `message`, a `primary` labeled span, optional secondary labels,
/// an optional help string, and an optional typed `payload` for codes
/// that benefit from structured field-level assertion.
#[derive(Clone, Debug)]
pub struct Diagnostic {
    pub code: String,
    pub severity: Severity,
    pub message: String,
    pub primary: LabeledSpan,
    pub secondary: Vec<LabeledSpan>,
    pub help: Option<String>,
    /// Optional typed payload — set via [`Diagnostic::with_payload`] at
    /// emission sites that want to expose structured fields for tests
    /// or downstream tooling. `None` for codes that have no structured
    /// data beyond the message string.
    pub payload: Option<DiagnosticPayload>,
}

impl Diagnostic {
    pub fn error(
        code: impl Into<String>,
        message: impl Into<String>,
        primary: LabeledSpan,
    ) -> Self {
        Self {
            code: code.into(),
            severity: Severity::Error,
            message: message.into(),
            primary,
            secondary: Vec::new(),
            help: None,
            payload: None,
        }
    }

    pub fn warning(
        code: impl Into<String>,
        message: impl Into<String>,
        primary: LabeledSpan,
    ) -> Self {
        Self {
            code: code.into(),
            severity: Severity::Warning,
            message: message.into(),
            primary,
            secondary: Vec::new(),
            help: None,
            payload: None,
        }
    }

    pub fn with_secondary(mut self, label: LabeledSpan) -> Self {
        self.secondary.push(label);
        self
    }

    pub fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    /// Attach a typed structured payload. Callers consume the diagnostic
    /// via `Diagnostic::error(...).with_payload(payload)`.
    pub fn with_payload(mut self, payload: DiagnosticPayload) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Convert a `serde_saphyr` parse error into a diagnostic. The error's
    /// reported byte offset (if any) is carried through on the primary span;
    /// callers are responsible for supplying the owning [`crate::span::FileId`].
    ///
    /// Currently records a zero-length primary span at offset 0.
    pub fn from_serde_saphyr_error(file: crate::span::FileId, err: &serde_saphyr::Error) -> Self {
        let message = err.to_string();
        Self::error(
            "E000",
            message,
            LabeledSpan::new(Span::point(file, 0), None),
        )
    }
}

impl Diagnostic {
    /// Convenience accessor — destructure the input-reference payload
    /// if this diagnostic carries one. Returns `None` for diagnostics
    /// without a payload or with a different payload variant.
    pub fn input_ref_payload(&self) -> Option<(&str, Option<&str>, &str)> {
        match self.payload.as_ref()? {
            DiagnosticPayload::InputRefUndeclared {
                consumer,
                qualifier,
                reference,
            } => Some((consumer.as_str(), qualifier.as_deref(), reference.as_str())),
        }
    }
}

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
    CompositionBodyError {
        composition_name: String,
        inner: Box<PipelineError>,
    },
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
    /// rendered diagnostic carries the composition's name.
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

#[cfg(test)]
mod diagnostic_tests {
    use super::*;
    use crate::span::{FileId, Span};
    use std::num::NonZeroU32;

    fn fake_file() -> FileId {
        FileId::new(NonZeroU32::new(1).unwrap())
    }

    #[test]
    fn test_diagnostic_carries_primary_span() {
        let file = fake_file();
        let span = Span {
            file,
            start: 42,
            len: 7,
        };
        let diag = Diagnostic::error(
            "E001",
            "duplicate node name",
            LabeledSpan::primary(span, "first defined here"),
        )
        .with_secondary(LabeledSpan::new(
            Span {
                file,
                start: 99,
                len: 7,
            },
            Some("redefined here".to_string()),
        ))
        .with_help("rename one of the nodes");

        assert_eq!(diag.code, "E001");
        assert_eq!(diag.severity, Severity::Error);
        assert_eq!(diag.primary.span, span);
        assert_eq!(diag.primary.label.as_deref(), Some("first defined here"));
        assert_eq!(diag.secondary.len(), 1);
        assert_eq!(diag.help.as_deref(), Some("rename one of the nodes"));
    }

    #[test]
    fn test_error_registry_e101_through_e108_documented() {
        let source = include_str!("error.rs");
        for code in [
            "E101", "E102", "E103", "E104", "E105", "E106", "E107", "E108", "E109", "E111", "E112",
        ] {
            let pattern = format!("`{code}`");
            assert!(
                source.contains(&pattern),
                "error.rs registry missing entry for {code}"
            );
        }
        // Also verify W101 is registered
        assert!(
            source.contains("`W101`"),
            "error.rs registry missing entry for W101"
        );
    }

    #[test]
    fn test_error_registry_combine_codes_documented() {
        let source = include_str!("error.rs");
        // E302 was dropped earlier: structurally unreachable with
        // `QualifiedField`-keyed merged rows. Re-adding it requires an
        // explicit entry both here and in the registry table above.
        for code in [
            "E300", "E301", "E303", "E304", "E305", "E306", "E307", "E308", "E309", "E310", "E311",
            "E313", "E314",
        ] {
            let pattern = format!("`{code}`");
            assert!(
                source.contains(&pattern),
                "error.rs registry missing entry for {code}"
            );
        }
        for code in ["W302", "W305", "W306"] {
            let pattern = format!("`{code}`");
            assert!(
                source.contains(&pattern),
                "error.rs registry missing entry for {code}"
            );
        }
    }
}
