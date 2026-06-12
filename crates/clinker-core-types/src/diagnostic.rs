//! Structured compile-time diagnostics.
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
//! | `E210`      | error    | Source declares more than one of `{path,glob,regex,paths}` |
//! | `E211`      | error    | Source declares none of `{path,glob,regex,paths}`    |
//! | `E212`      | error    | Invalid glob pattern in source matcher               |
//! | `E213`      | error    | Invalid regex pattern in source matcher              |
//! | `E214`      | error    | Invalid duration string for `modified_after`/`_before` |
//! | `E215`      | error    | Invalid byte size for `min_size`/`max_size`          |
//! | `E216`      | error    | Source matched zero files (with `on_no_match: error`) |
//! | `E217`      | error    | Schema mismatch across multi-file source's files     |
//! | `E218`      | error    | `files.take_first` and `files.take_last` both set    |
//! | `E219`      | error    | `rest` transport declares a file matcher (path/glob/regex/paths) |
//! | `E220`      | error    | `rest` transport declares a non-`json`/`xml` decode format |
//! | `E221`      | error    | REST source read failure (HTTP request / body-read error) |
//! | `E15Y`      | error    | Aggregate with streaming strategy over relaxed-CK group_by |
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
//! | `E310`      | error    | Memory-budget surface exceeded the configured hard limit |
//! | `E311`      | error    | Combine `match: collect` has a non-empty `cxl:` body |
//! | `E313`      | error    | Combine has no equality conjuncts (HashBuildProbe needs ≥1) |
//! | `E314`      | error    | Schema mismatch at operator entry (column list divergence) |
//! | `E319`      | error    | Combine `on_miss: error` had no matching build row   |
//! | `W302`      | warning  | Pure-equi combine with all small inputs — consider InMemoryHash |
//! | `W305`      | warning  | Combine where-clause has no equality conjuncts       |
//! | `W306`      | warning  | Combine planner cannot determine optimal driving input |
//!
//! DLQ threshold + per-source routing diagnostics:
//!
//! | Code        | Severity | Meaning                                              |
//! |-------------|----------|------------------------------------------------------|
//! | `E315`      | error    | Pipeline-wide DLQ rate exceeded `error_handling.dlq.max_rate` |
//! | `E316`      | error    | Per-source DLQ rate exceeded `error_handling.dlq.per_source.<name>.max_rate` |
//! | `E317`      | error    | `error_handling.dlq.per_source` key does not name a declared Source |
//! | `E318`      | error    | `error_handling.dlq.*.max_rate` out of `[0.0, 1.0]` or DLQ path collides |
//! | `E322`      | error    | Two output destinations (Output nodes, or an Output node and a DLQ path) resolve to the same file |
//! | `E323`      | error    | `edifact` output combined with byte-limit `split` (an interchange is one indivisible UNB..UNZ envelope) |
//! | `E338`      | error    | `x12` output combined with byte-limit `split` (an interchange is one indivisible ISA..IEA envelope) |
//! | `E339`      | error    | `hl7` output combined with byte-limit `split` (a batch/file envelope is one indivisible FHS..FTS structure) |
//! | `E340`      | error    | A `$doc.<section>.<field>` access is indexed by a non-literal, so its declared document path cannot be resolved at compile time |

use crate::span::{FileId, Span};

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
    /// callers are responsible for supplying the owning [`FileId`].
    ///
    /// Currently records a zero-length primary span at offset 0.
    pub fn from_serde_saphyr_error(file: FileId, err: &serde_saphyr::Error) -> Self {
        let message = err.to_string();
        Self::error(
            "E000",
            message,
            LabeledSpan::new(Span::point(file, 0), None),
        )
    }

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

#[cfg(test)]
mod diagnostic_tests {
    use super::*;
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
        let source = include_str!("diagnostic.rs");
        for code in [
            "E101", "E102", "E103", "E104", "E105", "E106", "E107", "E108", "E109", "E111", "E112",
        ] {
            let pattern = format!("`{code}`");
            assert!(
                source.contains(&pattern),
                "diagnostic registry missing entry for {code}"
            );
        }
        // Also verify W101 is registered
        assert!(
            source.contains("`W101`"),
            "diagnostic registry missing entry for W101"
        );
    }

    #[test]
    fn test_error_registry_combine_codes_documented() {
        let source = include_str!("diagnostic.rs");
        // E302 was dropped earlier: structurally unreachable with
        // `QualifiedField`-keyed merged rows. Re-adding it requires an
        // explicit entry both here and in the registry table above.
        for code in [
            "E300", "E301", "E303", "E304", "E305", "E306", "E307", "E308", "E309", "E310", "E311",
            "E313", "E314", "E315", "E316", "E317", "E318", "E319", "E322",
        ] {
            let pattern = format!("`{code}`");
            assert!(
                source.contains(&pattern),
                "diagnostic registry missing entry for {code}"
            );
        }
        for code in ["W302", "W305", "W306"] {
            let pattern = format!("`{code}`");
            assert!(
                source.contains(&pattern),
                "diagnostic registry missing entry for {code}"
            );
        }
    }
}
