//! Structured compile-time diagnostics.
//!
//! # Diagnostic code registry
//!
//! Every `Diagnostic::error` / `Diagnostic::warning` call site in the
//! workspace MUST use one of the codes in [`REGISTRY`]. Adding a new code
//! requires a new row in the `diagnostic_registry!` invocation below. No
//! orphan literals.
//!
//! The registry is data, not prose: [`REGISTRY`] is the single home for the
//! code list and its rendered documentation table is generated from those
//! same rows, so the two cannot drift. Two checks enforce the contract:
//!
//! - `crates/clinker-core-types/tests/registry_no_orphan_codes.rs` scans the
//!   workspace source for code literals in diagnostic-carrying positions and
//!   fails on any that [`REGISTRY`] does not list.
//! - [`Diagnostic::error`] and [`Diagnostic::warning`] carry a
//!   `debug_assert!` on registry membership and severity, which catches codes
//!   chosen at runtime (match arms, codes lifted back out of an error message)
//!   that a source scan cannot see, as well as an error code emitted as a
//!   warning or vice versa.

use std::path::{Component, Path};

use crate::failure::{FailureCategory, FailureClassification, RetryAdvice};
use crate::span::{FileId, Span};

/// One row of the diagnostic-code registry.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RegistryEntry {
    /// The code literal as it appears in `Diagnostic::code` and in
    /// `clinker explain --code <CODE>`.
    pub code: &'static str,
    /// Severity the emitting site uses for this code.
    pub severity: Severity,
    /// One-line statement of the condition the code reports.
    pub meaning: &'static str,
}

/// Declare the diagnostic-code registry once, as data, and render its
/// documentation table from the same rows.
///
/// Each row is `"CODE", Severity, "meaning";`. The expansion produces the
/// [`REGISTRY`] slice and the Markdown table in its rustdoc, so a row added
/// here shows up in both without a second edit.
macro_rules! diagnostic_registry {
    ($($code:literal, $severity:ident, $meaning:literal;)+) => {
        /// Every diagnostic code the workspace may emit, with its severity
        /// and the condition it reports.
        ///
        /// Ordered by code family rather than alphabetically: topology and
        /// naming (`E0xx`), composition and channel binding (`E1xx`), CXL and
        /// source declaration (`E2xx`), combine / DLQ / envelope / multi-value
        /// (`E3xx`), then the non-numeric codes.
        ///
        /// | Code | Severity | Meaning |
        /// |------|----------|---------|
        $(#[doc = concat!("| `", $code, "` | ", stringify!($severity), " | ", $meaning, " |")])+
        pub const REGISTRY: &[RegistryEntry] = &[
            $(RegistryEntry {
                code: $code,
                severity: Severity::$severity,
                meaning: $meaning,
            },)+
        ];
    };
}

diagnostic_registry! {
    // ── Topology, naming, and node-level config ─────────────────────────
    "E000", Error, "serde-saphyr parse error (from `from_serde_saphyr_error`)";
    "E001", Error, "Duplicate node name (exact match)";
    "E002", Error, "Self-referential node input";
    "E003", Error, "Cycle detected between nodes";
    "E004", Error, "Node input references undeclared producer (unified pass; payload `InputRefUndeclared`)";
    "E010", Error, "Dotted-name check (`.` reserved for branch refs)";
    "E011", Error, "Log directive sanity (`every` must be valid)";
    // ── Composition binding and the channel/group overlay ───────────────
    "E101", Error, "Composition signature parse error (malformed `.comp.yaml`)";
    "E102", Error, "Composition body references undeclared port";
    "E103", Error, "Call site or channel binds undeclared input/config/resource";
    "E104", Error, "Call site or channel missing required input/config";
    "E106", Error, "Name collision after composition expansion";
    "E107", Error, "Cycle detected in flat post-expansion graph";
    "E108", Error, "Composition body references enclosing scope (IsolatedFromAbove)";
    "E109", Error, "A channel overlay targeting a composition carries `vars:` overrides, which a composition overlay does not support";
    "E110", Error, "An extraction selection names a node the execution-plan DAG does not contain";
    "E111", Error, "Composition body has zero nodes (rejected at bind time)";
    "E112", Error, "Runtime composition recursion depth exceeded";
    "E113", Error, "Channel `config`/override key matches no parameter in the compiled plan (unknown key)";
    "E114", Error, "A structural overlay op cannot be applied (missing target, orphaning remove, breaking schema patch)";
    "E115", Error, "Composition body node fails node-scoped config validation (same checks as top-level nodes)";
    // Split out of E107 / E110 / E111, which each also named an unrelated
    // composition-binding condition. One code answering two questions makes
    // `clinker explain --code` send half its readers to the wrong page. Placed
    // with the rest of the channel/overlay family rather than appended at the
    // end of the registry.
    "E116", Error, "A channel var declaration is type-incompatible: an override changes the pipeline's type, or a default does not match the entry's declared type";
    "E117", Error, "A channel var's name shadows a reserved system field";
    "E118", Error, "A channel `vars.source` block is keyed by a source name the pipeline does not declare";
    "E119", Error, "Pipeline `date_locale` is explicit but has no supported locale-aware behavior";
    "E120", Error, "Channel discovery exceeded the tenant-folder scan budget";
    "E121", Error, "Channel manifest failed to parse";
    "E122", Error, "Channel discovery exceeded the group-file scan budget";
    "E123", Error, "Group file failed to parse";
    "E124", Error, "Pipeline `log_rules` is explicit but pipeline-authored telemetry routing is unsupported";
    "E125", Error, "Pipeline `include_provenance` is explicit but does not attach output provenance";
    "E126", Error, "Decoded source value violates its declared column type";
    "E127", Error, "Provenance field query is empty";
    "E128", Error, "Provenance exact address or shorthand matches no field";
    "E129", Error, "Provenance shorthand matches multiple scoped fields";
    // ── Windows, watermarks, phases, and scoped vars ────────────────────
    "E150", Error, "Analytic window on a transform fed by a `correlation_key:` source (per-group arena construction is unsupported)";
    "E150b", Error, "Node-rooted window references a field its rooted upstream operator does not emit";
    "E150c", Error, "Cross-source window references a source whose ingestion tier is downstream of the window-bearing transform";
    "E150d", Error, "Windowed transform is rooted at a Merge node, which concatenates streams without a single producer identity";
    "E150e", Error, "Windowed transform references an array-typed field; the window builtin has no array support";
    "E152", Error, "Composition node has an untagged (portless) incoming edge";
    "E153", Error, "Source declares a `correlation_key` field its `schema:` block does not contain";
    "E154", Error, "Source declares a `watermark.column` its `schema:` block does not contain";
    "E155", Error, "Source declares a `watermark.column` whose type is not event-time-coercible";
    "E156", Error, "Aggregate declares `time_window:` while an upstream-reachable source declares no `watermark.column`";
    "E157", Error, "Source declares an external schema file that failed to load";
    "E158", Error, "Source column declares the non-concrete type `numeric` (inference-only)";
    "E159", Error, "Source declares a `generated` schema on a format with no engine-generated positional column model";
    "E160", Error, "Composition-body window has no resolvable owning input root";
    "E161", Error, "Composition-body window input root is ambiguous";
    "E162", Error, "Composition-body window or ownership path is cyclic";
    "E163", Error, "Composition body is missing from or referenced outside its owning scope";
    "E164", Error, "Init-phase node has a runtime-phase descendant consuming its records";
    "E165", Error, "Composition-body window attempts an unsupported cross-input or cross-source lookup";
    "E166", Error, "Composition-body window is rooted at a Merge node";
    "E167", Error, "Composition-body window resolves through an undeclared input port";
    "E168", Error, "Composition-body window root schema is missing or incompatible";
    "E171", Error, "Scoped-var reader is not a DAG descendant of the Transform that writes it";
    "E172", Error, "`$source` scoped var read downstream of a Merge/Combine node (the per-source value is ambiguous there)";
    "E173", Error, "Composition body reads a parent-scope `$pipeline` / `$source` var its `_compose.scoped_vars` schema does not opt into";
    "E174", Error, "Composition `scoped_vars` schema disagrees with the parent pipeline's declaration";
    "E175", Error, "Init-phase node reads a scoped var only a runtime-phase node writes";
    "E15Y", Error, "Aggregate with streaming strategy over relaxed-CK group_by";
    // ── CXL compilation and source declaration ──────────────────────────
    "E200", Error, "CXL type error (compile-time typecheck failure)";
    "E201", Error, "Source declaration missing required `schema:` field";
    "E202", Error, "CXL parse error (compile-time CXL syntax failure)";
    "E203", Error, "CXL name-resolution error (unresolved field / variable / module reference)";
    "E210", Error, "Source declares more than one of `{path,glob,regex,paths}`";
    "E211", Error, "Source declares none of `{path,glob,regex,paths}`";
    "E212", Error, "Invalid glob pattern in source matcher";
    "E213", Error, "Invalid regex pattern in source matcher";
    "E214", Error, "Invalid duration string for `modified_after`/`_before`";
    "E215", Error, "Invalid byte size for `min_size`/`max_size`";
    "E216", Error, "Source matched zero files (with `on_no_match: error`)";
    "E217", Error, "Schema mismatch across multi-file source's files";
    "E218", Error, "`files.take_first` and `files.take_last` both set";
    "E219", Error, "`rest` transport declares a file matcher (path/glob/regex/paths)";
    "E220", Error, "`rest` transport declares a non-`json`/`xml` decode format";
    "E221", Error, "REST source read failure (HTTP request / body-read error)";
    "E222", Error, "An envelope section declares a `json_pointer` that is not a valid RFC 6901 pointer";
    // ── Channel `sources:` patches ──────────────────────────────────────
    "E230", Error, "Channel source patch key is malformed, unaddressable, or targets a source no node declares";
    "E231", Error, "Channel schema patch names a column the source does not declare";
    "E232", Error, "Channel schema patch adds a column that already exists";
    "E233", Error, "Channel schema patch renames a column onto an existing column name";
    "E234", Error, "Channel envelope-section patch removes a field the section does not declare";
    "E235", Error, "Channel options patch is rejected by the source format's option struct";
    "E236", Error, "Channel schema patch adds a column without the required `type`";
    "E237", Error, "Channel schema column ops applied to a schema with no flat column list (multi-record / generated / file)";
    "E238", Error, "Channel patch uses a format-specific section op on a source of the wrong format";
    "E239", Error, "Channel patch removes a declaration the source does not carry";
    "E240", Error, "Channel patch names an invalid target, or creates a declaration without the fields that requires";
    "E241", Error, "Channel `records` / `discriminator` ops applied to a schema that is not multi-record";
    "E242", Error, "Channel records patch names an unknown record type";
    "E243", Error, "Channel records patch adds a record type that already exists";
    "E244", Error, "Merged discriminator is not exactly one of byte-range or field";
    "E245", Error, "Two record types share a discriminator tag after the patch";
    // ── Combine ─────────────────────────────────────────────────────────
    "E300", Error, "Combine input count is out of bounds (must be 2..=8)";
    "E301", Error, "Combine input qualifier collides with reserved namespace";
    "E303", Error, "Combine where-clause is not boolean";
    "E304", Error, "Field not in combine merged row";
    "E305", Error, "Combine where-clause has no cross-input comparisons OR forms a disconnected join graph";
    "E306", Error, "Combine drive hint references unknown input";
    "E307", Error, "Combine input references undeclared upstream";
    "E308", Error, "Combine cxl body references unknown field";
    "E309", Error, "Combine output schema is empty";
    "E310", Error, "Memory-budget surface exceeded the configured hard limit";
    "E311", Error, "Combine `match: collect` has a non-empty `cxl:` body";
    "E313", Error, "Combine `where:` has neither an equality nor a range conjunct";
    "E314", Error, "Schema mismatch at operator entry (column list divergence)";
    "E319", Error, "Combine `on_miss: error` had no matching build row";
    "E325", Error, "Combine output exceeded the opt-in `max_output_rows` cap";
    "E327", Error, "Combine range conjunct operands don't reduce to a supported range axis (ambiguous `numeric`, or non-orderable)";
    // ── DLQ thresholds and per-source routing ───────────────────────────
    "E315", Error, "Pipeline-wide DLQ rate exceeded `error_handling.dlq.max_rate`";
    "E316", Error, "Per-source DLQ rate exceeded `error_handling.dlq.per_source.<name>.max_rate`";
    "E317", Error, "`error_handling.dlq.per_source` key does not name a declared Source";
    "E318", Error, "`error_handling.dlq.*.max_rate` out of `[0.0, 1.0]` or DLQ path collides";
    "E322", Error, "Two output destinations (Output nodes, or an Output node and a DLQ path) resolve to the same file";
    "E324", Error, "`pipeline.memory.resume_threshold` does not sit below the soft/spill threshold, so the two form no hysteresis band";
    // ── Output splitting, document context, and envelopes ───────────────
    "E323", Error, "`edifact` output combined with byte-limit `split` (an interchange is one indivisible UNB..UNZ envelope)";
    "E338", Error, "`x12` output combined with byte-limit `split` (an interchange is one indivisible ISA..IEA envelope)";
    "E339", Error, "`hl7` output combined with byte-limit `split` (a batch/file envelope is one indivisible FHS..FTS structure)";
    "E340", Error, "A `$doc.<section>.<field>` access is indexed by a non-literal expression, so its declared document path cannot be resolved at compile time";
    "E341", Error, "A `$doc.<section>.<field>` access names an envelope section or field a feeding closed-schema source (XML / JSON) does not declare";
    "E342", Error, "`swift` output combined with byte-limit `split` (a SWIFT MT message is one indivisible brace-balanced `{1:..}..{5:..}` envelope)";
    "E343", Error, "A per-source-file output template (`{source_file}` / `{source_path}`) combined with a source declaring `dlq_granularity: document` (a buffered-and-flushed document is incompatible with per-record file fan-out)";
    "E348", Error, "A `$doc.<section>.<field>` access against a segment/positional source (X12 / EDIFACT / HL7) names a section the format does not synthesize, or a positional element outside the `e`/`f`-prefix pattern or beyond the configured `max_elements` / `max_fields`";
    "E349", Error, "A `$doc.<section>.<field>` access is attributed to a `rest` source (or a `rest` source declares an `envelope:` block) — a REST pull buffers no document, so the access can never resolve";
    "E344", Error, "A source declares `dlq_granularity: document` together with `error_handling.strategy: fail_fast`, which contradict each other";
    "E346", Error, "An output's envelope references a section, or requires a `footer_from_doc`, that its feeding sources do not provide";
    "E347", Error, "`reconstruct_envelope` combined with per-file output splitting, a per-source-file path template, or a document-granularity DLQ source";
    "E353", Error, "Envelope header section references a body column — the header is emitted before the body streams, so it may read only `$vars` / `$source` / `$pipeline` / `$doc`";
    "E354", Error, "Envelope footer section declares an aggregate a streaming footer fold cannot compute";
    "E355", Error, "A single-document-envelope output can be fed more than one document with no consolidating node on the path, which would silently merge distinct messages into one envelope";
    "E357", Error, "An envelope section on a segment/positional source names a tier other than the one file-level header segment the reader resolves from its bounded pre-scan";
    "E356", Error, "A plain single-schema CSV / fixed-width source declares an `envelope:` block — a plain flat file carries no header/trailer structure to extract, so the declared sections are inert (a multi-record source declaring `discriminator:` + `records:` is unaffected)";
    // ── Multi-value declarations (fan-out, split, join) ──────────────────
    "E358", Error, "Malformed `split_to_rows:` / `split_values:` source declaration (duplicate, nested, undeclared column, or a format whose reader is never handed it)";
    "E359", Error, "A `multiple:` column reaches an output whose format has no encoding for a field holding more than one value";
    "E360", Error, "Source declares the removed `array_paths:` key, which the multi-value declarations replaced";
    "E361", Error, "`multiple: true` column on a source whose format has no way to produce more than one value";
    "E362", Error, "Malformed `join_values:` output declaration (the write-side mirror of E358)";
    "E363", Error, "A source's `record_path` is not a path in its format's grammar — an XPath descendant step (`//`), a JSONPath root marker (`$.`), a leading `/`, an empty segment, or an XML segment no element can be named";
    "E364", Error, "An Output `mapping:` block uses the superseded map form or declares a projection that cannot produce unique, meaningful columns";
    "E365", Error, "An Output `mapping:` item reads a column the pipeline does not carry at that point in the graph";
    "E367", Error, "An Output `split.naming` pattern is malformed or does not contain exactly one valid sequence token";
    "E366", Error, "A declared-sorted source has nested, repeated, inherited, or otherwise non-sortable per-file framing";
    "E368", Error, "Declared source-type error fraction exceeded `error_handling.type_error_threshold`";
    "E370", Error, "A pipeline combines document-level DLQ with correlation-key rejection, whose distinct atomic populations cannot share one physical writer boundary";
    "E371", Error, "Unsafe or invalid retained attempt refused";
    "E372", Error, "Attempt cleanup incomplete or budget exhausted";
    "E373", Error, "A transform log directive's `condition` is more than one predicate";
    "E374", Error, "A transform log directive requests a `fields` selector the input record does not carry";
    // ── Path security ───────────────────────────────────────────────────
    "E-SEC-001", Error, "Path security violation (escape, symlink, etc.)";
    // ── Warnings ────────────────────────────────────────────────────────
    "W002", Warning, "Node names differ only in case";
    "W100", Warning, "Aggregate lowering deferred (stub)";
    "W101", Warning, "Pass-through column shadowed by composition body column";
    "W102", Warning, "Composition signature validation (required+default contradiction, suspicious port)";
    "W302", Warning, "Pure-equi combine with all small inputs — consider InMemoryHash";
    "W305", Warning, "Combine where-clause has no equality conjuncts";
    "W306", Warning, "Combine planner cannot determine optimal driving input";
    "W307", Warning, "A physical source file violated its declared record order and was repaired before release";
}

/// Whether `code` is listed in [`REGISTRY`].
///
/// The orphan-code test and the constructors' `debug_assert!` both resolve
/// membership through here, so there is one definition of "registered".
pub fn is_registered(code: &str) -> bool {
    registered_severity(code).is_some()
}

/// Registered severity for `code`, or `None` when the code is not listed.
pub fn registered_severity(code: &str) -> Option<Severity> {
    REGISTRY
        .iter()
        .find(|entry| entry.code == code)
        .map(|entry| entry.severity)
}

/// Severity level for a [`Diagnostic`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Severity {
    Error,
    Warning,
    Note,
}

/// Operator action that produced an attempt-publication diagnostic.
///
/// The variants and [`AttemptOperation::as_str`] spellings are stable so the
/// CLI can render the same logical operation in human and machine output.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum AttemptOperation {
    /// Finalize or publish one execution attempt.
    Publication,
    /// Enumerate retained attempts.
    List,
    /// Inspect one retained attempt.
    Inspect,
    /// Preview or execute retained-attempt cleanup.
    Purge,
}

impl AttemptOperation {
    /// Return the stable machine spelling for this operation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Publication => "publication",
            Self::List => "list",
            Self::Inspect => "inspect",
            Self::Purge => "purge",
        }
    }
}

/// Whether a failed attempt operation made any final artifact visible.
///
/// This reports exact per-attempt truth without implying that a set of
/// artifacts is atomically published.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum FinalVisibility {
    /// No final artifact became visible.
    None,
    /// At least one final artifact became visible.
    Some,
    /// The operation could not determine final visibility safely.
    Unknown,
}

impl FinalVisibility {
    /// Return the stable machine spelling for this visibility result.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Some => "some",
            Self::Unknown => "unknown",
        }
    }
}

/// Stable, redacted rendering data for E371 and E372.
///
/// Construction accepts only the six registered `attempt.retention` failures,
/// a canonical execution ID, an optional canonical artifact ID, and a
/// workspace-relative pipeline path. This keeps machine-local paths,
/// credentials, record values, and arbitrary debug text out of the payload.
/// The later CLI renderer can therefore consume fields directly instead of
/// reverse-mapping message strings into retry or recovery semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptDiagnosticData {
    diagnostic_code: &'static str,
    failure: FailureClassification,
    operation: AttemptOperation,
    execution_id: String,
    artifact_id: Option<String>,
    final_visibility: FinalVisibility,
    durability_uncertain: bool,
    recovery_command: String,
    recovery_argv: Vec<String>,
}

impl AttemptDiagnosticData {
    /// Build typed E371/E372 data for a registered attempt-retention failure.
    ///
    /// Returns `None` when the failure is outside the E371/E372 families or
    /// identifying data is not a canonical logical ID and workspace-relative
    /// pipeline path. No raw failure detail or physical staging path is
    /// accepted by this boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn for_failure(
        failure_code: &str,
        operation: AttemptOperation,
        execution_id: &str,
        artifact_id: Option<&str>,
        final_visibility: FinalVisibility,
        durability_uncertain: bool,
        workspace_pipeline: &str,
    ) -> Option<Self> {
        let diagnostic_code = match failure_code {
            "attempt.retention.ownership_refused"
            | "attempt.retention.manifest_invalid"
            | "attempt.retention.live"
            | "attempt.retention.clock_ambiguous" => "E371",
            "attempt.retention.budget_exhausted" | "attempt.retention.cleanup_failed" => "E372",
            _ => return None,
        };
        if !is_canonical_execution_id(execution_id)
            || artifact_id.is_some_and(|id| !is_canonical_artifact_id(id))
            || !is_workspace_pipeline(workspace_pipeline)
        {
            return None;
        }

        let failure = FailureClassification::for_code(failure_code)?;
        let recovery_argv = vec![
            "clinker".to_owned(),
            "attempts".to_owned(),
            "inspect".to_owned(),
            workspace_pipeline.to_owned(),
            "--execution-id".to_owned(),
            execution_id.to_owned(),
        ];
        let recovery_command = render_command(&recovery_argv);

        Some(Self {
            diagnostic_code,
            failure,
            operation,
            execution_id: execution_id.to_owned(),
            artifact_id: artifact_id.map(str::to_owned),
            final_visibility,
            durability_uncertain,
            recovery_command,
            recovery_argv,
        })
    }

    /// Return the stable diagnostic code selected by the failure family.
    pub const fn diagnostic_code(&self) -> &'static str {
        self.diagnostic_code
    }

    /// Return the stable logical failure code.
    pub const fn failure_code(&self) -> &'static str {
        self.failure.code()
    }

    /// Return the registered broad failure category.
    pub const fn failure_category(&self) -> FailureCategory {
        self.failure.category()
    }

    /// Return the operation that produced the diagnostic.
    pub const fn operation(&self) -> AttemptOperation {
        self.operation
    }

    /// Return the canonical logical execution ID.
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Return the canonical logical artifact ID when one failure is artifact-specific.
    pub fn artifact_id(&self) -> Option<&str> {
        self.artifact_id.as_deref()
    }

    /// Return exact final-visibility truth for the failed operation.
    pub const fn final_visibility(&self) -> FinalVisibility {
        self.final_visibility
    }

    /// Return whether successful durability could not be established.
    pub const fn durability_uncertain(&self) -> bool {
        self.durability_uncertain
    }

    /// Return retry policy from the registered logical failure row.
    pub const fn retry_advice(&self) -> RetryAdvice {
        self.failure.retry_advice()
    }

    /// Return the pasteable workspace-relative inspection command.
    pub fn recovery_command(&self) -> &str {
        &self.recovery_command
    }

    /// Return the shell-independent recovery argument vector.
    pub fn recovery_argv(&self) -> &[String] {
        &self.recovery_argv
    }
}

fn is_canonical_execution_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase(),
        })
}

fn is_canonical_artifact_id(value: &str) -> bool {
    value.len() == 17
        && value.starts_with("artifact-")
        && value[9..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn is_workspace_pipeline(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 512
        || value.contains(['\0', '\n', '\r'])
        || value.contains('\\')
        || value.starts_with('/')
        || value.as_bytes().get(1) == Some(&b':')
        || !(value.ends_with(".yaml") || value.ends_with(".yml"))
    {
        return false;
    }

    Path::new(value)
        .components()
        .all(|component| matches!(component, Component::CurDir | Component::Normal(_)))
}

fn render_command(argv: &[String]) -> String {
    argv.iter()
        .map(|argument| quote_command_argument(argument))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(not(windows))]
fn quote_command_argument(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return value.to_owned();
    }

    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for character in value.chars() {
        if character == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(character);
        }
    }
    quoted.push('\'');
    quoted
}

#[cfg(windows)]
fn quote_command_argument(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return value.to_owned();
    }
    quote_windows_argument(value)
}

#[allow(dead_code)]
fn quote_windows_argument(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    let mut backslashes = 0_usize;
    for character in value.chars() {
        match character {
            '\\' => backslashes += 1,
            '"' => {
                for _ in 0..backslashes {
                    quoted.push('\\');
                    quoted.push('\\');
                }
                quoted.push('\\');
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                for _ in 0..backslashes {
                    quoted.push('\\');
                }
                backslashes = 0;
                quoted.push(character);
            }
        }
    }
    for _ in 0..backslashes {
        quoted.push('\\');
        quoted.push('\\');
    }
    quoted.push('"');
    quoted
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
    /// Build an error-severity diagnostic.
    ///
    /// `code` must appear in [`REGISTRY`]. Debug builds assert this, so a
    /// code chosen at runtime — a match arm, or one lifted back out of an
    /// error message — is caught the first time a test exercises the path,
    /// where the source scan in
    /// `tests/registry_no_orphan_codes.rs` cannot see it. `#[track_caller]`
    /// puts the emission site in the panic message.
    #[track_caller]
    pub fn error(
        code: impl Into<String>,
        message: impl Into<String>,
        primary: LabeledSpan,
    ) -> Self {
        let code = code.into();
        debug_assert_eq!(
            registered_severity(&code),
            Some(Severity::Error),
            "diagnostic code {code:?} is missing from the registry or is not registered as Error; \
             add or correct its row in the `diagnostic_registry!` invocation in \
             clinker-core-types/src/diagnostic.rs"
        );
        Self {
            code,
            severity: Severity::Error,
            message: message.into(),
            primary,
            secondary: Vec::new(),
            help: None,
            payload: None,
        }
    }

    /// Build a warning-severity diagnostic. `code` must appear in
    /// [`REGISTRY`]; see [`Diagnostic::error`] for how that is enforced.
    #[track_caller]
    pub fn warning(
        code: impl Into<String>,
        message: impl Into<String>,
        primary: LabeledSpan,
    ) -> Self {
        let code = code.into();
        debug_assert_eq!(
            registered_severity(&code),
            Some(Severity::Warning),
            "diagnostic code {code:?} is missing from the registry or is not registered as Warning; \
             add or correct its row in the `diagnostic_registry!` invocation in \
             clinker-core-types/src/diagnostic.rs"
        );
        Self {
            code,
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

    #[test]
    fn windows_recovery_quoting_preserves_quotes_and_trailing_backslashes() {
        assert_eq!(quote_windows_argument(""), "\"\"");
        assert_eq!(
            quote_windows_argument(r#"C:\Program Files\"#),
            "\"C:\\Program Files\\\\\""
        );
        assert_eq!(
            quote_windows_argument(r#"say "hello"\"#),
            "\"say \\\"hello\\\"\\\\\""
        );
    }

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
    fn test_registry_codes_are_unique() {
        let mut seen: Vec<&str> = Vec::with_capacity(REGISTRY.len());
        for entry in REGISTRY {
            assert!(
                !seen.contains(&entry.code),
                "diagnostic registry lists {} more than once",
                entry.code
            );
            seen.push(entry.code);
        }
    }

    #[test]
    fn test_registry_codes_match_the_documented_shape() {
        // The orphan-code scanner recognizes literals by shape, so a code
        // outside the shape would be invisible to it. Keeping the registry
        // inside the shape keeps the scanner's coverage honest.
        for entry in REGISTRY {
            let ok = if let Some(rest) = entry.code.strip_prefix("E-SEC-") {
                !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit())
            } else {
                let rest = match entry.code.as_bytes().first() {
                    Some(b'E') | Some(b'W') => &entry.code[1..],
                    _ => "",
                };
                let digits = rest.trim_end_matches(|c: char| c.is_ascii_alphabetic());
                digits.len() >= 2
                    && digits.bytes().all(|b| b.is_ascii_digit())
                    && rest.len() - digits.len() <= 1
            };
            assert!(
                ok,
                "registry code {:?} is outside the recognized shape (`E123`, `E123a`, or \
                 `E-SEC-001`); the orphan-code scanner would not see it",
                entry.code
            );
        }
    }

    #[test]
    fn test_registry_meanings_are_non_empty_and_table_safe() {
        for entry in REGISTRY {
            assert!(
                !entry.meaning.is_empty(),
                "registry entry {} has no meaning text",
                entry.code
            );
            // The rustdoc table is generated from these strings; a literal
            // pipe would split the row into extra columns.
            assert!(
                !entry.meaning.contains('|'),
                "registry meaning for {} contains a `|`, which breaks the generated table",
                entry.code
            );
        }
    }

    #[test]
    fn test_is_registered_rejects_an_unlisted_code() {
        assert!(is_registered("E001"));
        assert!(is_registered("W306"));
        assert!(is_registered("E-SEC-001"));
        // E302 was dropped earlier: structurally unreachable with
        // `QualifiedField`-keyed merged rows. Re-adding it needs a registry
        // row, which this asserts is currently absent.
        assert!(!is_registered("E302"));
        assert!(!is_registered("E999"));
    }

    #[test]
    fn registered_severity_comes_from_the_registry_row() {
        assert_eq!(registered_severity("E001"), Some(Severity::Error));
        assert_eq!(registered_severity("W002"), Some(Severity::Warning));
        assert_eq!(registered_severity("E999"), None);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "not registered as Error")]
    fn error_constructor_rejects_a_warning_code() {
        let _ = Diagnostic::error(
            "W002",
            "wrong severity",
            LabeledSpan::primary(Span::SYNTHETIC, ""),
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "not registered as Warning")]
    fn warning_constructor_rejects_an_error_code() {
        let _ = Diagnostic::warning(
            "E001",
            "wrong severity",
            LabeledSpan::primary(Span::SYNTHETIC, ""),
        );
    }
}
