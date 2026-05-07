pub mod compile_context;
pub mod composition;
pub mod node_header;
pub mod pipeline_node;

pub use compile_context::CompileContext;
pub use composition::{
    CompositionFile, CompositionSignature, CompositionSymbolTable, LayerKind, NodeRef, OutputAlias,
    ParamDecl, ParamName, ParamType, PortDecl, PortName, ProvenanceDb, ProvenanceLayer,
    ResolvedValue, Resource, ResourceDecl, ResourceKind, ResourceName, SourceMap, SpannedNodeRef,
    WORKSPACE_COMPOSITION_BUDGET, scan_workspace_signatures, validate_signatures,
};
pub use node_header::{MergeHeader, NodeHeader, NodeInput, SourceHeader};
pub use pipeline_node::{
    AggregateBody, AnalyticWindowSpec, MergeBody, OutputBody, PipelineNode, RouteBody, SourceBody,
    TransformBody,
};

use crate::yaml::Spanned;
use clinker_record::schema_def::{FieldDef, LineSeparator, SchemaDefinition};
use indexmap::IndexMap;
use regex::Regex;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;

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
    /// Kiln IDE metadata: pipeline-level notes. Ignored by the engine.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit: Option<String>,
    /// User-defined variable declarations grouped by scope (pipeline, source,
    /// record). Pipeline-scope defaults seed the runtime registry at init;
    /// source- and record-scope entries are placeholders for runtime writes
    /// by `state` nodes (no readers yet).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vars: Option<ScopedVarsDecl>,
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

/// User-defined variable declarations partitioned by scope.
///
/// Three scopes mirror the CXL read namespaces: `$pipeline.<key>` is
/// init-bound and broadcast to every record; `$source.<key>` is
/// per-source with a fresh slot per ingestion stream; `$record.<key>` is
/// per-record scratch state distinct from `$meta.*` (writable by `state`
/// nodes only, declared with type for compile-time checking). The
/// `record` name (rather than `row`) reflects Clinker's multi-format
/// scope — CSV rows, JSON objects, XML elements, and fixed-width records
/// all flow through the same `Record` type.
///
/// Pipeline-scope defaults flow into [`StableEvalContext.pipeline_vars`]
/// at init via [`convert_pipeline_vars`]. Source- and record-scope
/// readers arrive in a follow-up commit; their declarations parse and
/// validate today so the YAML surface is stable across the sprint.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ScopedVarsDecl {
    #[serde(skip_serializing_if = "IndexMap::is_empty")]
    pub pipeline: IndexMap<String, ScopedVarDecl>,
    #[serde(skip_serializing_if = "IndexMap::is_empty")]
    pub source: IndexMap<String, ScopedVarDecl>,
    #[serde(skip_serializing_if = "IndexMap::is_empty")]
    pub record: IndexMap<String, ScopedVarDecl>,
}

/// One scoped variable's declared type and optional default value.
///
/// `default` must match `var_type` — enforced by [`validate_scoped_vars`].
/// Source- and record-scope variables typically omit the default (their
/// value arrives at runtime from a `state` node write).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedVarDecl {
    #[serde(rename = "type")]
    pub var_type: ScopedVarType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

/// Scoped-variable primitive type set.
///
/// Mirrors the CXL primitive types the typecheck pass uses to validate
/// `$pipeline.<key>` / `$source.<key>` / `$record.<key>` reads. `Date`
/// and `DateTime` accept ISO-8601 string defaults (parsed at the eval
/// boundary, not at YAML load).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScopedVarType {
    String,
    Int,
    Float,
    Bool,
    Date,
    DateTime,
}

impl From<ScopedVarType> for cxl::resolve::ScopedVarType {
    fn from(t: ScopedVarType) -> Self {
        match t {
            ScopedVarType::String => cxl::resolve::ScopedVarType::String,
            ScopedVarType::Int => cxl::resolve::ScopedVarType::Int,
            ScopedVarType::Float => cxl::resolve::ScopedVarType::Float,
            ScopedVarType::Bool => cxl::resolve::ScopedVarType::Bool,
            ScopedVarType::Date => cxl::resolve::ScopedVarType::Date,
            ScopedVarType::DateTime => cxl::resolve::ScopedVarType::DateTime,
        }
    }
}

/// Build a CXL-side [`cxl::resolve::ScopedVarsRegistry`] from a parsed
/// pipeline-level [`ScopedVarsDecl`]. The CXL crate doesn't depend on
/// `clinker-core`, so this conversion lives here at the boundary.
pub fn scoped_vars_registry(decl: &ScopedVarsDecl) -> cxl::resolve::ScopedVarsRegistry {
    cxl::resolve::ScopedVarsRegistry {
        pipeline: decl
            .pipeline
            .iter()
            .map(|(k, d)| (k.clone(), d.var_type.into()))
            .collect(),
        source: decl
            .source
            .iter()
            .map(|(k, d)| (k.clone(), d.var_type.into()))
            .collect(),
        record: decl
            .record
            .iter()
            .map(|(k, d)| (k.clone(), d.var_type.into()))
            .collect(),
    }
}

/// Input source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub name: String,
    /// Literal file path (the simple case). Mutually exclusive with
    /// `glob`/`regex`/`paths`; exactly one of the four must be set.
    /// Validated post-deserialize via `SourceConfig::validate_matching`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Glob pattern matching one or more files (POSIX/gitignore semantics
    /// via the `glob` crate). Mutually exclusive with `path`/`regex`/`paths`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glob: Option<String>,
    /// Regex pattern matched against full paths under the workspace root.
    /// Mutually exclusive with `path`/`glob`/`paths`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    /// Explicit list of files. Mutually exclusive with `path`/`glob`/`regex`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paths: Option<Vec<String>>,

    /// Glob patterns to remove from the discovered file set (gitignore
    /// semantics). Applied after primary discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude: Option<Vec<String>>,
    /// Skip files modified before this point. Accepts a duration relative
    /// to "now" (`"5m"`, `"2h"`, `"3d"`) or an RFC3339 timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_after: Option<TimeBound>,
    /// Skip files modified after this point. Accepts the same forms as
    /// `modified_after`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_before: Option<TimeBound>,
    /// Skip files smaller than this size. Accepts `"1KB"`, `"10MB"`, etc.
    /// (decimal, 1KB = 1000 bytes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_size: Option<ByteSize>,
    /// Skip files larger than this size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_size: Option<ByteSize>,

    /// File-listing controls (sort order, take N, recursion, no-match
    /// policy). Nested to keep the file-level `sort_order` separate from
    /// the existing record-level `sort_order` field below.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<FileListingControls>,

    /// Format-layer schema pointer (e.g. fixed-width field layouts).
    /// Distinct from the CXL-type-level `SourceBody.schema` declared
    /// at the parent `SourceBody` scope — this one points at on-disk
    /// format metadata, the other declares column CXL types for
    /// compile-time typecheck.
    #[serde(rename = "format_schema", skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_overrides: Option<Vec<FieldDef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array_paths: Option<Vec<ArrayPathConfig>>,
    /// Record-level sortedness inside the file (used by combine/aggregate
    /// planning to enable streaming strategies). Distinct from
    /// `files.sort_order` which orders the file set itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<Vec<SortFieldSpec>>,
    #[serde(flatten)]
    pub format: InputFormat,
    /// Kiln IDE metadata: stage notes + field annotations. Ignored by the engine.
    #[serde(default, rename = "_notes", skip_serializing_if = "Option::is_none")]
    pub notes: Option<serde_json::Value>,
}

/// File-listing controls — file-set ordering, take-N, recursion,
/// no-match policy. Nested under `files:` to avoid colliding with the
/// record-level `sort_order:` field at the SourceConfig top level.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FileListingControls {
    /// Sort key for the file set. Default `Name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<FileSortKey>,
    /// Sort direction. Default `Asc`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<SortDir>,
    /// Take only the first N files after sort. Mutually exclusive with
    /// `take_last`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_first: Option<usize>,
    /// Take only the last N files after sort. Mutually exclusive with
    /// `take_first`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_last: Option<usize>,
    /// Walk directories recursively. Default: true if the glob contains
    /// `**`, false otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive: Option<bool>,
    /// Behavior when no files match. Default `Error`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_no_match: Option<NoMatchPolicy>,
}

/// Sort key for ordering the discovered file set.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileSortKey {
    /// Lexicographic sort on the full path.
    #[default]
    Name,
    /// Sort by file creation time (`birthtime` on supporting filesystems;
    /// falls back to mtime where unavailable).
    Created,
    /// Sort by last-modified time.
    Modified,
}

/// Sort direction for the file set ordering.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDir {
    #[default]
    Asc,
    Desc,
}

/// Behavior when discovery matches zero files.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoMatchPolicy {
    /// Fail the run with E216.
    #[default]
    Error,
    /// Log a warning and continue with an empty record stream.
    Warn,
    /// Silently produce an empty record stream.
    Skip,
}

/// Time bound for `modified_after` / `modified_before`. Resolves to a
/// concrete `SystemTime` at parse time. Two YAML forms:
///
/// - **Relative duration**: `"5m"`, `"2h"`, `"3d"`, `"30s"` — interpreted
///   relative to the time of deserialization (effectively "now").
/// - **Absolute timestamp**: any RFC3339 string (`"2026-05-01T00:00:00Z"`).
#[derive(Debug, Clone, Copy)]
pub struct TimeBound(pub std::time::SystemTime);

impl TimeBound {
    /// Parse `"<n><unit>"` with unit ∈ {s, m, h, d}. Returns `None` if the
    /// string is not in this form.
    fn parse_duration(s: &str) -> Option<std::time::Duration> {
        let (num_str, unit) = s.split_at(s.len().checked_sub(1)?);
        let n: u64 = num_str.parse().ok()?;
        let secs = match unit {
            "s" => n,
            "m" => n.checked_mul(60)?,
            "h" => n.checked_mul(60 * 60)?,
            "d" => n.checked_mul(60 * 60 * 24)?,
            _ => return None,
        };
        Some(std::time::Duration::from_secs(secs))
    }
}

impl Serialize for TimeBound {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        // Round-trip as RFC3339 — the relative form is parse-time only.
        let dt: chrono::DateTime<chrono::Utc> = self.0.into();
        s.serialize_str(&dt.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for TimeBound {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        if let Some(dur) = Self::parse_duration(&raw) {
            // Relative-to-now: clip rather than panic if the duration
            // somehow underflows (it won't for any reasonable value).
            let when = std::time::SystemTime::now()
                .checked_sub(dur)
                .unwrap_or(std::time::UNIX_EPOCH);
            return Ok(TimeBound(when));
        }
        let dt = chrono::DateTime::parse_from_rfc3339(&raw).map_err(|e| {
            de::Error::custom(format!(
                "expected duration like \"5m\"/\"2h\"/\"3d\" or RFC3339 timestamp; \
                 got {raw:?}: {e}"
            ))
        })?;
        Ok(TimeBound(dt.with_timezone(&chrono::Utc).into()))
    }
}

/// Byte size for `min_size` / `max_size`. Decimal units (1KB = 1000 bytes,
/// 1MB = 1_000_000) — matches the convention used by `du`, `df`, AWS CLI,
/// and most file-tooling. Plain `"1024"` (no unit) is interpreted as bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub u64);

impl ByteSize {
    fn parse(s: &str) -> Option<u64> {
        let s = s.trim();
        let (num_part, mult) = if let Some(rest) = s.strip_suffix("GB") {
            (rest, 1_000_000_000)
        } else if let Some(rest) = s.strip_suffix("MB") {
            (rest, 1_000_000)
        } else if let Some(rest) = s.strip_suffix("KB") {
            (rest, 1_000)
        } else if let Some(rest) = s.strip_suffix('B') {
            (rest, 1)
        } else {
            (s, 1)
        };
        let n: u64 = num_part.trim().parse().ok()?;
        n.checked_mul(mult)
    }
}

impl Serialize for ByteSize {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{}B", self.0))
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Accept either an integer (bytes) or a string with a unit suffix.
        struct V;
        impl<'de> de::Visitor<'de> for V {
            type Value = ByteSize;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a byte size as integer or string like \"1KB\"/\"100MB\"")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<ByteSize, E> {
                Ok(ByteSize(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<ByteSize, E> {
                u64::try_from(v)
                    .map(ByteSize)
                    .map_err(|_| de::Error::custom("byte size cannot be negative"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<ByteSize, E> {
                ByteSize::parse(v).map(ByteSize).ok_or_else(|| {
                    de::Error::custom(format!(
                        "expected byte size like \"100\"/\"1KB\"/\"5MB\"/\"2GB\"; got {v:?}"
                    ))
                })
            }
        }
        d.deserialize_any(V)
    }
}

/// Adjacently tagged format enum for inputs.
/// `type` selects the format, `options` provides format-specific settings.
/// `options` is optional — `type: csv` with no `options:` key gives `Csv(None)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "options", rename_all = "snake_case")]
pub enum InputFormat {
    Csv(Option<CsvInputOptions>),
    Json(Option<JsonInputOptions>),
    Xml(Option<XmlInputOptions>),
    FixedWidth(Option<FixedWidthInputOptions>),
}

impl InputFormat {
    /// Short lowercase format name for display.
    pub fn format_name(&self) -> &'static str {
        match self {
            InputFormat::Csv(_) => "csv",
            InputFormat::Json(_) => "json",
            InputFormat::Xml(_) => "xml",
            InputFormat::FixedWidth(_) => "fixed_width",
        }
    }

    /// Whether this format has a header row concept (CSV only).
    pub fn has_header(&self) -> Option<bool> {
        match self {
            InputFormat::Csv(Some(opts)) => opts.has_header,
            _ => None,
        }
    }
}

impl OutputFormat {
    /// Short lowercase format name for display.
    pub fn format_name(&self) -> &'static str {
        match self {
            OutputFormat::Csv(_) => "csv",
            OutputFormat::Json(_) => "json",
            OutputFormat::Xml(_) => "xml",
            OutputFormat::FixedWidth(_) => "fixed_width",
        }
    }
}

/// CSV-specific input options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CsvInputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_char: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_header: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
}

/// JSON-specific input options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct JsonInputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<JsonFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_path: Option<String>,
}

/// XML-specific input options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct XmlInputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_handling: Option<NamespaceHandling>,
}

/// JSON input format mode (auto-detect if not specified).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonFormat {
    Array,
    Ndjson,
    Object,
}

/// XML namespace handling mode.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceHandling {
    #[default]
    Strip,
    Qualify,
}

/// Array path configuration for nested array explosion/joining.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayPathConfig {
    pub path: String,
    #[serde(default)]
    pub mode: ArrayMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub separator: Option<String>,
}

/// Array path processing mode.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrayMode {
    #[default]
    Explode,
    Join,
}

impl SourceConfig {
    /// Get CSV input options, if this is a CSV input.
    pub fn csv_options(&self) -> Option<&CsvInputOptions> {
        match &self.format {
            InputFormat::Csv(opts) => opts.as_ref(),
            _ => None,
        }
    }

    /// Borrow the literal `path:` for the simple-case source. Returns `""`
    /// for sources using `glob`/`regex`/`paths` matchers — the latter
    /// resolve to a `Vec<PathBuf>` at discovery time and don't have a
    /// single literal path to display.
    ///
    /// Use this only on call sites that genuinely want the literal path
    /// (display, security checks of one path, schema-stem extraction for
    /// the simple case). For runtime file enumeration use the discovery
    /// module instead.
    pub fn path_str(&self) -> &str {
        self.path.as_deref().unwrap_or("")
    }

    /// Human-readable target description for diagnostics and Kiln display.
    /// Reflects the active matcher (`path` / `glob` / `regex` / `paths`).
    pub fn display_target(&self) -> String {
        if let Some(p) = self.path.as_deref() {
            p.to_string()
        } else if let Some(g) = self.glob.as_deref() {
            format!("glob: {g}")
        } else if let Some(r) = self.regex.as_deref() {
            format!("regex: {r}")
        } else if let Some(ps) = self.paths.as_deref() {
            format!("paths: [{}]", ps.join(", "))
        } else {
            String::new()
        }
    }
}

/// Output destination configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub include_unmapped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_header: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapping: Option<IndexMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<Vec<SortFieldSpec>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preserve_nulls: Option<bool>,
    /// Controls whether per-record `$meta.*` metadata is included in output.
    /// Default: none (metadata stripped from output).
    #[serde(default, skip_serializing_if = "IncludeMetadata::is_none")]
    pub include_metadata: IncludeMetadata,
    /// Controls whether engine-stamped correlation snapshot columns
    /// (`$ck.<field>`) appear in the default writer output. The shadow
    /// columns preserve correlation-group identity through Transforms
    /// that may rewrite the user-declared field; they are an internal
    /// engine namespace and are stripped from output unless this flag
    /// is set. Defaults to `false`.
    #[serde(default)]
    pub include_correlation_keys: bool,
    /// Explicit schema for output formats that require field definitions
    /// (e.g., fixed-width output needs field names, widths, and positions).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaSource>,
    /// File splitting configuration. When present, output is split into
    /// multiple files based on record count or byte size limits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split: Option<SplitConfig>,
    /// Optional per-Output override for the correlation fan-out policy.
    /// Wins against the per-Combine override and the per-pipeline default
    /// because the sink has the most context for whether collateral
    /// rollback is acceptable (audit-style sinks typically opt down to
    /// `Primary` or `All`; integrity-style sinks keep the default `Any`).
    /// Additive opt-in: `None` defers to upstream resolution, preserving
    /// today's behavior unchanged for every existing pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_fanout_policy: Option<CorrelationFanoutPolicy>,
    /// Collision policy when the resolved output path already exists.
    /// Defaults to `Overwrite` to preserve today's `File::create` behavior.
    #[serde(default, skip_serializing_if = "IfExistsPolicy::is_default")]
    pub if_exists: IfExistsPolicy,
    /// Zero-pad width for the `{n}` collision counter when
    /// `if_exists = unique_suffix`. `0` (default) emits the bare integer.
    #[serde(default, skip_serializing_if = "is_zero_u8")]
    pub unique_suffix_width: u8,
    /// Write a `<resolved_path>.meta.json` provenance sidecar after the
    /// output stream is flushed. Opt-in.
    #[serde(default, skip_serializing_if = "is_false_bool")]
    pub write_meta: bool,
    #[serde(flatten)]
    pub format: OutputFormat,
    /// Kiln IDE metadata: stage notes + field annotations. Ignored by the engine.
    #[serde(default, rename = "_notes", skip_serializing_if = "Option::is_none")]
    pub notes: Option<serde_json::Value>,
}

/// Controls which `$meta.*` fields appear in output.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncludeMetadata {
    /// No metadata in output (default).
    #[default]
    None,
    /// Include all metadata fields, prefixed with `meta.`.
    All,
    /// Include only the listed metadata keys, prefixed with `meta.`.
    Allowlist(Vec<String>),
}

impl IncludeMetadata {
    pub fn is_none(&self) -> bool {
        matches!(self, IncludeMetadata::None)
    }
}

/// Collision policy when an Output node's resolved path already exists.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IfExistsPolicy {
    /// Truncate and overwrite. Today's `File::create` behavior.
    #[default]
    Overwrite,
    /// Refuse to clobber; emit a diagnostic. `--force` downgrades to `Overwrite`.
    Error,
    /// Walk integer suffixes via `OpenOptions::create_new` until one succeeds.
    UniqueSuffix,
}

impl IfExistsPolicy {
    pub fn is_default(&self) -> bool {
        matches!(self, IfExistsPolicy::Overwrite)
    }
}

fn is_zero_u8(n: &u8) -> bool {
    *n == 0
}

fn is_false_bool(b: &bool) -> bool {
    !*b
}

/// Output file splitting configuration.
///
/// When `group_key` is set, files are split only at key-group boundaries
/// (greedy: first boundary after threshold). Without `group_key`, mechanical
/// split at exact limit (like `split -l`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitConfig {
    /// Soft record count limit per file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_records: Option<u64>,
    /// Soft byte size limit per file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    /// Optional key field — never split mid-group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_key: Option<String>,
    /// File naming pattern. Default: `"{stem}_{seq:04}.{ext}"`.
    #[serde(default = "default_split_naming")]
    pub naming: String,
    /// CSV: repeat header row in each split file. Default: true.
    #[serde(default = "default_true")]
    pub repeat_header: bool,
    /// Behavior when a single key group exceeds file limits.
    #[serde(default)]
    pub oversize_group: SplitOversizeGroupPolicy,
}

fn default_split_naming() -> String {
    "{stem}_{seq:04}.{ext}".to_string()
}

fn default_true() -> bool {
    true
}

/// Policy when a single key group exceeds split file limits.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SplitOversizeGroupPolicy {
    /// Log a warning and allow the oversized file.
    #[default]
    Warn,
    /// Error — pipeline stops.
    Error,
    /// Silently allow.
    Allow,
}

/// Adjacently tagged format enum for outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "options", rename_all = "snake_case")]
pub enum OutputFormat {
    Csv(Option<CsvOutputOptions>),
    Json(Option<JsonOutputOptions>),
    Xml(Option<XmlOutputOptions>),
    FixedWidth(Option<FixedWidthOutputOptions>),
}

/// CSV-specific output options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CsvOutputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
}

/// JSON-specific output options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct JsonOutputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<JsonOutputFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pretty: Option<bool>,
}

/// JSON output format mode.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonOutputFormat {
    #[default]
    Array,
    Ndjson,
}

/// XML-specific output options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct XmlOutputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_element: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_element: Option<String>,
}

/// Fixed-width input options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FixedWidthInputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_separator: Option<LineSeparator>,
}

/// Fixed-width output options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FixedWidthOutputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_separator: Option<LineSeparator>,
}

/// Sort field specification for output and window partition ordering.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortField {
    pub field: String,
    #[serde(default = "default_sort_order")]
    pub order: SortOrder,
    /// Null handling during sort. None for output sorting; Some(Last) default for windows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_order: Option<NullOrder>,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Accepts either a plain string shorthand or a full SortField object in YAML.
///
/// Shorthand: `"field_name"` expands to `SortField { field: "field_name", order: Asc, null_order: None }`.
/// Full: `{ field: "name", order: desc, null_order: first }` deserializes as SortField.
///
/// Custom Deserialize: visit_str -> Short, visit_map -> Full.
/// This gives specific error messages instead of serde(untagged)'s generic "no variant matched".
#[derive(Debug, Clone, Serialize)]
pub enum SortFieldSpec {
    Short(String),
    Full(SortField),
}

impl<'de> Deserialize<'de> for SortFieldSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SortFieldSpecVisitor;

        impl<'de> Visitor<'de> for SortFieldSpecVisitor {
            type Value = SortFieldSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a field name (string) or a sort field object (map with 'field', 'order', 'null_order')")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SortFieldSpec::Short(v.to_owned()))
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let sf = SortField::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(SortFieldSpec::Full(sf))
            }
        }

        deserializer.deserialize_any(SortFieldSpecVisitor)
    }
}

impl SortFieldSpec {
    /// Resolve to a concrete SortField.
    pub fn into_sort_field(self) -> SortField {
        match self {
            SortFieldSpec::Short(name) => SortField {
                field: name,
                order: SortOrder::Asc,
                null_order: None,
            },
            SortFieldSpec::Full(sf) => sf,
        }
    }
}

fn default_sort_order() -> SortOrder {
    SortOrder::Asc
}

/// Null handling in sort operations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum NullOrder {
    /// Nulls sort before all non-null values.
    First,
    /// Nulls sort after all non-null values (SQL convention default).
    #[default]
    Last,
    /// Remove records with null sort keys from the partition.
    Drop,
}

/// Supported format types.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FormatKind {
    Csv,
    Json,
    Xml,
    #[serde(rename = "fixed_width")]
    FixedWidth,
}

/// Schema source -- file path or inline definition.
/// Custom Deserialize: YAML string -> FilePath, YAML map -> Inline(SchemaDefinition).
#[derive(Debug, Clone, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum SchemaSource {
    FilePath(String),
    Inline(SchemaDefinition),
}

impl<'de> Deserialize<'de> for SchemaSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SchemaSourceVisitor;

        impl<'de> Visitor<'de> for SchemaSourceVisitor {
            type Value = SchemaSource;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter
                    .write_str("a schema file path (string) or an inline schema definition (map)")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SchemaSource::FilePath(v.to_owned()))
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let def =
                    SchemaDefinition::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(SchemaSource::Inline(def))
            }
        }

        deserializer.deserialize_any(SchemaSourceVisitor)
    }
}

/// Routing mode for record dispatch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteMode {
    /// First-match: evaluate predicates in order, first true wins.
    #[default]
    Exclusive,
    /// All-match: evaluate all predicates, record sent to every matching branch.
    Inclusive,
}

/// A named routing branch with a CXL boolean condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteBranch {
    pub name: String,
    pub condition: String,
}

/// Route configuration for multi-output record dispatch.
///
/// Conditions are CXL boolean expressions evaluated per record.
/// Mandatory `default` prevents silent record drops.
#[derive(Debug, Clone, Serialize)]
pub struct RouteConfig {
    #[serde(default)]
    pub mode: RouteMode,
    pub branches: Vec<RouteBranch>,
    pub default: String,
}

impl<'de> serde::Deserialize<'de> for RouteConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            mode: RouteMode,
            branches: Vec<RouteBranch>,
            default: Option<String>,
        }

        let raw = Raw::deserialize(deserializer)?;

        // Mandatory default
        let default = raw
            .default
            .ok_or_else(|| serde::de::Error::custom("route must have a 'default' output name"))?;

        // Non-empty branches
        if raw.branches.is_empty() {
            return Err(serde::de::Error::custom(
                "route must have at least one branch",
            ));
        }

        // Max 256 outputs
        if raw.branches.len() > 256 {
            return Err(serde::de::Error::custom(format!(
                "route has {} branches, maximum is 256",
                raw.branches.len()
            )));
        }

        // Unique branch names
        let mut seen = std::collections::HashSet::new();
        for branch in &raw.branches {
            if !seen.insert(&branch.name) {
                return Err(serde::de::Error::custom(format!(
                    "duplicate route branch name '{}'",
                    branch.name
                )));
            }
        }

        // Default must not collide with a branch name
        if seen.contains(&default) {
            return Err(serde::de::Error::custom(format!(
                "route default '{}' collides with a branch name",
                default
            )));
        }

        Ok(RouteConfig {
            mode: raw.mode,
            branches: raw.branches,
            default,
        })
    }
}

/// Input wiring for a transform — specifies which upstream transform(s) feed records.
///
/// String values become `Single`; arrays become `Multiple`.
/// Custom deserialization handles both forms.
#[derive(Debug, Clone, Serialize)]
pub enum TransformInput {
    /// Single upstream: `"categorize.high_value"` or `"transform_name"`.
    Single(String),
    /// Multiple upstreams (union): `["branch_a", "branch_b"]`.
    Multiple(Vec<String>),
}

impl<'de> Deserialize<'de> for TransformInput {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TransformInputVisitor;

        impl<'de> de::Visitor<'de> for TransformInputVisitor {
            type Value = TransformInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or array of strings")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(TransformInput::Single(v.to_owned()))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element::<String>()? {
                    items.push(item);
                }
                if items.is_empty() {
                    return Err(de::Error::custom(
                        "transform input array must not be empty (use a single string for one upstream, or omit for default flow)",
                    ));
                }
                Ok(TransformInput::Multiple(items))
            }
        }

        deserializer.deserialize_any(TransformInputVisitor)
    }
}

/// User-supplied hint for aggregation execution strategy.
///
/// `Auto` (default) lets the optimizer pick Hash vs Streaming based on
/// upstream `OrderingProvenance` and the `qualifies_for_streaming` rules.
/// `Hash` and `Streaming` are user overrides modeled on Informatica's
/// `sorted_input` flag — `Streaming` is a declared performance contract:
/// if the input is not provably sorted for the group-by keys, the
/// planner hard-errors at compile time rather than silently inserting
/// a sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateStrategyHint {
    /// Optimizer chooses based on upstream ordering (default).
    #[default]
    Auto,
    /// Force hash aggregation regardless of input ordering.
    Hash,
    /// Require streaming aggregation; compile-time error if input is
    /// not provably sorted for the group-by keys.
    Streaming,
}

/// Configuration for GROUP BY aggregation on a transform.
///
/// Nested `aggregate:` block follows the universal ETL pattern (Beam YAML
/// Combine, SOPE `group_by`, Informatica Aggregator).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateConfig {
    /// Fields to group by. Empty = global fold (one output row).
    #[serde(default)]
    pub group_by: Vec<String>,
    /// CXL source with aggregate function calls.
    pub cxl: String,
    /// User-supplied execution strategy hint. Defaults to `Auto`.
    /// Resolved to a concrete `AggregateStrategy` by the
    /// `select_aggregation_strategies` post-pass in 16.4.9.
    #[serde(default)]
    pub strategy: AggregateStrategyHint,
}

/// User-supplied hint for combine execution strategy.
///
/// `Auto` (default) lets [`crate::plan::combine::select_combine_strategies`]
/// pick from predicate shape and cardinality estimates. `GraceHash` is a
/// user override that forces the disk-spilling partitioned hash join even
/// when cardinality estimates are absent or below the soft-limit threshold
/// — useful for benchmarks and for production pipelines where the user
/// knows the build side does not fit in memory.
///
/// Mirrors [`AggregateStrategyHint`] in shape. `GraceHash` only applies to
/// pure-equi predicates; the planner ignores the hint on mixed equi+range
/// or pure-range nodes, where partition-IEJoin / IEJoin / SortMerge remain
/// the correct strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CombineStrategyHint {
    /// Optimizer picks the strategy (default).
    #[default]
    Auto,
    /// Force grace hash join — disk-spilling partitioned hash. Applies
    /// only to pure-equi predicates; ignored otherwise.
    GraceHash,
}

/// A declarative validation attached to a transform.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationEntry {
    pub name: Option<String>,
    pub field: Option<String>,
    pub check: String,
    pub args: Option<IndexMap<String, serde_json::Value>>,
    #[serde(default = "default_severity")]
    pub severity: ValidationSeverity,
    pub message: Option<String>,
}

fn default_severity() -> ValidationSeverity {
    ValidationSeverity::Error
}

impl ValidationEntry {
    /// Auto-derive name from field and check if not specified.
    pub fn resolved_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| match &self.field {
            Some(f) => format!("{}:{}", f, self.check),
            None => self.check.clone(),
        })
    }
}

/// Validation severity: error routes to DLQ, warn logs and continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationSeverity {
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "warn")]
    Warn,
}

/// A logging directive attached to a transform.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogDirective {
    pub level: LogLevel,
    pub when: LogTiming,
    pub condition: Option<String>,
    pub message: String,
    pub fields: Option<Vec<String>>,
    pub every: Option<u64>,
    pub log_rule: Option<String>,
}

/// When a log directive fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogTiming {
    #[serde(rename = "before_transform")]
    BeforeTransform,
    #[serde(rename = "after_transform")]
    AfterTransform,
    #[serde(rename = "per_record")]
    PerRecord,
    #[serde(rename = "on_error")]
    OnError,
}

/// Log level for directives (YAML config domain).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    #[serde(rename = "trace")]
    Trace,
    #[serde(rename = "debug")]
    Debug,
    #[serde(rename = "info")]
    Info,
    #[serde(rename = "warn")]
    Warn,
    #[serde(rename = "error")]
    Error,
}

/// Lightweight read-only view over a transform-like node
/// (`Transform`, `Aggregate`, `Route`) yielded by
/// [`PipelineConfig::transform_views`]. Carries the fields the Kiln IDE
/// and schema-validation passes need; callers that need variant-specific
/// bodies (`TransformBody`, `AggregateBody`, etc.) should walk
/// [`PipelineConfig::nodes`] directly.
#[derive(Debug, Clone, Copy)]
pub struct TransformView<'a> {
    pub name: &'a str,
    pub description: Option<&'a str>,
    pub cxl_source: &'a str,
    pub notes: Option<&'a serde_json::Value>,
    pub kind: TransformViewKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformViewKind {
    Transform,
    Aggregate,
    Route,
}

impl<'a> TransformView<'a> {
    pub fn cxl_source(&self) -> &'a str {
        self.cxl_source
    }
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

    /// Public iterator over output nodes.
    pub fn output_configs(&self) -> impl Iterator<Item = &OutputConfig> + '_ {
        self.nodes.iter().filter_map(|n| match &n.value {
            PipelineNode::Output { config: body, .. } => Some(&body.output),
            _ => None,
        })
    }

    /// Public iterator over transform-like nodes (Transform + Aggregate +
    /// Route), yielding a lightweight [`TransformView`] with the minimum
    /// surface the Kiln IDE + schema validation need. Merge nodes are
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
    pub fn compile_topology_only(&self, ctx: &CompileContext) -> Vec<crate::error::Diagnostic> {
        use crate::error::{Diagnostic, LabeledSpan};
        use crate::graph::NameGraph;
        use crate::span::Span;
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
            let mut self_ref = false;
            match node {
                PipelineNode::Source { .. } => {}
                PipelineNode::Transform { header, .. }
                | PipelineNode::Aggregate { header, .. }
                | PipelineNode::Route { header, .. }
                | PipelineNode::Output { header, .. }
                | PipelineNode::Composition { header, .. } => {
                    if input_target(&header.input.value) == name {
                        self_ref = true;
                    }
                }
                PipelineNode::Merge { header, .. } => {
                    if header.inputs.iter().any(|i| input_target(&i.value) == name) {
                        self_ref = true;
                    }
                }
                PipelineNode::Combine { header, .. } => {
                    if header
                        .input
                        .values()
                        .any(|i| input_target(&i.value) == name)
                    {
                        self_ref = true;
                    }
                }
            }
            if self_ref {
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
            match node {
                PipelineNode::Source { .. } => {}
                PipelineNode::Transform { header, .. }
                | PipelineNode::Aggregate { header, .. }
                | PipelineNode::Route { header, .. }
                | PipelineNode::Output { header, .. }
                | PipelineNode::Composition { header, .. } => {
                    let producer = input_target(&header.input.value);
                    if producer != consumer && graph.index_of(producer).is_some() {
                        graph.add_edge(producer, consumer);
                    }
                }
                PipelineNode::Merge { header, .. } => {
                    for i in &header.inputs {
                        let producer = input_target(&i.value);
                        if producer != consumer && graph.index_of(producer).is_some() {
                            graph.add_edge(producer, consumer);
                        }
                    }
                }
                PipelineNode::Combine { header, .. } => {
                    for i in header.input.values() {
                        let producer = input_target(&i.value);
                        if producer != consumer && graph.index_of(producer).is_some() {
                            graph.add_edge(producer, consumer);
                        }
                    }
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
    ) -> Result<crate::plan::CompiledPlan, Vec<crate::error::Diagnostic>> {
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
        (crate::plan::CompiledPlan, Vec<crate::error::Diagnostic>),
        Vec<crate::error::Diagnostic>,
    > {
        use crate::config::composition::scan_workspace_signatures;
        use crate::error::{Diagnostic, LabeledSpan};
        use crate::plan::CompiledPlan;
        use crate::plan::execution::{
            DependencyType, ExecutionPlanDag, ParallelismProfile, PlanEdge, PlanNode,
        };
        use crate::span::Span;
        use petgraph::graph::{DiGraph, NodeIndex};
        use std::collections::HashMap;

        // Stage 1-4: name/topology/path validation pre-pass.
        let mut diags = self.compile_topology_only(ctx);
        // Hard-error stop: stages 1-4 already collected; stage 5
        // refuses to lower if any error-severity diagnostic is present.
        let has_errors = diags
            .iter()
            .any(|d| matches!(d.severity, crate::error::Severity::Error));
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
                        .any(|d| matches!(d.severity, crate::error::Severity::Error));
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
        let scoped_vars_registry = self
            .pipeline
            .vars
            .as_ref()
            .map(scoped_vars_registry)
            .unwrap_or_default();
        let mut artifacts = crate::plan::bind_schema::bind_schema(
            &self.nodes,
            &mut diags,
            ctx,
            &symbol_table,
            &ctx.pipeline_dir,
            scoped_vars_registry,
        );
        // Only abort on non-composition CXL errors (E200/E201) and
        // source-CK validation errors (E153). Composition binding
        // errors (E102–E109) are non-fatal for the rest of the pipeline
        // — the composition node is omitted from the DAG.
        let has_cxl_errors = diags.iter().any(|d| {
            matches!(d.severity, crate::error::Severity::Error)
                && (d.code == "E200" || d.code == "E201" || d.code == "E153")
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
            analytic_window: Option<serde_json::Value>,
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
        // Keep an analysis-by-name index so we can surface the analysis
        // alongside its matching entry regardless of filter order.
        let mut analysis_by_name: HashMap<String, &cxl::analyzer::TransformAnalysis> =
            HashMap::new();
        for a in &report.transforms {
            analysis_by_name.insert(a.name.clone(), a);
        }
        let window_configs: Vec<Option<crate::plan::index::LocalWindowConfig>> = entries
            .iter()
            .map(|e| crate::plan::index::parse_analytic_window_value(&e.analytic_window, &e.name))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                vec![Diagnostic::error(
                    "E003",
                    format!("analytic_window parse error: {e}"),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                )]
            })?;
        // Validate: if a transform uses window.* but has no local_window, error.
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
                            "transform '{}' uses window.* functions but declares no local_window",
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
                            "transform '{}' references unknown source '{}' in local_window",
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
            let consumer_name = node.name();
            let Some(&consumer_idx) = name_to_idx.get(consumer_name) else {
                continue;
            };
            let mut wire = |producer_full: &str, port: Option<String>| {
                let producer_key = strip_port_for_edge(producer_full);
                if let Some(&producer_idx) = name_to_idx.get(producer_key) {
                    graph.add_edge(
                        producer_idx,
                        consumer_idx,
                        PlanEdge {
                            dependency_type: DependencyType::Data,
                            port,
                        },
                    );
                }
            };
            match node {
                PipelineNode::Source { .. } => {}
                PipelineNode::Transform { header, .. }
                | PipelineNode::Aggregate { header, .. }
                | PipelineNode::Route { header, .. }
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
                crate::plan::index::PlanIndexRoot::Source(other)
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
                                 local_window requires a predecessor in the DAG",
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
                    crate::plan::execution::PlanNode::Source { name, .. } => {
                        // Source-rooted: `wc.source` is `None` (or matches
                        // the primary) and the immediate non-pass-through
                        // ancestor is a `Source`. The arena builds at
                        // Phase-0 from that source's stream — source-rooted
                        // lookups go through the source name in
                        // `pipeline/ingestion.rs`.
                        crate::plan::index::PlanIndexRoot::Source(name.clone())
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

            let already_sorted = match &root {
                crate::plan::index::PlanIndexRoot::Source(name) => {
                    crate::plan::execution::check_already_sorted(&source_configs, name, &wc.sort_by)
                }
                // Node-rooted / parent-node-rooted arenas have no
                // declared source ordering — partitions are sorted
                // post-build at the upstream-arm exit. Treat as
                // unsorted; the executor's per-partition `sort_partition`
                // call normalizes the slice before window evaluation.
                crate::plan::index::PlanIndexRoot::Node { .. }
                | crate::plan::index::PlanIndexRoot::ParentNode { .. } => false,
            };

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
                crate::plan::index::PlanIndexRoot::Source(other)
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
                match &graph[rooted_idx] {
                    crate::plan::execution::PlanNode::Source { name, .. } => {
                        crate::plan::index::PlanIndexRoot::Source(name.clone())
                    }
                    other => {
                        let Some(anchor_schema) = other.stored_output_schema().cloned() else {
                            continue;
                        };
                        crate::plan::index::PlanIndexRoot::Node {
                            upstream: rooted_idx,
                            anchor_schema,
                        }
                    }
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
            .any(|d| matches!(d.severity, crate::error::Severity::Error))
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
            self.pipeline.memory_limit.as_deref(),
        );
        // Companion sweep over composition body mini-DAGs. Body
        // graphs hold their own `PlanNode::Combine` nodes that the
        // top-level pass above cannot reach, so without this call
        // body-context combines never get their strategy + driving
        // qualifier stamped and short-circuit at dispatch.
        crate::plan::combine::select_combine_strategies_in_bodies(
            &mut artifacts,
            &mut diags,
            self.pipeline.memory_limit.as_deref(),
        );

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
                    // 1. Node-rooted / ParentNode-rooted spec: the
                    //    arena materializes from the upstream operator's
                    //    emit buffer, not from per-CK-group source rows,
                    //    so the original "per-group arena construction"
                    //    concern does not apply. CK-aligned partitions
                    //    fall under this case.
                    // 2. Source-rooted spec in buffer-recompute mode:
                    //    the orchestrator's commit phase reruns the
                    //    window over surviving partition rows after a
                    //    CK group is retracted, so the source-rooted
                    //    arena is safe to materialize.
                    let safe = dag
                        .indices_to_build
                        .get(*idx_num)
                        .map(|s| {
                            !matches!(s.root, crate::plan::index::PlanIndexRoot::Source(_))
                                || s.requires_buffer_recompute
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
            matches!(d.severity, crate::error::Severity::Error) && !d.code.starts_with("E10")
        });
        if has_fatal_errors {
            return Err(diags);
        }

        let plan = CompiledPlan::new(dag, self.clone(), artifacts);
        Ok((plan, diags))
    }
}

fn input_target(input: &node_header::NodeInput) -> &str {
    match input {
        node_header::NodeInput::Single(s) => s.as_str(),
        node_header::NodeInput::Port { node, .. } => node.as_str(),
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

/// Unified input-reference resolution pass. Walks every node's
/// declared input(s) and emits [`Diagnostic`] code `E004` with a
/// structured [`crate::error::DiagnosticPayload::InputRefUndeclared`]
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
    diags: &mut Vec<crate::error::Diagnostic>,
) {
    use crate::error::{Diagnostic, DiagnosticPayload, LabeledSpan};
    use crate::span::Span;

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
        }
    }
}

/// Cross-cutting inputs threaded into [`lower_node_to_plan_node`] for
/// variants that need derived fields (Transform, Aggregate).
///
/// Top-level callers in `compile_with_diagnostics` populate every field
/// from the already-computed analyzer report / window configs;
/// body-node callers in `bind_composition` use
/// [`LoweringCtx::default`] (all fields `None`/empty), which falls back
/// to minimal placeholder lowering suitable for Kiln drill-in
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
    pub window_config: Option<&'a crate::plan::index::LocalWindowConfig>,
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
    span: crate::span::Span,
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
    ctx: &LoweringCtx<'_>,
    diags: &mut Vec<crate::error::Diagnostic>,
) -> Option<crate::plan::execution::PlanNode> {
    use crate::aggregation::AggregateStrategy;
    use crate::error::{Diagnostic, LabeledSpan};
    use crate::plan::composition_body::CompositionBodyId;
    use crate::plan::execution::{
        NodeExecutionReqs, ParallelismClass, PartitionLookupKind, PlanNode, PlanOutputPayload,
        PlanSourcePayload, PlanTransformPayload, derive_parallelism_class, extract_has_distinct,
        extract_write_set,
    };
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
        PipelineNode::Source { config, .. } => Some(PlanNode::Source {
            name: name.to_string(),
            span,
            resolved: Some(Box::new(PlanSourcePayload {
                source: config.source.clone(),
                validated_path: None,
            })),
            output_schema: schema_from_bound(name),
        }),
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
            // shape — this is fine for Kiln drill-in inspection; body
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
            Some(crate::plan::execution::PlanNode::Combine {
                name: name.to_string(),
                span,
                strategy: CombineStrategy::HashBuildProbe,
                driving_input: String::new(),
                build_inputs: Vec::new(),
                predicate_summary,
                match_mode: config.match_mode,
                on_miss: config.on_miss,
                propagate_ck: config.propagate_ck.clone(),
                decomposed_from: None,
                output_schema: schema_from_bound(name),
                resolved_column_map,
            })
        }
    }
}

/// Correlation key for grouped DLQ rejection.
///
/// When set on [`ErrorHandlingConfig`], every record in a group sharing a
/// correlation-key value is DLQ'd atomically if any single record in that
/// group fails Transform / Route eval / Output write. Group identity is
/// fixed at ingest: a Transform that rewrites the key field does not
/// change a row's group. Custom [`Deserialize`] accepts a YAML scalar
/// (`correlation_key: foo`) for a single-field key or a sequence
/// (`correlation_key: [a, b]`) for a compound key.
#[derive(Debug, Clone, Serialize)]
pub enum CorrelationKey {
    Single(String),
    Compound(Vec<String>),
}

impl CorrelationKey {
    /// Field names that compose this correlation key, in declaration order.
    pub fn fields(&self) -> Vec<&str> {
        match self {
            Self::Single(f) => vec![f.as_str()],
            Self::Compound(fs) => fs.iter().map(|f| f.as_str()).collect(),
        }
    }
}

impl<'de> Deserialize<'de> for CorrelationKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CorrelationKeyVisitor;

        impl<'de> Visitor<'de> for CorrelationKeyVisitor {
            type Value = CorrelationKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string (single key) or array of strings (compound key)")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(CorrelationKey::Single(v.to_string()))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut fields = Vec::new();
                while let Some(field) = seq.next_element::<String>()? {
                    fields.push(field);
                }
                if fields.is_empty() {
                    return Err(de::Error::custom("correlation_key array must not be empty"));
                }
                if fields.len() == 1 {
                    return Ok(CorrelationKey::Single(fields.remove(0)));
                }
                Ok(CorrelationKey::Compound(fields))
            }
        }

        deserializer.deserialize_any(CorrelationKeyVisitor)
    }
}

fn default_max_group_buffer() -> Option<u64> {
    Some(100_000)
}

/// Selects how a triggered correlation group's collateral records are
/// disposed at commit time.
///
/// Resolution precedence (latter wins): per-pipeline default →
/// per-Combine override (per-input-set fan-out shape) → per-Output override
/// (per-sink fan-out shape). The override surface lets audit-style sinks
/// keep failing-group records that an integrity-style sink would discard.
///
/// * `Any` — every record sharing any correlation-key field with a
///   triggering record is collateral-DLQ'd. The default; matches "if any
///   contributing source had bad data, the joined output is suspect."
/// * `All` — only records sharing the FULL correlation-key tuple with a
///   trigger are collateral-DLQ'd. Records that derived only some CK
///   columns from a failing source pass through to the writer.
/// * `Primary` — only records on the primary correlation-key field
///   (first-listed in the source's `correlation_key`) face collateral
///   rollback. Audit-dump opt-out for sinks that retain enough provenance
///   to accept partial-rollback semantics.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CorrelationFanoutPolicy {
    #[default]
    Any,
    All,
    Primary,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DlqConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_reason: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_source_row: Option<bool>,
}

/// Errors during config loading and parsing.
#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Yaml(crate::yaml::YamlError),
    EnvVar { var_name: String, position: usize },
    Validation(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "config I/O error: {e}"),
            Self::Yaml(e) => write!(f, "YAML parse error: {e}"),
            Self::EnvVar { var_name, position } => {
                write!(
                    f,
                    "undefined environment variable ${{{var_name}}} at position {position}"
                )
            }
            Self::Validation(msg) => write!(f, "config validation error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<crate::yaml::YamlError> for ConfigError {
    fn from(e: crate::yaml::YamlError) -> Self {
        Self::Yaml(e)
    }
}

// Regex for ${VAR} and ${VAR:-default} interpolation
static ENV_VAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}").unwrap());

/// Pre-deserialize environment variable interpolation.
///
/// Replaces `${VAR}` with `env::var("VAR")` and `${VAR:-default}` with
/// the env value or the default. Bare text substitution — no YAML quoting
/// applied (industry standard: dbt, Docker Compose, Helm, envsubst).
///
/// `extra_vars` are checked before `std::env::var` — highest priority among
/// runtime vars. This allows channel variables to shadow system env vars.
///
/// `$${VAR}` produces literal `${VAR}` (escape, same as Docker Compose /
/// Kubernetes convention). The `$$` is replaced with a NULL byte placeholder
/// before the main regex, then restored after all substitution.
///
/// Missing var without default produces `ConfigError::EnvVar`.
pub fn interpolate_env_vars(
    yaml: &str,
    extra_vars: &[(&str, &str)],
) -> Result<String, ConfigError> {
    // Step 1: Replace $$ with NULL byte placeholder before main regex.
    // NULL bytes do not appear in valid YAML config files.
    debug_assert!(
        !yaml.contains('\0'),
        "input YAML contains NULL bytes — $$ escape placeholder collision"
    );
    let escaped = yaml.replace("$$", "\0");

    // Step 2: Run regex substitution with extra_vars priority
    let mut result = String::with_capacity(escaped.len());
    let mut last_end = 0;

    for caps in ENV_VAR_RE.captures_iter(&escaped) {
        let full_match = caps.get(0).unwrap();
        let var_name = caps.get(1).unwrap().as_str();
        let default_value = caps.get(2).map(|m| m.as_str());

        // Validate env var name: must be UPPERCASE + underscores only
        if !var_name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
            || var_name.starts_with(|c: char| c.is_ascii_digit())
            || var_name.is_empty()
        {
            return Err(ConfigError::Validation(format!(
                "invalid environment variable name '{}' at position {} — must match [A-Z_][A-Z0-9_]*",
                var_name,
                full_match.start()
            )));
        }

        result.push_str(&escaped[last_end..full_match.start()]);

        // Check extra_vars first, then system env
        if let Some(value) = extra_vars
            .iter()
            .find(|(k, _)| *k == var_name)
            .map(|(_, v)| *v)
        {
            result.push_str(value);
        } else {
            match std::env::var(var_name) {
                Ok(value) => result.push_str(&value),
                Err(_) => match default_value {
                    Some(default) => result.push_str(default),
                    None => {
                        return Err(ConfigError::EnvVar {
                            var_name: var_name.to_string(),
                            position: full_match.start(),
                        });
                    }
                },
            }
        }

        last_end = full_match.end();
    }

    result.push_str(&escaped[last_end..]);

    // Step 3: Restore NULL byte placeholder → $
    Ok(result.replace('\0', "$"))
}

/// Parse a pipeline config from a YAML string (after interpolation).
///
/// All YAML parsing flows through `crate::yaml::from_str`, the single
/// chokepoint that owns the DoS-defense [`Budget`].
pub fn parse_config(yaml: &str) -> Result<PipelineConfig, ConfigError> {
    let interpolated = interpolate_env_vars(yaml, &[])?;
    let config: PipelineConfig = crate::yaml::from_str(&interpolated)?;
    validate_config(&config)?;
    Ok(config)
}

/// Reserved `$pipeline.*` member names that cannot be used as user variable
/// names. Mirrors `crates/cxl/src/resolve/pass.rs::PIPELINE_MEMBERS`.
const RESERVED_PIPELINE_NAMES: &[&str] = &[
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
const RESERVED_SOURCE_NAMES: &[&str] = &[
    "file",
    "row",
    "path",
    "count",
    "batch",
    "ingestion_timestamp",
];

/// Post-deserialization validation.
fn validate_config(config: &PipelineConfig) -> Result<(), ConfigError> {
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
    }

    // Validate pipeline.vars (typed, scope-partitioned)
    if let Some(ref vars) = config.pipeline.vars {
        validate_scoped_vars(vars)?;
    }

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

    Ok(())
}

/// Validate the scope-partitioned `vars:` block: each scope's keys cannot
/// collide with that scope's reserved member names, and any declared
/// default must match its declared type.
fn validate_scoped_vars(vars: &ScopedVarsDecl) -> Result<(), ConfigError> {
    validate_scope("pipeline", &vars.pipeline, RESERVED_PIPELINE_NAMES)?;
    validate_scope("source", &vars.source, RESERVED_SOURCE_NAMES)?;
    validate_scope("record", &vars.record, &[])?;
    Ok(())
}

fn validate_scope(
    scope: &str,
    decls: &IndexMap<String, ScopedVarDecl>,
    reserved: &[&str],
) -> Result<(), ConfigError> {
    for (name, decl) in decls {
        if reserved.contains(&name.as_str()) {
            return Err(ConfigError::Validation(format!(
                "vars.{scope}: '{name}' is a reserved {scope} member name and cannot be used as a variable",
            )));
        }
        if let Some(default) = &decl.default {
            check_default_type(scope, name, decl.var_type, default)?;
        }
    }
    Ok(())
}

fn check_default_type(
    scope: &str,
    name: &str,
    var_type: ScopedVarType,
    default: &serde_json::Value,
) -> Result<(), ConfigError> {
    let mismatch = || {
        ConfigError::Validation(format!(
            "vars.{scope}.{name}: default value does not match declared type {var_type:?}",
        ))
    };
    match (var_type, default) {
        (_, serde_json::Value::Null) => Err(ConfigError::Validation(format!(
            "vars.{scope}.{name}: default value cannot be null",
        ))),
        (
            ScopedVarType::String | ScopedVarType::Date | ScopedVarType::DateTime,
            serde_json::Value::String(_),
        ) => Ok(()),
        (ScopedVarType::Int, serde_json::Value::Number(n)) if n.is_i64() => Ok(()),
        (ScopedVarType::Float, serde_json::Value::Number(_)) => Ok(()),
        (ScopedVarType::Bool, serde_json::Value::Bool(_)) => Ok(()),
        _ => Err(mismatch()),
    }
}

/// Convert pipeline-scope variable defaults into the runtime
/// `IndexMap<String, Value>` consumed by [`StableEvalContext::pipeline_vars`].
///
/// Source- and row-scope declarations are intentionally not threaded here —
/// their values are produced at runtime by `state` nodes (Phase D), not
/// from YAML defaults. Only call after [`validate_scoped_vars`] has passed.
pub fn convert_pipeline_vars(vars: &ScopedVarsDecl) -> IndexMap<String, clinker_record::Value> {
    vars.pipeline
        .iter()
        .filter_map(|(name, decl)| {
            decl.default
                .as_ref()
                .map(|default| (name.clone(), default_to_value(decl.var_type, default)))
        })
        .collect()
}

fn default_to_value(var_type: ScopedVarType, default: &serde_json::Value) -> clinker_record::Value {
    use clinker_record::Value;
    match (var_type, default) {
        (ScopedVarType::Bool, serde_json::Value::Bool(b)) => Value::Bool(*b),
        (ScopedVarType::Int, serde_json::Value::Number(n)) => {
            Value::Integer(n.as_i64().unwrap_or(0))
        }
        (ScopedVarType::Float, serde_json::Value::Number(n)) => {
            Value::Float(n.as_f64().unwrap_or(0.0))
        }
        (
            ScopedVarType::String | ScopedVarType::Date | ScopedVarType::DateTime,
            serde_json::Value::String(s),
        ) => Value::String(s.clone().into_boxed_str()),
        // validate_scoped_vars rejects every other (type, default) combo.
        _ => unreachable!("validate_scoped_vars should reject mismatched defaults"),
    }
}

/// Load and parse a pipeline config from a YAML file path with extra variables.
/// `extra_vars` are checked before system env vars during interpolation.
///
/// Stamps `config.source_hash` with the BLAKE3 of the post-env-var
/// interpolated YAML so the executor can resolve `{pipeline_hash}` tokens
/// and write provenance sidecars without retaining the source bytes.
pub fn load_config_with_vars(
    path: &std::path::Path,
    extra_vars: &[(&str, &str)],
) -> Result<PipelineConfig, ConfigError> {
    let yaml = std::fs::read_to_string(path)?;
    let interpolated = interpolate_env_vars(&yaml, extra_vars)?;
    let mut config: PipelineConfig = crate::yaml::from_str(&interpolated)?;
    config.source_hash = *blake3::hash(interpolated.as_bytes()).as_bytes();
    validate_config(&config)?;
    Ok(config)
}

/// Load and parse a pipeline config from a YAML file path.
pub fn load_config(path: &std::path::Path) -> Result<PipelineConfig, ConfigError> {
    load_config_with_vars(path, &[])
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
