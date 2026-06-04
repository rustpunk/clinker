//! Source node configuration: file discovery, transports, and per-format input options.

use super::*;
use clinker_record::schema_def::{FieldDef, LineSeparator};
use serde::de::{self};
use serde::{Deserialize, Deserializer, Serialize};

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

    /// Event-time watermark declaration. Names the column whose value
    /// is observed at ingest and folded into a per-(source, file)
    /// max-event-time stamp; rolled up at the read side by
    /// `PerSourceWatermarks::min_across_sources`. Mirrors Flink SQL's
    /// `WATERMARK FOR <col> AS <col> - INTERVAL` declaration site;
    /// the `INTERVAL` delay column lands with the time-window operator
    /// at https://github.com/rustpunk/clinker/issues/61.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<WatermarkConfig>,

    /// Envelope-section configuration for document-aware sources.
    /// Declared sections are extracted by the reader's one-time
    /// pre-scan and exposed to CXL via `$doc.<section>.<field>`.
    /// Section names are user-chosen identifiers; the engine reserves
    /// none. Absent (`None`) when the source has no envelope to extract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<clinker_format::EnvelopeConfig>,

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
    /// Transport selecting WHERE the records come from, sitting above the
    /// on-disk FORMAT (`format` / `type:` below). A plain optional
    /// snake_case enum field — deliberately NOT a second
    /// `#[serde(flatten)]`+tagged enum, because a flattened tagged enum
    /// loses YAML source spans (the same edge case `InputFormat` already
    /// hits), and a second one would compound that loss. Defaults to
    /// [`SourceTransport::File`] so every existing file pipeline that
    /// omits `transport:` parses byte-identically.
    #[serde(default)]
    pub transport: SourceTransport,
    #[serde(flatten)]
    pub format: InputFormat,
    /// Kiln IDE metadata: stage notes + field annotations. Ignored by the engine.
    #[serde(default, rename = "_notes", skip_serializing_if = "Option::is_none")]
    pub notes: Option<serde_json::Value>,
}

/// Event-time watermark declaration on a `SourceConfig`.
///
/// `column` names a record field whose value is the source's event-
/// time axis. At ingest, each record's value at this column is
/// converted to an i64 nanosecond stamp, shifted earlier by `delay`,
/// and folded into a per-(source, file) max by the executor's watermark
/// tracking. The column type must coerce to
/// [`clinker_record::Value::Timestamp`] or
/// [`clinker_record::Value::Date`] (validated at plan time, see
/// `crate::plan::bind_schema`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatermarkConfig {
    /// Name of the event-time column on the source's declared schema.
    pub column: String,
    /// Duration the source's live mpsc receiver may stay quiet before
    /// its partitions flip to the idle watermark state in the executor.
    /// `None` (default) means "never go idle" — preserves prior behavior
    /// for pipelines without a window-close consumer.
    ///
    /// YAML form: a duration string with one of the `s` / `m` / `h` /
    /// `d` / `ms` unit suffixes, e.g. `"30s"`, `"500ms"`, `"5m"`. Same
    /// parser as [`TimeBound`]'s relative form, extended with `ms`
    /// for the sub-second cadences a streaming consumer needs.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub idle_timeout: Option<std::time::Duration>,
    /// Bounded out-of-order tolerance for this source. Each record's
    /// event-time, before being folded into the watermark, is shifted
    /// earlier by `delay`; equivalently, the source's effective
    /// watermark trails its observed max event-time by this amount.
    /// Mirrors Flink's `BoundedOutOfOrdernessWatermarks` and Beam's
    /// `WithAllowedTimestampSkew` — a source-system property declared
    /// once at the source, distinct from the operator-side
    /// `allowed_lateness` knob.
    ///
    /// `None` (default) means no shift — the watermark advances with
    /// the observed max. Same YAML duration form as `idle_timeout`.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub delay: Option<std::time::Duration>,
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

/// Transport selecting where a source's records originate, layered ABOVE
/// the on-disk [`InputFormat`]. `File` (the default) reads bytes from the
/// filesystem and decodes them with the declared format; the `Rest`
/// transport (a paginated HTTP cursor) yields rows without going through
/// fs discovery or the file matchers and carries its own
/// connection/pagination config inline.
///
/// Selected by a nested `transport:` value, NOT a `#[serde(flatten)]`+
/// tagged enum on `SourceConfig`. A second flattened tagged enum on
/// `SourceConfig` would lose YAML source spans (the same edge case
/// [`InputFormat`] already hits); a nested `transport:` value keeps the
/// span on every payload field. Tagged with an internal `kind:` so the
/// network variant can carry a payload struct:
///
/// ```yaml
/// transport:
///   kind: rest
///   url: https://api.example.com/v1/records
///   pagination: { strategy: link_header }
///   max_pages: 50
/// ```
///
/// Defaults to [`SourceTransport::File`] so every existing file pipeline
/// that omits `transport:` (or writes the bare `transport: file` scalar)
/// parses byte-identically — the `untagged`-free default arm plus the
/// `kind` tag accept both the bare-`file` scalar and the nested forms.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceTransport {
    /// Read bytes from the filesystem; resolve the file set through the
    /// discovery layer's `path`/`glob`/`regex`/`paths` matchers.
    #[default]
    File,
    /// Paginated HTTP GET cursor. Issues GETs to cursor exhaustion or a
    /// hard page/record cap, decoding each response body through the
    /// declared on-disk [`InputFormat`] (`json`/`xml`).
    Rest(RestSourceConfig),
}

impl<'de> Deserialize<'de> for SourceTransport {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Two accepted YAML shapes:
        //   transport: file                  (bare scalar)
        //   transport: { kind: rest, ... }   (internally-tagged map)
        // serde's internally-tagged enums reject a bare scalar for a unit
        // variant, so the scalar form is handled explicitly and the map
        // form delegates to a private `kind`-tagged mirror.
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case")]
        enum Tagged {
            File,
            Rest(RestSourceConfig),
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Scalar(String),
            Tagged(Tagged),
        }

        match Either::deserialize(d)? {
            Either::Scalar(s) if s == "file" => Ok(SourceTransport::File),
            Either::Scalar(other) => Err(de::Error::custom(format!(
                "unknown transport {other:?}; the bare scalar form only accepts `file` — \
                 the rest transport uses a nested `transport: {{ kind: rest, … }}`"
            ))),
            Either::Tagged(Tagged::File) => Ok(SourceTransport::File),
            Either::Tagged(Tagged::Rest(c)) => Ok(SourceTransport::Rest(c)),
        }
    }
}

impl SourceTransport {
    /// Whether this transport reads from the filesystem (the only
    /// transport that goes through fs discovery and the file matchers).
    pub fn is_file(&self) -> bool {
        matches!(self, SourceTransport::File)
    }

    /// Short lowercase transport name for diagnostics.
    pub fn transport_name(&self) -> &'static str {
        match self {
            SourceTransport::File => "file",
            SourceTransport::Rest(_) => "rest",
        }
    }
}

/// Pagination strategy for a [`RestSourceConfig`]. Determines how the
/// reader advances from one page to the next and how it detects the last
/// page. Every strategy is bounded by the source's hard page/record cap —
/// finiteness is a HARD reader property, not a server promise.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum RestPagination {
    /// Single GET, no pagination. The response body is the whole result.
    #[default]
    None,
    /// `?<offset_param>=N&<limit_param>=L`, advancing the offset by the
    /// page size each request; stops when a page returns fewer than
    /// `limit` records (or the cap trips).
    Offset {
        /// Query parameter carrying the row offset (default `offset`).
        #[serde(default = "default_offset_param")]
        offset_param: String,
        /// Query parameter carrying the page size (default `limit`).
        #[serde(default = "default_limit_param")]
        limit_param: String,
        /// Page size requested per GET.
        limit: u32,
    },
    /// Cursor-token pagination: the reader reads a token from a JSON
    /// pointer in each response and sends it back as a query parameter
    /// on the next request; stops when the token field is absent/null.
    CursorToken {
        /// Query parameter carrying the continuation token.
        cursor_param: String,
        /// JSON pointer (RFC 6901) into the response body locating the
        /// next-page token (e.g. `/meta/next_cursor`).
        next_token_pointer: String,
    },
    /// RFC 5988 `Link` header pagination: the reader follows the URL in
    /// the `rel="next"` link until no such link is present.
    LinkHeader,
}

fn default_offset_param() -> String {
    "offset".to_string()
}

fn default_limit_param() -> String {
    "limit".to_string()
}

/// HTTP authentication for a [`RestSourceConfig`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "scheme", rename_all = "snake_case")]
pub enum RestAuth {
    /// No authentication header.
    #[default]
    None,
    /// `Authorization: Bearer <token>`.
    Bearer {
        /// The bearer token, sent verbatim after `Bearer `.
        token: String,
    },
    /// An arbitrary static header, e.g. `X-API-Key: <value>`.
    Header {
        /// Header name.
        name: String,
        /// Header value.
        value: String,
    },
}

/// Connection + pagination config for the `rest` transport. The reader
/// issues paginated GETs against [`url`](Self::url) until the pagination
/// strategy reports no next page OR a hard cap trips, decoding each
/// response body through the source's declared on-disk format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestSourceConfig {
    /// Base request URL. Pagination appends/overrides query parameters.
    pub url: String,
    /// Pagination strategy (default [`RestPagination::None`] — a single
    /// GET).
    #[serde(default)]
    pub pagination: RestPagination,
    /// Authentication (default [`RestAuth::None`]).
    #[serde(default)]
    pub auth: RestAuth,
    /// HARD page cap. The reader stops after this many GETs even if the
    /// server keeps offering a next page. Required — finiteness is
    /// unenforceable downstream, so an explicit ceiling is mandatory.
    pub max_pages: u32,
    /// Optional HARD record cap, applied in addition to `max_pages`. The
    /// reader stops emitting once this many records have been yielded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records: Option<u64>,
    /// Bounded transient-failure retry count per request (5xx / timeout /
    /// connect error). Default 3.
    #[serde(default = "default_rest_retries")]
    pub retries: u32,
    /// Per-request timeout in seconds. Bounds in-flight request time so
    /// SIGINT cancellation lands within the documented shutdown bound.
    /// Default 30.
    #[serde(default = "default_rest_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_rest_retries() -> u32 {
    3
}

fn default_rest_timeout_secs() -> u64 {
    30
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

/// Fixed-width input options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FixedWidthInputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_separator: Option<LineSeparator>,
}
