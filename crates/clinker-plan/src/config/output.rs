//! Output node configuration: split policy and per-format output options.

use super::*;
use clinker_format::JoinValues;
use clinker_record::schema_def::LineSeparator;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// Output destination configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    pub name: String,
    pub path: String,
    /// When `true` (default), every input field not explicitly emitted
    /// passes through to the output unchanged. When `false`, only
    /// explicitly emitted fields appear in the output. Also the path
    /// that surfaces `OnUnmapped::AutoWiden`-discovered columns at the
    /// sink by expanding the `$widened` sidecar payload back to
    /// top-level fields.
    #[serde(default = "default_include_unmapped")]
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
    /// Controls whether engine-stamped correlation snapshot columns
    /// (`$ck.<field>`) appear in the default writer output. The shadow
    /// columns preserve correlation-group identity through Transforms
    /// that may rewrite the user-declared field; they are an internal
    /// engine namespace and are stripped from output unless this flag
    /// is set. Defaults to `false`.
    #[serde(default)]
    pub include_correlation_keys: bool,
    /// Explicit schema for output formats that require field definitions
    /// (e.g., fixed-width output needs column names, widths, and positions).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SourceSchema>,
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
    /// Reconstruct the per-document output envelope around each document's
    /// body records: the writer's `begin_document` fires on a document's first
    /// record, its records stream straight through, and `end_document` fires
    /// when the document ends (its `$source.file` changes, or at end of
    /// input). When set, the executor routes this Output through an arm that
    /// detects document boundaries from each record's document context, and
    /// excludes the Output from the fused streaming-writer thread (which does
    /// no framing). Records still stream one at a time across the boundary —
    /// no document is buffered. Incompatible with per-file `split:` /
    /// fan-out, `dlq_granularity: document`, and `correlation_key` (rejected
    /// at config-validation time). Opt-in; writers render no framing until a
    /// format implements `begin_document` / `end_document`.
    #[serde(default, skip_serializing_if = "is_false_bool")]
    pub reconstruct_envelope: bool,
    /// In-cell join declarations: each names a `multiple:` field whose values a
    /// CSV writer collapses into one delimited cell, with a per-field delimiter
    /// and collision policy. The write-side inverse of `split_values`. Empty by
    /// default; a `multiple:` field with no entry joins with the default `;` and
    /// `on_conflict: error`. Consumed by the CSV writer only — declaring it on
    /// another output format is rejected (E362).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_values: Option<Vec<JoinValues>>,
    #[serde(flatten)]
    pub format: OutputFormat,
    /// External tooling metadata: stage notes + field annotations. Ignored by the engine.
    #[serde(default, rename = "_notes", skip_serializing_if = "Option::is_none")]
    pub notes: Option<serde_json::Value>,
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

/// Per-document output-envelope reconstruction for the generic formats
/// (CSV / JSON / XML / fixed-width).
///
/// Mirrors the EDI `*_from_doc` vocabulary: a named `$doc` section is echoed
/// as the per-document HEADER before the body and another as the FOOTER after
/// it, so a header/trailer-shaped source round-trips through a generic output.
/// The footer may additionally carry a streaming-computed record count under
/// a named field. Section names are arbitrary user-chosen identifiers — the
/// engine reserves none; an unknown name is rejected at plan-validation time
/// with diagnostic **E346**.
///
/// Bounded memory: the header is emitted on the document's first record and
/// the footer on its close, with the body streamed one record at a time
/// between them — no document's records are ever buffered. Active only when
/// the Output also sets `reconstruct_envelope: true`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct OutputEnvelopeConfig {
    /// Name of the `$doc` section echoed as the per-document header, emitted
    /// before the document's first body record. `None` writes no header.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header_from_doc: Option<String>,
    /// Name of the `$doc` section echoed as the per-document footer, emitted
    /// after the document's last body record. `None` writes no footer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub footer_from_doc: Option<String>,
    /// When set, the footer additionally carries the streaming-computed count
    /// of body records written for the document, under this field name. The
    /// count is maintained incrementally (one running integer), never by
    /// buffering the body. Requires a `footer_from_doc` section to attach to.
    /// Rejected with **E346** on a format that cannot inject a computed field
    /// into its footer (fixed-width).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub footer_record_count_field: Option<String>,
}

impl From<&OutputEnvelopeConfig> for clinker_format::OutputEnvelopeSpec {
    /// Lower the plan config onto the format-local spec the writers consume.
    /// The executor calls this only under `reconstruct_envelope: true`; the
    /// resulting spec's `is_empty` / `into_framer` decide whether any framing
    /// is rendered (so an empty config stays byte-identical to flag-off).
    fn from(cfg: &OutputEnvelopeConfig) -> Self {
        Self {
            header_from_doc: cfg.header_from_doc.clone(),
            footer_from_doc: cfg.footer_from_doc.clone(),
            footer_record_count_field: cfg.footer_record_count_field.clone(),
        }
    }
}

/// CSV-specific output options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CsvOutputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    /// Per-document envelope reconstruction (header/footer sections). Active
    /// only with `reconstruct_envelope: true` on the Output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envelope: Option<OutputEnvelopeConfig>,
}

/// JSON-specific output options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct JsonOutputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<JsonOutputFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pretty: Option<bool>,
    /// Per-document envelope reconstruction. With this set and
    /// `reconstruct_envelope: true`, the whole-stream array framing is
    /// replaced by one JSON object per document: `{ "<header>": {...},
    /// "body": [ ... ], "<footer>": {...} }`. Active only with
    /// `reconstruct_envelope: true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envelope: Option<OutputEnvelopeConfig>,
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
    /// Field-name prefix marking a field as an XML attribute of its
    /// enclosing element rather than a child element (default `@`,
    /// mirroring the XML source option so attribute fields round-trip).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute_prefix: Option<String>,
    /// Per-document envelope reconstruction. With this set and
    /// `reconstruct_envelope: true`, each document is wrapped in its own
    /// `<record_element-doc>`-style frame carrying the header section, the
    /// body records, and the footer section. Active only with
    /// `reconstruct_envelope: true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envelope: Option<OutputEnvelopeConfig>,
}

/// Fixed-width output options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FixedWidthOutputOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_separator: Option<LineSeparator>,
    /// Per-document envelope reconstruction (header/footer rows). The header
    /// section's fields are emitted as one leading line and the footer's as
    /// one trailing line, joined positionally in declared field order. A
    /// computed footer record count (`footer_record_count_field`) is rejected
    /// at plan time with **E346** — a fixed-width line has no field to inject
    /// a count into without a width declaration. Active only with
    /// `reconstruct_envelope: true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envelope: Option<OutputEnvelopeConfig>,
}

/// EDIFACT output options.
///
/// The writer reconstructs the interchange envelope around emitted
/// records. The `UNB` header comes from `interchange` (literal elements)
/// or, when that is unset, is echoed from the `$doc` section named by
/// `interchange_from_doc` for round-trip reconstruction.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EdifactOutputOptions {
    /// Literal `UNB` data elements written verbatim. Takes precedence
    /// over `interchange_from_doc` when both are set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interchange: Option<Vec<String>>,
    /// Name of a `$doc` section to echo the `UNB` elements from (the
    /// reader's positional `UNB` envelope section), enabling EDIFACT
    /// round-trip reconstruction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interchange_from_doc: Option<String>,
    /// Fallback message type when a record carries no `msg_type` column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_type: Option<String>,
    /// Emit a leading `UNA` service-string-advice segment. Defaults to
    /// `false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_una: Option<bool>,
    /// Write a newline after each segment terminator for readability.
    /// Defaults to `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_newline: Option<bool>,
}

/// X12 output options.
///
/// The writer reconstructs the three-tier interchange envelope
/// (`ISA..IEA` → `GS..GE` → `ST..SE`) around emitted records. The `ISA`
/// header comes from `interchange` (literal elements) or, when that is
/// unset, is echoed from the `$doc` section named by `interchange_from_doc`
/// for round-trip reconstruction. The `GS` functional-group header comes
/// from `group_header`. Functional groups are grouped on the optional
/// `group_ref` column and transaction sets on the `set_ref` column, so a
/// stream carrying several distinct `group_ref` values writes one `GS..GE`
/// group per value inside the single interchange (with no `group_ref`
/// column the whole stream is one functional group). The `SE`/`GE`/`IEA`
/// control counts are recomputed per set, per group, and for the
/// interchange respectively.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct X12OutputOptions {
    /// Literal `ISA` data elements (the 16 fixed-width ISA fields) written
    /// verbatim. Takes precedence over `interchange_from_doc` when both
    /// are set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interchange: Option<Vec<String>>,
    /// Name of a `$doc` section to echo the `ISA` elements from (the
    /// reader's positional `ISA` envelope section), enabling X12
    /// round-trip reconstruction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interchange_from_doc: Option<String>,
    /// Literal `GS` functional-group header elements (`GS01..GS08`). The
    /// `GS06` group control number is recomputed by the writer. Required
    /// to wrap transaction sets in a functional group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_header: Option<Vec<String>>,
    /// Fallback transaction set type (`ST01`) when a record carries no
    /// `set_type` column value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub set_type: Option<String>,
    /// Write a newline after each segment terminator for readability.
    /// Defaults to `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_newline: Option<bool>,
    /// Character set element text is encoded through. Defaults to UTF-8; set
    /// to match the source's `encoding` so a non-UTF-8 interchange
    /// round-trips byte-faithfully. Supported values are `utf-8` and
    /// `iso-8859-1` (Latin-1); a character the charset cannot represent is
    /// rejected rather than emitted truncated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
}

/// HL7 v2 output options.
///
/// The writer re-emits the `MSH` and body segments from the record stream
/// and optionally wraps them in batch/file envelopes. An `FHS` file header
/// comes from `file_header` (literal fields) or, when that is unset, is
/// echoed from the `$doc` section named by `file_header_from_doc` for
/// round-trip reconstruction; a `BHS` batch header comes from
/// `batch_header`. When either envelope is configured the writer recomputes
/// the closing `BTS`/`FTS` counts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Hl7OutputOptions {
    /// Literal `FHS` file-header fields written verbatim. Takes precedence
    /// over `file_header_from_doc` when both are set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_header: Option<Vec<String>>,
    /// Name of a `$doc` section to echo the `FHS` fields from (the reader's
    /// positional `FHS` envelope section), enabling round-trip file-header
    /// reconstruction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_header_from_doc: Option<String>,
    /// Literal `BHS` batch-header fields written verbatim. When set, the
    /// writer wraps the messages in a `BHS..BTS` batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_header: Option<Vec<String>>,
    /// Write a newline after each segment's carriage-return terminator for
    /// readability. Defaults to `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_newline: Option<bool>,
}

/// SWIFT MT output options.
///
/// The writer re-emits the block-4 `:tag:value` lines from the record stream
/// and re-frames the single SWIFT envelope around them: the service blocks
/// 1/2/3 first, then block 4, then the optional block-5 trailer. Each service
/// block is written from a literal body or echoed from a user-declared `$doc`
/// section (the `*_from_doc` options name the section the user wrote on the
/// source — the engine reserves no section name). Block-4 free text is opaque,
/// so values are written verbatim with no escaping, making the
/// reader → writer → reader round-trip byte-faithful.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SwiftOutputOptions {
    /// Literal block-1 (basic header) body written verbatim. Takes precedence
    /// over `basic_header_from_doc` when both are set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basic_header: Option<String>,
    /// Name of a `$doc` section to echo the block-1 body from, for round-trip
    /// header reconstruction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basic_header_from_doc: Option<String>,
    /// Literal block-2 (application header) body. Takes precedence over
    /// `app_header_from_doc`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_header: Option<String>,
    /// Name of a `$doc` section to echo the block-2 body from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_header_from_doc: Option<String>,
    /// Literal block-3 (user header) body, including any nested `{sub:tag}`
    /// content written verbatim. Takes precedence over `user_header_from_doc`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_header: Option<String>,
    /// Name of a `$doc` section to echo the block-3 body from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_header_from_doc: Option<String>,
    /// Literal block-5 (trailer) body, written after block 4 closes. Takes
    /// precedence over `trailer_from_doc`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trailer: Option<String>,
    /// Name of a `$doc` section to echo the block-5 body from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trailer_from_doc: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_output_options_parse_attribute_prefix() {
        let opts: XmlOutputOptions =
            crate::yaml::from_str("record_element: row\nattribute_prefix: \"_\"\n").unwrap();
        assert_eq!(opts.attribute_prefix, Some("_".to_string()));
        assert_eq!(opts.record_element, Some("row".to_string()));
    }

    #[test]
    fn xml_output_options_attribute_prefix_absent_is_none() {
        // Genuinely optional — an omitted prefix leaves `None`, so the
        // writer applies its documented `@` default.
        let opts: XmlOutputOptions = crate::yaml::from_str("record_element: row\n").unwrap();
        assert_eq!(opts.attribute_prefix, None);
    }
}
