use clinker_record::schema_def::{FieldDef, LineSeparator, SchemaDefinition};
use indexmap::IndexMap;
use regex::Regex;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::LazyLock;

/// Top-level pipeline configuration, deserialized from YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub pipeline: PipelineMeta,
    pub inputs: Vec<InputConfig>,
    pub outputs: Vec<OutputConfig>,
    pub transformations: Vec<TransformEntry>,
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
    /// Kiln IDE metadata: pipeline-level notes. Ignored by the engine.
    #[serde(default, rename = "_notes", skip_serializing_if = "Option::is_none")]
    pub notes: Option<serde_json::Value>,
}

/// Pipeline-level metadata and global settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineMeta {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vars: Option<IndexMap<String, serde_json::Value>>,
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

/// Input source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    pub name: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_overrides: Option<Vec<FieldDef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array_paths: Option<Vec<ArrayPathConfig>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<Vec<SortFieldSpec>>,
    #[serde(flatten)]
    pub format: InputFormat,
    /// Kiln IDE metadata: stage notes + field annotations. Ignored by the engine.
    #[serde(default, rename = "_notes", skip_serializing_if = "Option::is_none")]
    pub notes: Option<serde_json::Value>,
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

impl InputConfig {
    /// Get CSV input options, if this is a CSV input.
    pub fn csv_options(&self) -> Option<&CsvInputOptions> {
        match &self.format {
            InputFormat::Csv(opts) => opts.as_ref(),
            _ => None,
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
    /// File splitting configuration. When present, output is split into
    /// multiple files based on record count or byte size limits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split: Option<SplitConfig>,
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

/// Per-transform parallelism override.
///
/// Allows pipeline authors to override the auto-detected parallelism class
/// for a specific transform.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelismOverride {
    /// Let the engine decide based on CXL analysis.
    Auto,
    /// Force stateless (no shared state, fully parallelizable).
    Stateless,
    /// Force sequential (single-threaded execution).
    Sequential,
}

/// Per-transform configuration block.
///
/// The `cxl` field contains the multi-line CXL source text.
#[derive(Debug, Clone, Serialize)]
pub struct TransformConfig {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub cxl: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_window: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<Vec<LogDirective>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validations: Option<Vec<ValidationEntry>>,
    /// Route configuration for multi-output dispatch. Optional.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<RouteConfig>,
    /// Optional upstream wiring. None = receives from previous transform in
    /// declaration order (implicit linear chain). Some = explicit DAG wiring.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<TransformInput>,
    /// Optional parallelism override for this transform.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<ParallelismOverride>,
    /// Kiln IDE metadata: stage notes + field annotations. Ignored by the engine.
    #[serde(default, rename = "_notes", skip_serializing_if = "Option::is_none")]
    pub notes: Option<serde_json::Value>,
}

impl<'de> Deserialize<'de> for TransformConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            name: String,
            description: Option<String>,
            cxl: String,
            local_window: Option<serde_json::Value>,
            log: Option<Vec<LogDirective>>,
            validations: Option<Vec<ValidationEntry>>,
            route: Option<RouteConfig>,
            input: Option<TransformInput>,
            parallelism: Option<ParallelismOverride>,
            #[serde(default, rename = "_notes")]
            notes: Option<serde_json::Value>,
        }

        let raw = Raw::deserialize(deserializer)?;

        // Dot-ban: transform names must not contain '.' (reserved for branch references).
        if raw.name.contains('.') {
            return Err(de::Error::custom(format!(
                "transform name '{}' is invalid: '.' is reserved for branch references (use underscores or hyphens)",
                raw.name
            )));
        }

        Ok(TransformConfig {
            name: raw.name,
            description: raw.description,
            cxl: raw.cxl,
            local_window: raw.local_window,
            log: raw.log,
            validations: raw.validations,
            route: raw.route,
            input: raw.input,
            parallelism: raw.parallelism,
            notes: raw.notes,
        })
    }
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

/// A transformation list entry: either an inline transform or an `_import` directive.
///
/// Uses custom `Deserialize` (not `#[serde(untagged)]`) for clear error messages.
/// Consistent with `SortFieldSpec` and `SchemaSource` (Phase 9 decision #40).
#[derive(Debug, Clone, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum TransformEntry {
    Import { _import: String },
    Transform(TransformConfig),
}

impl<'de> Deserialize<'de> for TransformEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.get("_import").is_some() {
            #[derive(Deserialize)]
            struct ImportRef {
                _import: String,
            }
            let import: ImportRef = serde_json::from_value(value).map_err(de::Error::custom)?;
            Ok(TransformEntry::Import {
                _import: import._import,
            })
        } else {
            let transform: TransformConfig =
                serde_json::from_value(value).map_err(de::Error::custom)?;
            Ok(TransformEntry::Transform(transform))
        }
    }
}

impl PipelineConfig {
    /// Returns resolved transforms. Panics if called before import resolution.
    /// Cargo `InheritableField::normalized()` pattern.
    pub fn transforms(&self) -> impl Iterator<Item = &TransformConfig> + '_ {
        self.transformations.iter().map(|entry| match entry {
            TransformEntry::Transform(t) => t,
            TransformEntry::Import { _import } => {
                panic!("unresolved _import '{_import}' — call resolve_compositions() first")
            }
        })
    }
}

/// Correlation key for grouped DLQ rejection.
///
/// When set on `ErrorHandlingConfig`, all records sharing a correlation key value
/// are DLQ'd atomically if any single record in the group fails evaluation.
/// Custom `Deserialize`: accepts `"field"` string or `["f1", "f2"]` array.
#[derive(Debug, Clone, Serialize)]
pub enum CorrelationKey {
    Single(String),
    Compound(Vec<String>),
}

impl CorrelationKey {
    /// Return the field names that make up this correlation key.
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
    /// Correlation key for grouped DLQ rejection. When set, all records sharing
    /// a key value are DLQ'd atomically if any record in the group fails.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_key: Option<CorrelationKey>,
    /// Maximum records buffered per correlation group. Groups exceeding this cap
    /// are DLQ'd entirely with a `group_size_exceeded` summary entry. Default: 100,000.
    #[serde(
        default = "default_max_group_buffer",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_group_buffer: Option<u64>,
}

impl Default for ErrorHandlingConfig {
    fn default() -> Self {
        Self {
            strategy: default_strategy(),
            dlq: None,
            type_error_threshold: None,
            correlation_key: None,
            max_group_buffer: None,
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
    Yaml(serde_saphyr::Error),
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

impl From<serde_saphyr::Error> for ConfigError {
    fn from(e: serde_saphyr::Error) -> Self {
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

/// Maximum YAML document size (10 MB).
const MAX_YAML_SIZE: usize = 10_485_760;

/// Parse a pipeline config from a YAML string (after interpolation).
pub fn parse_config(yaml: &str) -> Result<PipelineConfig, ConfigError> {
    let interpolated = interpolate_env_vars(yaml, &[])?;
    let config: PipelineConfig = parse_yaml_with_budget(&interpolated)?;
    validate_config(&config)?;
    Ok(config)
}

/// Parse YAML with DoS protection budgets.
fn parse_yaml_with_budget(input: &str) -> Result<PipelineConfig, ConfigError> {
    if input.len() > MAX_YAML_SIZE {
        return Err(ConfigError::Validation(format!(
            "YAML document exceeds 10MB limit ({} bytes)",
            input.len()
        )));
    }
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_depth: 64,
            max_nodes: 10_000,
        },
    };
    serde_saphyr::from_str_with_options(input, options).map_err(ConfigError::Yaml)
}

/// Reserved pipeline member names that cannot be used as user variable names.
const RESERVED_PIPELINE_NAMES: &[&str] = &[
    "start_time",
    "name",
    "execution_id",
    "batch_id",
    "total_count",
    "ok_count",
    "dlq_count",
    "source_file",
    "source_row",
];

/// Post-deserialization validation.
fn validate_config(config: &PipelineConfig) -> Result<(), ConfigError> {
    for input in &config.inputs {
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

    // Validate pipeline.vars
    if let Some(ref vars) = config.pipeline.vars {
        validate_pipeline_vars(vars)?;
    }

    // Validate log directives (iterate raw entries to avoid panic on unresolved imports)
    for entry in &config.transformations {
        if let TransformEntry::Transform(t) = entry
            && let Some(ref directives) = t.log
        {
            for (i, d) in directives.iter().enumerate() {
                if let Some(every) = d.every {
                    if every == 0 {
                        return Err(ConfigError::Validation(format!(
                            "transform '{}': log directive #{}: every must be >= 1",
                            t.name,
                            i + 1,
                        )));
                    }
                    if d.when != LogTiming::PerRecord {
                        return Err(ConfigError::Validation(format!(
                            "transform '{}': log directive #{}: 'every' is only valid with when: per_record",
                            t.name,
                            i + 1,
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Validate pipeline.vars: no reserved name collisions, no nested objects/arrays/null.
fn validate_pipeline_vars(vars: &IndexMap<String, serde_json::Value>) -> Result<(), ConfigError> {
    for (name, value) in vars {
        // Check reserved name collision
        if RESERVED_PIPELINE_NAMES.contains(&name.as_str()) {
            return Err(ConfigError::Validation(format!(
                "pipeline.vars: '{}' is a reserved pipeline member name and cannot be used as a variable",
                name
            )));
        }

        // Check value type — only scalars allowed
        match value {
            serde_json::Value::Object(_) => {
                return Err(ConfigError::Validation(format!(
                    "pipeline.vars.{}: nested objects are not supported — only scalar values (string, number, bool)",
                    name
                )));
            }
            serde_json::Value::Array(_) => {
                return Err(ConfigError::Validation(format!(
                    "pipeline.vars.{}: arrays are not supported — only scalar values (string, number, bool)",
                    name
                )));
            }
            serde_json::Value::Null => {
                return Err(ConfigError::Validation(format!(
                    "pipeline.vars.{}: null values are not supported — provide a scalar value",
                    name
                )));
            }
            _ => {} // String, Number, Bool — all ok
        }
    }
    Ok(())
}

/// Convert validated pipeline vars from serde_json to clinker_record::Value.
/// Only call after `validate_pipeline_vars` has passed (no null/object/array).
pub fn convert_pipeline_vars(
    vars: &IndexMap<String, serde_json::Value>,
) -> IndexMap<String, clinker_record::Value> {
    vars.iter()
        .map(|(k, v)| {
            let val = match v {
                serde_json::Value::Bool(b) => clinker_record::Value::Bool(*b),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        clinker_record::Value::Integer(i)
                    } else {
                        clinker_record::Value::Float(n.as_f64().unwrap_or(0.0))
                    }
                }
                serde_json::Value::String(s) => {
                    clinker_record::Value::String(s.clone().into_boxed_str())
                }
                // Null/Object/Array rejected by validate_pipeline_vars
                _ => unreachable!("validate_pipeline_vars should reject non-scalar values"),
            };
            (k.clone(), val)
        })
        .collect()
}

/// Load and parse a pipeline config from a YAML file path with extra variables.
/// `extra_vars` are checked before system env vars during interpolation.
pub fn load_config_with_vars(
    path: &std::path::Path,
    extra_vars: &[(&str, &str)],
) -> Result<PipelineConfig, ConfigError> {
    let yaml = std::fs::read_to_string(path)?;
    let interpolated = interpolate_env_vars(&yaml, extra_vars)?;
    let config: PipelineConfig = parse_yaml_with_budget(&interpolated)?;
    validate_config(&config)?;
    Ok(config)
}

/// Load and parse a pipeline config from a YAML file path.
pub fn load_config(path: &std::path::Path) -> Result<PipelineConfig, ConfigError> {
    load_config_with_vars(path, &[])
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_YAML: &str = r#"
pipeline:
  name: test-pipeline

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: identity
    cxl: |
      emit full_name = first_name + " " + last_name
"#;

    #[test]
    fn test_config_minimal_valid_yaml() {
        let config = parse_config(MINIMAL_YAML).unwrap();
        assert_eq!(config.pipeline.name, "test-pipeline");
        assert_eq!(config.inputs.len(), 1);
        assert_eq!(config.inputs[0].name, "source");
        assert!(matches!(config.inputs[0].format, InputFormat::Csv(_)));
        assert_eq!(config.inputs[0].path, "/tmp/input.csv");
        assert_eq!(config.outputs.len(), 1);
        assert_eq!(config.transformations.len(), 1);
        assert!(
            config
                .transforms()
                .next()
                .unwrap()
                .cxl
                .contains("emit full_name")
        );
        // Default error strategy
        assert_eq!(config.error_handling.strategy, ErrorStrategy::FailFast);
    }

    #[test]
    fn test_config_env_var_interpolation() {
        // Use HOME which is reliably set
        let yaml = r#"
pipeline:
  name: env-test

inputs:
  - name: source
    type: csv
    path: ${HOME}/data/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        let home = std::env::var("HOME").unwrap();
        assert_eq!(config.inputs[0].path, format!("{home}/data/input.csv"));
    }

    #[test]
    fn test_config_env_var_default_fallback() {
        // _CLINKER_TEST_MISSING should not exist
        unsafe { std::env::remove_var("_CLINKER_TEST_MISSING") };
        let yaml = r#"
pipeline:
  name: default-test

inputs:
  - name: source
    type: csv
    path: ${_CLINKER_TEST_MISSING:-/tmp}/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        assert_eq!(config.inputs[0].path, "/tmp/input.csv");
    }

    #[test]
    fn test_config_env_var_missing_no_default() {
        unsafe { std::env::remove_var("_CLINKER_TEST_MISSING_NODEF") };
        let yaml = r#"
pipeline:
  name: missing-test

inputs:
  - name: source
    type: csv
    path: ${_CLINKER_TEST_MISSING_NODEF}/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let err = parse_config(yaml).unwrap_err();
        match err {
            ConfigError::EnvVar { var_name, .. } => {
                assert_eq!(var_name, "_CLINKER_TEST_MISSING_NODEF");
            }
            other => panic!("expected EnvVar error, got: {other}"),
        }
    }

    #[test]
    fn test_config_unknown_key_rejected() {
        let yaml = r#"
pipeline:
  name: unknown-test
  bogus_field: bad

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let err = parse_config(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("bogus_field") || msg.contains("unknown field"),
            "error should mention the unknown key: {msg}"
        );
    }

    #[test]
    fn test_config_missing_required_field() {
        // Missing path in input
        let yaml = r#"
pipeline:
  name: missing-field-test

inputs:
  - name: source
    type: csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let err = parse_config(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("path") || msg.contains("missing"),
            "error should mention missing field: {msg}"
        );
    }

    #[test]
    fn test_config_error_strategy_variants() {
        for (variant_str, expected) in [
            ("fail_fast", ErrorStrategy::FailFast),
            ("continue", ErrorStrategy::Continue),
            ("best_effort", ErrorStrategy::BestEffort),
        ] {
            let yaml = format!(
                r#"
pipeline:
  name: strategy-test

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: t1
    cxl: "emit x = a"

error_handling:
  strategy: {variant_str}
"#
            );
            let config = parse_config(&yaml).unwrap();
            assert_eq!(
                config.error_handling.strategy, expected,
                "failed for {variant_str}"
            );
        }
    }

    #[test]
    fn test_config_full_example() {
        let yaml = r#"
pipeline:
  name: full-pipeline
  memory_limit: 2GB
  date_formats:
    - "%Y-%m-%d"
    - "%m/%d/%Y"
  rules_path: /opt/rules
  concurrency:
    threads: 4
    chunk_size: 1000
  include_provenance: true

inputs:
  - name: employees
    type: csv
    path: /data/employees.csv
    schema_overrides:
      - name: hire_date
        type: date
        format: "%Y-%m-%d"
    options:
      delimiter: ","
      has_header: true

outputs:
  - name: transformed
    type: csv
    path: /data/output.csv
    include_unmapped: true
    include_header: true
    mapping:
      full_name: employee_name
      dept: department
    exclude:
      - internal_id
      - temp_field
    sort_order:
      - field: last_name
        order: asc
      - field: first_name
    preserve_nulls: false
    options:
      delimiter: ","

transformations:
  - name: compute_full_name
    description: "Concatenate first and last names"
    cxl: |
      emit full_name = first_name + " " + last_name
      emit dept = department.to_upper()
  - name: validate_email
    cxl: |
      emit email_valid = email.contains("@")

error_handling:
  strategy: continue
  dlq:
    path: /data/errors.csv
    include_reason: true
    include_source_row: true
  type_error_threshold: 0.05
"#;
        let config = parse_config(yaml).unwrap();

        // Pipeline meta
        assert_eq!(config.pipeline.name, "full-pipeline");
        assert_eq!(config.pipeline.memory_limit.as_deref(), Some("2GB"));
        assert_eq!(config.pipeline.date_formats.as_ref().unwrap().len(), 2);
        assert_eq!(
            config.pipeline.concurrency.as_ref().unwrap().threads,
            Some(4)
        );
        assert_eq!(config.pipeline.include_provenance, Some(true));

        // Input
        assert_eq!(config.inputs[0].name, "employees");
        assert_eq!(config.inputs[0].schema_overrides.as_ref().unwrap().len(), 1);
        assert_eq!(
            config.inputs[0].csv_options().unwrap().has_header,
            Some(true)
        );

        // Output
        assert!(config.outputs[0].include_unmapped);
        assert_eq!(config.outputs[0].mapping.as_ref().unwrap().len(), 2);
        assert_eq!(config.outputs[0].exclude.as_ref().unwrap().len(), 2);
        assert_eq!(config.outputs[0].sort_order.as_ref().unwrap().len(), 2);
        // SortFieldSpec resolves to SortField — verify via into_sort_field()
        let sf = config.outputs[0].sort_order.as_ref().unwrap()[0]
            .clone()
            .into_sort_field();
        assert_eq!(sf.order, SortOrder::Asc);
        assert_eq!(config.outputs[0].preserve_nulls, Some(false));

        // Transforms
        assert_eq!(config.transformations.len(), 2);
        let t0 = config.transforms().next().unwrap();
        assert!(t0.cxl.contains("emit full_name"));
        assert!(t0.description.is_some());

        // Error handling
        assert_eq!(config.error_handling.strategy, ErrorStrategy::Continue);
        assert!(config.error_handling.dlq.is_some());
        let dlq = config.error_handling.dlq.as_ref().unwrap();
        assert_eq!(dlq.include_reason, Some(true));
        assert_eq!(config.error_handling.type_error_threshold, Some(0.05));
    }

    // ── Phase 9 Task 9.2 gate tests ─────────────────────────────────

    #[test]
    fn test_override_inline_schema_conflict() {
        let yaml = r#"
pipeline:
  name: conflict-test

inputs:
  - name: source
    type: csv
    path: /tmp/input.csv
    schema:
      fields:
        - name: id
          type: integer
    schema_overrides:
      - name: id
        type: float

outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv

transformations:
  - name: t1
    cxl: "emit x = id"
"#;
        let err = parse_config(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("inline") || msg.contains("schema_overrides"),
            "error should mention the conflict: {msg}"
        );
    }

    // ── Phase 7 Task 7.0 gate tests ─────────────────────────────────

    #[test]
    fn test_config_csv_input_parses() {
        let yaml = r#"
pipeline:
  name: csv-test
inputs:
  - name: src
    type: csv
    path: /tmp/input.csv
    options:
      delimiter: "|"
      has_header: false
outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        assert!(matches!(config.inputs[0].format, InputFormat::Csv(Some(_))));
        let opts = config.inputs[0].csv_options().unwrap();
        assert_eq!(opts.delimiter.as_deref(), Some("|"));
        assert_eq!(opts.has_header, Some(false));
    }

    #[test]
    fn test_config_json_input_parses() {
        let yaml = r#"
pipeline:
  name: json-test
inputs:
  - name: src
    type: json
    path: /tmp/input.json
    options:
      format: ndjson
      record_path: data.results
outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        match &config.inputs[0].format {
            InputFormat::Json(Some(opts)) => {
                assert!(matches!(opts.format, Some(JsonFormat::Ndjson)));
                assert_eq!(opts.record_path.as_deref(), Some("data.results"));
            }
            other => panic!("Expected Json with options, got {:?}", other),
        }
    }

    #[test]
    fn test_config_xml_input_parses() {
        let yaml = r#"
pipeline:
  name: xml-test
inputs:
  - name: src
    type: xml
    path: /tmp/input.xml
    options:
      record_path: Orders/Order
      attribute_prefix: "_"
      namespace_handling: qualify
outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        match &config.inputs[0].format {
            InputFormat::Xml(Some(opts)) => {
                assert_eq!(opts.record_path.as_deref(), Some("Orders/Order"));
                assert_eq!(opts.attribute_prefix.as_deref(), Some("_"));
                assert!(matches!(
                    opts.namespace_handling,
                    Some(NamespaceHandling::Qualify)
                ));
            }
            other => panic!("Expected Xml with options, got {:?}", other),
        }
    }

    #[test]
    fn test_config_unknown_field_rejected() {
        // attribute_prefix is XML-only — should fail on CSV input
        let yaml = r#"
pipeline:
  name: reject-test
inputs:
  - name: src
    type: csv
    path: /tmp/input.csv
    options:
      attribute_prefix: "@"
outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let result = parse_config(yaml);
        assert!(
            result.is_err(),
            "CSV input with attribute_prefix should fail to parse"
        );
    }

    #[test]
    fn test_config_array_path_struct() {
        let yaml = r#"
pipeline:
  name: array-test
inputs:
  - name: src
    type: json
    path: /tmp/input.json
    array_paths:
      - path: orders
        mode: explode
      - path: tags
        mode: join
        separator: "|"
outputs:
  - name: dest
    type: csv
    path: /tmp/output.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        let aps = config.inputs[0].array_paths.as_ref().unwrap();
        assert_eq!(aps.len(), 2);
        assert_eq!(aps[0].path, "orders");
        assert!(matches!(aps[0].mode, ArrayMode::Explode));
        assert_eq!(aps[1].path, "tags");
        assert!(matches!(aps[1].mode, ArrayMode::Join));
        assert_eq!(aps[1].separator.as_deref(), Some("|"));
    }

    #[test]
    fn test_config_json_output_parses() {
        let yaml = r#"
pipeline:
  name: json-out-test
inputs:
  - name: src
    type: csv
    path: /tmp/input.csv
outputs:
  - name: dest
    type: json
    path: /tmp/output.json
    options:
      format: ndjson
      pretty: true
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        match &config.outputs[0].format {
            OutputFormat::Json(Some(opts)) => {
                assert!(matches!(opts.format, Some(JsonOutputFormat::Ndjson)));
                assert_eq!(opts.pretty, Some(true));
            }
            other => panic!("Expected Json output with options, got {:?}", other),
        }
    }

    #[test]
    fn test_config_xml_output_parses() {
        let yaml = r#"
pipeline:
  name: xml-out-test
inputs:
  - name: src
    type: csv
    path: /tmp/input.csv
outputs:
  - name: dest
    type: xml
    path: /tmp/output.xml
    options:
      root_element: Data
      record_element: Row
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        match &config.outputs[0].format {
            OutputFormat::Xml(Some(opts)) => {
                assert_eq!(opts.root_element.as_deref(), Some("Data"));
                assert_eq!(opts.record_element.as_deref(), Some("Row"));
            }
            other => panic!("Expected Xml output with options, got {:?}", other),
        }
    }

    // ── Phase 8 SortFieldSpec tests ─────────────────────────────────

    #[test]
    fn test_sort_field_spec_shorthand_yaml() {
        let yaml = r#"
pipeline:
  name: sort-test
inputs:
  - name: src
    type: csv
    path: /tmp/in.csv
    sort_order:
      - field_a
      - field_b
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        let sort = config.inputs[0].sort_order.as_ref().unwrap();
        assert_eq!(sort.len(), 2);
        let sf0 = sort[0].clone().into_sort_field();
        assert_eq!(sf0.field, "field_a");
        assert_eq!(sf0.order, SortOrder::Asc);
        assert!(sf0.null_order.is_none());
    }

    #[test]
    fn test_sort_field_spec_full_yaml() {
        let yaml = r#"
pipeline:
  name: sort-test
inputs:
  - name: src
    type: csv
    path: /tmp/in.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
    sort_order:
      - field: name
        order: desc
        null_order: first
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        let sort = config.outputs[0].sort_order.as_ref().unwrap();
        assert_eq!(sort.len(), 1);
        let sf = sort[0].clone().into_sort_field();
        assert_eq!(sf.field, "name");
        assert_eq!(sf.order, SortOrder::Desc);
        assert_eq!(sf.null_order, Some(NullOrder::First));
    }

    #[test]
    fn test_sort_field_spec_mixed_yaml() {
        let yaml = r#"
pipeline:
  name: sort-test
inputs:
  - name: src
    type: csv
    path: /tmp/in.csv
    sort_order:
      - simple_field
      - field: complex_field
        order: desc
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let config = parse_config(yaml).unwrap();
        let sort = config.inputs[0].sort_order.as_ref().unwrap();
        assert_eq!(sort.len(), 2);
        let sf0 = sort[0].clone().into_sort_field();
        assert_eq!(sf0.field, "simple_field");
        assert_eq!(sf0.order, SortOrder::Asc);
        let sf1 = sort[1].clone().into_sort_field();
        assert_eq!(sf1.field, "complex_field");
        assert_eq!(sf1.order, SortOrder::Desc);
    }

    #[test]
    fn test_sort_field_spec_into_sort_field() {
        let short = SortFieldSpec::Short("name".into());
        let sf = short.into_sort_field();
        assert_eq!(sf.field, "name");
        assert_eq!(sf.order, SortOrder::Asc);
        assert!(sf.null_order.is_none());
    }

    #[test]
    fn test_sort_output_removed_from_pipeline_meta() {
        // sort_output at pipeline level should be rejected (deny_unknown_fields)
        let yaml = r#"
pipeline:
  name: sort-test
  sort_output:
    - field: name
      order: asc
inputs:
  - name: src
    type: csv
    path: /tmp/in.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let result = parse_config(yaml);
        assert!(
            result.is_err(),
            "sort_output at pipeline level should be rejected"
        );
    }

    // ── Pipeline vars tests ───────────────────────────────────────

    fn yaml_with_vars(vars_block: &str) -> String {
        format!(
            r#"
pipeline:
  name: test
  vars:
{vars_block}

inputs:
  - name: src
    type: csv
    path: /tmp/in.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#
        )
    }

    #[test]
    fn test_vars_int() {
        let yaml = yaml_with_vars("    count: 42");
        let config = parse_config(&yaml).unwrap();
        let vars = config.pipeline.vars.unwrap();
        assert_eq!(vars["count"], serde_json::json!(42));
        let converted = convert_pipeline_vars(&vars);
        assert!(matches!(
            converted["count"],
            clinker_record::Value::Integer(42)
        ));
    }

    #[test]
    fn test_vars_float() {
        let yaml = yaml_with_vars("    rate: 0.05");
        let config = parse_config(&yaml).unwrap();
        let vars = config.pipeline.vars.unwrap();
        let converted = convert_pipeline_vars(&vars);
        assert!(
            matches!(converted["rate"], clinker_record::Value::Float(f) if (f - 0.05).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn test_vars_bool() {
        let yaml = yaml_with_vars("    active: true");
        let config = parse_config(&yaml).unwrap();
        let vars = config.pipeline.vars.unwrap();
        let converted = convert_pipeline_vars(&vars);
        assert!(matches!(
            converted["active"],
            clinker_record::Value::Bool(true)
        ));
    }

    #[test]
    fn test_vars_string() {
        let yaml = yaml_with_vars("    region: \"US\"");
        let config = parse_config(&yaml).unwrap();
        let vars = config.pipeline.vars.unwrap();
        let converted = convert_pipeline_vars(&vars);
        match &converted["region"] {
            clinker_record::Value::String(s) => assert_eq!(&**s, "US"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_vars_collision_start_time() {
        let yaml = yaml_with_vars("    start_time: 123");
        let err = parse_config(&yaml).unwrap_err();
        assert!(err.to_string().contains("reserved pipeline member name"));
    }

    #[test]
    fn test_vars_collision_execution_id() {
        let yaml = yaml_with_vars("    execution_id: \"abc\"");
        let err = parse_config(&yaml).unwrap_err();
        assert!(err.to_string().contains("reserved pipeline member name"));
    }

    #[test]
    fn test_vars_collision_all_reserved() {
        for name in RESERVED_PIPELINE_NAMES {
            let yaml = yaml_with_vars(&format!("    {}: 1", name));
            let err = parse_config(&yaml).unwrap_err();
            assert!(
                err.to_string().contains("reserved pipeline member name"),
                "Expected reserved name error for '{}', got: {}",
                name,
                err
            );
        }
    }

    #[test]
    fn test_vars_nested_object_error() {
        let yaml = yaml_with_vars("    nested:\n      a: 1");
        let err = parse_config(&yaml).unwrap_err();
        assert!(err.to_string().contains("nested objects are not supported"));
    }

    #[test]
    fn test_vars_array_error() {
        let yaml = yaml_with_vars("    list:\n      - 1\n      - 2");
        let err = parse_config(&yaml).unwrap_err();
        assert!(err.to_string().contains("arrays are not supported"));
    }

    #[test]
    fn test_vars_null_value() {
        let yaml = yaml_with_vars("    empty: null");
        let err = parse_config(&yaml).unwrap_err();
        assert!(err.to_string().contains("null values are not supported"));
    }

    #[test]
    fn test_vars_empty_section() {
        let yaml = yaml_with_vars("    {}");
        let config = parse_config(&yaml).unwrap();
        // Empty map is valid
        let vars = config.pipeline.vars.unwrap();
        assert!(vars.is_empty());
    }

    #[test]
    fn test_vars_yaml12_bool_inference() {
        // serde-saphyr follows YAML 1.1 rules: true/false/yes/no/on/off are all Bool.
        // Users must quote values like "NO" to get strings.
        let yaml = yaml_with_vars("    flag: true\n    explicit_no: false");
        let config = parse_config(&yaml).unwrap();
        let vars = config.pipeline.vars.unwrap();
        let converted = convert_pipeline_vars(&vars);
        assert!(matches!(
            converted["flag"],
            clinker_record::Value::Bool(true)
        ));
        assert!(matches!(
            converted["explicit_no"],
            clinker_record::Value::Bool(false)
        ));
    }

    #[test]
    fn test_vars_quoted_number_is_string() {
        let yaml = yaml_with_vars("    version: \"1.0\"\n    count: \"42\"");
        let config = parse_config(&yaml).unwrap();
        let vars = config.pipeline.vars.unwrap();
        let converted = convert_pipeline_vars(&vars);
        assert!(matches!(&converted["version"], clinker_record::Value::String(s) if &**s == "1.0"));
        assert!(matches!(&converted["count"], clinker_record::Value::String(s) if &**s == "42"));
    }

    // ── Log directive config tests ────────────────────────────────

    fn yaml_with_log(log_block: &str) -> String {
        format!(
            r#"
pipeline:
  name: test

inputs:
  - name: src
    type: csv
    path: /tmp/in.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = a"
    log:
{log_block}
"#
        )
    }

    #[test]
    fn test_log_level1_basic_emit() {
        let yaml = yaml_with_log(
            r#"
      - level: info
        when: per_record
        message: "processed {name}"
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let t = config.transforms().next().unwrap();
        let directives = t.log.as_ref().unwrap();
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].level, LogLevel::Info);
        assert_eq!(directives[0].when, LogTiming::PerRecord);
        assert_eq!(directives[0].message, "processed {name}");
    }

    #[test]
    fn test_log_level1_when_condition() {
        let yaml = yaml_with_log(
            r#"
      - level: warn
        when: per_record
        condition: "Amount > 1000"
        message: "high value"
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let d = &config.transforms().next().unwrap().log.as_ref().unwrap()[0];
        assert_eq!(d.condition.as_deref(), Some("Amount > 1000"));
    }

    #[test]
    fn test_log_level1_fields_structured() {
        let yaml = yaml_with_log(
            r#"
      - level: info
        when: per_record
        message: "rec"
        fields: [name, amount]
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let d = &config.transforms().next().unwrap().log.as_ref().unwrap()[0];
        assert_eq!(d.fields.as_ref().unwrap(), &["name", "amount"]);
    }

    #[test]
    fn test_log_level1_missing_message_error() {
        // message is required — missing it should be a YAML parse error
        let yaml = yaml_with_log(
            r#"
      - level: info
        when: per_record
"#,
        );
        assert!(parse_config(&yaml).is_err());
    }

    #[test]
    fn test_log_level1_invalid_level_error() {
        let yaml = yaml_with_log(
            r#"
      - level: critical
        when: per_record
        message: "msg"
"#,
        );
        assert!(parse_config(&yaml).is_err());
    }

    #[test]
    fn test_log_level1_invalid_when_error() {
        let yaml = yaml_with_log(
            r#"
      - level: info
        when: always
        message: "msg"
"#,
        );
        assert!(parse_config(&yaml).is_err());
    }

    #[test]
    fn test_log_level1_on_error_timing() {
        let yaml = yaml_with_log(
            r#"
      - level: error
        when: on_error
        message: "error: {_cxl_dlq_error_category}"
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let d = &config.transforms().next().unwrap().log.as_ref().unwrap()[0];
        assert_eq!(d.when, LogTiming::OnError);
    }

    // ── Validation config tests ───────────────────────────────────

    fn yaml_with_validations(validations_block: &str) -> String {
        format!(
            r#"
pipeline:
  name: test

inputs:
  - name: src
    type: csv
    path: /tmp/in.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = a"
    validations:
{validations_block}
"#
        )
    }

    #[test]
    fn test_validation_config_basic() {
        let yaml = yaml_with_validations(
            r#"
      - check: "Amount > 0"
        severity: error
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let t = config.transforms().next().unwrap();
        let validations = t.validations.as_ref().unwrap();
        assert_eq!(validations.len(), 1);
        assert_eq!(validations[0].check, "Amount > 0");
        assert_eq!(validations[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn test_validation_config_with_field() {
        let yaml = yaml_with_validations(
            r#"
      - field: Email
        check: "validators.is_valid_email"
        severity: error
        message: "invalid email for {employee_id}"
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let v = &config
            .transforms()
            .next()
            .unwrap()
            .validations
            .as_ref()
            .unwrap()[0];
        assert_eq!(v.field.as_deref(), Some("Email"));
        assert_eq!(
            v.message.as_deref(),
            Some("invalid email for {employee_id}")
        );
    }

    #[test]
    fn test_validation_config_default_severity() {
        let yaml = yaml_with_validations(
            r#"
      - check: "Amount > 0"
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let v = &config
            .transforms()
            .next()
            .unwrap()
            .validations
            .as_ref()
            .unwrap()[0];
        assert_eq!(v.severity, ValidationSeverity::Error); // default
    }

    #[test]
    fn test_validation_config_with_args() {
        let yaml = yaml_with_validations(
            r#"
      - field: salary
        check: "validators.in_range"
        args:
          max: 500000
          min: 0
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let v = &config
            .transforms()
            .next()
            .unwrap()
            .validations
            .as_ref()
            .unwrap()[0];
        let args = v.args.as_ref().unwrap();
        assert_eq!(args["max"], serde_json::json!(500000));
        assert_eq!(args["min"], serde_json::json!(0));
    }

    #[test]
    fn test_validation_order_before_emit() {
        // Validations should be checked before emit — they appear in config
        let yaml = yaml_with_validations(
            r#"
      - check: "Amount > 0"
        severity: error
      - check: "Amount < 1000000"
        severity: warn
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let vs = config
            .transforms()
            .next()
            .unwrap()
            .validations
            .as_ref()
            .unwrap();
        assert_eq!(vs.len(), 2);
        assert_eq!(vs[0].check, "Amount > 0");
        assert_eq!(vs[1].check, "Amount < 1000000");
    }

    #[test]
    fn test_validation_module_fn_call() {
        let yaml = yaml_with_validations(
            r#"
      - field: salary
        check: "validators.is_positive"
        severity: error
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let v = &config
            .transforms()
            .next()
            .unwrap()
            .validations
            .as_ref()
            .unwrap()[0];
        assert_eq!(v.check, "validators.is_positive");
    }

    #[test]
    fn test_validation_module_fn_with_args() {
        let yaml = yaml_with_validations(
            r#"
      - field: salary
        check: "validators.in_range"
        args:
          max: 500000
        severity: error
"#,
        );
        let config = parse_config(&yaml).unwrap();
        let v = &config
            .transforms()
            .next()
            .unwrap()
            .validations
            .as_ref()
            .unwrap()[0];
        assert_eq!(v.check, "validators.in_range");
        assert!(v.args.is_some());
    }

    // ── YAML DoS budget tests ─────────────────────────────────────

    #[test]
    fn test_yaml_dos_document_size() {
        // >10MB YAML → error
        let huge = "a".repeat(MAX_YAML_SIZE + 1);
        let err = parse_yaml_with_budget(&huge).unwrap_err();
        assert!(err.to_string().contains("10MB limit"));
    }

    #[test]
    fn test_yaml_dos_recursion_depth() {
        // 100 levels of nesting → should exceed budget max_depth=64
        let mut yaml = String::from("pipeline:\n");
        for i in 0..100 {
            yaml.push_str(&format!("{}level{}:\n", "  ".repeat(i + 1), i));
        }
        // This may fail as invalid pipeline config or as budget exceeded
        assert!(parse_config(&yaml).is_err());
    }

    #[test]
    fn test_yaml_dos_sequence_length() {
        // >10k nodes → should exceed budget
        let mut yaml = String::from("items:\n");
        for i in 0..11_000 {
            yaml.push_str(&format!("  - item{}\n", i));
        }
        assert!(parse_yaml_with_budget(&yaml).is_err());
    }

    // ── Env var name validation tests ─────────────────────────────

    #[test]
    fn test_env_var_name_valid() {
        // ${DATABASE_URL} is valid uppercase
        let yaml = r#"
pipeline:
  name: ${CLINKER_TEST_NAME:-test}
inputs:
  - name: src
    type: csv
    path: /tmp/in.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        assert!(parse_config(yaml).is_ok());
    }

    #[test]
    fn test_env_var_name_invalid_lowercase() {
        let yaml = r#"
pipeline:
  name: ${database_url:-test}
inputs:
  - name: src
    type: csv
    path: /tmp/in.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = a"
"#;
        let err = parse_config(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("invalid environment variable name")
        );
    }

    #[test]
    fn test_env_var_name_invalid_leading_digit() {
        let result = interpolate_env_vars("${1BAD}", &[]);
        // Regex won't match — 1BAD doesn't start with [A-Za-z_]
        // So it stays as literal text, which is fine
        assert!(result.is_ok());
    }

    #[test]
    fn test_env_var_name_empty() {
        // ${} → regex won't match (requires at least one char)
        let result = interpolate_env_vars("${}", &[]);
        assert!(result.is_ok()); // stays literal
    }

    #[test]
    fn test_env_var_name_special_chars() {
        // ${DB-NAME} → regex won't match (hyphen not in char class)
        let result = interpolate_env_vars("${DB-NAME}", &[]);
        assert!(result.is_ok()); // stays literal
    }

    #[test]
    fn test_env_var_unset_with_default() {
        unsafe { std::env::remove_var("_CLINKER_UNSET_VAR") };
        let result = interpolate_env_vars("${_CLINKER_UNSET_VAR:-fallback}", &[]).unwrap();
        assert_eq!(result, "fallback");
    }

    #[test]
    fn test_env_var_special_value_chars() {
        // Values can contain anything — only names are validated
        let result = interpolate_env_vars("test", &[("DB_URL", "postgres://user:pass@host/db")]);
        assert!(result.is_ok());
    }

    // --- Route config tests (Phase 13, Task 13.3) ---

    #[test]
    fn test_route_config_exclusive_deser() {
        let yaml = r#"
mode: exclusive
branches:
  - name: high
    condition: "amount > 10000"
  - name: medium
    condition: "amount > 1000"
default: low
"#;
        let rc: RouteConfig = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(rc.mode, RouteMode::Exclusive);
        assert_eq!(rc.branches.len(), 2);
        assert_eq!(rc.branches[0].name, "high");
        assert_eq!(rc.default, "low");
    }

    #[test]
    fn test_route_config_inclusive_deser() {
        let yaml = r#"
mode: inclusive
branches:
  - name: audit
    condition: "amount > 50000"
  - name: report
    condition: "country == 'US'"
default: standard
"#;
        let rc: RouteConfig = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(rc.mode, RouteMode::Inclusive);
        assert_eq!(rc.branches.len(), 2);
    }

    #[test]
    fn test_route_config_default_mode_exclusive() {
        let yaml = r#"
branches:
  - name: high
    condition: "amount > 10000"
default: low
"#;
        let rc: RouteConfig = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(rc.mode, RouteMode::Exclusive);
    }

    #[test]
    fn test_route_config_missing_default_error() {
        let yaml = r#"
branches:
  - name: high
    condition: "amount > 10000"
"#;
        let result: Result<RouteConfig, _> = serde_saphyr::from_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("default"),
            "error should mention default: {err}"
        );
    }

    #[test]
    fn test_route_config_empty_branches_error() {
        let yaml = r#"
branches: []
default: low
"#;
        let result: Result<RouteConfig, _> = serde_saphyr::from_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("at least one branch"),
            "error should mention branches: {err}"
        );
    }

    #[test]
    fn test_route_config_duplicate_branch_names_error() {
        let yaml = r#"
branches:
  - name: high
    condition: "amount > 10000"
  - name: high
    condition: "amount > 5000"
default: low
"#;
        let result: Result<RouteConfig, _> = serde_saphyr::from_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate"),
            "error should mention duplicate: {err}"
        );
    }

    #[test]
    fn test_route_config_default_collides_branch_error() {
        let yaml = r#"
branches:
  - name: high
    condition: "amount > 10000"
default: high
"#;
        let result: Result<RouteConfig, _> = serde_saphyr::from_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("collides"),
            "error should mention collision: {err}"
        );
    }

    #[test]
    fn test_transform_config_without_route_unchanged() {
        let yaml = r#"
pipeline:
  name: test
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: passthrough
    cxl: |
      emit *
"#;
        let config = parse_config(yaml).unwrap();
        let transforms: Vec<_> = config.transforms().collect();
        assert!(transforms[0].route.is_none());
    }

    #[test]
    fn test_route_branch_condition_complex() {
        let yaml = r#"
branches:
  - name: high_intl
    condition: "amount > 10000 && country != 'US'"
default: standard
"#;
        let rc: RouteConfig = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(
            rc.branches[0].condition,
            "amount > 10000 && country != 'US'"
        );
    }

    // --- Phase 15 gate tests: TransformInput + dot-ban + ParallelismOverride ---

    /// String input deserializes to Single variant.
    #[test]
    fn test_transform_input_single_deser() {
        let yaml = r#""source_a""#;
        let input: TransformInput = serde_json::from_str(yaml).unwrap();
        match input {
            TransformInput::Single(s) => assert_eq!(s, "source_a"),
            _ => panic!("expected Single"),
        }
    }

    /// Array input deserializes to Multiple variant.
    #[test]
    fn test_transform_input_multiple_deser() {
        let input: TransformInput = serde_json::from_str(r#"["a", "b"]"#).unwrap();
        match input {
            TransformInput::Multiple(v) => assert_eq!(v, vec!["a", "b"]),
            _ => panic!("expected Multiple"),
        }
    }

    /// Dotted branch reference deserializes correctly.
    #[test]
    fn test_transform_input_dotted_branch_deser() {
        let input: TransformInput = serde_json::from_str(r#""categorize.high_value""#).unwrap();
        match input {
            TransformInput::Single(s) => assert_eq!(s, "categorize.high_value"),
            _ => panic!("expected Single"),
        }
    }

    /// Empty array produces a clear error.
    #[test]
    fn test_transform_input_empty_array_error() {
        let result = serde_json::from_str::<TransformInput>("[]");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("must not be empty"),
            "expected empty array error, got: {err}"
        );
    }

    /// No input field means None (default flow).
    #[test]
    fn test_transform_input_none_default_flow() {
        let yaml = r#"
pipeline:
  name: test

inputs:
  - name: source
    type: csv
    path: /tmp/in.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv

transformations:
  - name: identity
    cxl: |
      emit x = 1
"#;
        let config = parse_config(yaml).unwrap();
        let t = match &config.transformations[0] {
            TransformEntry::Transform(t) => t,
            _ => panic!("expected Transform"),
        };
        assert!(t.input.is_none());
    }

    /// Transform name containing dot is rejected.
    #[test]
    fn test_transform_name_dot_banned() {
        let yaml = r#"
pipeline:
  name: test

inputs:
  - name: source
    type: csv
    path: /tmp/in.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv

transformations:
  - name: my.transform
    cxl: |
      emit x = 1
"#;
        let result = parse_config(yaml);
        assert!(result.is_err());
        let err = format!("{:?}", result.unwrap_err());
        assert!(
            err.contains("'.' is reserved for branch references"),
            "expected dot-ban error, got: {err}"
        );
    }

    /// Transform name with underscores and hyphens is accepted.
    #[test]
    fn test_transform_name_valid_chars() {
        let yaml = r#"
pipeline:
  name: test

inputs:
  - name: source
    type: csv
    path: /tmp/in.csv

outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv

transformations:
  - name: my-transform_v2
    cxl: |
      emit x = 1
"#;
        let config = parse_config(yaml).unwrap();
        let t = match &config.transformations[0] {
            TransformEntry::Transform(t) => t,
            _ => panic!("expected Transform"),
        };
        assert_eq!(t.name, "my-transform_v2");
    }
}
