use indexmap::IndexMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

/// Top-level pipeline configuration, deserialized from YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub pipeline: PipelineMeta,
    pub inputs: Vec<InputConfig>,
    pub outputs: Vec<OutputConfig>,
    pub transformations: Vec<TransformConfig>,
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
}

/// Pipeline-level metadata and global settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineMeta {
    pub name: String,
    pub memory_limit: Option<String>,
    pub vars: Option<IndexMap<String, serde_json::Value>>,
    pub date_formats: Option<Vec<String>>,
    pub rules_path: Option<String>,
    pub concurrency: Option<ConcurrencyConfig>,
    // Spec stubs — processed in later phases
    pub date_locale: Option<String>,
    pub log_rules: Option<serde_json::Value>,
    pub include_provenance: Option<bool>,
    /// Execution metrics spool configuration.
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
    pub spool_dir: Option<String>,
}

/// Concurrency settings for parallel chunk processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConcurrencyConfig {
    pub threads: Option<usize>,
    pub chunk_size: Option<usize>,
}

/// Input source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    pub name: String,
    pub path: String,
    pub schema: Option<String>,
    pub schema_overrides: Option<Vec<SchemaOverride>>,
    pub array_paths: Option<Vec<ArrayPathConfig>>,
    pub sort_order: Option<Vec<SortFieldSpec>>,
    #[serde(flatten)]
    pub format: InputFormat,
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
}

/// CSV-specific input options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CsvInputOptions {
    pub delimiter: Option<String>,
    pub quote_char: Option<String>,
    pub has_header: Option<bool>,
    pub encoding: Option<String>,
}

/// JSON-specific input options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct JsonInputOptions {
    pub format: Option<JsonFormat>,
    pub record_path: Option<String>,
}

/// XML-specific input options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct XmlInputOptions {
    pub record_path: Option<String>,
    pub attribute_prefix: Option<String>,
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

/// Schema override for a named column.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaOverride {
    pub name: String,
    pub r#type: String,
    pub format: Option<String>,
}

/// Output destination configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub include_unmapped: bool,
    pub include_header: Option<bool>,
    pub mapping: Option<IndexMap<String, String>>,
    pub exclude: Option<Vec<String>>,
    pub sort_order: Option<Vec<SortFieldSpec>>,
    pub preserve_nulls: Option<bool>,
    #[serde(flatten)]
    pub format: OutputFormat,
}

/// Adjacently tagged format enum for outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "options", rename_all = "snake_case")]
pub enum OutputFormat {
    Csv(Option<CsvOutputOptions>),
    Json(Option<JsonOutputOptions>),
    Xml(Option<XmlOutputOptions>),
}

/// CSV-specific output options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CsvOutputOptions {
    pub delimiter: Option<String>,
}

/// JSON-specific output options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct JsonOutputOptions {
    pub format: Option<JsonOutputFormat>,
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
    pub root_element: Option<String>,
    pub record_element: Option<String>,
}

/// Sort field specification for output and window partition ordering.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortField {
    pub field: String,
    #[serde(default = "default_sort_order")]
    pub order: SortOrder,
    /// Null handling during sort. None for output sorting; Some(Last) default for windows.
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
/// This is a serde-only type. Resolve to `Vec<SortField>` via `into_sort_field()`
/// at config load time. All downstream code uses `Vec<SortField>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SortFieldSpec {
    Short(String),
    Full(SortField),
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
pub enum NullOrder {
    /// Nulls sort before all non-null values.
    First,
    /// Nulls sort after all non-null values (SQL convention default).
    Last,
    /// Remove records with null sort keys from the partition.
    Drop,
}

impl Default for NullOrder {
    fn default() -> Self {
        NullOrder::Last
    }
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

/// Per-transform configuration block.
///
/// The `cxl` field contains the multi-line CXL source text.
/// `local_window`, `log`, and `validations` are structurally present
/// but processed in later phases (Phase 5, 10).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    pub name: String,
    pub description: Option<String>,
    pub cxl: String,
    pub local_window: Option<serde_json::Value>,
    pub log: Option<serde_json::Value>,
    pub validations: Option<serde_json::Value>,
}

/// Error handling strategy and DLQ configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorHandlingConfig {
    #[serde(default = "default_strategy")]
    pub strategy: ErrorStrategy,
    pub dlq: Option<DlqConfig>,
    pub type_error_threshold: Option<f64>,
}

impl Default for ErrorHandlingConfig {
    fn default() -> Self {
        Self {
            strategy: default_strategy(),
            dlq: None,
            type_error_threshold: None,
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
    pub path: Option<String>,
    pub include_reason: Option<bool>,
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
                write!(f, "undefined environment variable ${{{var_name}}} at position {position}")
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
static ENV_VAR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}").unwrap()
});

/// Pre-deserialize environment variable interpolation.
///
/// Replaces `${VAR}` with `env::var("VAR")` and `${VAR:-default}` with
/// the env value or the default. YAML-escapes substituted values by
/// wrapping in single quotes if they contain YAML-special characters.
/// Missing var without default produces `ConfigError::EnvVar`.
pub fn interpolate_env_vars(yaml: &str) -> Result<String, ConfigError> {
    let mut result = String::with_capacity(yaml.len());
    let mut last_end = 0;

    for caps in ENV_VAR_RE.captures_iter(yaml) {
        let full_match = caps.get(0).unwrap();
        let var_name = caps.get(1).unwrap().as_str();
        let default_value = caps.get(2).map(|m| m.as_str());

        result.push_str(&yaml[last_end..full_match.start()]);

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

        last_end = full_match.end();
    }

    result.push_str(&yaml[last_end..]);
    Ok(result)
}

/// Parse a pipeline config from a YAML string (after interpolation).
pub fn parse_config(yaml: &str) -> Result<PipelineConfig, ConfigError> {
    let interpolated = interpolate_env_vars(yaml)?;
    let config: PipelineConfig = serde_saphyr::from_str(&interpolated)?;
    Ok(config)
}

/// Load and parse a pipeline config from a YAML file path.
pub fn load_config(path: &std::path::Path) -> Result<PipelineConfig, ConfigError> {
    let yaml = std::fs::read_to_string(path)?;
    parse_config(&yaml)
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
        assert!(config.transformations[0].cxl.contains("emit full_name"));
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
        assert!(msg.contains("bogus_field") || msg.contains("unknown field"), "error should mention the unknown key: {msg}");
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
        assert!(msg.contains("path") || msg.contains("missing"), "error should mention missing field: {msg}");
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
            assert_eq!(config.error_handling.strategy, expected, "failed for {variant_str}");
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
        assert_eq!(config.pipeline.concurrency.as_ref().unwrap().threads, Some(4));
        assert_eq!(config.pipeline.include_provenance, Some(true));

        // Input
        assert_eq!(config.inputs[0].name, "employees");
        assert_eq!(config.inputs[0].schema_overrides.as_ref().unwrap().len(), 1);
        assert_eq!(config.inputs[0].csv_options().unwrap().has_header, Some(true));

        // Output
        assert!(config.outputs[0].include_unmapped);
        assert_eq!(config.outputs[0].mapping.as_ref().unwrap().len(), 2);
        assert_eq!(config.outputs[0].exclude.as_ref().unwrap().len(), 2);
        assert_eq!(config.outputs[0].sort_order.as_ref().unwrap().len(), 2);
        // SortFieldSpec resolves to SortField — verify via into_sort_field()
        let sf = config.outputs[0].sort_order.as_ref().unwrap()[0].clone().into_sort_field();
        assert_eq!(sf.order, SortOrder::Asc);
        assert_eq!(config.outputs[0].preserve_nulls, Some(false));

        // Transforms
        assert_eq!(config.transformations.len(), 2);
        assert!(config.transformations[0].cxl.contains("emit full_name"));
        assert!(config.transformations[0].description.is_some());

        // Error handling
        assert_eq!(config.error_handling.strategy, ErrorStrategy::Continue);
        assert!(config.error_handling.dlq.is_some());
        let dlq = config.error_handling.dlq.as_ref().unwrap();
        assert_eq!(dlq.include_reason, Some(true));
        assert_eq!(config.error_handling.type_error_threshold, Some(0.05));
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
                assert!(matches!(opts.namespace_handling, Some(NamespaceHandling::Qualify)));
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
        assert!(result.is_err(), "CSV input with attribute_prefix should fail to parse");
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
        assert!(result.is_err(), "sort_output at pipeline level should be rejected");
    }
}
