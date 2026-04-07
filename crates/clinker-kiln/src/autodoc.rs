/// Auto-generated stage documentation with structured sections.
///
/// All content is derived deterministically from the `PipelineConfig` —
/// never from run results. The doc model is section-based: each stage
/// gets applicable sections (schema, lineage, contract, config, provenance,
/// channel overrides) populated from config metadata.
use clinker_core::config::{
    ErrorStrategy, InputFormat, OutputFormat, PipelineConfig, SchemaSource,
};
use clinker_record::schema_def::FieldDef;

// ── StageDoc model ──────────────────────────────────────────────────────────

/// Structural documentation for a single pipeline stage.
#[derive(Clone, Debug, PartialEq)]
pub struct StageDoc {
    /// What kind of stage this is.
    pub kind: StageKindDoc,
    /// One-line summary.
    pub summary: String,
    /// User-authored description from transform `description` field.
    pub user_description: Option<String>,
    /// Schema information: fields, types, constraints.
    pub schema: Option<SchemaSection>,
    /// CXL analysis: classified statements with field refs (transforms only).
    pub cxl_analysis: Option<CxlAnalysis>,
    /// Composition contract: requires/produces.
    pub contract: Option<ContractSection>,
    /// Format and behavioral config details.
    pub config: ConfigSection,
    /// Composition provenance: origin path, overrides.
    pub provenance: Option<ProvenanceSection>,
    /// Channel override provenance (when documenting a resolved pipeline).
    pub channel_override: Option<ChannelOverrideSection>,
    /// Raw CXL source code (transforms only).
    pub cxl_source: Option<String>,
    /// Expanded sub-stage docs (compositions only — each internal transform).
    pub sub_stages: Vec<StageDoc>,
}

impl Default for StageDoc {
    fn default() -> Self {
        Self {
            kind: StageKindDoc::Transform,
            summary: "No documentation available.".to_string(),
            user_description: None,
            schema: None,
            cxl_analysis: None,
            contract: None,
            config: ConfigSection { entries: vec![] },
            provenance: None,
            channel_override: None,
            cxl_source: None,
            sub_stages: vec![],
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum StageKindDoc {
    Source,
    Transform,
    Output,
}

// ── Schema section ──────────────────────────────────────────────────────────

/// Schema section — field inventory for inputs or outputs.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaSection {
    pub source: SchemaOrigin,
    pub fields: Vec<FieldDoc>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SchemaOrigin {
    /// From a schema file path.
    File(String),
    /// Inline schema definition.
    Inline,
    /// Inferred from schema_overrides only.
    OverridesOnly,
    /// No schema specified.
    None,
}

/// A documented field with constraint information from FieldDef.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldDoc {
    pub name: String,
    pub field_type: Option<String>,
    pub required: bool,
    pub format: Option<String>,
    pub coerce: bool,
    pub default_value: Option<String>,
    pub allowed_values: Option<Vec<String>>,
    pub alias: Option<String>,
}

// ── CXL analysis section ────────────────────────────────────────────────────

/// Full CXL analysis for a transform stage — classifies every statement.
#[derive(Clone, Debug, PartialEq)]
pub struct CxlAnalysis {
    /// Classified CXL statements.
    pub statements: Vec<CxlStatement>,
    /// All unique field references across all statements.
    pub all_field_refs: Vec<String>,
}

/// A classified CXL statement.
#[derive(Clone, Debug, PartialEq)]
pub struct CxlStatement {
    /// What kind of statement this is.
    pub kind: CxlStatementKind,
    /// The raw CXL expression.
    pub expression: String,
    /// Field references in this statement.
    pub field_refs: Vec<String>,
    /// Output field name (for emit/let statements only).
    pub output_field: Option<String>,
}

/// CXL statement intent classification.
#[derive(Clone, Debug, PartialEq)]
pub enum CxlStatementKind {
    /// `emit X = expr` — derives a new output field.
    Emit,
    /// `if expr` / conditional — filters rows based on field values.
    Filter,
    /// `let X = expr` — intermediate variable binding.
    Let,
    /// `log ...` — logging/debugging.
    Log,
    /// Anything else.
    Expression,
}

impl CxlStatementKind {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Emit => "EMIT",
            Self::Filter => "FILTER",
            Self::Let => "LET",
            Self::Log => "LOG",
            Self::Expression => "EXPR",
        }
    }
}

// ── Contract section ────────────────────────────────────────────────────────

/// Contract section — from composition metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct ContractSection {
    pub composition_name: String,
    pub version: Option<String>,
    pub requires: Vec<ContractFieldDoc>,
    pub produces: Vec<ContractFieldDoc>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContractFieldDoc {
    pub name: String,
    pub field_type: String,
}

// ── Config section ──────────────────────────────────────────────────────────

/// Flat config details — format-specific settings, sort orders, etc.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigSection {
    pub entries: Vec<ConfigEntry>,
}

/// A typed config entry, grouped by category.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigEntry {
    pub category: ConfigCategory,
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConfigCategory {
    Format,
    Sort,
    ArrayPath,
    ErrorHandling,
    Mapping,
    Window,
    Validation,
}

impl ConfigCategory {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Format => "FORMAT",
            Self::Sort => "SORT ORDER",
            Self::ArrayPath => "ARRAY PATHS",
            Self::ErrorHandling => "ERROR HANDLING",
            Self::Mapping => "FIELD MAPPINGS",
            Self::Window => "WINDOW",
            Self::Validation => "VALIDATION",
        }
    }
}

// ── Provenance section ──────────────────────────────────────────────────────

/// Provenance section — composition origin tracking.
#[derive(Clone, Debug, PartialEq)]
pub struct ProvenanceSection {
    pub composition_path: String,
    pub composition_name: String,
    pub composition_version: Option<String>,
    pub is_overridden: bool,
    pub original_cxl: Option<String>,
    pub current_cxl: Option<String>,
}

// ── Channel override section ────────────────────────────────────────────────

/// Channel override provenance placeholder. Composition/channel features are
/// removed in Phase 16b; the type is kept as a transparent stub for the UI.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelOverrideSection {
    pub channel_id: String,
    pub override_kind: String,
    pub override_source: String,
    pub override_file: String,
}

// ── Doc generation ──────────────────────────────────────────────────────────

/// Generate documentation for the stage with the given name.
pub fn generate_stage_doc(config: &PipelineConfig, stage_name: &str) -> Option<StageDoc> {
    if let Some(input) = config.inputs.iter().find(|i| i.name == stage_name) {
        return Some(generate_input_doc(config, input));
    }

    if let Some(transform) = config.transforms().find(|t| t.name == stage_name) {
        return Some(generate_transform_doc(config, transform));
    }

    if let Some(output) = config.outputs.iter().find(|o| o.name == stage_name) {
        return Some(generate_output_doc(config, output));
    }

    None
}

fn generate_input_doc(
    config: &PipelineConfig,
    input: &clinker_core::config::InputConfig,
) -> StageDoc {
    let format_name = input.format.format_name();

    // Build schema section
    let schema = build_input_schema(input);
    let field_count = schema.as_ref().map(|s| s.fields.len()).unwrap_or(0);

    let summary = if field_count > 0 {
        format!(
            "Ingests {} from `{}` with {} schema-defined field(s).",
            format_name, input.path, field_count,
        )
    } else {
        format!(
            "Ingests {} from `{}` with inferred schema.",
            format_name, input.path,
        )
    };

    // Build config section
    let mut entries = Vec::new();
    entries.push(ConfigEntry {
        category: ConfigCategory::Format,
        key: "TYPE".to_string(),
        value: format_name.to_string(),
    });
    entries.push(ConfigEntry {
        category: ConfigCategory::Format,
        key: "PATH".to_string(),
        value: input.path.clone(),
    });

    // Format-specific options
    match &input.format {
        InputFormat::Csv(Some(opts)) => {
            if let Some(ref d) = opts.delimiter {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "DELIMITER".to_string(),
                    value: d.clone(),
                });
            }
            if let Some(ref q) = opts.quote_char {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "QUOTE CHAR".to_string(),
                    value: q.clone(),
                });
            }
            if let Some(h) = opts.has_header {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "HAS HEADER".to_string(),
                    value: if h { "yes" } else { "no" }.to_string(),
                });
            }
            if let Some(ref enc) = opts.encoding {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "ENCODING".to_string(),
                    value: enc.clone(),
                });
            }
        }
        InputFormat::Json(Some(opts)) => {
            if let Some(ref fmt) = opts.format {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "JSON FORMAT".to_string(),
                    value: format!("{:?}", fmt).to_lowercase(),
                });
            }
            if let Some(ref rp) = opts.record_path {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "RECORD PATH".to_string(),
                    value: rp.clone(),
                });
            }
        }
        InputFormat::Xml(Some(opts)) => {
            if let Some(ref rp) = opts.record_path {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "RECORD PATH".to_string(),
                    value: rp.clone(),
                });
            }
            if let Some(ref ap) = opts.attribute_prefix {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "ATTRIBUTE PREFIX".to_string(),
                    value: ap.clone(),
                });
            }
        }
        InputFormat::FixedWidth(Some(opts)) => {
            if let Some(ref ls) = opts.line_separator {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "LINE SEPARATOR".to_string(),
                    value: format!("{:?}", ls).to_lowercase(),
                });
            }
        }
        _ => {}
    }

    // Array paths
    if let Some(ref array_paths) = input.array_paths {
        for ap in array_paths {
            let mode = format!("{:?}", ap.mode).to_lowercase();
            let sep = ap.separator.as_deref().unwrap_or("-");
            entries.push(ConfigEntry {
                category: ConfigCategory::ArrayPath,
                key: ap.path.clone(),
                value: format!("{} (sep: {})", mode, sep),
            });
        }
    }

    // Sort order
    if let Some(ref sort) = input.sort_order {
        for spec in sort.iter() {
            let sf = spec.clone().into_sort_field();
            entries.push(ConfigEntry {
                category: ConfigCategory::Sort,
                key: sf.field.clone(),
                value: format!("{:?}", sf.order).to_lowercase(),
            });
        }
    }

    // Error handling (pipeline-level, applies to all stages)
    push_error_handling_entries(&mut entries, config);

    StageDoc {
        kind: StageKindDoc::Source,
        summary,
        user_description: None,
        schema,
        cxl_analysis: None,
        contract: None,
        config: ConfigSection { entries },
        provenance: None,
        channel_override: None,
        cxl_source: None,
        sub_stages: vec![],
    }
}

fn generate_transform_doc(
    _config: &PipelineConfig,
    transform: &clinker_core::config::TransformConfig,
) -> StageDoc {
    // Analyze CXL statements
    let analysis = analyze_cxl(transform.cxl_source());

    let emit_stmts: Vec<_> = analysis
        .statements
        .iter()
        .filter(|s| s.kind == CxlStatementKind::Emit)
        .collect();
    let filter_stmts: Vec<_> = analysis
        .statements
        .iter()
        .filter(|s| s.kind == CxlStatementKind::Filter)
        .collect();
    let emit_count = emit_stmts.len();
    let filter_count = filter_stmts.len();
    let emit_names: Vec<_> = emit_stmts
        .iter()
        .filter_map(|s| s.output_field.as_ref())
        .cloned()
        .collect();

    // Build intent-aware summary
    let summary = if let Some(ref desc) = transform.description {
        desc.clone()
    } else {
        let filter_fields: Vec<_> = filter_stmts
            .iter()
            .flat_map(|s| s.field_refs.iter())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        match (emit_count > 0, filter_count > 0) {
            (true, true) => {
                let emit_list = emit_names.join(", ");
                let filter_list = filter_fields.join(", ");
                format!(
                    "Derives {} field(s) ({}) and filters on {}.",
                    emit_count, emit_list, filter_list
                )
            }
            (true, false) => {
                let field_list = emit_names.join(", ");
                format!("Derives {} field(s): {}.", emit_count, field_list)
            }
            (false, true) => {
                let filter_list = filter_fields.join(", ");
                format!("Filters rows on {}. Row count may be reduced.", filter_list)
            }
            (false, false) => {
                if !analysis.all_field_refs.is_empty() {
                    let refs = analysis.all_field_refs.join(", ");
                    format!("Evaluates CXL referencing {}.", refs)
                } else {
                    "Evaluates CXL expressions.".to_string()
                }
            }
        }
    };

    let cxl_analysis = if !analysis.statements.is_empty() {
        Some(analysis)
    } else {
        None
    };

    let contract: Option<ContractSection> = None;
    let provenance: Option<ProvenanceSection> = None;

    // Build config section
    let has_filters = filter_count > 0;
    let mut entries = vec![ConfigEntry {
        category: ConfigCategory::Format,
        key: "TYPE".to_string(),
        value: "transform".to_string(),
    }];
    if emit_count > 0 {
        entries.push(ConfigEntry {
            category: ConfigCategory::Format,
            key: "EMITS".to_string(),
            value: format!("{} field(s)", emit_count),
        });
    }
    if filter_count > 0 {
        entries.push(ConfigEntry {
            category: ConfigCategory::Format,
            key: "FILTERS".to_string(),
            value: format!("{} condition(s)", filter_count),
        });
    }
    entries.push(ConfigEntry {
        category: ConfigCategory::Format,
        key: "PRESERVES ROWS".to_string(),
        value: if has_filters { "conditional" } else { "yes" }.to_string(),
    });

    if transform.local_window.is_some() {
        entries.push(ConfigEntry {
            category: ConfigCategory::Window,
            key: "LOCAL WINDOW".to_string(),
            value: "configured".to_string(),
        });
    }
    if transform.validations.is_some() {
        entries.push(ConfigEntry {
            category: ConfigCategory::Validation,
            key: "VALIDATIONS".to_string(),
            value: "configured".to_string(),
        });
    }

    StageDoc {
        kind: StageKindDoc::Transform,
        summary,
        user_description: transform.description.clone(),
        schema: None,
        cxl_analysis,
        contract,
        config: ConfigSection { entries },
        provenance,
        channel_override: None,
        cxl_source: Some(transform.cxl_source().to_string()),
        sub_stages: vec![],
    }
}

fn generate_output_doc(
    _config: &PipelineConfig,
    output: &clinker_core::config::OutputConfig,
) -> StageDoc {
    let format_name = output.format.format_name();
    let mapping_count = output.mapping.as_ref().map(|m| m.len()).unwrap_or(0);
    let exclude_count = output.exclude.as_ref().map(|e| e.len()).unwrap_or(0);

    let mut summary_parts = vec![format!("Writes {} to `{}`.", format_name, output.path)];
    if mapping_count > 0 {
        summary_parts.push(format!("{} field mapping(s).", mapping_count));
    }
    if exclude_count > 0 {
        summary_parts.push(format!("{} exclusion(s).", exclude_count));
    }
    let summary = summary_parts.join(" ");

    // Build schema section from mappings and exclusions
    let schema = build_output_schema(output);

    // Build config section
    let mut entries = Vec::new();
    entries.push(ConfigEntry {
        category: ConfigCategory::Format,
        key: "TYPE".to_string(),
        value: "output".to_string(),
    });
    entries.push(ConfigEntry {
        category: ConfigCategory::Format,
        key: "FORMAT".to_string(),
        value: format_name.to_string(),
    });
    entries.push(ConfigEntry {
        category: ConfigCategory::Format,
        key: "PATH".to_string(),
        value: output.path.clone(),
    });
    entries.push(ConfigEntry {
        category: ConfigCategory::Format,
        key: "INCLUDE UNMAPPED".to_string(),
        value: if output.include_unmapped { "yes" } else { "no" }.to_string(),
    });
    if let Some(h) = output.include_header {
        entries.push(ConfigEntry {
            category: ConfigCategory::Format,
            key: "INCLUDE HEADER".to_string(),
            value: if h { "yes" } else { "no" }.to_string(),
        });
    }
    if let Some(pn) = output.preserve_nulls {
        entries.push(ConfigEntry {
            category: ConfigCategory::Format,
            key: "PRESERVE NULLS".to_string(),
            value: if pn { "yes" } else { "no" }.to_string(),
        });
    }

    // Output format-specific options
    match &output.format {
        OutputFormat::Csv(Some(opts)) => {
            if let Some(ref d) = opts.delimiter {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "DELIMITER".to_string(),
                    value: d.clone(),
                });
            }
        }
        OutputFormat::Json(Some(opts)) => {
            if let Some(ref fmt) = opts.format {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "JSON FORMAT".to_string(),
                    value: format!("{:?}", fmt).to_lowercase(),
                });
            }
            if let Some(p) = opts.pretty {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "PRETTY".to_string(),
                    value: if p { "yes" } else { "no" }.to_string(),
                });
            }
        }
        OutputFormat::Xml(Some(opts)) => {
            if let Some(ref re) = opts.root_element {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "ROOT ELEMENT".to_string(),
                    value: re.clone(),
                });
            }
            if let Some(ref re) = opts.record_element {
                entries.push(ConfigEntry {
                    category: ConfigCategory::Format,
                    key: "RECORD ELEMENT".to_string(),
                    value: re.clone(),
                });
            }
        }
        _ => {}
    }

    // Mapping entries
    if let Some(ref mapping) = output.mapping {
        for (output_name, source_name) in mapping {
            entries.push(ConfigEntry {
                category: ConfigCategory::Mapping,
                key: output_name.clone(),
                value: format!("← {}", source_name),
            });
        }
    }

    // Sort order
    if let Some(ref sort) = output.sort_order {
        for spec in sort {
            let sf = spec.clone().into_sort_field();
            entries.push(ConfigEntry {
                category: ConfigCategory::Sort,
                key: sf.field.clone(),
                value: format!("{:?}", sf.order).to_lowercase(),
            });
        }
    }

    StageDoc {
        kind: StageKindDoc::Output,
        summary,
        user_description: None,
        schema,
        cxl_analysis: None,
        contract: None,
        config: ConfigSection { entries },
        provenance: None,
        channel_override: None,
        cxl_source: None,
        sub_stages: vec![],
    }
}

// ── Helper: build input schema ──────────────────────────────────────────────

fn build_input_schema(input: &clinker_core::config::InputConfig) -> Option<SchemaSection> {
    let mut fields = Vec::new();
    let source;

    match (&input.schema, &input.schema_overrides) {
        (Some(SchemaSource::Inline(def)), _) => {
            source = SchemaOrigin::Inline;
            if let Some(ref field_defs) = def.fields {
                for fd in field_defs {
                    fields.push(field_def_to_doc(fd));
                }
            }
            // Merge overrides on top
            if let Some(ref overrides) = input.schema_overrides {
                merge_overrides(&mut fields, overrides);
            }
        }
        (Some(SchemaSource::FilePath(path)), _) => {
            source = SchemaOrigin::File(path.clone());
            // Can't read from disk — just note the path. If overrides exist, show those.
            if let Some(ref overrides) = input.schema_overrides {
                for fd in overrides {
                    fields.push(field_def_to_doc(fd));
                }
            }
        }
        (None, Some(overrides)) => {
            source = SchemaOrigin::OverridesOnly;
            for fd in overrides {
                fields.push(field_def_to_doc(fd));
            }
        }
        (None, None) => {
            return None;
        }
    }

    Some(SchemaSection { source, fields })
}

fn build_output_schema(output: &clinker_core::config::OutputConfig) -> Option<SchemaSection> {
    let mut fields = Vec::new();

    // Document mapping renames as pseudo-schema
    if let Some(ref mapping) = output.mapping {
        for (output_name, source_name) in mapping {
            fields.push(FieldDoc {
                name: output_name.clone(),
                field_type: None,
                required: false,
                format: None,
                coerce: false,
                default_value: Some(format!("mapped from {}", source_name)),
                allowed_values: None,
                alias: Some(source_name.clone()),
            });
        }
    }

    // Document excluded fields
    if let Some(ref exclude) = output.exclude {
        for field_name in exclude {
            fields.push(FieldDoc {
                name: field_name.clone(),
                field_type: None,
                required: false,
                format: None,
                coerce: false,
                default_value: Some("excluded".to_string()),
                allowed_values: None,
                alias: None,
            });
        }
    }

    if fields.is_empty() {
        None
    } else {
        Some(SchemaSection {
            source: SchemaOrigin::None,
            fields,
        })
    }
}

fn field_def_to_doc(fd: &FieldDef) -> FieldDoc {
    FieldDoc {
        name: fd.name.clone(),
        field_type: fd
            .field_type
            .as_ref()
            .map(|t| format!("{:?}", t).to_lowercase()),
        required: fd.required.unwrap_or(false),
        format: fd.format.clone(),
        coerce: fd.coerce.unwrap_or(false),
        default_value: fd.default.as_ref().map(|v| v.to_string()),
        allowed_values: fd.allowed_values.clone(),
        alias: fd.alias.clone(),
    }
}

fn merge_overrides(fields: &mut Vec<FieldDoc>, overrides: &[FieldDef]) {
    for ovr in overrides {
        if let Some(existing) = fields.iter_mut().find(|f| f.name == ovr.name) {
            if let Some(ref t) = ovr.field_type {
                existing.field_type = Some(format!("{:?}", t).to_lowercase());
            }
            if let Some(r) = ovr.required {
                existing.required = r;
            }
            if ovr.format.is_some() {
                existing.format = ovr.format.clone();
            }
        } else {
            fields.push(field_def_to_doc(ovr));
        }
    }
}

// ── Helper: error handling entries ──────────────────────────────────────────

fn push_error_handling_entries(entries: &mut Vec<ConfigEntry>, config: &PipelineConfig) {
    let eh = &config.error_handling;
    let strategy_str = match eh.strategy {
        ErrorStrategy::FailFast => "fail-fast",
        ErrorStrategy::Continue => "continue",
        ErrorStrategy::BestEffort => "best-effort",
    };
    entries.push(ConfigEntry {
        category: ConfigCategory::ErrorHandling,
        key: "STRATEGY".to_string(),
        value: strategy_str.to_string(),
    });

    if let Some(ref dlq) = eh.dlq
        && let Some(ref path) = dlq.path
    {
        entries.push(ConfigEntry {
            category: ConfigCategory::ErrorHandling,
            key: "DLQ PATH".to_string(),
            value: path.clone(),
        });
    }
    if let Some(threshold) = eh.type_error_threshold {
        entries.push(ConfigEntry {
            category: ConfigCategory::ErrorHandling,
            key: "TYPE ERROR THRESHOLD".to_string(),
            value: format!("{}%", threshold * 100.0),
        });
    }
}

// ── CXL analyzer ───────────────────────────────────────────────────────────

/// CXL keywords and builtins to exclude from field reference extraction.
const CXL_KEYWORDS: &[&str] = &[
    "if",
    "then",
    "else",
    "end",
    "true",
    "false",
    "null",
    "nil",
    "and",
    "or",
    "not",
    "in",
    "match",
    "when",
    "let",
    "emit",
    "coalesce",
    "concat",
    "trim",
    "upper",
    "lower",
    "len",
    "abs",
    "round",
    "floor",
    "ceil",
    "min",
    "max",
    "sum",
    "count",
    "avg",
    "substr",
    "replace",
    "split",
    "join",
    "contains",
    "starts_with",
    "ends_with",
    "to_int",
    "to_float",
    "to_string",
    "to_date",
    "to_bool",
    "format_date",
    "parse_date",
    "now",
    "today",
    "is_null",
    "is_empty",
    "is_numeric",
    "typeof",
    "default",
    "guard",
    "log",
    "year",
    "month",
    "day",
];

/// Analyze CXL source into classified statements with field references.
///
/// Heuristic parser — classifies each non-empty line by intent (emit, filter,
/// let, log) and extracts field references. Does not require a full AST.
fn analyze_cxl(cxl: &str) -> CxlAnalysis {
    let mut statements = Vec::new();
    let mut all_refs = std::collections::BTreeSet::new();

    for line in cxl.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("//") {
            continue;
        }

        let (kind, output_field, expr) = classify_cxl_line(trimmed);
        let field_refs = extract_field_refs(&expr);
        for r in &field_refs {
            all_refs.insert(r.clone());
        }

        statements.push(CxlStatement {
            kind,
            expression: trimmed.to_string(),
            field_refs,
            output_field,
        });
    }

    CxlAnalysis {
        statements,
        all_field_refs: all_refs.into_iter().collect(),
    }
}

/// Classify a single CXL line by its statement kind.
fn classify_cxl_line(line: &str) -> (CxlStatementKind, Option<String>, String) {
    if let Some(stripped) = line.strip_prefix("emit ") {
        let rest = stripped.trim_start();
        if let Some(eq_pos) = rest.find('=') {
            let field_name = rest[..eq_pos].trim().to_string();
            let expr = rest[eq_pos + 1..].trim().to_string();
            return (CxlStatementKind::Emit, Some(field_name), expr);
        }
        return (CxlStatementKind::Emit, None, rest.to_string());
    }
    if let Some(stripped) = line.strip_prefix("let ") {
        let rest = stripped.trim_start();
        if let Some(eq_pos) = rest.find('=') {
            let binding = rest[..eq_pos].trim().to_string();
            let expr = rest[eq_pos + 1..].trim().to_string();
            return (CxlStatementKind::Let, Some(binding), expr);
        }
        return (CxlStatementKind::Let, None, rest.to_string());
    }
    if line.starts_with("if ") || line.starts_with("when ") || line.starts_with("guard ") {
        return (CxlStatementKind::Filter, None, line.to_string());
    }
    if let Some(stripped) = line.strip_prefix("log ") {
        return (CxlStatementKind::Log, None, stripped.to_string());
    }
    (CxlStatementKind::Expression, None, line.to_string())
}

/// Extract plausible field references from a CXL expression.
///
/// Heuristic: tokenize on operators/delimiters, filter out keywords,
/// numeric literals, and string literals. What remains are field references.
fn extract_field_refs(expr: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut in_string = false;
    let mut string_char = '"';
    let mut token = String::new();

    for ch in expr.chars() {
        if in_string {
            if ch == string_char {
                in_string = false;
            }
            continue;
        }

        if ch == '"' || ch == '\'' {
            in_string = true;
            string_char = ch;
            if !token.is_empty() {
                maybe_add_ref(&token, &mut refs);
                token.clear();
            }
            continue;
        }

        if ch.is_alphanumeric() || ch == '_' {
            token.push(ch);
        } else if !token.is_empty() {
            maybe_add_ref(&token, &mut refs);
            token.clear();
        }
    }
    if !token.is_empty() {
        maybe_add_ref(&token, &mut refs);
    }

    refs.sort();
    refs.dedup();
    refs
}

fn maybe_add_ref(token: &str, refs: &mut Vec<String>) {
    // Skip numeric literals
    if token
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
    {
        return;
    }
    // Skip keywords
    let lower = token.to_lowercase();
    if CXL_KEYWORDS.contains(&lower.as_str()) {
        return;
    }
    refs.push(token.to_string());
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_cxl_emit_statements() {
        let cxl = r#"
            emit full_name = first_name + " " + last_name
            emit age = year(today()) - birth_year
        "#;
        let analysis = analyze_cxl(cxl);
        let emits: Vec<_> = analysis
            .statements
            .iter()
            .filter(|s| s.kind == CxlStatementKind::Emit)
            .collect();
        assert_eq!(emits.len(), 2);
        assert_eq!(emits[0].output_field.as_deref(), Some("full_name"));
        assert!(emits[0].field_refs.contains(&"first_name".to_string()));
        assert!(emits[0].field_refs.contains(&"last_name".to_string()));
        assert_eq!(emits[1].output_field.as_deref(), Some("age"));
        assert!(emits[1].field_refs.contains(&"birth_year".to_string()));
        // all_field_refs should contain all unique refs
        assert!(analysis.all_field_refs.contains(&"first_name".to_string()));
        assert!(analysis.all_field_refs.contains(&"birth_year".to_string()));
    }

    #[test]
    fn test_analyze_cxl_filter_statements() {
        let cxl = r#"if status == "active""#;
        let analysis = analyze_cxl(cxl);
        assert_eq!(analysis.statements.len(), 1);
        assert_eq!(analysis.statements[0].kind, CxlStatementKind::Filter);
        assert!(
            analysis.statements[0]
                .field_refs
                .contains(&"status".to_string())
        );
        assert!(analysis.all_field_refs.contains(&"status".to_string()));
    }

    #[test]
    fn test_analyze_cxl_mixed() {
        let cxl = r#"
            if department == "sales"
            emit bonus = salary * 0.1
        "#;
        let analysis = analyze_cxl(cxl);
        let filters: Vec<_> = analysis
            .statements
            .iter()
            .filter(|s| s.kind == CxlStatementKind::Filter)
            .collect();
        let emits: Vec<_> = analysis
            .statements
            .iter()
            .filter(|s| s.kind == CxlStatementKind::Emit)
            .collect();
        assert_eq!(filters.len(), 1);
        assert_eq!(emits.len(), 1);
        assert!(filters[0].field_refs.contains(&"department".to_string()));
        assert_eq!(emits[0].output_field.as_deref(), Some("bonus"));
        assert!(emits[0].field_refs.contains(&"salary".to_string()));
    }

    #[test]
    fn test_analyze_cxl_let_binding() {
        let cxl = "let total = price + tax";
        let analysis = analyze_cxl(cxl);
        assert_eq!(analysis.statements.len(), 1);
        assert_eq!(analysis.statements[0].kind, CxlStatementKind::Let);
        assert_eq!(
            analysis.statements[0].output_field.as_deref(),
            Some("total")
        );
        assert!(
            analysis.statements[0]
                .field_refs
                .contains(&"price".to_string())
        );
        assert!(
            analysis.statements[0]
                .field_refs
                .contains(&"tax".to_string())
        );
    }

    #[test]
    fn test_extract_field_refs_filters_keywords() {
        let refs = extract_field_refs("if status == true then amount else 0 end");
        assert!(refs.contains(&"status".to_string()));
        assert!(refs.contains(&"amount".to_string()));
        assert!(!refs.contains(&"if".to_string()));
        assert!(!refs.contains(&"true".to_string()));
        assert!(!refs.contains(&"then".to_string()));
        assert!(!refs.contains(&"else".to_string()));
        assert!(!refs.contains(&"end".to_string()));
    }

    #[test]
    fn test_extract_field_refs_filters_strings() {
        let refs = extract_field_refs(r#"concat(first_name, " ", last_name)"#);
        assert!(refs.contains(&"first_name".to_string()));
        assert!(refs.contains(&"last_name".to_string()));
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn test_extract_field_refs_filters_numbers() {
        let refs = extract_field_refs("amount * 100 + tax_rate");
        assert!(refs.contains(&"amount".to_string()));
        assert!(refs.contains(&"tax_rate".to_string()));
        assert!(!refs.iter().any(|r| r == "100"));
    }

    #[test]
    fn test_extract_field_refs_deduplicates() {
        let refs = extract_field_refs("a + a + b");
        assert_eq!(refs, vec!["a".to_string(), "b".to_string()]);
    }
}
