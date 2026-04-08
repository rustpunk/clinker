//! Pipeline template model and instantiation.
//!
//! Templates are valid Clinker pipeline YAML files with `_template` metadata.
//! No template engine, no variables — Kiln copies the file, strips the
//! `_template` block, opens it as a new tab, and the user edits it.
//!
//! Spec: clinker-kiln-search-schemas-templates-addendum.md §S4.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Source of a template.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TemplateSource {
    /// Bundled in the Kiln binary (embedded at compile time).
    Bundled,
    /// Found in the workspace's `templates/` directory.
    Workspace,
}

/// Parsed `_template` metadata block from a pipeline YAML file.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TemplateMetadata {
    /// Display name for the template.
    pub name: String,
    /// Short description of what the template does.
    pub description: String,
    /// Category for grouping (e.g., "transform", "join", "etl").
    #[serde(default)]
    pub category: Option<String>,
    /// Tags for search/filtering.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Author identifier.
    #[serde(default)]
    pub author: Option<String>,
    /// Template version.
    #[serde(default)]
    pub version: Option<String>,
    /// Guide annotation hints (YAML path → hint text).
    #[serde(default)]
    pub hints: HashMap<String, String>,
}

/// A resolved template ready for display in the gallery.
#[derive(Clone, Debug, PartialEq)]
pub struct Template {
    /// Parsed metadata from `_template` block.
    pub metadata: TemplateMetadata,
    /// Full YAML content (including `_template` block).
    pub raw_yaml: String,
    /// Where this template came from.
    pub source: TemplateSource,
    /// Format category for filtering (derived from input type in the YAML).
    pub format_category: String,
}

/// A guide annotation overlay for template-instantiated pipelines.
#[derive(Clone, Debug)]
pub struct GuideAnnotation {
    /// 1-based line number in the YAML text.
    pub line: usize,
    /// Hint text to display.
    #[allow(dead_code)]
    pub hint: String,
}

// ── Bundled templates (embedded at compile time) ────────────────────────

const BUNDLED_TEMPLATES: &[(&str, &str)] = &[
    (
        "csv_transform",
        include_str!("templates/csv_transform.yaml"),
    ),
    ("csv_join", include_str!("templates/csv_join.yaml")),
    ("csv_dedup", include_str!("templates/csv_dedup.yaml")),
    ("json_flatten", include_str!("templates/json_flatten.yaml")),
    ("xml_extract", include_str!("templates/xml_extract.yaml")),
    ("full_etl", include_str!("templates/full_etl.yaml")),
];

// ── Parsing ─────────────────────────────────────────────────────────────

/// Intermediate struct for deserializing just the `_template` block.
#[derive(Deserialize)]
struct TemplateYaml {
    _template: TemplateMetadata,
}

/// Parse a template from YAML content.
///
/// Extracts the `_template` metadata block. Returns `None` if the YAML
/// doesn't contain a `_template` block.
pub fn parse_template(yaml: &str, source: TemplateSource) -> Option<Template> {
    let parsed: TemplateYaml = clinker_core::yaml::from_str(yaml).ok()?;
    let format_category = detect_format_category(yaml);

    Some(Template {
        metadata: parsed._template,
        raw_yaml: yaml.to_string(),
        source,
        format_category,
    })
}

/// Detect the primary format from the YAML content.
fn detect_format_category(yaml: &str) -> String {
    for line in yaml.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("type:") {
            let value = trimmed.strip_prefix("type:").unwrap().trim();
            return match value {
                "csv" => "csv",
                "json" | "jsonl" => "json",
                "xml" => "xml",
                _ => "other",
            }
            .to_string();
        }
    }
    "other".to_string()
}

// ── Template loading ────────────────────────────────────────────────────

/// Load all bundled templates.
pub fn load_bundled_templates() -> Vec<Template> {
    BUNDLED_TEMPLATES
        .iter()
        .filter_map(|(_name, yaml)| parse_template(yaml, TemplateSource::Bundled))
        .collect()
}

/// Discover workspace templates from the `templates/` directory.
pub fn load_workspace_templates(workspace_root: &Path) -> Vec<Template> {
    let templates_dir = workspace_root.join("templates");
    if !templates_dir.is_dir() {
        return Vec::new();
    }

    let Ok(entries) = fs::read_dir(&templates_dir) else {
        return Vec::new();
    };

    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml")
        })
        .filter_map(|e| {
            let content = fs::read_to_string(e.path()).ok()?;
            parse_template(&content, TemplateSource::Workspace)
        })
        .collect()
}

/// Load all templates (bundled + workspace).
pub fn load_all_templates(workspace_root: Option<&Path>) -> Vec<Template> {
    let mut templates = load_bundled_templates();
    if let Some(root) = workspace_root {
        templates.extend(load_workspace_templates(root));
    }
    templates
}

// ── Instantiation ───────────────────────────────────────────────────────

/// Strip the `_template` block from YAML, producing a valid pipeline YAML.
///
/// This is a text-level operation — it removes the `_template:` block
/// (from `_template:` to the next top-level key) without parsing/re-serializing
/// the entire YAML, preserving formatting and comments.
pub fn strip_template_block(yaml: &str) -> String {
    let mut result = String::with_capacity(yaml.len());
    let mut in_template_block = false;

    for line in yaml.lines() {
        if line.starts_with("_template:") {
            in_template_block = true;
            continue;
        }

        if in_template_block {
            // Check if this line is a new top-level key (not indented)
            if !line.is_empty() && !line.starts_with(' ') && !line.starts_with('\t') {
                in_template_block = false;
                result.push_str(line);
                result.push('\n');
            }
            // Skip indented lines that are part of _template block
            continue;
        }

        result.push_str(line);
        result.push('\n');
    }

    // Trim leading blank lines
    result.trim_start_matches('\n').to_string()
}

// ── Pipeline-from-schema generation (spec §S4.7) ────────────────────────

/// Generate a pipeline YAML scaffold from a schema.
///
/// Creates a pipeline with: source (format + placeholder path + schema link),
/// identity map stage with all fields, sink with same format.
#[allow(dead_code)]
pub fn generate_pipeline_from_schema(schema: &clinker_schema::SourceSchema) -> String {
    let name = &schema.metadata.name;
    let format = schema.metadata.format.label();
    let schema_path = schema.path.display();

    // Determine clinker format type
    let clinker_format = match schema.metadata.format {
        clinker_schema::SourceFormat::Csv | clinker_schema::SourceFormat::Tsv => "csv",
        clinker_schema::SourceFormat::Json | clinker_schema::SourceFormat::Jsonl => "json",
        clinker_schema::SourceFormat::Xml => "xml",
        clinker_schema::SourceFormat::Parquet => "csv", // fallback
    };

    let mut yaml = String::new();
    yaml.push_str(&format!("# Auto-generated from {schema_path}\n"));
    yaml.push_str(&format!("pipeline:\n  name: {name}-pipeline\n\n"));

    // Input
    yaml.push_str("inputs:\n");
    yaml.push_str(&format!("  - name: {name}\n"));
    yaml.push_str(&format!("    type: {clinker_format}\n"));
    yaml.push_str(&format!("    path: \"./data/{name}_*.{format}\"\n"));
    yaml.push_str(&format!("    schema: {schema_path}\n"));

    if clinker_format == "csv" {
        yaml.push_str("    options:\n      has_header: true\n");
    }
    if clinker_format == "xml"
        && let Some(ref elem) = schema.metadata.record_element
    {
        yaml.push_str(&format!("    options:\n      record_element: {elem}\n"));
    }

    // Identity map transformation
    yaml.push_str("\ntransformations:\n");
    yaml.push_str("  - name: select_fields\n");
    yaml.push_str("    cxl: |\n");

    let top_level_fields: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();

    for field in &top_level_fields {
        let clean = field.trim_start_matches('@');
        yaml.push_str(&format!("      emit {clean} = {clean}\n"));
    }

    // Output
    yaml.push_str("\noutputs:\n");
    yaml.push_str(&format!("  - name: {name}_output\n"));
    yaml.push_str(&format!("    type: {clinker_format}\n"));
    yaml.push_str(&format!("    path: \"./output/{name}_output.{format}\"\n"));

    yaml
}

// ── Guide annotations (spec §S4.6) ──────────────────────────────────────

/// Generate guide annotations for a template-instantiated pipeline.
///
/// If the template has `hints`, map them to line numbers in the stripped YAML.
/// If no hints, auto-generate for common placeholder patterns.
pub fn generate_guide_annotations(
    template: &Template,
    stripped_yaml: &str,
) -> Vec<GuideAnnotation> {
    if !template.metadata.hints.is_empty() {
        return generate_from_hints(&template.metadata.hints, stripped_yaml);
    }

    // Auto-generate: look for obvious placeholder values
    auto_generate_annotations(stripped_yaml)
}

/// Generate annotations from explicit template hints.
fn generate_from_hints(hints: &HashMap<String, String>, yaml: &str) -> Vec<GuideAnnotation> {
    let mut annotations = Vec::new();

    for (yaml_path, hint_text) in hints {
        // Find the line that contains this path's leaf key
        let leaf_key = yaml_path
            .split('.')
            .next_back()
            .and_then(|s| s.split('[').next())
            .unwrap_or(yaml_path);

        for (idx, line) in yaml.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with(&format!("{leaf_key}:"))
                || trimmed.starts_with(&format!("{leaf_key} "))
            {
                annotations.push(GuideAnnotation {
                    line: idx + 1,
                    hint: hint_text.clone(),
                });
                break; // First match only per hint
            }
        }
    }

    annotations.sort_by_key(|a| a.line);
    annotations
}

/// Auto-generate annotations for common placeholder patterns.
fn auto_generate_annotations(yaml: &str) -> Vec<GuideAnnotation> {
    let mut annotations = Vec::new();
    let placeholder_patterns = [
        ("./data/input", "replace with your source path"),
        ("./data/primary", "replace with your primary source path"),
        ("./data/lookup", "replace with your lookup source path"),
        ("./output/", "replace with your output path"),
        ("\"\"", "provide a value"),
    ];

    for (idx, line) in yaml.lines().enumerate() {
        for (pattern, hint) in &placeholder_patterns {
            if line.contains(pattern) {
                annotations.push(GuideAnnotation {
                    line: idx + 1,
                    hint: hint.to_string(),
                });
                break; // One annotation per line max
            }
        }
    }

    annotations
}

/// All available format categories for gallery filtering.
pub const FORMAT_CATEGORIES: &[&str] = &["All", "CSV", "JSON", "XML", "Multi"];

/// Filter label matches a template's format category.
pub fn format_filter_matches(filter: &str, category: &str) -> bool {
    match filter {
        "All" => true,
        "CSV" => category == "csv",
        "JSON" => category == "json",
        "XML" => category == "xml",
        "Multi" => category == "other",
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bundled_templates() {
        let templates = load_bundled_templates();
        assert!(
            templates.len() >= 6,
            "expected at least 6 bundled templates, got {}",
            templates.len()
        );

        let names: Vec<&str> = templates.iter().map(|t| t.metadata.name.as_str()).collect();
        assert!(names.contains(&"CSV Transform"));
        assert!(names.contains(&"Multi-Source Join"));
        assert!(names.contains(&"JSON Flatten"));
        assert!(names.contains(&"XML Extract"));
        assert!(names.contains(&"Full ETL Pipeline"));
    }

    #[test]
    fn test_strip_template_block() {
        let yaml = r#"_template:
  name: "Test"
  description: "A test template"
  tags: ["csv"]

pipeline:
  name: test

inputs:
  - name: source
    type: csv
    path: ./data/input.csv
"#;
        let stripped = strip_template_block(yaml);
        assert!(!stripped.contains("_template"));
        assert!(stripped.contains("pipeline:"));
        assert!(stripped.contains("inputs:"));
    }

    #[test]
    fn test_detect_format_category() {
        assert_eq!(detect_format_category("  type: csv\n"), "csv");
        assert_eq!(detect_format_category("  type: json\n"), "json");
        assert_eq!(detect_format_category("  type: xml\n"), "xml");
    }

    #[test]
    fn test_template_metadata_parsing() {
        let yaml = include_str!("templates/csv_transform.yaml");
        let template = parse_template(yaml, TemplateSource::Bundled).unwrap();
        assert_eq!(template.metadata.name, "CSV Transform");
        assert_eq!(template.metadata.tags, vec!["csv", "filter", "map"]);
        assert!(!template.metadata.hints.is_empty());
        assert_eq!(template.format_category, "csv");
    }

    #[test]
    fn test_format_filter_matches() {
        assert!(format_filter_matches("All", "csv"));
        assert!(format_filter_matches("CSV", "csv"));
        assert!(!format_filter_matches("CSV", "json"));
        assert!(format_filter_matches("JSON", "json"));
    }

    #[test]
    fn test_generate_pipeline_from_schema() {
        let schema_yaml = r#"
_schema:
  name: customers
  format: csv
  description: "Customer data"

fields:
  - name: id
    type: int
    nullable: false
  - name: email
    type: string
  - name: status
    type: string
"#;
        let schema = clinker_schema::parse_schema(
            schema_yaml,
            std::path::Path::new("schemas/customers.schema.yaml"),
        )
        .unwrap();

        let yaml = generate_pipeline_from_schema(&schema);

        assert!(yaml.contains("name: customers-pipeline"));
        assert!(yaml.contains("type: csv"));
        assert!(yaml.contains("schema: schemas/customers.schema.yaml"));
        assert!(yaml.contains("emit id = id"));
        assert!(yaml.contains("emit email = email"));
        assert!(yaml.contains("emit status = status"));
        assert!(yaml.contains("has_header: true"));
    }

    #[test]
    fn test_guide_annotations_from_hints() {
        let yaml = include_str!("templates/csv_transform.yaml");
        let template = parse_template(yaml, TemplateSource::Bundled).unwrap();
        let stripped = strip_template_block(yaml);
        let annotations = generate_guide_annotations(&template, &stripped);

        // Should have annotations for the hints defined in csv_transform.yaml
        assert!(!annotations.is_empty());
        // Check that path hint was found
        assert!(annotations.iter().any(|a| a.hint.contains("source path")));
    }

    #[test]
    fn test_guide_annotations_auto_generate() {
        let yaml = r#"_template:
  name: "Bare"
  description: "No hints"

pipeline:
  name: test

inputs:
  - name: source
    type: csv
    path: "./data/input.csv"

outputs:
  - name: dest
    type: csv
    path: "./output/result.csv"
"#;
        let template = parse_template(yaml, TemplateSource::Bundled).unwrap();
        let stripped = strip_template_block(yaml);
        let annotations = generate_guide_annotations(&template, &stripped);

        // Auto-generated: should detect ./data/input and ./output/ placeholders
        assert!(annotations.len() >= 2);
        assert!(annotations.iter().any(|a| a.hint.contains("source path")));
        assert!(annotations.iter().any(|a| a.hint.contains("output path")));
    }
}
