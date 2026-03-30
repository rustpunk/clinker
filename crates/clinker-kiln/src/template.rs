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
    pub hint: String,
}

// ── Bundled templates (embedded at compile time) ────────────────────────

const BUNDLED_TEMPLATES: &[(&str, &str)] = &[
    ("csv_transform", include_str!("templates/csv_transform.yaml")),
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
    let parsed: TemplateYaml = serde_saphyr::from_str(yaml).ok()?;
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
        assert!(templates.len() >= 6, "expected at least 6 bundled templates, got {}", templates.len());

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
}
