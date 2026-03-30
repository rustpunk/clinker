//! Schema validation engine — validate pipeline field references against linked schemas.
//!
//! All schema issues are **warnings** (never errors). Schema validation helps,
//! it doesn't block. Missing schemas or unlinked pipelines produce no warnings.
//!
//! Spec: clinker-kiln-search-schemas-templates-addendum.md §S3.5, §S3.6.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::model::{FieldType, SchemaIndex, SourceSchema};
use clinker_core::config::{FormatKind, InputConfig, PipelineConfig, TransformConfig};

/// A schema validation warning.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaWarning {
    /// Which stage/input produced the warning.
    pub stage_name: String,
    /// Warning message.
    pub message: String,
    /// Optional "did you mean" suggestion.
    pub suggestion: Option<String>,
    /// Category for UI rendering.
    pub kind: WarningKind,
}

/// Warning categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarningKind {
    /// Field referenced in CXL not found in schema.
    FieldNotFound,
    /// Schema file referenced but missing on disk.
    SchemaMissing,
    /// Source format doesn't match schema format.
    FormatMismatch,
    /// Join key not found in one of the schemas.
    JoinKeyMissing,
    /// Join key type mismatch between schemas.
    JoinKeyTypeMismatch,
}

/// Validate a pipeline against the schema index.
///
/// Returns warnings for field references that don't match linked schemas.
/// Pipelines without `schema:` references produce no warnings.
pub fn validate_pipeline(
    config: &PipelineConfig,
    index: &SchemaIndex,
    workspace_root: &Path,
) -> Vec<SchemaWarning> {
    let mut warnings = Vec::new();

    for input in &config.inputs {
        validate_input(input, index, workspace_root, &mut warnings);
    }

    // Collect known fields from all inputs for CXL validation
    let mut available_fields: HashSet<String> = HashSet::new();
    for input in &config.inputs {
        if let Some(schema) = resolve_input_schema(input, index, workspace_root) {
            for field_name in schema.all_field_names() {
                available_fields.insert(field_name);
            }
        }
    }

    // Validate transformations against accumulated fields
    for transform in &config.transformations {
        validate_transform(transform, &available_fields, &mut warnings);
        // Add emitted fields to available set
        for emitted in extract_emit_fields(&transform.cxl) {
            available_fields.insert(emitted);
        }
    }

    warnings
}

/// Validate an input stage against its linked schema.
fn validate_input(
    input: &InputConfig,
    index: &SchemaIndex,
    workspace_root: &Path,
    warnings: &mut Vec<SchemaWarning>,
) {
    let Some(schema_ref) = &input.schema else {
        return; // No schema linked — no validation
    };

    let schema_path = workspace_root.join(schema_ref);
    let canonical = schema_path.canonicalize().unwrap_or(schema_path.clone());

    // Check if schema file exists in index
    let schema = index.schemas.values().find(|s| {
        s.path.canonicalize().unwrap_or_else(|_| s.path.clone()) == canonical
    });

    let Some(schema) = schema else {
        warnings.push(SchemaWarning {
            stage_name: input.name.clone(),
            message: format!("Schema file not found: {schema_ref}"),
            suggestion: None,
            kind: WarningKind::SchemaMissing,
        });
        return;
    };

    // Check format mismatch
    let format_matches = match input.r#type {
        FormatKind::Csv => matches!(
            schema.metadata.format,
            crate::model::SourceFormat::Csv | crate::model::SourceFormat::Tsv
        ),
        FormatKind::Json => matches!(
            schema.metadata.format,
            crate::model::SourceFormat::Json | crate::model::SourceFormat::Jsonl
        ),
        FormatKind::Xml => schema.metadata.format == crate::model::SourceFormat::Xml,
        FormatKind::FixedWidth => false, // No schema format for fixed-width yet
    };

    if !format_matches {
        warnings.push(SchemaWarning {
            stage_name: input.name.clone(),
            message: format!(
                "Source format {:?} doesn't match schema format {:?}",
                input.r#type, schema.metadata.format
            ),
            suggestion: None,
            kind: WarningKind::FormatMismatch,
        });
    }
}

/// Validate a transformation's CXL against available fields.
fn validate_transform(
    transform: &TransformConfig,
    available_fields: &HashSet<String>,
    warnings: &mut Vec<SchemaWarning>,
) {
    if available_fields.is_empty() {
        return; // No schema info — skip validation
    }

    // Extract field references from CXL (simple heuristic)
    let referenced = extract_referenced_fields(&transform.cxl);
    let schema_fields: Vec<&str> = available_fields.iter().map(|s| s.as_str()).collect();

    for field in referenced {
        if !available_fields.contains(&field) {
            let suggestion = find_closest_field(&field, &schema_fields);
            warnings.push(SchemaWarning {
                stage_name: transform.name.clone(),
                message: format!("Field '{field}' not found in upstream schema"),
                suggestion,
                kind: WarningKind::FieldNotFound,
            });
        }
    }
}

/// Resolve the schema for an input stage.
fn resolve_input_schema<'a>(
    input: &InputConfig,
    index: &'a SchemaIndex,
    workspace_root: &Path,
) -> Option<&'a SourceSchema> {
    let schema_ref = input.schema.as_ref()?;
    let schema_path = workspace_root.join(schema_ref);
    let canonical = schema_path.canonicalize().unwrap_or(schema_path);

    index.schemas.values().find(|s| {
        s.path.canonicalize().unwrap_or_else(|_| s.path.clone()) == canonical
    })
}

// ── CXL field extraction (heuristic) ────────────────────────────────────

/// Extract field names referenced in CXL (simple heuristic parser).
///
/// Looks for identifiers that appear after operators or at the start of
/// expressions. This is intentionally conservative — false negatives are
/// acceptable, false positives (warning about valid fields) are not.
fn extract_referenced_fields(cxl: &str) -> Vec<String> {
    let mut fields = HashSet::new();

    for line in cxl.lines() {
        let trimmed = line.trim();

        // Skip comments and empty lines
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("//") {
            continue;
        }

        // "filter <expr>" — extract identifiers from the expression
        if let Some(expr) = trimmed.strip_prefix("filter ") {
            extract_identifiers(expr, &mut fields);
        }

        // "emit <name> = <expr>" — extract from the RHS
        if let Some(rest) = trimmed.strip_prefix("emit ") {
            if let Some((_name, expr)) = rest.split_once('=') {
                extract_identifiers(expr.trim(), &mut fields);
            }
        }

        // "distinct by <fields>" — extract field list
        if let Some(rest) = trimmed.strip_prefix("distinct by ") {
            for field in rest.split(',') {
                let f = field.trim();
                if is_identifier(f) {
                    fields.insert(f.to_string());
                }
            }
        }

        // "sort by <field> [asc|desc]"
        if let Some(rest) = trimmed.strip_prefix("sort by ") {
            let field = rest.split_whitespace().next().unwrap_or("");
            if is_identifier(field) {
                fields.insert(field.to_string());
            }
        }

        // "lookup_join <source> on key = <field>"
        if trimmed.starts_with("lookup_join") {
            if let Some(key_part) = trimmed.split("key").nth(1) {
                if let Some((_eq, field)) = key_part.split_once('=') {
                    let f = field.trim();
                    if is_identifier(f) {
                        fields.insert(f.to_string());
                    }
                }
            }
        }
    }

    fields.into_iter().collect()
}

/// Extract fields emitted by a CXL block (for upstream field tracking).
fn extract_emit_fields(cxl: &str) -> Vec<String> {
    let mut fields = Vec::new();
    for line in cxl.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("emit ") {
            if let Some((name, _expr)) = rest.split_once('=') {
                let name = name.trim();
                if is_identifier(name) {
                    fields.push(name.to_string());
                }
            }
        }
    }
    fields
}

/// Extract identifiers from a CXL expression string.
fn extract_identifiers(expr: &str, fields: &mut HashSet<String>) {
    // Strip quoted strings first to avoid extracting string literal content
    let stripped = strip_quoted_strings(expr);

    // Split on operators and delimiters, keep potential identifiers
    let delimiters = [
        ' ', '+', '-', '*', '/', '(', ')', ',', '=', '!', '<', '>', '&', '|',
    ];

    for token in stripped.split(|c: char| delimiters.contains(&c)) {
        let token = token.trim().trim_matches('.');
        if is_identifier(token) && !is_keyword_or_literal(token) {
            fields.insert(token.to_string());
        }
    }
}

/// Remove quoted string content, replacing with empty strings.
fn strip_quoted_strings(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '"' || ch == '\'' {
            // Skip until matching quote
            for inner in chars.by_ref() {
                if inner == ch {
                    break;
                }
            }
            result.push(' '); // Replace quoted content with space
        } else {
            result.push(ch);
        }
    }
    result
}

/// Check if a string is a valid CXL identifier (field name).
fn is_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Check if a token is a CXL keyword or literal (not a field reference).
fn is_keyword_or_literal(s: &str) -> bool {
    matches!(
        s,
        "true" | "false" | "null" | "nil" | "and" | "or" | "not"
        | "filter" | "emit" | "distinct" | "sort" | "by" | "asc" | "desc"
        | "lookup_join" | "on" | "key" | "pull" | "if" | "else" | "then"
        | "contains" | "starts_with" | "ends_with" | "to_upper" | "to_lower"
        | "len" | "trim" | "split" | "last" | "first" | "count" | "sum"
        | "min" | "max" | "avg" | "abs" | "round" | "floor" | "ceil"
    )
}

// ── Levenshtein "did you mean" ──────────────────────────────────────────

/// Find the closest field name within edit distance 2.
fn find_closest_field(target: &str, candidates: &[&str]) -> Option<String> {
    let mut best: Option<(&str, usize)> = None;

    for &candidate in candidates {
        let dist = levenshtein(target, candidate);
        if dist <= 2 {
            if best.is_none() || dist < best.unwrap().1 {
                best = Some((candidate, dist));
            }
        }
    }

    best.map(|(s, _)| format!("did you mean '{s}'?"))
}

/// Levenshtein edit distance.
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let n = a.len();
    let m = b.len();

    if n == 0 { return m; }
    if m == 0 { return n; }

    let mut prev: Vec<usize> = (0..=m).collect();
    let mut curr = vec![0; m + 1];

    for i in 1..=n {
        curr[0] = i;
        for j in 1..=m {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1)
                .min(curr[j - 1] + 1)
                .min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[m]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levenshtein() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("email", "emal"), 1);
        assert_eq!(levenshtein("customer_id", "custmer_id"), 1);
    }

    #[test]
    fn test_find_closest_field() {
        let candidates = vec!["email", "name", "customer_id", "status"];
        assert_eq!(
            find_closest_field("emal", &candidates),
            Some("did you mean 'email'?".to_string())
        );
        assert_eq!(
            find_closest_field("completely_different", &candidates),
            None
        );
    }

    #[test]
    fn test_extract_emit_fields() {
        let cxl = r#"
emit full_name = first_name + " " + last_name
emit domain = email.split("@").last()
filter status == "active"
"#;
        let emitted = extract_emit_fields(cxl);
        assert_eq!(emitted, vec!["full_name", "domain"]);
    }

    #[test]
    fn test_extract_referenced_fields() {
        let cxl = r#"
filter status == "active"
emit full_name = first_name + " " + last_name
distinct by customer_id
"#;
        let fields = extract_referenced_fields(cxl);
        assert!(fields.contains(&"status".to_string()));
        assert!(fields.contains(&"first_name".to_string()));
        assert!(fields.contains(&"last_name".to_string()));
        assert!(fields.contains(&"customer_id".to_string()));
        // Should not contain keywords or literals
        assert!(!fields.contains(&"active".to_string())); // it's in quotes
        assert!(!fields.contains(&"filter".to_string()));
        assert!(!fields.contains(&"emit".to_string()));
    }

    #[test]
    fn test_is_identifier() {
        assert!(is_identifier("email"));
        assert!(is_identifier("customer_id"));
        assert!(is_identifier("_private"));
        assert!(!is_identifier("123"));
        assert!(!is_identifier(""));
        assert!(!is_identifier("hello world"));
    }

    #[test]
    fn test_validate_pipeline_no_schema() {
        // Pipeline without schema references should produce no warnings
        let config = PipelineConfig {
            pipeline: clinker_core::config::PipelineMeta {
                name: "test".to_string(),
                memory_limit: None,
                vars: None,
                date_formats: None,
                rules_path: None,
                concurrency: None,
                date_locale: None,
                log_rules: None,
                sort_output: None,
                include_provenance: None,
            },
            inputs: vec![InputConfig {
                name: "src".to_string(),
                r#type: FormatKind::Csv,
                path: "./data.csv".to_string(),
                schema: None,
                schema_overrides: None,
                options: None,
                notes: None,
            }],
            outputs: vec![],
            transformations: vec![],
            error_handling: Default::default(),
            notes: None,
        };

        let index = SchemaIndex::default();
        let warnings = validate_pipeline(&config, &index, Path::new("/tmp"));
        assert!(warnings.is_empty());
    }
}
