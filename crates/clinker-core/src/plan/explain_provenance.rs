//! Field-level provenance rendering for `clinker explain --field`.
//!
//! Given a dotted path like `node_name.param_name`, walks the [`ProvenanceDb`]
//! to produce a human-readable provenance chain showing which configuration
//! layer won and which were shadowed.

use std::fmt;

use crate::plan::CompiledPlan;
use crate::span::Span;

/// Error returned when field provenance cannot be resolved.
#[derive(Debug)]
pub enum ProvenanceExplainError {
    /// Dotted path has no `.` separator — must be `node_name.param_name`.
    InvalidPath(String),
    /// The node exists in the provenance DB but the param does not.
    ParamNotFound {
        node_name: String,
        param_name: String,
        valid_params: Vec<String>,
    },
    /// No provenance entries exist for this node name.
    NodeNotFound {
        node_name: String,
        valid_nodes: Vec<String>,
    },
}

impl fmt::Display for ProvenanceExplainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProvenanceExplainError::InvalidPath(path) => {
                write!(
                    f,
                    "invalid field path '{path}': expected format 'node_name.param_name'"
                )
            }
            ProvenanceExplainError::ParamNotFound {
                node_name,
                param_name,
                valid_params,
            } => {
                write!(f, "no provenance for '{node_name}.{param_name}'")?;
                if valid_params.is_empty() {
                    write!(f, "\n  node '{node_name}' has no tracked config params")
                } else {
                    write!(f, "\n  valid params for '{node_name}':")?;
                    for p in valid_params {
                        write!(f, "\n    - {node_name}.{p}")?;
                    }
                    Ok(())
                }
            }
            ProvenanceExplainError::NodeNotFound {
                node_name,
                valid_nodes,
            } => {
                write!(f, "no provenance entries for node '{node_name}'")?;
                if valid_nodes.is_empty() {
                    write!(f, "\n  no composition nodes with tracked config found")
                } else {
                    write!(f, "\n  nodes with provenance:")?;
                    for n in valid_nodes {
                        write!(f, "\n    - {n}")?;
                    }
                    Ok(())
                }
            }
        }
    }
}

impl std::error::Error for ProvenanceExplainError {}

/// Format a span for human-readable output.
///
/// Since the compile path uses `Span::line_only` (synthetic spans without
/// file-backed byte offsets), we extract the embedded line number when
/// available.
fn format_span(span: Span) -> String {
    if let Some(line) = span.synthetic_line_number() {
        format!("line {line}")
    } else if span.file.get() == u32::MAX {
        // Fully synthetic (no line info).
        String::new()
    } else {
        // Real span with FileId — without SourceDb we can only show the offset.
        format!("offset {}", span.start)
    }
}

/// Render a field provenance chain as human-readable text.
///
/// Output format matches the spec:
/// ```text
/// Field: enrich1.fuzzy_threshold
///
///   Resolved value: 0.92
///
///   Provenance chain (outermost to innermost):
///   [WON] ChannelFixed     →  0.92  (line 12)
///         ChannelDefault   →  0.85  (shadowed)  (line 15)
///         CompositionDefault →  0.75  (shadowed)  (line 8)
/// ```
pub fn explain_field_provenance(
    plan: &CompiledPlan,
    dotted_path: &str,
) -> Result<String, ProvenanceExplainError> {
    let (node_name, param_name) = parse_dotted_path(dotted_path)?;

    let provenance_db = plan.provenance();

    // Check if the node exists in the provenance DB at all.
    let node_params = provenance_db.params_for_node(node_name);
    if node_params.is_empty() {
        return Err(ProvenanceExplainError::NodeNotFound {
            node_name: node_name.to_owned(),
            valid_nodes: provenance_db
                .node_names()
                .into_iter()
                .map(String::from)
                .collect(),
        });
    }

    // Look up the specific (node, param) entry.
    let resolved = provenance_db.get(node_name, param_name).ok_or_else(|| {
        ProvenanceExplainError::ParamNotFound {
            node_name: node_name.to_owned(),
            param_name: param_name.to_owned(),
            valid_params: node_params.into_iter().map(String::from).collect(),
        }
    })?;

    let mut output = String::new();
    output.push_str(&format!("Field: {dotted_path}\n\n"));
    output.push_str(&format!(
        "  Resolved value: {}\n\n",
        format_json_value(&resolved.value)
    ));
    output.push_str("  Provenance chain (outermost to innermost):\n");

    // Sort layers by kind priority descending (highest priority first).
    let mut layers: Vec<_> = resolved.provenance.iter().collect();
    layers.sort_by(|a, b| b.kind.cmp(&a.kind));

    for layer in &layers {
        let prefix = if layer.won { "[WON]" } else { "     " };
        let suffix = if layer.won { "" } else { "  (shadowed)" };
        let kind_label = format!("{}", layer.kind);
        let value_str = resolved
            .layer_value(layer.kind)
            .map(format_json_value)
            .unwrap_or_else(|| "?".to_owned());
        let span_str = format_span(layer.span);
        let span_part = if span_str.is_empty() {
            String::new()
        } else {
            format!("  ({span_str})")
        };
        output.push_str(&format!(
            "  {prefix} {kind_label:<22} →  {value_str}{suffix}{span_part}\n"
        ));
    }

    Ok(output)
}

/// Format a JSON value for display — strip quotes from strings, compact arrays.
fn format_json_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => format!("\"{s}\""),
        serde_json::Value::Null => "null".to_owned(),
        _ => v.to_string(),
    }
}

/// Split a dotted path into `(node_name, param_name)`.
fn parse_dotted_path(path: &str) -> Result<(&str, &str), ProvenanceExplainError> {
    let dot_idx = path
        .find('.')
        .ok_or_else(|| ProvenanceExplainError::InvalidPath(path.to_owned()))?;
    let node_name = &path[..dot_idx];
    let param_name = &path[dot_idx + 1..];
    if node_name.is_empty() || param_name.is_empty() {
        return Err(ProvenanceExplainError::InvalidPath(path.to_owned()));
    }
    Ok((node_name, param_name))
}

/// Look up error/warning code documentation embedded at compile time.
///
/// Returns the doc content for any code registered in this function's
/// match body, or `None` for unknown codes. Every registered code must
/// have a matching `docs/explain/<code>.md` file; the section-coverage
/// contract is enforced by `test_explain_docs_all_have_required_sections`.
pub fn explain_code(code: &str) -> Option<&'static str> {
    match code {
        "E101" => Some(include_str!("../../../../docs/explain/E101.md")),
        "E102" => Some(include_str!("../../../../docs/explain/E102.md")),
        "E103" => Some(include_str!("../../../../docs/explain/E103.md")),
        "E104" => Some(include_str!("../../../../docs/explain/E104.md")),
        "E105" => Some(include_str!("../../../../docs/explain/E105.md")),
        "E106" => Some(include_str!("../../../../docs/explain/E106.md")),
        "E107" => Some(include_str!("../../../../docs/explain/E107.md")),
        "E108" => Some(include_str!("../../../../docs/explain/E108.md")),
        "E300" => Some(include_str!("../../../../docs/explain/E300.md")),
        "E301" => Some(include_str!("../../../../docs/explain/E301.md")),
        "E303" => Some(include_str!("../../../../docs/explain/E303.md")),
        "E304" => Some(include_str!("../../../../docs/explain/E304.md")),
        "E305" => Some(include_str!("../../../../docs/explain/E305.md")),
        "E306" => Some(include_str!("../../../../docs/explain/E306.md")),
        "E307" => Some(include_str!("../../../../docs/explain/E307.md")),
        "E308" => Some(include_str!("../../../../docs/explain/E308.md")),
        "E309" => Some(include_str!("../../../../docs/explain/E309.md")),
        "E310" => Some(include_str!("../../../../docs/explain/E310.md")),
        "E311" => Some(include_str!("../../../../docs/explain/E311.md")),
        "E313" => Some(include_str!("../../../../docs/explain/E313.md")),
        "E150b" => Some(include_str!("../../../../docs/explain/E150b.md")),
        "E150c" => Some(include_str!("../../../../docs/explain/E150c.md")),
        "E150d" => Some(include_str!("../../../../docs/explain/E150d.md")),
        "E150e" => Some(include_str!("../../../../docs/explain/E150e.md")),
        "E15Y" => Some(include_str!("../../../../docs/explain/E15Y.md")),
        "W101" => Some(include_str!("../../../../docs/explain/W101.md")),
        "W302" => Some(include_str!("../../../../docs/explain/W302.md")),
        "W305" => Some(include_str!("../../../../docs/explain/W305.md")),
        "W306" => Some(include_str!("../../../../docs/explain/W306.md")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::composition::{LayerKind, ResolvedValue};
    use crate::span::Span;

    #[test]
    fn test_parse_dotted_path_valid() {
        let (node, param) = parse_dotted_path("enrich1.fuzzy_threshold").unwrap();
        assert_eq!(node, "enrich1");
        assert_eq!(param, "fuzzy_threshold");
    }

    #[test]
    fn test_parse_dotted_path_no_dot() {
        assert!(parse_dotted_path("nodot").is_err());
    }

    #[test]
    fn test_parse_dotted_path_empty_parts() {
        assert!(parse_dotted_path(".param").is_err());
        assert!(parse_dotted_path("node.").is_err());
    }

    #[test]
    fn test_format_span_line_only() {
        let span = Span::line_only(42);
        assert_eq!(format_span(span), "line 42");
    }

    #[test]
    fn test_format_span_synthetic() {
        assert_eq!(format_span(Span::SYNTHETIC), "");
    }

    #[test]
    fn test_layer_value_stored_on_new() {
        let rv = ResolvedValue::new(
            serde_json::json!(0.75),
            LayerKind::CompositionDefault,
            Span::line_only(8),
        );
        assert_eq!(
            rv.layer_value(LayerKind::CompositionDefault),
            Some(&serde_json::json!(0.75))
        );
    }

    #[test]
    fn test_layer_value_stored_on_apply() {
        let mut rv = ResolvedValue::new(
            serde_json::json!(0.75),
            LayerKind::CompositionDefault,
            Span::line_only(8),
        );
        rv.apply_layer(
            serde_json::json!(0.92),
            LayerKind::ChannelFixed,
            Span::line_only(12),
        );
        assert_eq!(
            rv.layer_value(LayerKind::CompositionDefault),
            Some(&serde_json::json!(0.75))
        );
        assert_eq!(
            rv.layer_value(LayerKind::ChannelFixed),
            Some(&serde_json::json!(0.92))
        );
        assert!(rv.provenance.iter().find(|l| l.won).unwrap().kind == LayerKind::ChannelFixed);
    }

    #[test]
    fn test_explain_code_e105() {
        let doc = explain_code("E105").unwrap();
        assert!(!doc.is_empty());
        assert!(doc.contains("E105"));
    }

    #[test]
    fn test_explain_code_unknown() {
        assert!(explain_code("E999").is_none());
    }

    #[test]
    fn test_explain_docs_all_have_required_sections() {
        let codes = [
            "E101", "E102", "E103", "E104", "E105", "E106", "E107", "E108", "E150b", "E150c",
            "E150d", "E150e", "E300", "E301", "E303", "E304", "E305", "E306", "E307", "E308",
            "E309", "E310", "E311", "E313", "E15Y", "W101", "W302", "W305", "W306",
        ];
        let required_sections = [
            "## What it means",
            "## Example",
            "## How to fix",
            "## Technical context",
            "## See also",
        ];

        for code in &codes {
            let doc =
                explain_code(code).unwrap_or_else(|| panic!("explain_code({code}) returned None"));
            for section in &required_sections {
                assert!(
                    doc.contains(section),
                    "doc {code} is missing required section: {section}"
                );
            }
        }
    }
}
