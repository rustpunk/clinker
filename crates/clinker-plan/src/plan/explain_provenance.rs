//! Field-level provenance rendering for `clinker explain --field`.
//!
//! Given a dotted path like `node_name.param_name`, walks the [`ProvenanceDb`]
//! to produce a human-readable provenance chain showing which configuration
//! layer won and which were shadowed.

use std::fmt;

use crate::plan::CompiledPlan;
use clinker_core_types::span::Span;

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
        "E312" => Some(include_str!("../../../../docs/explain/E312.md")),
        "E313" => Some(include_str!("../../../../docs/explain/E313.md")),
        "E319" => Some(include_str!("../../../../docs/explain/E319.md")),
        "E320" => Some(include_str!("../../../../docs/explain/E320.md")),
        "E321" => Some(include_str!("../../../../docs/explain/E321.md")),
        "E330" => Some(include_str!("../../../../docs/explain/E330.md")),
        "E331" => Some(include_str!("../../../../docs/explain/E331.md")),
        "E332" => Some(include_str!("../../../../docs/explain/E332.md")),
        "E333" => Some(include_str!("../../../../docs/explain/E333.md")),
        "E334" => Some(include_str!("../../../../docs/explain/E334.md")),
        "E335" => Some(include_str!("../../../../docs/explain/E335.md")),
        "E336" => Some(include_str!("../../../../docs/explain/E336.md")),
        "E337" => Some(include_str!("../../../../docs/explain/E337.md")),
        "E338" => Some(include_str!("../../../../docs/explain/E338.md")),
        "E339" => Some(include_str!("../../../../docs/explain/E339.md")),
        "E340" => Some(include_str!("../../../../docs/explain/E340.md")),
        "E341" => Some(include_str!("../../../../docs/explain/E341.md")),
        "E342" => Some(include_str!("../../../../docs/explain/E342.md")),
        "E323" => Some(include_str!("../../../../docs/explain/E323.md")),
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
    use clinker_core_types::span::Span;

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
    fn test_explain_code_e319() {
        let doc = explain_code("E319").unwrap();
        assert!(doc.contains("E319"));
        assert!(doc.contains("on_miss: error"));
        assert!(doc.contains("no matching build row"));
    }

    #[test]
    fn test_explain_code_e320_disk_cap() {
        let doc = explain_code("E320").unwrap();
        assert!(doc.contains("E320"));
        assert!(doc.contains("disk_cap_bytes"));
        // The doc must keep E320 distinct from OOM and from a full disk.
        assert!(doc.contains("not an out-of-memory"));
    }

    #[test]
    fn test_explain_code_e321_disk_full() {
        let doc = explain_code("E321").unwrap();
        assert!(doc.contains("E321"));
        assert!(doc.contains("out of space"));
        assert!(doc.contains("not the configured spill cap"));
    }

    #[test]
    fn test_explain_code_e330_spill_tmpfs() {
        let doc = explain_code("E330").unwrap();
        assert!(doc.contains("E330"));
        assert!(doc.contains("tmpfs"));
        assert!(doc.contains("storage.spill.dir"));
    }

    #[test]
    fn test_explain_code_e331_spill_network() {
        let doc = explain_code("E331").unwrap();
        assert!(doc.contains("E331"));
        assert!(doc.contains("network"));
        assert!(doc.contains("storage.spill.dir"));
    }

    #[test]
    fn test_explain_code_e332_staging_network() {
        let doc = explain_code("E332").unwrap();
        assert!(doc.contains("E332"));
        assert!(doc.contains("network"));
        assert!(doc.contains("storage.staging.dir"));
    }

    #[test]
    fn test_explain_code_e333_staging_same_device() {
        let doc = explain_code("E333").unwrap();
        assert!(doc.contains("E333"));
        assert!(doc.contains("same"));
        assert!(doc.contains("storage.staging.dir"));
    }

    #[test]
    fn test_explain_code_e334_spill_equals_staging() {
        let doc = explain_code("E334").unwrap();
        assert!(doc.contains("E334"));
        assert!(doc.contains("storage.spill.dir"));
        assert!(doc.contains("storage.staging.dir"));
    }

    #[test]
    fn test_explain_code_e335_staging_verify_mismatch() {
        let doc = explain_code("E335").unwrap();
        assert!(doc.contains("E335"));
        assert!(doc.contains("BLAKE3"));
        // Must keep the content-hash mismatch distinct from a plain I/O fault.
        assert!(doc.contains("corrupt"));
    }

    #[test]
    fn test_explain_code_e336_staging_disk_cap() {
        let doc = explain_code("E336").unwrap();
        assert!(doc.contains("E336"));
        assert!(doc.contains("storage.staging.disk_cap_bytes"));
        // The configured cap is distinct from a physically full volume.
        assert!(doc.contains("cap"));
    }

    #[test]
    fn test_explain_code_e337_staging_already_exists() {
        let doc = explain_code("E337").unwrap();
        assert!(doc.contains("E337"));
        assert!(doc.contains("on_existing"));
    }

    #[test]
    fn test_explain_code_e338_x12_split_rejected() {
        let doc = explain_code("E338").unwrap();
        assert!(doc.contains("E338"));
        // The doc must name the three-tier envelope so the user sees why a
        // single interchange cannot span split files.
        assert!(doc.contains("ISA..IEA"));
        assert!(doc.contains("split"));
    }

    #[test]
    fn test_explain_code_e341_undeclared_doc_path() {
        let doc = explain_code("E341").unwrap();
        assert!(doc.contains("E341"));
        // The doc must name the `$doc` namespace and tie the check to the
        // source's declared envelope sections.
        assert!(doc.contains("$doc"));
        assert!(doc.contains("envelope"));
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
            "E309", "E310", "E311", "E312", "E313", "E319", "E320", "E321", "E323", "E330", "E331",
            "E332", "E333", "E334", "E335", "E336", "E337", "E338", "E339", "E340", "E341", "E342",
            "E15Y", "W101", "W302", "W305", "W306",
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
