//! Field-level provenance rendering for `clinker explain --field`.
//!
//! Given a dotted path like `node_name.param_name`, walks the [`ProvenanceDb`]
//! to produce a human-readable provenance chain showing which configuration
//! layer won and which were shadowed.

use std::fmt;

use crate::config::composition::{
    ProvenanceLookupError, ProvenanceQuery, ProvenanceQueryParseError, SchemaResolvedValue,
    ScopedSchemaAddress,
};
use crate::plan::CompiledPlan;
use clinker_core_types::span::Span;

/// Error returned when field provenance cannot be resolved.
#[derive(Debug)]
pub enum ProvenanceExplainError {
    /// The author passed an empty `--field` value.
    EmptyQuery,
    /// No config provenance entry matched the exact address or shorthand.
    UnknownQuery {
        query: String,
        candidates: Vec<String>,
    },
    /// A shorthand matched multiple scoped config entries.
    AmbiguousQuery {
        query: String,
        candidates: Vec<String>,
    },
    /// Dotted path is neither `node.param` (config) nor `source.column.attr`
    /// (schema).
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
    /// No schema provenance tracked for this source name.
    SchemaSourceNotFound {
        source: String,
        valid_sources: Vec<String>,
    },
    /// The source is tracked but the column is not.
    SchemaColumnNotFound {
        source: String,
        column: String,
        valid_columns: Vec<String>,
    },
    /// The column is tracked but the attribute is not.
    SchemaAttrNotFound {
        source: String,
        column: String,
        attribute: String,
        valid_attributes: Vec<String>,
    },
}

impl fmt::Display for ProvenanceExplainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProvenanceExplainError::EmptyQuery => write!(
                f,
                "[E127] provenance query is empty\n  help: pass a field, for example --field 'node.param'"
            ),
            ProvenanceExplainError::UnknownQuery { query, candidates } => {
                write!(f, "[E128] no provenance entry matches {query:?}")?;
                if candidates.is_empty() {
                    write!(
                        f,
                        "\n  help: remove --field {query:?} or use a known exact /v1/config or /v1/schema address"
                    )
                } else {
                    write!(f, "\n  help: use one exact same-field address:")?;
                    for candidate in candidates {
                        write!(f, "\n    --field '{candidate}'")?;
                    }
                    Ok(())
                }
            }
            ProvenanceExplainError::AmbiguousQuery { query, candidates } => {
                write!(
                    f,
                    "[E129] provenance shorthand {query:?} matches multiple scoped nodes\n  help: replace it with one exact address:"
                )?;
                for candidate in candidates {
                    write!(f, "\n    --field '{candidate}'")?;
                }
                Ok(())
            }
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
            ProvenanceExplainError::SchemaSourceNotFound {
                source,
                valid_sources,
            } => {
                write!(f, "no schema provenance for source '{source}'")?;
                if valid_sources.is_empty() {
                    write!(f, "\n  no sources with tracked schema found")
                } else {
                    write!(f, "\n  sources with schema provenance:")?;
                    for s in valid_sources {
                        write!(f, "\n    - {s}")?;
                    }
                    Ok(())
                }
            }
            ProvenanceExplainError::SchemaColumnNotFound {
                source,
                column,
                valid_columns,
            } => {
                write!(f, "no schema provenance for '{source}.{column}'")?;
                write!(f, "\n  columns of '{source}':")?;
                for c in valid_columns {
                    write!(f, "\n    - {source}.{c}")?;
                }
                Ok(())
            }
            ProvenanceExplainError::SchemaAttrNotFound {
                source,
                column,
                attribute,
                valid_attributes,
            } => {
                write!(
                    f,
                    "no schema provenance for '{source}.{column}.{attribute}'"
                )?;
                write!(f, "\n  attributes of '{source}.{column}':")?;
                for a in valid_attributes {
                    write!(f, "\n    - {source}.{column}.{a}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ProvenanceExplainError {}

impl ProvenanceExplainError {
    /// Stable diagnostic code for query-class errors.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::EmptyQuery => Some("E116"),
            Self::UnknownQuery { .. } => Some("E117"),
            Self::AmbiguousQuery { .. } => Some("E118"),
            _ => None,
        }
    }
}

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
///   [WON] ChannelPerTarget →  0.92  (line 12)
///         ChannelWide      →  0.85  (shadowed)  (line 15)
///         PipelineDefault  →  0.75  (shadowed)  (line 8)
/// ```
pub fn explain_field_provenance(
    plan: &CompiledPlan,
    query_text: &str,
) -> Result<String, ProvenanceExplainError> {
    let query = ProvenanceQuery::parse(query_text).map_err(|error| match error {
        ProvenanceQueryParseError::Empty => ProvenanceExplainError::EmptyQuery,
        ProvenanceQueryParseError::Malformed(_) => ProvenanceExplainError::UnknownQuery {
            query: query_text.to_owned(),
            candidates: Vec::new(),
        },
    })?;

    if let ProvenanceQuery::SchemaExact(address) | ProvenanceQuery::SchemaShorthand(address) =
        &query
    {
        return explain_schema_field(
            plan,
            address.source(),
            address.column(),
            address.attribute(),
            query_text,
        );
    }

    let found = plan.provenance().resolve_query(&query).map_err(|error| {
        let (candidates, ambiguous) = match error {
            ProvenanceLookupError::Unknown { candidates } => (candidates, false),
            ProvenanceLookupError::Ambiguous { candidates } => (candidates, true),
        };
        let candidates = candidates
            .into_iter()
            .map(|candidate| candidate.render())
            .collect();
        if ambiguous {
            ProvenanceExplainError::AmbiguousQuery {
                query: query_text.to_owned(),
                candidates,
            }
        } else {
            ProvenanceExplainError::UnknownQuery {
                query: query_text.to_owned(),
                candidates,
            }
        }
    })?;
    let resolved = found.resolved;

    let mut output = String::new();
    output.push_str(&format!("Field: {query_text}\n"));
    output.push_str(&format!("Exact address: {}\n\n", found.address.render()));
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

/// Render schema-attribute provenance for a `source.column.attribute` path.
///
/// Mirrors [`explain_field_provenance`]'s config-param output, showing the
/// winning [`SchemaLayer`](crate::config::composition::SchemaLayer) and each
/// shadowed layer's value plus its source span (honestly synthetic for the Base
/// seed, which retains no per-column byte span).
fn explain_schema_field(
    plan: &CompiledPlan,
    source: &str,
    column: &str,
    attribute: &str,
    query_text: &str,
) -> Result<String, ProvenanceExplainError> {
    let db = plan.schema_provenance();
    let resolved = db.get(source, column, attribute).ok_or_else(|| {
        // Distinguish a missing source / column / attribute for a useful hint.
        let sources = db.sources();
        if !sources.contains(&source) {
            return ProvenanceExplainError::SchemaSourceNotFound {
                source: source.to_owned(),
                valid_sources: sources.into_iter().map(String::from).collect(),
            };
        }
        let columns = db.columns_for(source);
        if !columns.contains(&column) {
            return ProvenanceExplainError::SchemaColumnNotFound {
                source: source.to_owned(),
                column: column.to_owned(),
                valid_columns: columns.into_iter().map(String::from).collect(),
            };
        }
        ProvenanceExplainError::SchemaAttrNotFound {
            source: source.to_owned(),
            column: column.to_owned(),
            attribute: attribute.to_owned(),
            valid_attributes: db
                .attrs_for(source, column)
                .into_iter()
                .map(String::from)
                .collect(),
        }
    })?;

    let address = ScopedSchemaAddress::new(source, column, attribute);
    Ok(render_schema_resolved(
        query_text,
        &address.render(),
        resolved,
    ))
}

/// Render a resolved schema-attribute leaf as the `[WON]` / `(shadowed)` chain.
fn render_schema_resolved(
    field: &str,
    exact_address: &str,
    resolved: &SchemaResolvedValue,
) -> String {
    let mut output = String::new();
    output.push_str(&format!("Field: {field}\n"));
    output.push_str(&format!("Exact address: {exact_address}\n\n"));
    output.push_str(&format!("  Resolved value: {}\n\n", resolved.value));
    output.push_str("  Provenance chain (outermost to innermost):\n");

    // Highest-precedence layer first.
    let mut layers: Vec<_> = resolved.provenance.iter().collect();
    layers.sort_by(|a, b| b.kind.cmp(&a.kind));
    for layer in &layers {
        let prefix = if layer.won { "[WON]" } else { "     " };
        let suffix = if layer.won { "" } else { "  (shadowed)" };
        let kind_label = format!("{}", layer.kind);
        let value_str = resolved
            .layer_value(layer.kind)
            .map(|v| v.to_string())
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
    output
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
#[cfg(test)]
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

/// Every code `clinker explain --code` can answer for, paired with its page.
///
/// One home for the list: [`explain_code`] looks up here and [`explain_codes`]
/// enumerates the same rows, so the CLI cannot advertise a set of valid codes
/// that has drifted from the set it actually serves. Every entry needs a
/// matching `docs/explain/<code>.md`; the section-coverage contract is
/// enforced by `test_explain_docs_all_have_required_sections`.
pub const EXPLAIN_PAGES: &[(&str, &str)] = &[
    ("E200", include_str!("../../../../docs/explain/E200.md")),
    ("E202", include_str!("../../../../docs/explain/E202.md")),
    ("E203", include_str!("../../../../docs/explain/E203.md")),
    ("E101", include_str!("../../../../docs/explain/E101.md")),
    ("E102", include_str!("../../../../docs/explain/E102.md")),
    ("E103", include_str!("../../../../docs/explain/E103.md")),
    ("E104", include_str!("../../../../docs/explain/E104.md")),
    ("E106", include_str!("../../../../docs/explain/E106.md")),
    ("E107", include_str!("../../../../docs/explain/E107.md")),
    ("E108", include_str!("../../../../docs/explain/E108.md")),
    ("E110", include_str!("../../../../docs/explain/E110.md")),
    ("E115", include_str!("../../../../docs/explain/E115.md")),
    ("E116", include_str!("../../../../docs/explain/E116.md")),
    ("E117", include_str!("../../../../docs/explain/E117.md")),
    ("E118", include_str!("../../../../docs/explain/E118.md")),
    ("E127", include_str!("../../../../docs/explain/E127.md")),
    ("E128", include_str!("../../../../docs/explain/E128.md")),
    ("E129", include_str!("../../../../docs/explain/E129.md")),
    ("E300", include_str!("../../../../docs/explain/E300.md")),
    ("E301", include_str!("../../../../docs/explain/E301.md")),
    ("E303", include_str!("../../../../docs/explain/E303.md")),
    ("E304", include_str!("../../../../docs/explain/E304.md")),
    ("E305", include_str!("../../../../docs/explain/E305.md")),
    ("E306", include_str!("../../../../docs/explain/E306.md")),
    ("E307", include_str!("../../../../docs/explain/E307.md")),
    ("E308", include_str!("../../../../docs/explain/E308.md")),
    ("E309", include_str!("../../../../docs/explain/E309.md")),
    ("E310", include_str!("../../../../docs/explain/E310.md")),
    ("E311", include_str!("../../../../docs/explain/E311.md")),
    ("E312", include_str!("../../../../docs/explain/E312.md")),
    ("E313", include_str!("../../../../docs/explain/E313.md")),
    ("E319", include_str!("../../../../docs/explain/E319.md")),
    ("E320", include_str!("../../../../docs/explain/E320.md")),
    ("E321", include_str!("../../../../docs/explain/E321.md")),
    ("E324", include_str!("../../../../docs/explain/E324.md")),
    ("E325", include_str!("../../../../docs/explain/E325.md")),
    ("E326", include_str!("../../../../docs/explain/E326.md")),
    ("E327", include_str!("../../../../docs/explain/E327.md")),
    ("E330", include_str!("../../../../docs/explain/E330.md")),
    ("E331", include_str!("../../../../docs/explain/E331.md")),
    ("E332", include_str!("../../../../docs/explain/E332.md")),
    ("E333", include_str!("../../../../docs/explain/E333.md")),
    ("E334", include_str!("../../../../docs/explain/E334.md")),
    ("E335", include_str!("../../../../docs/explain/E335.md")),
    ("E336", include_str!("../../../../docs/explain/E336.md")),
    ("E337", include_str!("../../../../docs/explain/E337.md")),
    ("E338", include_str!("../../../../docs/explain/E338.md")),
    ("E339", include_str!("../../../../docs/explain/E339.md")),
    ("E340", include_str!("../../../../docs/explain/E340.md")),
    ("E341", include_str!("../../../../docs/explain/E341.md")),
    ("E342", include_str!("../../../../docs/explain/E342.md")),
    ("E343", include_str!("../../../../docs/explain/E343.md")),
    ("E344", include_str!("../../../../docs/explain/E344.md")),
    ("E345", include_str!("../../../../docs/explain/E345.md")),
    ("E346", include_str!("../../../../docs/explain/E346.md")),
    ("E347", include_str!("../../../../docs/explain/E347.md")),
    ("E348", include_str!("../../../../docs/explain/E348.md")),
    ("E349", include_str!("../../../../docs/explain/E349.md")),
    ("E350", include_str!("../../../../docs/explain/E350.md")),
    ("E351", include_str!("../../../../docs/explain/E351.md")),
    ("E352", include_str!("../../../../docs/explain/E352.md")),
    ("E353", include_str!("../../../../docs/explain/E353.md")),
    ("E354", include_str!("../../../../docs/explain/E354.md")),
    ("E355", include_str!("../../../../docs/explain/E355.md")),
    ("E356", include_str!("../../../../docs/explain/E356.md")),
    ("E357", include_str!("../../../../docs/explain/E357.md")),
    ("E358", include_str!("../../../../docs/explain/E358.md")),
    ("E359", include_str!("../../../../docs/explain/E359.md")),
    ("E360", include_str!("../../../../docs/explain/E360.md")),
    ("E361", include_str!("../../../../docs/explain/E361.md")),
    ("E362", include_str!("../../../../docs/explain/E362.md")),
    ("E363", include_str!("../../../../docs/explain/E363.md")),
    ("E364", include_str!("../../../../docs/explain/E364.md")),
    ("E365", include_str!("../../../../docs/explain/E365.md")),
    ("E367", include_str!("../../../../docs/explain/E367.md")),
    ("E368", include_str!("../../../../docs/explain/E368.md")),
    ("E370", include_str!("../../../../docs/explain/E370.md")),
    ("E323", include_str!("../../../../docs/explain/E323.md")),
    ("E150b", include_str!("../../../../docs/explain/E150b.md")),
    ("E150c", include_str!("../../../../docs/explain/E150c.md")),
    ("E150d", include_str!("../../../../docs/explain/E150d.md")),
    ("E150e", include_str!("../../../../docs/explain/E150e.md")),
    ("E15Y", include_str!("../../../../docs/explain/E15Y.md")),
    ("W101", include_str!("../../../../docs/explain/W101.md")),
    ("W302", include_str!("../../../../docs/explain/W302.md")),
    ("W305", include_str!("../../../../docs/explain/W305.md")),
    ("W306", include_str!("../../../../docs/explain/W306.md")),
    ("W365", include_str!("../../../../docs/explain/W365.md")),
    ("W366", include_str!("../../../../docs/explain/W366.md")),
];

/// Look up error/warning code documentation embedded at compile time.
///
/// Returns the doc content for any code in [`EXPLAIN_PAGES`], or `None` for
/// an unknown code.
pub fn explain_code(code: &str) -> Option<&'static str> {
    EXPLAIN_PAGES
        .iter()
        .find(|(c, _)| *c == code)
        .map(|(_, doc)| *doc)
}

/// Every code [`explain_code`] can answer for, sorted.
///
/// The CLI prints this when a user names a code that does not exist, so it is
/// derived rather than retyped. A hand-maintained list goes stale the first
/// time a code is added, and then tells a user who mistyped after following an
/// `explain --code` hint that the valid range stops before the code the hint
/// named.
pub fn explain_codes() -> Vec<&'static str> {
    let mut codes: Vec<&'static str> = EXPLAIN_PAGES.iter().map(|(c, _)| *c).collect();
    codes.sort_unstable();
    codes
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
            LayerKind::PipelineDefault,
            Span::line_only(8),
        );
        assert_eq!(
            rv.layer_value(LayerKind::PipelineDefault),
            Some(&serde_json::json!(0.75))
        );
    }

    #[test]
    fn test_layer_value_stored_on_apply() {
        let mut rv = ResolvedValue::new(
            serde_json::json!(0.75),
            LayerKind::PipelineDefault,
            Span::line_only(8),
        );
        rv.apply_layer(
            serde_json::json!(0.92),
            LayerKind::ChannelPerTarget,
            Span::line_only(12),
        );
        assert_eq!(
            rv.layer_value(LayerKind::PipelineDefault),
            Some(&serde_json::json!(0.75))
        );
        assert_eq!(
            rv.layer_value(LayerKind::ChannelPerTarget),
            Some(&serde_json::json!(0.92))
        );
        assert!(rv.provenance.iter().find(|l| l.won).unwrap().kind == LayerKind::ChannelPerTarget);
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
    fn test_explain_code_e345_unknown_discriminator() {
        let doc = explain_code("E345").unwrap();
        assert!(doc.contains("E345"));
        // The doc must name the discriminator concept and the `records:`
        // declaration the unmatched tag should have matched.
        assert!(doc.contains("discriminator"));
        assert!(doc.contains("records:"));
    }

    #[test]
    fn test_explain_code_e363_record_path_syntax() {
        let doc = explain_code("E363").unwrap();
        assert!(doc.contains("E363"));
        assert!(doc.contains("record_path"));
        // Both grammars an author is likely to reach for must be named, so the
        // page answers "why was mine rejected" for either.
        assert!(doc.contains("XPath"));
        assert!(doc.contains("JSONPath"));
    }

    #[test]
    fn test_explain_code_unknown() {
        assert!(explain_code("E999").is_none());
    }

    #[test]
    fn test_explain_codes_enumerates_exactly_what_explain_code_serves() {
        // The CLI prints this list when a code is not found, so a list that
        // reports fewer codes than are served tells a user the code they were
        // just pointed at does not exist.
        let listed = explain_codes();
        assert_eq!(listed.len(), EXPLAIN_PAGES.len());
        for code in &listed {
            assert!(
                explain_code(code).is_some(),
                "{code} is listed as valid but serves no page"
            );
        }
        assert!(listed.windows(2).all(|w| w[0] < w[1]), "must be sorted");
    }

    #[test]
    fn test_overlay_conditions_have_their_own_pages() {
        // Split out of E107 / E110 / E111 so `explain --code` stops sending a
        // reader of the overlay condition to a page about composition binding.
        // Each page must describe the overlay condition, not the one it left.
        for (code, marker) in [
            ("E116", "incompatible with its type contract"),
            ("E117", "shadows a reserved system field"),
            ("E118", "the pipeline does not declare"),
        ] {
            let doc = explain_code(code).unwrap_or_else(|| panic!("{code} has no page"));
            assert!(doc.contains(code), "{code} page must name its own code");
            assert!(
                doc.contains(marker),
                "{code} page must describe the overlay condition"
            );
        }
        // The code they were split out of keeps its original meaning rather
        // than being repurposed.
        let e107 = explain_code("E107").expect("E107 has a page");
        assert!(
            e107.contains("ycle"),
            "E107 must still describe the composition cycle it always did"
        );
    }

    #[test]
    fn test_e110_and_e117_each_explain_only_their_primary_condition() {
        let e110 = explain_code("E110").expect("E110 has an extraction page");
        assert_eq!(
            e110.lines().next(),
            Some(
                "# Error E110: an extraction selection names a node absent from the execution-plan DAG"
            )
        );
        assert!(e110.contains("selected node 'clean_oder'"));

        let e117 = explain_code("E117").expect("E117 has a reserved-name page");
        assert_eq!(
            e117.lines().next(),
            Some("# Error E117: a channel var's name shadows a reserved system field")
        );
        assert!(e117.contains("$pipeline.execution_id"));
    }

    #[test]
    fn test_explain_docs_all_have_required_sections() {
        // Derived from the page table, not retyped: a hand-listed set silently
        // stops covering a page the moment one is added, which is how a page
        // could ship without its sections in the first place.
        let codes = explain_codes();
        assert_eq!(
            codes.len(),
            EXPLAIN_PAGES.len(),
            "every page must be covered exactly once"
        );
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
