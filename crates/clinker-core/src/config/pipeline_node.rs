//! Unified pipeline node enum (Phase 16b Wave 1 — Tasks 16b.2 Group A/B;
//! two-pass deser refactor Phase Combine Task C.0.1a).
//!
//! `PipelineNode` is an enum with peer variants for every node kind in
//! the unified `nodes:` topology. Each variant carries a header struct
//! (with `name`, optional `description`, `input` / `inputs`) and a
//! `config:` sub-block with the operator-specific fields.
//!
//! Deserialization uses a two-pass approach via
//! `#[serde(try_from = "serde_json::Value")]` — see the [`PipelineNode`]
//! type docs for the rationale. The outer [`crate::yaml::Spanned`] wrap
//! runs BEFORE the TryFrom conversion, so every node carries its
//! source-span info regardless of the two-pass rewrite.
//!
//! Per-variant body structs use the `*Body` family suffix.
//!
//! See `docs/plans/cxl-engine/phase-16b-node-taxonomy-lift.md` Task 16b.2
//! and `docs/internal/plans/cxl-engine/phase-combine-node/phase-c0-scaffolds-config.md`
//! Task C.0.1a.

use std::path::PathBuf;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::config::node_header::{MergeHeader, NodeHeader, SourceHeader};
use crate::yaml::CxlSource;

/// Unified pipeline node taxonomy. Every node in the YAML `nodes:` list
/// deserializes to a [`PipelineNode`] variant. The variant tag is the
/// YAML `type:` field; per-variant fields are split between a header
/// (flattened to top level) and a `config:` block.
///
/// # Two-pass deserialization (C.0.1a)
///
/// Deserialization goes through [`TryFrom<serde_json::Value>`] rather
/// than serde's derived tagged-enum path. The tagged-enum path had two
/// silent bugs interacting with the per-variant `#[serde(flatten)]`
/// header structs:
///
/// 1. `#[serde(deny_unknown_fields)]` on the outer enum was silently
///    broken by `#[serde(flatten)]` in the variant bodies — unknown
///    fields were accepted instead of rejected.
/// 2. Flatten buffers erased field names from error messages, so
///    unknown-field errors read as generic "unknown field" strings
///    with no actionable detail.
///
/// The two-pass approach:
/// - Pass 1: deserialize the YAML mapping to [`serde_json::Value`],
///   which preserves every key.
/// - Pass 2: match on the `type` tag, then use [`extract_sub`] /
///   [`extract_field`] to feed selected keys into each variant's
///   header and body structs (which keep their `#[serde(deny_unknown_fields)]`
///   attributes — now functional because they deserialize standalone).
/// - [`reject_unknown`] rejects any top-level key outside the expected
///   set, with the offending key name in the error message.
///
/// The per-variant header and body structs are unchanged. Spanned<PipelineNode>
/// at the top level continues to capture node-level source location because
/// serde-saphyr's Spanned wrapper runs before the TryFrom conversion.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    try_from = "serde_json::Value"
)]
pub enum PipelineNode {
    Source {
        #[serde(flatten)]
        header: SourceHeader,
        config: SourceBody,
    },
    Transform {
        #[serde(flatten)]
        header: NodeHeader,
        config: TransformBody,
    },
    Aggregate {
        #[serde(flatten)]
        header: NodeHeader,
        config: AggregateBody,
    },
    Route {
        #[serde(flatten)]
        header: NodeHeader,
        config: RouteBody,
    },
    Merge {
        #[serde(flatten)]
        header: MergeHeader,
        #[serde(default)]
        config: MergeBody,
    },
    Output {
        #[serde(flatten)]
        header: NodeHeader,
        config: OutputBody,
    },
    /// Composition call-site node. Lowered to `PlanNode::Composition` in
    /// Stage 5; body nodes live in `CompileArtifacts.composition_bodies`
    /// keyed by the `body` handle.
    Composition {
        #[serde(flatten)]
        header: NodeHeader,
        /// Path to the `.comp.yaml` file defining the composition.
        #[serde(rename = "use")]
        r#use: PathBuf,
        /// Optional alias for namespace-mangling after expansion.
        #[serde(default)]
        alias: Option<String>,
        /// Port bindings: composition input port → upstream node ref.
        #[serde(default)]
        inputs: IndexMap<String, String>,
        /// Port bindings: composition output port → downstream node ref.
        #[serde(default)]
        outputs: IndexMap<String, String>,
        /// Behavioural config param overrides.
        #[serde(default)]
        config: IndexMap<String, serde_json::Value>,
        /// Resource bindings (file paths, connection strings, etc.).
        #[serde(default)]
        resources: IndexMap<String, serde_json::Value>,
        /// Populated by `bind_composition` in 16c.2. Serde-default to a
        /// sentinel; any consumer reading this before `bind_schema` runs
        /// gets a clear debug-panic.
        #[serde(skip)]
        body: crate::plan::composition_body::CompositionBodyId,
    },
}

impl TryFrom<serde_json::Value> for PipelineNode {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let obj = value
            .as_object()
            .ok_or_else(|| "pipeline node must be a YAML mapping".to_string())?;
        let type_tag = obj
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "pipeline node must have a 'type' field".to_string())?;

        match type_tag {
            "source" => {
                let header: SourceHeader = extract_sub(obj, &["name", "description", "_notes"])?;
                let config: SourceBody = extract_field(obj, "config")?;
                reject_unknown(obj, &["type", "name", "description", "_notes", "config"])?;
                Ok(PipelineNode::Source { header, config })
            }
            "transform" => {
                let header: NodeHeader =
                    extract_sub(obj, &["name", "description", "input", "_notes"])?;
                let config: TransformBody = extract_field(obj, "config")?;
                reject_unknown(
                    obj,
                    &["type", "name", "description", "input", "_notes", "config"],
                )?;
                Ok(PipelineNode::Transform { header, config })
            }
            "aggregate" => {
                let header: NodeHeader =
                    extract_sub(obj, &["name", "description", "input", "_notes"])?;
                let config: AggregateBody = extract_field(obj, "config")?;
                reject_unknown(
                    obj,
                    &["type", "name", "description", "input", "_notes", "config"],
                )?;
                Ok(PipelineNode::Aggregate { header, config })
            }
            "route" => {
                let header: NodeHeader =
                    extract_sub(obj, &["name", "description", "input", "_notes"])?;
                let config: RouteBody = extract_field(obj, "config")?;
                reject_unknown(
                    obj,
                    &["type", "name", "description", "input", "_notes", "config"],
                )?;
                Ok(PipelineNode::Route { header, config })
            }
            "merge" => {
                let header: MergeHeader =
                    extract_sub(obj, &["name", "description", "inputs", "_notes"])?;
                // `config:` is optional on Merge — defaults to empty MergeBody.
                let config: MergeBody = extract_optional(obj, "config")?.unwrap_or_default();
                reject_unknown(
                    obj,
                    &["type", "name", "description", "inputs", "_notes", "config"],
                )?;
                Ok(PipelineNode::Merge { header, config })
            }
            "output" => {
                let header: NodeHeader =
                    extract_sub(obj, &["name", "description", "input", "_notes"])?;
                let config: OutputBody = extract_field(obj, "config")?;
                reject_unknown(
                    obj,
                    &["type", "name", "description", "input", "_notes", "config"],
                )?;
                Ok(PipelineNode::Output { header, config })
            }
            "composition" => {
                let header: NodeHeader =
                    extract_sub(obj, &["name", "description", "input", "_notes"])?;
                let r#use: PathBuf = extract_field(obj, "use")?;
                let alias: Option<String> = extract_optional(obj, "alias")?;
                let inputs: IndexMap<String, String> =
                    extract_optional(obj, "inputs")?.unwrap_or_default();
                let outputs: IndexMap<String, String> =
                    extract_optional(obj, "outputs")?.unwrap_or_default();
                let config: IndexMap<String, serde_json::Value> =
                    extract_optional(obj, "config")?.unwrap_or_default();
                let resources: IndexMap<String, serde_json::Value> =
                    extract_optional(obj, "resources")?.unwrap_or_default();
                reject_unknown(
                    obj,
                    &[
                        "type",
                        "name",
                        "description",
                        "input",
                        "_notes",
                        "use",
                        "alias",
                        "inputs",
                        "outputs",
                        "config",
                        "resources",
                    ],
                )?;
                Ok(PipelineNode::Composition {
                    header,
                    r#use,
                    alias,
                    inputs,
                    outputs,
                    config,
                    resources,
                    body: crate::plan::composition_body::CompositionBodyId::default(),
                })
            }
            other => Err(format!(
                "unknown node type {other:?}, expected one of: source, transform, aggregate, route, merge, output, composition"
            )),
        }
    }
}

/// Build a sub-object from selected keys and deserialize into T.
/// Reuses existing `#[derive(Deserialize)]` on header/body structs.
/// Missing keys are simply skipped — the target type's own `#[serde(default)]`
/// and `Option<_>` handling applies.
fn extract_sub<T: serde::de::DeserializeOwned>(
    obj: &serde_json::Map<String, serde_json::Value>,
    fields: &[&str],
) -> Result<T, String> {
    let mut sub = serde_json::Map::new();
    for &f in fields {
        if let Some(v) = obj.get(f) {
            sub.insert(f.to_string(), v.clone());
        }
    }
    serde_json::from_value(serde_json::Value::Object(sub)).map_err(|e| e.to_string())
}

/// Extract a required field and deserialize into T. Returns an error
/// naming the missing field if absent.
fn extract_field<T: serde::de::DeserializeOwned>(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<T, String> {
    let val = obj
        .get(field)
        .ok_or_else(|| format!("missing required field '{field}'"))?;
    serde_json::from_value(val.clone()).map_err(|e| format!("invalid '{field}': {e}"))
}

/// Extract an optional field and deserialize into `Option<T>`.
fn extract_optional<T: serde::de::DeserializeOwned>(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<T>, String> {
    match obj.get(field) {
        Some(val) => serde_json::from_value(val.clone())
            .map(Some)
            .map_err(|e| format!("invalid '{field}': {e}")),
        None => Ok(None),
    }
}

/// Reject any keys not in the expected set. Emits an error naming the
/// offending key — this is the `deny_unknown_fields` replacement that
/// actually works when headers use `#[serde(flatten)]` internally.
fn reject_unknown(
    obj: &serde_json::Map<String, serde_json::Value>,
    expected: &[&str],
) -> Result<(), String> {
    for key in obj.keys() {
        if !expected.contains(&key.as_str()) {
            return Err(format!(
                "unknown field '{key}', expected one of: {}",
                expected.join(", ")
            ));
        }
    }
    Ok(())
}

impl PipelineNode {
    /// The author-given node name (from the variant's header).
    pub fn name(&self) -> &str {
        match self {
            PipelineNode::Source { header, .. } => &header.name,
            PipelineNode::Transform { header, .. }
            | PipelineNode::Aggregate { header, .. }
            | PipelineNode::Route { header, .. }
            | PipelineNode::Output { header, .. }
            | PipelineNode::Composition { header, .. } => &header.name,
            PipelineNode::Merge { header, .. } => &header.name,
        }
    }

    /// String tag of the variant for display.
    pub fn type_tag(&self) -> &'static str {
        match self {
            PipelineNode::Source { .. } => "source",
            PipelineNode::Transform { .. } => "transform",
            PipelineNode::Aggregate { .. } => "aggregate",
            PipelineNode::Route { .. } => "route",
            PipelineNode::Merge { .. } => "merge",
            PipelineNode::Output { .. } => "output",
            PipelineNode::Composition { .. } => "composition",
        }
    }
}

// ---------------------------------------------------------------------
// Per-variant body structs (`*Body` family)
//
// The parse-time shape for the `nodes:` YAML taxonomy. These are the
// operator-facing config types; they carry the format, mapping, CXL
// source, etc. that the executor consumes.
// ---------------------------------------------------------------------

/// Source variant body. Wraps the existing source-format configuration
/// (formats, sort orders) — see `crate::config::SourceConfig` — and
/// carries a **required** `schema:` declaration of the source's
/// top-level columns with their CXL types. The schema drives
/// compile-time CXL typechecking in
/// [`crate::config::PipelineConfig::compile`] (Phase 16b Task 16b.9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBody {
    /// Declared top-level columns and their CXL types. Required —
    /// missing this field is a serde parse error routed to E201 via
    /// the diagnostic layer.
    pub schema: SchemaDecl,
    #[serde(flatten)]
    pub source: crate::config::SourceConfig,
}

/// Phase 16b Task 16b.9 — inline schema declaration on `SourceBody`.
///
/// Deserializes from a YAML sequence of `{ name, type }` entries:
///
/// ```yaml
/// schema:
///   - { name: employee_id, type: string }
///   - { name: salary, type: int }
///   - { name: hired_at, type: date_time }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaDecl {
    pub columns: Vec<ColumnDecl>,
}

/// One declared column in a [`SchemaDecl`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnDecl {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: cxl::typecheck::Type,
}

/// Transform variant body. The new shape: a mandatory `cxl:` field
/// carrying the CXL source as a `CxlSource` (so it captures its YAML
/// span where serde-saphyr can deliver one), plus the row-level
/// transform's optional sidebars.
///
/// The `analytic_window` field is the Phase 16b rename of the legacy
/// `local_window` field. The CXL `$window.*` namespace is unrelated and
/// is preserved unchanged in the cxl crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformBody {
    pub cxl: CxlSource,
    /// Renamed from `local_window` in Phase 16b. The CXL `$window.*`
    /// runtime binding is orthogonal and is NOT renamed.
    #[serde(default)]
    pub analytic_window: Option<AnalyticWindowSpec>,
    /// Reference table lookup enrichment. Loads a secondary source into
    /// memory and matches each input record against it using a CXL
    /// predicate. Matched fields are available in the `cxl:` block as
    /// `source_name.field_name`.
    #[serde(default)]
    pub lookup: Option<LookupConfig>,
    #[serde(default)]
    pub log: Option<Vec<crate::config::LogDirective>>,
    #[serde(default)]
    pub validations: Option<Vec<crate::config::ValidationEntry>>,
}

/// Configuration for reference table lookup enrichment on a transform.
///
/// The `where` predicate is a CXL boolean expression evaluated against
/// each candidate row in the lookup source. Bare field names reference
/// the primary input record; qualified names (`source_name.field`)
/// reference the lookup source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LookupConfig {
    /// Name of the source node providing the reference table.
    pub source: String,
    /// CXL boolean expression evaluated per candidate lookup row.
    /// Both primary input fields (bare) and lookup fields
    /// (`source.field`) are in scope.
    #[serde(rename = "where")]
    pub where_expr: String,
    /// Behavior when no lookup row matches the predicate.
    #[serde(default)]
    pub on_miss: OnMiss,
    /// Whether to take the first matching row or all matching rows.
    #[serde(default, rename = "match")]
    pub match_mode: MatchMode,
}

/// Behavior when a lookup finds no matching rows.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnMiss {
    /// All lookup fields resolve to null. Record is still emitted.
    #[default]
    NullFields,
    /// Record is silently skipped (not emitted).
    Skip,
    /// Pipeline errors on the first unmatched record.
    Error,
}

/// Match cardinality for lookup enrichment.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatchMode {
    /// Return the first matching row (scalar enrichment, 1:1).
    #[default]
    First,
    /// Return all matching rows (fan-out, 1:N).
    All,
}

/// Phase 16b rename of `LocalWindowSpec`. Wave 1 keeps the payload
/// structurally opaque (a JSON value) while the executor still reads
/// the legacy field; Wave 2 promotes this to the typed shape.
pub type AnalyticWindowSpec = serde_json::Value;

/// Aggregate variant body. Peer to Transform (no longer nested).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateBody {
    pub group_by: Vec<String>,
    pub cxl: CxlSource,
    #[serde(default)]
    pub strategy: crate::config::AggregateStrategyHint,
}

/// Route variant body. `conditions:` is an [`IndexMap`] so that branch
/// declaration order is preserved — legacy `RouteConfig::branches` is a
/// `Vec<RouteBranch>` evaluated first-match-wins, and the unified
/// `nodes:` shape must honour the same ordering contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteBody {
    #[serde(default)]
    pub mode: crate::config::RouteMode,
    pub conditions: IndexMap<String, CxlSource>,
    pub default: String,
}

/// Merge variant body. The plan-specified shape is empty: `inputs:` lives
/// on `MergeHeader`, not in the body.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MergeBody {}

/// Output variant body. Wraps the existing sink config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputBody {
    #[serde(flatten)]
    pub output: crate::config::OutputConfig,
}

// ---------------------------------------------------------------------
// Task C.0.1a: Two-pass deserialization tests
//
// Validate that the TryFrom<serde_json::Value> path correctly:
//   - rejects unknown top-level keys with field-named errors
//   - parses every existing variant (regression gate)
//   - rejects unknown `type:` tags with a clear message
//   - preserves Spanned<PipelineNode> node-level location capture
//   - preserves CxlSource behavior (span UNKNOWN in tagged context — same
//     as before, no regression)
//   - preserves inner flatten on SourceBody → SourceConfig
// ---------------------------------------------------------------------

#[cfg(test)]
mod two_pass_tests {
    use super::*;

    /// Verifies that unknown fields on a transform node are rejected.
    /// This was silently accepted before the two-pass refactor.
    #[test]
    fn test_reject_unknown_field_on_transform() {
        let yaml = r#"
pipeline:
  name: test
nodes:
  - type: source
    name: raw
    config:
      name: raw
      type: csv
      path: data.csv
      schema:
        - { name: id, type: string }
  - type: transform
    name: clean
    input: raw
    bogus_field: should_fail
    config:
      cxl: "emit id = id"
"#;
        let err = crate::yaml::from_str::<crate::config::PipelineConfig>(yaml)
            .expect_err("should reject unknown field");
        let msg = err.to_string();
        assert!(
            msg.contains("bogus_field"),
            "error should name the field, got: {msg}"
        );
    }

    /// Verifies all 7 existing variant types still parse correctly.
    /// Regression gate for the refactor. Inline fixture exercises
    /// Source, Transform, Aggregate, Route, Merge, Output, Composition
    /// in a single doc (composition points at a relative path; we only
    /// need parse success, not load success, here).
    #[test]
    fn test_all_existing_variants_parse() {
        let yaml = r#"
pipeline:
  name: all_variants
nodes:
  - type: source
    name: raw
    config:
      name: raw
      type: csv
      path: data.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
        - { name: region, type: string }

  - type: transform
    name: clean
    input: raw
    config:
      cxl: |
        emit id = id
        emit amount = amount
        emit region = region

  - type: aggregate
    name: totals
    input: clean
    config:
      group_by: [region]
      cxl: "emit total = sum(amount)"

  - type: route
    name: split
    input: clean
    config:
      conditions:
        big: "amount > 1000"
      default: small

  - type: merge
    name: combined
    inputs: [split.big, split.small]

  - type: composition
    name: norm
    input: clean
    use: ./nonexistent_comp.yaml
    alias: norm_alias
    inputs:
      port_a: clean
    outputs:
      port_b: downstream
    config:
      threshold: 42
    resources:
      db: "postgres://localhost"

  - type: output
    name: out
    input: totals
    config:
      name: out
      type: csv
      path: out.csv
"#;
        let doc: crate::config::PipelineConfig =
            crate::yaml::from_str(yaml).expect("all 7 variants should parse");
        assert_eq!(doc.nodes.len(), 7);
        assert!(matches!(&doc.nodes[0].value, PipelineNode::Source { .. }));
        assert!(matches!(
            &doc.nodes[1].value,
            PipelineNode::Transform { .. }
        ));
        assert!(matches!(
            &doc.nodes[2].value,
            PipelineNode::Aggregate { .. }
        ));
        assert!(matches!(&doc.nodes[3].value, PipelineNode::Route { .. }));
        assert!(matches!(&doc.nodes[4].value, PipelineNode::Merge { .. }));
        assert!(matches!(
            &doc.nodes[5].value,
            PipelineNode::Composition { .. }
        ));
        assert!(matches!(&doc.nodes[6].value, PipelineNode::Output { .. }));
    }

    /// Verifies unknown node types produce a clear error naming the tag.
    #[test]
    fn test_unknown_node_type_rejected() {
        let yaml = r#"
pipeline:
  name: test
nodes:
  - type: foobar
    name: bad
"#;
        let err = crate::yaml::from_str::<crate::config::PipelineConfig>(yaml)
            .expect_err("should reject unknown type");
        let msg = err.to_string();
        assert!(
            msg.contains("foobar"),
            "error should name the type, got: {msg}"
        );
    }

    /// Spike test exercising ALL edge cases from the deep-dive analysis.
    /// Validates: Composition variant, Spanned location capture, CxlSource
    /// span behavior, SourceBody inner flatten, error position quality.
    #[test]
    fn test_two_pass_spike_all_edge_cases() {
        use crate::config::PipelineNode;
        use crate::yaml::Location;

        // 1. Composition with all optional fields
        let yaml_comp = r#"
pipeline:
  name: spike_comp
nodes:
  - type: composition
    name: my_comp
    input: upstream
    use: path/to/comp.yaml
    alias: comp_alias
    inputs:
      port_a: upstream_a
    outputs:
      port_b: downstream_b
    config:
      threshold: 42
    resources:
      db: "postgres://localhost"
"#;
        let doc: crate::config::PipelineConfig =
            crate::yaml::from_str(yaml_comp).expect("composition should parse");
        assert!(matches!(
            &doc.nodes[0].value,
            PipelineNode::Composition { .. }
        ));

        // 2. Spanned captures non-zero location (before TryFrom runs)
        assert_ne!(
            doc.nodes[0].referenced,
            Location::UNKNOWN,
            "Spanned must capture location before TryFrom"
        );

        // 3. Transform with CxlSource — Span::UNKNOWN is expected
        // (documented behavior — CxlSource spans are UNKNOWN when buried
        // inside a context that went through a Value roundtrip, same as
        // the tagged-enum path).
        let yaml_transform = r#"
pipeline:
  name: spike_xform
nodes:
  - type: source
    name: raw
    config:
      name: raw
      type: csv
      path: data.csv
      schema:
        - { name: id, type: string }
  - type: transform
    name: clean
    input: raw
    config:
      cxl: "emit id = id"
"#;
        let doc2: crate::config::PipelineConfig =
            crate::yaml::from_str(yaml_transform).expect("transform should parse");
        if let PipelineNode::Transform { config, .. } = &doc2.nodes[1].value {
            assert_eq!(config.cxl.source, "emit id = id");
        } else {
            panic!("expected transform");
        }

        // 4. Source with SourceBody (inner flatten on SourceConfig)
        if let PipelineNode::Source { config, .. } = &doc2.nodes[0].value {
            assert!(!config.schema.columns.is_empty());
        } else {
            panic!("expected source");
        }

        // 5. Unknown field includes field name in error
        let yaml_bad = r#"
pipeline:
  name: spike_bad
nodes:
  - type: source
    name: raw
    config:
      name: raw
      type: csv
      path: data.csv
      schema:
        - { name: id, type: string }
  - type: transform
    name: bad
    input: raw
    bogus: true
    config:
      cxl: "emit id = id"
"#;
        let err = crate::yaml::from_str::<crate::config::PipelineConfig>(yaml_bad)
            .expect_err("unknown field should fail");
        assert!(
            err.to_string().contains("bogus"),
            "error must name the field, got: {}",
            err
        );
    }
}
