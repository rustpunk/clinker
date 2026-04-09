//! Unified pipeline node enum (Phase 16b Wave 1 — Tasks 16b.2 Group A/B).
//!
//! `PipelineNode` is a serde-internally-tagged enum (`#[serde(tag = "type")]`)
//! with peer variants for every node kind in the unified `nodes:` topology.
//! Each variant carries a header struct (with `name`, optional `description`,
//! `input` / `inputs`) flattened in via `#[serde(flatten)]`, and a `config:`
//! sub-block with the operator-specific fields.
//!
//! The whole enum is wrapped in [`crate::yaml::Spanned`] at the top level so
//! every node carries source-span info that survives the
//! `#[serde(tag = ...)] + #[serde(flatten)]` interaction (a documented
//! serde-saphyr limitation).
//!
//! Per-variant body structs use the `*Body` family suffix. The
//! operator-facing config layer.
//!
//! See `docs/plans/cxl-engine/phase-16b-node-taxonomy-lift.md` Task 16b.2.

use std::collections::BTreeMap;
use std::path::PathBuf;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::config::node_header::{MergeHeader, NodeHeader, SourceHeader};
use crate::yaml::CxlSource;

/// Unified pipeline node taxonomy. Every node in the YAML `nodes:` list
/// deserializes to a [`PipelineNode`] variant. The variant tag is the
/// YAML `type:` field; per-variant fields are split between a header
/// (flattened to top level) and a `config:` block.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
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
    /// Phase 16c stub. Parsing this is allowed; `compile()` returns a
    /// single `E100` diagnostic per Composition node and otherwise
    /// continues lowering the rest of the pipeline.
    Composition {
        #[serde(flatten)]
        header: NodeHeader,
        config: CompositionBody,
    },
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
/// (formats, schemas, sort orders) — see `crate::config::SourceConfig`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBody {
    #[serde(flatten)]
    pub source: crate::config::SourceConfig,
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
    #[serde(default)]
    pub log: Option<Vec<crate::config::LogDirective>>,
    #[serde(default)]
    pub validations: Option<Vec<crate::config::ValidationEntry>>,
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

/// Composition stub body (Phase 16c placeholder).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionBody {
    #[serde(rename = "use")]
    pub r#use: PathBuf,
    #[serde(default)]
    pub config: BTreeMap<String, serde_json::Value>,
}
