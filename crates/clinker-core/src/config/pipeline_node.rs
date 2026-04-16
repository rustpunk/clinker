//! Unified pipeline node enum (Phase 16b Wave 1 — Tasks 16b.2 Group A/B;
//! custom-Deserialize refactor, pre-C.1, supersedes the C.0.1a two-pass
//! approach).
//!
//! `PipelineNode` is an enum with peer variants for every node kind in
//! the unified `nodes:` topology. Each variant carries a header struct
//! (with `name`, optional `description`, `input` / `inputs`) and a
//! `config:` sub-block with the operator-specific fields.
//!
//! # Deserialization (pre-C.1)
//!
//! `PipelineNode` uses a hand-written [`Deserialize`] impl built around a
//! [`serde::de::Visitor::visit_map`] that peeks the `type:` discriminator
//! and dispatches to a per-variant serde-saphyr-native deserializer —
//! NOT through a `serde_json::Value` intermediate. This is the fourth
//! application of the key-presence-dispatch pattern in this codebase
//! (peers: [`crate::config::SortFieldSpec`], [`crate::config::SchemaSource`],
//! [`crate::config::composition::raw::RawOutputAlias`]) — see
//! `docs/internal/research/RESEARCH-span-preserving-deser.md` (Approach A)
//! and `docs/internal/research/RESEARCH-transform-entry-serde.md` for the
//! rationale.
//!
//! What this preserves (from C.0.1a, unchanged):
//! - The user-facing YAML shape (`{type: combine, name: ..., config: {...}}`).
//! - `deny_unknown_fields` semantics per variant — the visitor rejects
//!   unknown top-level keys with the offending name in the error message.
//! - Unknown-type-tag errors name the tag and list valid variants.
//! - Inner `#[serde(flatten)]` on `SourceBody` keeps working because
//!   variant deserialization is driven by serde-saphyr's native path.
//!
//! What this adds (over C.0.1a):
//! - Field-level [`crate::yaml::Spanned`] at the three header input
//!   fields ([`NodeHeader::input`], [`MergeHeader::inputs`],
//!   [`CombineHeader::input`]). Spans survive because the native path
//!   never goes through serde's type-erased `Content` buffer, which is
//!   what `serde_json::Value` would have routed through. E307 and
//!   future per-input diagnostics can now pin-point the offending
//!   reference.
//!
//! Per-variant body structs use the `*Body` family suffix.

use std::fmt;
use std::path::PathBuf;

use indexmap::IndexMap;
use serde::de::{self, IntoDeserializer, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::config::node_header::{CombineHeader, MergeHeader, NodeHeader, SourceHeader};
use crate::yaml::CxlSource;

/// Unified pipeline node taxonomy. Every node in the YAML `nodes:` list
/// deserializes to a [`PipelineNode`] variant. The variant tag is the
/// YAML `type:` field; per-variant fields are split between a header
/// (flattened to top level) and a `config:` block.
///
/// # Deserialization (pre-C.1)
///
/// Uses a hand-written [`Deserialize`] impl with a
/// [`serde::de::Visitor::visit_map`] that peeks the `type:` discriminator
/// and dispatches each variant to a dedicated serde-saphyr-native
/// deserializer. This supersedes the C.0.1a `#[serde(try_from =
/// "serde_json::Value")]` approach: it keeps every win of that refactor
/// (per-variant `deny_unknown_fields`, error messages include field
/// names, no `#[serde(flatten)]` footprint on the enum) and adds
/// field-level [`crate::yaml::Spanned`] preservation on consumer
/// headers, which a `serde_json::Value` intermediate always erased. See
/// the module-level docs and
/// `docs/internal/research/RESEARCH-span-preserving-deser.md` for the
/// full rationale.
///
/// The `Serialize` impl is still derived with `#[serde(tag = "type",
/// rename_all = "snake_case")]` — the YAML round-trip shape is
/// unchanged, only deserialization is re-architected.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
// Variant size is dominated by `SourceBody`/`OutputBody` (large wrapped
// config types) and by the `Locations` field in `Spanned<NodeInput>` that
// the pre-C.1 refactor adds to every consumer header. Boxing would force
// every consumer site to deref through the box — a pervasive ergonomics
// regression for a value that only lives for the lifetime of a pipeline
// compile (PipelineConfig is typically parsed once, then owned as a
// whole). The size cost is acceptable at config scale; the diagnostic
// quality gain is load-bearing.
#[allow(clippy::large_enum_variant)]
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
    /// N-ary record combining with mixed predicates (equi + range + arbitrary
    /// CXL). Distinct from Merge (which concatenates records streamwise) and
    /// from Transform+lookup (which is 1×1-table only). Introduced in Phase
    /// Combine; see `docs/internal/plans/cxl-engine/phase-combine-node/`.
    Combine {
        #[serde(flatten)]
        header: CombineHeader,
        config: CombineBody,
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

// ---------------------------------------------------------------------
// Custom Deserialize impl for PipelineNode (pre-C.1)
//
// The visitor reads map entries one-at-a-time, looking for the `type:`
// discriminator. Entries encountered before `type:` (the non-canonical
// key order case) are buffered as `(String, serde_value::Value)` pairs
// — spans on those specific entries are unavoidably lost during the
// buffer step, but the canonical YAML convention (used by every fixture
// in this codebase) places `type:` first, so no buffering happens in
// practice and every entry's span survives.
//
// Once `type:` is known, a `DispatchMapAccess` replays buffered entries
// first (from the lossy buffer), then drains the remaining `MapAccess`
// (preserving spans for those). The per-variant payload struct is
// deserialized via `MapAccessDeserializer` over this replaying adapter —
// and because the remaining drain stays inside serde-saphyr's native
// driver, `Spanned<T>` fields (notably `Spanned<NodeInput>` at all three
// header-input positions) capture real locations end-to-end.
//
// Spike validation lives in
// `yaml::tests::test_spanned_survives_mapaccess_deserializer_dispatch` and
// `yaml::tests::test_spanned_at_indexmap_value_position_captures_real_spans`.
// ---------------------------------------------------------------------

impl<'de> Deserialize<'de> for PipelineNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PipelineNodeVisitor;

        /// A MapAccess adapter that first yields buffered
        /// `(String, serde_json::Value)` pairs, then drains an
        /// underlying `MapAccess`. Used to implement the look-ahead for
        /// `type:` dispatch without losing spans on the post-`type:`
        /// tail. `serde_json::Value` is used for the pre-`type:` buffer
        /// because it is already a workspace dep and sufficient for
        /// config-time YAML scalars/maps; spans on those buffered
        /// entries are unavoidably lost (the ONLY span loss this
        /// refactor leaves behind, and only when users place non-
        /// canonical keys before `type:`).
        struct DispatchMapAccess<A> {
            buffered: std::vec::IntoIter<(String, serde_json::Value)>,
            tail: A,
            /// Set during a `next_key_seed` call that yielded a
            /// buffered key, so the follow-up `next_value_seed` can
            /// consume the pre-pulled Value.
            pending_value: Option<serde_json::Value>,
        }

        impl<'de, A> MapAccess<'de> for DispatchMapAccess<A>
        where
            A: MapAccess<'de>,
        {
            type Error = A::Error;

            fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
            where
                K: de::DeserializeSeed<'de>,
            {
                if let Some((key, value)) = self.buffered.next() {
                    self.pending_value = Some(value);
                    let key_de = serde::de::IntoDeserializer::<A::Error>::into_deserializer(key);
                    return seed.deserialize(key_de).map(Some);
                }
                self.tail.next_key_seed(seed)
            }

            fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
            where
                V: de::DeserializeSeed<'de>,
            {
                if let Some(value) = self.pending_value.take() {
                    // serde_json::Value implements Deserializer; route
                    // through an Error-adapting bridge so the seed
                    // error type matches `A::Error`.
                    return seed
                        .deserialize(value.into_deserializer())
                        .map_err(|e: serde_json::Error| de::Error::custom(e.to_string()));
                }
                self.tail.next_value_seed(seed)
            }

            fn size_hint(&self) -> Option<usize> {
                let hint = self.tail.size_hint().unwrap_or(0);
                Some(self.buffered.len() + hint)
            }
        }

        impl<'de> Visitor<'de> for PipelineNodeVisitor {
            type Value = PipelineNode;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(
                    "a pipeline node mapping with a 'type' field and variant-specific fields",
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<PipelineNode, A::Error>
            where
                A: MapAccess<'de>,
            {
                // Look-ahead: read keys until we find `type:`. Any
                // entries that come first are buffered (lossy for
                // spans on just those entries) so we can replay them
                // after dispatch. Canonical fixtures put `type:` first,
                // so in practice nothing is buffered.
                let mut buffered: Vec<(String, serde_json::Value)> = Vec::new();
                let type_tag: String = loop {
                    let key: Option<String> = map.next_key()?;
                    match key {
                        Some(k) if k == "type" => break map.next_value::<String>()?,
                        Some(k) => {
                            let value: serde_json::Value = map.next_value()?;
                            buffered.push((k, value));
                        }
                        None => {
                            return Err(de::Error::custom(
                                "pipeline node must have a 'type' field",
                            ));
                        }
                    }
                };

                // Now build a replaying MapAccess: buffered entries
                // first (span-lossy), then the remaining tail (native
                // saphyr, span-preserving).
                let dispatch = DispatchMapAccess {
                    buffered: buffered.into_iter(),
                    tail: map,
                    pending_value: None,
                };

                // Dispatch each `type:` tag to a dedicated variant
                // payload struct. `MapAccessDeserializer` drives
                // struct deserialization; where the fields come from
                // the tail (i.e. authored after `type:`), spans survive
                // because the deserialization stays inside the native
                // saphyr driver.
                match type_tag.as_str() {
                    "source" => {
                        let payload = SourcePayload::deserialize(
                            de::value::MapAccessDeserializer::new(dispatch),
                        )?;
                        let (header, config) = payload.into_variant_parts();
                        Ok(PipelineNode::Source { header, config })
                    }
                    "transform" => {
                        let payload = TransformPayload::deserialize(
                            de::value::MapAccessDeserializer::new(dispatch),
                        )?;
                        let (header, config) = payload.into_variant_parts();
                        Ok(PipelineNode::Transform { header, config })
                    }
                    "aggregate" => {
                        let payload = AggregatePayload::deserialize(
                            de::value::MapAccessDeserializer::new(dispatch),
                        )?;
                        let (header, config) = payload.into_variant_parts();
                        Ok(PipelineNode::Aggregate { header, config })
                    }
                    "route" => {
                        let payload = RoutePayload::deserialize(
                            de::value::MapAccessDeserializer::new(dispatch),
                        )?;
                        let (header, config) = payload.into_variant_parts();
                        Ok(PipelineNode::Route { header, config })
                    }
                    "merge" => {
                        let payload = MergePayload::deserialize(
                            de::value::MapAccessDeserializer::new(dispatch),
                        )?;
                        let (header, config) = payload.into_variant_parts();
                        Ok(PipelineNode::Merge { header, config })
                    }
                    "combine" => {
                        let payload = CombinePayload::deserialize(
                            de::value::MapAccessDeserializer::new(dispatch),
                        )?;
                        let (header, config) = payload.into_variant_parts();
                        Ok(PipelineNode::Combine { header, config })
                    }
                    "output" => {
                        let payload = OutputPayload::deserialize(
                            de::value::MapAccessDeserializer::new(dispatch),
                        )?;
                        let (header, config) = payload.into_variant_parts();
                        Ok(PipelineNode::Output { header, config })
                    }
                    "composition" => {
                        let payload = CompositionPayload::deserialize(
                            de::value::MapAccessDeserializer::new(dispatch),
                        )?;
                        Ok(payload.into_node())
                    }
                    other => Err(de::Error::unknown_variant(
                        other,
                        &[
                            "source",
                            "transform",
                            "aggregate",
                            "route",
                            "merge",
                            "combine",
                            "output",
                            "composition",
                        ],
                    )),
                }
            }
        }

        deserializer.deserialize_map(PipelineNodeVisitor)
    }
}

// Per-variant payload structs. Each enumerates every post-`type:` YAML
// key explicitly — no `#[serde(flatten)]`, because flatten + the
// type-erased Content buffer it requires would destroy `Spanned<T>`
// field-level locations inside the header structs (the bug C.0.1a's
// two-pass dispatch was originally meant to work around). Enumerating
// each field explicitly keeps the per-field path through serde-saphyr's
// native driver, preserving spans end-to-end.
//
// Each payload struct uses `#[serde(deny_unknown_fields)]` so typos in
// a variant's top-level fields surface as serde errors that name the
// offending field (the C.0.1a wording contract).
//
// The payload structs are then rebuilt into the variant's canonical
// `(header, config)` shape in the Visitor above.

// ---- Source ----------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePayload {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default, rename = "_notes")]
    notes: Option<serde_json::Value>,
    config: SourceBody,
}

impl SourcePayload {
    fn into_variant_parts(self) -> (SourceHeader, SourceBody) {
        (
            SourceHeader {
                name: self.name,
                description: self.description,
                notes: self.notes,
            },
            self.config,
        )
    }
}

// ---- Transform / Aggregate / Route / Output (consumer NodeHeader) ----

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransformPayload {
    name: String,
    #[serde(default)]
    description: Option<String>,
    input: crate::yaml::Spanned<crate::config::node_header::NodeInput>,
    #[serde(default, rename = "_notes")]
    notes: Option<serde_json::Value>,
    config: TransformBody,
}

impl TransformPayload {
    fn into_variant_parts(self) -> (NodeHeader, TransformBody) {
        (
            NodeHeader {
                name: self.name,
                description: self.description,
                input: self.input,
                notes: self.notes,
            },
            self.config,
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregatePayload {
    name: String,
    #[serde(default)]
    description: Option<String>,
    input: crate::yaml::Spanned<crate::config::node_header::NodeInput>,
    #[serde(default, rename = "_notes")]
    notes: Option<serde_json::Value>,
    config: AggregateBody,
}

impl AggregatePayload {
    fn into_variant_parts(self) -> (NodeHeader, AggregateBody) {
        (
            NodeHeader {
                name: self.name,
                description: self.description,
                input: self.input,
                notes: self.notes,
            },
            self.config,
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutePayload {
    name: String,
    #[serde(default)]
    description: Option<String>,
    input: crate::yaml::Spanned<crate::config::node_header::NodeInput>,
    #[serde(default, rename = "_notes")]
    notes: Option<serde_json::Value>,
    config: RouteBody,
}

impl RoutePayload {
    fn into_variant_parts(self) -> (NodeHeader, RouteBody) {
        (
            NodeHeader {
                name: self.name,
                description: self.description,
                input: self.input,
                notes: self.notes,
            },
            self.config,
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputPayload {
    name: String,
    #[serde(default)]
    description: Option<String>,
    input: crate::yaml::Spanned<crate::config::node_header::NodeInput>,
    #[serde(default, rename = "_notes")]
    notes: Option<serde_json::Value>,
    config: OutputBody,
}

impl OutputPayload {
    fn into_variant_parts(self) -> (NodeHeader, OutputBody) {
        (
            NodeHeader {
                name: self.name,
                description: self.description,
                input: self.input,
                notes: self.notes,
            },
            self.config,
        )
    }
}

// ---- Merge -----------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MergePayload {
    name: String,
    #[serde(default)]
    description: Option<String>,
    inputs: Vec<crate::yaml::Spanned<crate::config::node_header::NodeInput>>,
    #[serde(default, rename = "_notes")]
    notes: Option<serde_json::Value>,
    /// Optional on Merge — defaults to an empty `MergeBody`.
    #[serde(default)]
    config: Option<MergeBody>,
}

impl MergePayload {
    fn into_variant_parts(self) -> (MergeHeader, MergeBody) {
        (
            MergeHeader {
                name: self.name,
                description: self.description,
                inputs: self.inputs,
                notes: self.notes,
            },
            self.config.unwrap_or_default(),
        )
    }
}

// ---- Combine ---------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CombinePayload {
    name: String,
    #[serde(default)]
    description: Option<String>,
    input: IndexMap<String, crate::yaml::Spanned<crate::config::node_header::NodeInput>>,
    #[serde(default, rename = "_notes")]
    notes: Option<serde_json::Value>,
    config: CombineBody,
}

impl CombinePayload {
    fn into_variant_parts(self) -> (CombineHeader, CombineBody) {
        (
            CombineHeader {
                name: self.name,
                description: self.description,
                input: self.input,
                notes: self.notes,
            },
            self.config,
        )
    }
}

// ---- Composition -----------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompositionPayload {
    name: String,
    #[serde(default)]
    description: Option<String>,
    input: crate::yaml::Spanned<crate::config::node_header::NodeInput>,
    #[serde(default, rename = "_notes")]
    notes: Option<serde_json::Value>,
    #[serde(rename = "use")]
    r#use: PathBuf,
    #[serde(default)]
    alias: Option<String>,
    #[serde(default)]
    inputs: IndexMap<String, String>,
    #[serde(default)]
    outputs: IndexMap<String, String>,
    #[serde(default)]
    config: IndexMap<String, serde_json::Value>,
    #[serde(default)]
    resources: IndexMap<String, serde_json::Value>,
}

impl CompositionPayload {
    fn into_node(self) -> PipelineNode {
        PipelineNode::Composition {
            header: NodeHeader {
                name: self.name,
                description: self.description,
                input: self.input,
                notes: self.notes,
            },
            r#use: self.r#use,
            alias: self.alias,
            inputs: self.inputs,
            outputs: self.outputs,
            config: self.config,
            resources: self.resources,
            body: crate::plan::composition_body::CompositionBodyId::default(),
        }
    }
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
            PipelineNode::Combine { header, .. } => &header.name,
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
            PipelineNode::Combine { .. } => "combine",
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

/// Match cardinality for lookup/combine enrichment.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatchMode {
    /// Return the first matching row (scalar enrichment, 1:1).
    #[default]
    First,
    /// Return all matching rows (fan-out, 1:N).
    All,
    /// Aggregate all matches into Value::Array. Empty array on miss.
    /// Per-group limit 10K (D26, RESEARCH-collect-semantics.md).
    /// OQ-1 RESOLVED: ships with existing Type::Array.
    Collect,
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

/// Combine variant body. N-ary record combining with mixed predicates.
///
/// The `where:` CXL expression matches records across the named inputs
/// declared on [`CombineHeader`]; the `cxl:` body defines the output
/// schema via `emit` statements. See Phase Combine drill for the full
/// matching semantics (equi/range/arbitrary predicate decomposition).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CombineBody {
    /// CXL boolean predicate matching records across inputs.
    #[serde(rename = "where")]
    pub where_expr: CxlSource,
    /// Cardinality control.
    #[serde(default, rename = "match")]
    pub match_mode: MatchMode,
    /// Missing record handling.
    #[serde(default)]
    pub on_miss: OnMiss,
    /// CXL output body with emit statements defining the output schema.
    pub cxl: CxlSource,
    /// Optional explicit driving input name. When absent, the first entry
    /// in [`CombineHeader::input`] is used (IndexMap iteration order).
    #[serde(default)]
    pub drive: Option<String>,
}

/// Output variant body. Wraps the existing sink config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputBody {
    #[serde(flatten)]
    pub output: crate::config::OutputConfig,
}

// ---------------------------------------------------------------------
// Custom-Deserialize gate tests (pre-C.1, supersedes C.0.1a)
//
// These tests were originally authored against the
// `#[serde(try_from = "serde_json::Value")]` path in C.0.1a. They
// continue to run against the new custom-Deserialize impl; the behaviours
// they assert are architectural contracts that must survive any future
// refactor:
//
//   - unknown top-level keys are rejected with the field name in the error
//   - every existing variant parses (regression gate)
//   - unknown `type:` tags produce a clear error naming the tag
//   - outer Spanned<PipelineNode> node-level location capture is preserved
//   - SourceBody inner `#[serde(flatten)]` on `SourceConfig` still works
//   - NEW: Spanned<NodeInput> at header-input field level captures real
//     locations (the whole point of the pre-C.1 refactor).
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

    /// Architectural gate test exercising ALL edge cases from the C.0.1a
    /// deep-dive PLUS the new span-preservation contract.
    ///
    /// Validates:
    /// - Composition variant parses with all optional fields.
    /// - Outer `Spanned<PipelineNode>` captures non-zero location.
    /// - NEW (pre-C.1): `NodeHeader.input: Spanned<NodeInput>` carries a
    ///   real location, not `Location::UNKNOWN` — this is the property
    ///   the C.0.1a two-pass approach erased.
    /// - `SourceBody` inner `#[serde(flatten)]` on `SourceConfig` keeps
    ///   working (the custom-Deserialize path drives serde-saphyr
    ///   natively, not through a `serde_json::Value` roundtrip).
    /// - Unknown top-level fields produce errors that name the field.
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

        // 2. Outer Spanned<PipelineNode> captures non-zero location.
        assert_ne!(
            doc.nodes[0].referenced,
            Location::UNKNOWN,
            "outer Spanned<PipelineNode> must capture location"
        );

        // 3. NEW (pre-C.1): field-level Spanned on Composition's header
        // input carries a real location (Composition uses NodeHeader).
        if let PipelineNode::Composition { header, .. } = &doc.nodes[0].value {
            assert_ne!(
                header.input.referenced,
                Location::UNKNOWN,
                "NodeHeader.input: Spanned<NodeInput> must carry a real location (pre-C.1)"
            );
            assert!(
                header.input.referenced.line() >= 1,
                "NodeHeader.input line must be >= 1, got {}",
                header.input.referenced.line()
            );
        } else {
            panic!("expected composition variant");
        }

        // 4. Transform parses and SourceBody inner flatten still works.
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
        if let PipelineNode::Transform { header, config } = &doc2.nodes[1].value {
            assert_eq!(config.cxl.source, "emit id = id");
            // Transform header input also carries a real span.
            assert_ne!(
                header.input.referenced,
                Location::UNKNOWN,
                "Transform header input must carry a real span (pre-C.1)"
            );
        } else {
            panic!("expected transform");
        }

        // SourceBody inner flatten.
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

// ---------------------------------------------------------------------
// Task C.0.1: PipelineNode::Combine deserialization tests
//
// Validate that the `combine` arm in `TryFrom<serde_json::Value>`
// correctly parses the `CombineHeader` (IndexMap-based) and `CombineBody`
// (where_expr, match_mode, on_miss, cxl, drive). Exercises IndexMap
// insertion-order preservation, MatchMode::Collect, drive override, and
// unknown-field rejection on both the header and the body.
//
// The spanned fixtures in tests/fixtures/combine/ use a flat top-level
// `name:` (not `pipeline.name:`), so these tests use inline YAML with a
// proper `pipeline:` wrapper. The fixtures are separately exercised by
// tests/combine_test.rs.
// ---------------------------------------------------------------------

#[cfg(test)]
mod combine_deser_tests {
    use super::*;
    use crate::config::PipelineConfig;
    use crate::config::node_header::NodeInput;

    /// Parse a PipelineConfig and pull the Combine node named `node_name`.
    /// Panics if not found or not a Combine.
    fn parse_and_find_combine<'a>(
        doc: &'a PipelineConfig,
        node_name: &str,
    ) -> (&'a CombineHeader, &'a CombineBody) {
        for spanned in &doc.nodes {
            if let PipelineNode::Combine { header, config } = &spanned.value
                && header.name == node_name
            {
                return (header, config);
            }
        }
        panic!("combine node {node_name:?} not found in pipeline");
    }

    /// 2-input combine with equi predicate parses correctly. Verifies
    /// header name, input map keys and order, body where/cxl fields.
    #[test]
    fn test_combine_yaml_deser_two_input_equi() {
        let yaml = r#"
pipeline:
  name: combine_two_input_equi
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
  - type: combine
    name: enrich
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      cxl: |
        emit order_id = orders.order_id
        emit product_name = products.name
"#;
        let doc: PipelineConfig =
            crate::yaml::from_str(yaml).expect("two-input combine should parse");
        let (header, body) = parse_and_find_combine(&doc, "enrich");
        assert_eq!(header.name, "enrich");
        assert_eq!(header.description, None);
        assert_eq!(header.input.len(), 2);
        let keys: Vec<&str> = header.input.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["orders", "products"]);
        assert!(
            matches!(header.input.get("orders").map(|s| &s.value), Some(NodeInput::Single(s)) if s == "orders")
        );
        assert!(
            matches!(header.input.get("products").map(|s| &s.value), Some(NodeInput::Single(s)) if s == "products")
        );
        assert_eq!(
            body.where_expr.source,
            "orders.product_id == products.product_id"
        );
        assert!(body.cxl.source.contains("emit order_id = orders.order_id"));
        // Defaults stay defaults.
        assert_eq!(body.match_mode, MatchMode::First);
        assert_eq!(body.on_miss, OnMiss::NullFields);
        assert_eq!(body.drive, None);
    }

    /// 3-input combine; IndexMap preserves insertion order.
    #[test]
    fn test_combine_yaml_deser_three_input() {
        let yaml = r#"
pipeline:
  name: combine_three_input
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
  - type: source
    name: categories
    config:
      name: categories
      type: csv
      path: categories.csv
      schema:
        - { name: product_id, type: string }
  - type: combine
    name: fully_enriched
    input:
      orders: orders
      products: products
      categories: categories
    config:
      where: "orders.product_id == products.product_id and products.product_id == categories.product_id"
      cxl: |
        emit order_id = orders.order_id
"#;
        let doc: PipelineConfig =
            crate::yaml::from_str(yaml).expect("three-input combine should parse");
        let (header, _body) = parse_and_find_combine(&doc, "fully_enriched");
        assert_eq!(header.input.len(), 3);
        let keys: Vec<&str> = header.input.keys().map(String::as_str).collect();
        // IndexMap MUST preserve insertion order: orders, products, categories.
        assert_eq!(
            keys,
            vec!["orders", "products", "categories"],
            "IndexMap did not preserve insertion order"
        );
    }

    /// `match: collect` parses to `MatchMode::Collect`.
    #[test]
    fn test_combine_yaml_deser_match_collect() {
        let yaml = r#"
pipeline:
  name: combine_match_collect
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
  - type: combine
    name: collected
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: collect
      cxl: |
        emit order_id = orders.order_id
        emit products_list = products
"#;
        let doc: PipelineConfig = crate::yaml::from_str(yaml).expect("match: collect should parse");
        let (_header, body) = parse_and_find_combine(&doc, "collected");
        assert_eq!(body.match_mode, MatchMode::Collect);
    }

    /// `drive:` field parses correctly.
    #[test]
    fn test_combine_yaml_deser_drive_field() {
        let yaml = r#"
pipeline:
  name: combine_drive_hint
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
  - type: combine
    name: product_driven
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      drive: products
      cxl: |
        emit product_id = products.product_id
"#;
        let doc: PipelineConfig = crate::yaml::from_str(yaml).expect("drive: should parse");
        let (_header, body) = parse_and_find_combine(&doc, "product_driven");
        assert_eq!(body.drive.as_deref(), Some("products"));
    }

    /// `name()` returns header name; `type_tag()` returns `"combine"`.
    #[test]
    fn test_combine_name_and_type_tag() {
        use crate::yaml::Spanned;
        use indexmap::IndexMap;
        use serde_saphyr::Location;

        let mk_spanned = |ni: NodeInput| Spanned::new(ni, Location::UNKNOWN, Location::UNKNOWN);

        let mut input_map: IndexMap<String, Spanned<NodeInput>> = IndexMap::new();
        input_map.insert(
            "a".to_string(),
            mk_spanned(NodeInput::Single("src_a".to_string())),
        );
        input_map.insert(
            "b".to_string(),
            mk_spanned(NodeInput::Single("src_b".to_string())),
        );

        let header = CombineHeader {
            name: "my_combine".to_string(),
            description: None,
            input: input_map,
            notes: None,
        };
        let body = CombineBody {
            where_expr: CxlSource::unspanned("a.id == b.id"),
            match_mode: MatchMode::First,
            on_miss: OnMiss::NullFields,
            cxl: CxlSource::unspanned("emit id = a.id"),
            drive: None,
        };
        let node = PipelineNode::Combine {
            header,
            config: body,
        };

        assert_eq!(node.name(), "my_combine");
        assert_eq!(node.type_tag(), "combine");
    }

    /// Unknown fields in header or body produce serde errors with field names.
    #[test]
    fn test_combine_rejects_unknown_fields() {
        // (1) Unknown field at the node top-level (caught by reject_unknown
        // on the Combine arm — the expected-fields list names the field).
        let yaml_unknown_top = r#"
pipeline:
  name: combine_bad_top
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
  - type: combine
    name: bad
    input:
      orders: orders
      products: products
    bogus_header_field: oops
    config:
      where: "orders.product_id == products.product_id"
      cxl: "emit order_id = orders.order_id"
"#;
        let err = crate::yaml::from_str::<PipelineConfig>(yaml_unknown_top)
            .expect_err("unknown top-level field must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("bogus_header_field"),
            "error should name the unknown field, got: {msg}"
        );

        // (2) Unknown field inside `config:` body (caught by the
        // `#[serde(deny_unknown_fields)]` on CombineBody).
        let yaml_unknown_body = r#"
pipeline:
  name: combine_bad_body
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
  - type: combine
    name: bad_body
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      cxl: "emit order_id = orders.order_id"
      bogus_body_field: oops
"#;
        let err = crate::yaml::from_str::<PipelineConfig>(yaml_unknown_body)
            .expect_err("unknown body field must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("bogus_body_field"),
            "error should name the unknown body field, got: {msg}"
        );
    }
}
