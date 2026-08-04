//! Channel/group value-clobber merge.
//!
//! Applies an overlay layer's `config:` / `vars:` surface to a
//! [`CompiledPlan`] as a layered **clobber** over the plan's
//! [`ProvenanceDb`](clinker_plan::config::composition::ProvenanceDb): a higher
//! layer's value fully *replaces* the lower one (never a deep-merge), and each
//! resolved value maps to exactly one winning layer. The layer stack is driven
//! by [`crate::resolve`], which resolves the group / channel-wide / per-target
//! layers and calls these engines per layer.
//!
//! # Layer stack
//!
//! Config resolves through a fixed *semantic* order of layers — never lexical
//! or positional — encoded by
//! [`LayerKind`](clinker_plan::config::composition::LayerKind):
//!
//! ```text
//! PipelineDefault  <  Group(s) by priority  <  ChannelWide  <  ChannelPerTarget
//! ```
//!
//! - **PipelineDefault** is the base layer, already recorded in the plan's
//!   `ProvenanceDb` by composition compile.
//! - **Group** (selector-derived), **ChannelWide** (`channel.cfg.yaml`
//!   manifest), and **ChannelPerTarget** (per-target overlay) layers all plug
//!   into the same [`apply_config_candidates`] engine — it resolves at any
//!   `LayerKind`, so no layer needs bespoke resolution logic. A leaf whose
//!   `fixed: true` locks its value against every higher-precedence layer.
//!
//! Also resolves channel-supplied var overrides/adds for the four scoped
//! registries (`$vars.*`, `$pipeline.*`, `$source.*`, `$record.*`) against the
//! pipeline's declarations (these flow to the executor as runtime values via
//! `PipelineRunParams`, not into the AST).

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;

use clinker_core_types::Span;
use clinker_core_types::{Diagnostic, LabeledSpan};
use clinker_plan::config::ConfigOverrides;
use clinker_plan::config::composition::{
    LayerKind, ProvenanceDb, ProvenanceLookupError, ProvenanceQuery, ResolvedValue,
};
use clinker_plan::config::pipeline_node::{PipelineNode, VarScope};
use clinker_plan::config::{
    PipelineConfig, ScopedVarType, check_scoped_var_default, coerce_scoped_var_default,
    reserved_names_for,
};
use clinker_record::Value;

use crate::dotted::DottedPath;
use crate::manifest::{ChannelVarValue, ChannelVars, OverlayCandidate};

/// Resolved channel overlay output: typed var maps and any diagnostics
/// raised during validation.
#[derive(Debug, Default)]
pub struct ChannelOverlayResult {
    /// Channel overrides/adds for `$vars.*`. Keyed by var name.
    pub static_vars: IndexMap<String, Value>,
    /// Channel overrides/adds for `$pipeline.*`. Keyed by var name.
    pub pipeline_vars: IndexMap<String, Value>,
    /// Channel overrides/adds for `$source.<src>.<var>`. Outer key is
    /// the source-node name, inner key is the var name.
    pub source_vars: IndexMap<String, IndexMap<String, Value>>,
    /// Channel overrides/adds for `$record.*`. Channel-wide pre-seed
    /// applied to every record at materialization.
    pub record_vars: IndexMap<String, Value>,
    pub diagnostics: Vec<Diagnostic>,
    fixed_vars: HashSet<String>,
}

/// Fully folded per-node config produced only after every candidate has been
/// admitted and validated for the selected target.
///
/// The inner compiler map stays private so callers cannot accidentally pass
/// raw channel values across the validated-plan boundary.
#[derive(Debug, Clone, Default)]
pub struct ResolvedChannelConfig(ConfigOverrides);

impl ResolvedChannelConfig {
    /// Borrow the validated compiler overrides for inspection.
    pub fn compile_overrides(&self) -> &ConfigOverrides {
        &self.0
    }

    /// Consume this validated artifact into the compiler's input map.
    pub fn into_compile_overrides(self) -> ConfigOverrides {
        self.0
    }
}

/// Validate every authored candidate before adding it to provenance. Invalid
/// candidates never disappear merely because a later layer would win.
pub(crate) fn apply_config_candidates(
    provenance: &mut ProvenanceDb,
    config: &IndexMap<String, OverlayCandidate<serde_json::Value>>,
    kind: LayerKind,
    source_name: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for (name, candidate) in config {
        let span = Span::line_only(candidate.value_span.line() as u32);
        let Ok(dotted) = DottedPath::try_from(name.as_str()) else {
            diagnostics.push(Diagnostic::error(
                "E113",
                format!(
                    "overlay {source_name:?}: config key {name:?} is not a valid composition parameter address"
                ),
                LabeledSpan::primary(span, "invalid config key"),
            ));
            continue;
        };
        let Ok(query) = ProvenanceQuery::parse(dotted.as_str()) else {
            diagnostics.push(Diagnostic::error(
                "E113",
                format!("overlay {source_name:?}: config key {name:?} must use `alias.parameter`"),
                LabeledSpan::primary(span, "invalid config key"),
            ));
            continue;
        };
        let key = match provenance.resolve_query_key(&query).cloned() {
            Ok(key) => key,
            Err(ProvenanceLookupError::Unknown { .. }) => {
                diagnostics.push(Diagnostic::error(
                    "E113",
                    format!(
                        "overlay {source_name:?}: config key {name:?} does not match any composition parameter in the selected pipeline"
                    ),
                    LabeledSpan::primary(span, "unknown config candidate"),
                ));
                continue;
            }
            Err(ProvenanceLookupError::Ambiguous { candidates }) => {
                let candidates = candidates
                    .into_iter()
                    .map(|candidate| candidate.render())
                    .collect::<Vec<_>>()
                    .join(", ");
                diagnostics.push(Diagnostic::error(
                    "E118",
                    format!(
                        "overlay {source_name:?}: config key {name:?} is ambiguous; use one of: {candidates}"
                    ),
                    LabeledSpan::primary(span, "ambiguous config candidate"),
                ));
                continue;
            }
        };
        let Some(resolved) = provenance.get_by_key_mut(&key) else {
            continue;
        };
        let expected = resolved
            .layer_value(LayerKind::PipelineDefault)
            .unwrap_or(&resolved.value);
        if !same_json_type(expected, &candidate.value) {
            diagnostics.push(Diagnostic::error(
                "E103",
                format!(
                    "overlay {source_name:?}: config candidate {name:?} has type {}, but the declared parameter has type {}",
                    json_type(&candidate.value),
                    json_type(expected),
                ),
                LabeledSpan::primary(span, "type-mismatched config candidate"),
            ));
            continue;
        }
        if resolved
            .provenance
            .iter()
            .any(|layer| layer.fixed && layer.kind < kind)
        {
            diagnostics.push(Diagnostic::error(
                "E103",
                format!(
                    "overlay {source_name:?}: config candidate {name:?} cannot override a fixed lower-precedence value"
                ),
                LabeledSpan::primary(span, "override forbidden by fixed value"),
            ));
            continue;
        }
        if candidate.fixed {
            resolved.apply_layer_fixed(candidate.value.clone(), kind, span);
        } else {
            resolved.apply_layer(candidate.value.clone(), kind, span);
        }
    }
}

fn same_json_type(expected: &serde_json::Value, candidate: &serde_json::Value) -> bool {
    expected.is_null()
        || candidate.is_null()
        || matches!(
            (expected, candidate),
            (serde_json::Value::Bool(_), serde_json::Value::Bool(_))
                | (serde_json::Value::Number(_), serde_json::Value::Number(_))
                | (serde_json::Value::String(_), serde_json::Value::String(_))
                | (serde_json::Value::Array(_), serde_json::Value::Array(_))
                | (serde_json::Value::Object(_), serde_json::Value::Object(_))
        )
}

fn json_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Resolve the winning `config:` value per `(node, param)` across overlay
/// layers, for the pre-compile constant fold of `$config.<param>` in a
/// composition body.
///
/// Reuses the same [`ResolvedValue`] winner logic the post-compile
/// [`ProvenanceDb`] path uses, applied in the identical ascending-precedence
/// order (`apply_config_and_vars`: groups → channel-wide → per-target; the
/// leaf binding. Resolving the fold value and rendered provenance from the same
/// layer machinery keeps the executed value and the `channels resolve` /
/// `explain --field` `[WON]` layer in agreement.
#[derive(Default)]
pub(crate) struct EffectiveConfig {
    winners: HashMap<(String, String), ResolvedValue<serde_json::Value>>,
}

impl EffectiveConfig {
    /// Apply one layer's raw string-keyed `config:` map. A malformed key is
    /// skipped (it surfaces as a diagnostic on the clobber path); a
    /// single-segment key targets no composition node and never folds.
    pub(crate) fn apply(
        &mut self,
        config: &IndexMap<String, OverlayCandidate<serde_json::Value>>,
        kind: LayerKind,
    ) {
        for (key, candidate) in config {
            let Ok(dotted) = DottedPath::try_from(key.as_str()) else {
                continue;
            };
            if let (Some(node), param) = dotted.segments() {
                self.insert(node, param, &candidate.value, kind, candidate.fixed);
            }
        }
    }

    fn insert(
        &mut self,
        node: &str,
        param: &str,
        value: &serde_json::Value,
        kind: LayerKind,
        fixed: bool,
    ) {
        self.winners
            .entry((node.to_string(), param.to_string()))
            .and_modify(|rv| {
                if fixed {
                    rv.apply_layer_fixed(value.clone(), kind, Span::SYNTHETIC);
                } else {
                    rv.apply_layer(value.clone(), kind, Span::SYNTHETIC);
                }
            })
            .or_insert_with(|| {
                // Preserve the `fixed` lock on the first-touched layer too, so a
                // fixed lower tier keeps winning over a later higher tier —
                // matching `ResolvedValue::winner_kind`, which the ProvenanceDb
                // path uses.
                if fixed {
                    ResolvedValue::new_fixed(value.clone(), kind, Span::SYNTHETIC)
                } else {
                    ResolvedValue::new(value.clone(), kind, Span::SYNTHETIC)
                }
            });
    }

    /// Collapse to the per-node winning values for
    /// [`CompileContext::config_overrides`](clinker_plan::config::CompileContext).
    pub(crate) fn into_resolved(self) -> ResolvedChannelConfig {
        let mut out = ConfigOverrides::new();
        for ((node, param), rv) in self.winners {
            out.entry(node).or_default().insert(param, rv.value);
        }
        ResolvedChannelConfig(out)
    }
}

/// Resolve one overlay layer's [`ChannelVars`] against the pipeline's declared
/// registries and **merge** the results into `out` with later-layer-wins
/// semantics (a key an earlier layer set is overwritten by this layer).
///
/// Reuses the same per-registry validators every overlay layer uses, so groups,
/// the channel-wide manifest, and the per-target overlay resolve vars
/// identically. Callers apply layers in ascending precedence order so the
/// highest layer wins each key. `source_label` names the layer for diagnostics
/// (e.g. a group or channel name).
pub(crate) fn resolve_vars_layer(
    source_label: &str,
    vars: &ChannelVars,
    config: &PipelineConfig,
    out: &mut ChannelOverlayResult,
) {
    let static_values = resolve_static_overrides(
        source_label,
        &vars.static_scope,
        config,
        &mut out.diagnostics,
    );
    merge_var_layer(
        source_label,
        "static",
        &vars.static_scope,
        static_values,
        &mut out.static_vars,
        &mut out.fixed_vars,
        &mut out.diagnostics,
    );
    let pipeline_values = resolve_scoped_overrides(
        source_label,
        &vars.pipeline,
        config,
        VarScope::Pipeline,
        &mut out.diagnostics,
    );
    merge_var_layer(
        source_label,
        "pipeline",
        &vars.pipeline,
        pipeline_values,
        &mut out.pipeline_vars,
        &mut out.fixed_vars,
        &mut out.diagnostics,
    );
    let record_values = resolve_scoped_overrides(
        source_label,
        &vars.record,
        config,
        VarScope::Record,
        &mut out.diagnostics,
    );
    merge_var_layer(
        source_label,
        "record",
        &vars.record,
        record_values,
        &mut out.record_vars,
        &mut out.fixed_vars,
        &mut out.diagnostics,
    );
    for (source, values) in
        resolve_source_overrides(source_label, &vars.source, config, &mut out.diagnostics)
    {
        let Some(candidates) = vars.source.get(&source) else {
            continue;
        };
        let destination = out.source_vars.entry(source.clone()).or_default();
        merge_var_layer(
            source_label,
            &format!("source.{source}"),
            candidates,
            values,
            destination,
            &mut out.fixed_vars,
            &mut out.diagnostics,
        );
    }
}

fn merge_var_layer(
    source_label: &str,
    scope: &str,
    candidates: &IndexMap<String, ChannelVarValue>,
    values: IndexMap<String, Value>,
    destination: &mut IndexMap<String, Value>,
    fixed_vars: &mut HashSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for (name, value) in values {
        let key = format!("{scope}.{name}");
        let candidate = &candidates[&name];
        let span = candidate_value_span(candidate);
        if fixed_vars.contains(&key) {
            diagnostics.push(Diagnostic::error(
                "E103",
                format!(
                    "overlay {source_label:?}: variable candidate `{key}` cannot override a fixed lower-precedence value"
                ),
                LabeledSpan::primary(span, "override forbidden by fixed value"),
            ));
            continue;
        }
        destination.insert(name, value);
        if candidate.fixed {
            fixed_vars.insert(key);
        }
    }
}

// ── Var overlay resolvers ──────────────────────────────────────────────

/// View of the pipeline's declared `$vars.*` registry.
fn declared_static_vars(config: &PipelineConfig) -> IndexMap<String, ScopedVarType> {
    config
        .pipeline
        .vars
        .as_ref()
        .map(|m| m.iter().map(|(k, d)| (k.clone(), d.var_type)).collect())
        .unwrap_or_default()
}

/// View of the pipeline's declared `$<scope>.*` registry built from
/// every Transform's `declares:` filtered to `wanted` scope.
fn declared_scoped_vars(
    config: &PipelineConfig,
    wanted: VarScope,
) -> IndexMap<String, ScopedVarType> {
    let mut out = IndexMap::new();
    for spanned in &config.nodes {
        if let PipelineNode::Transform { config: body, .. } = &spanned.value {
            for entry in &body.declares {
                if entry.scope == wanted {
                    out.insert(entry.name.clone(), entry.var_type);
                }
            }
        }
    }
    out
}

fn declared_source_node_names(config: &PipelineConfig) -> Vec<String> {
    config
        .nodes
        .iter()
        .filter_map(|n| match &n.value {
            PipelineNode::Source { header, .. } => Some(header.name.clone()),
            _ => None,
        })
        .collect()
}

/// Resolve `$vars.*` channel overrides. `$vars.*` has no reserved
/// subset and no scope label — handled separately from
/// [`resolve_scoped_overrides`].
fn resolve_static_overrides(
    channel_name: &str,
    overrides: &IndexMap<String, ChannelVarValue>,
    config: &PipelineConfig,
    diagnostics: &mut Vec<Diagnostic>,
) -> IndexMap<String, Value> {
    let declared = declared_static_vars(config);
    let mut out = IndexMap::new();
    for (name, decl) in overrides {
        if let Some(value) = validate_and_coerce(
            channel_name,
            "static",
            name,
            decl,
            declared.get(name).copied(),
            None,
            diagnostics,
        ) {
            out.insert(name.clone(), value);
        }
    }
    out
}

/// Resolve `$pipeline.*` or `$record.*` channel overrides — flat
/// shared namespaces with reserved-name guards.
fn resolve_scoped_overrides(
    channel_name: &str,
    overrides: &IndexMap<String, ChannelVarValue>,
    config: &PipelineConfig,
    scope: VarScope,
    diagnostics: &mut Vec<Diagnostic>,
) -> IndexMap<String, Value> {
    let declared = declared_scoped_vars(config, scope);
    let scope_label = match scope {
        VarScope::Pipeline => "pipeline",
        VarScope::Source => "source",
        VarScope::Record => "record",
    };
    let mut out = IndexMap::new();
    for (name, decl) in overrides {
        if let Some(value) = validate_and_coerce(
            channel_name,
            scope_label,
            name,
            decl,
            declared.get(name).copied(),
            Some(scope),
            diagnostics,
        ) {
            out.insert(name.clone(), value);
        }
    }
    out
}

/// Resolve `$source.<src>.<var>` channel overrides. Outer dimension is
/// the source-node name (must exist in the pipeline; E118 otherwise);
/// inner dimension follows the same rules as
/// [`resolve_scoped_overrides`] for `Source` scope.
fn resolve_source_overrides(
    channel_name: &str,
    overrides: &IndexMap<String, IndexMap<String, ChannelVarValue>>,
    config: &PipelineConfig,
    diagnostics: &mut Vec<Diagnostic>,
) -> IndexMap<String, IndexMap<String, Value>> {
    let declared_sources = declared_source_node_names(config);
    let declared = declared_scoped_vars(config, VarScope::Source);
    let mut out: IndexMap<String, IndexMap<String, Value>> = IndexMap::new();
    for (src_name, inner) in overrides {
        if !declared_sources.iter().any(|n| n == src_name) {
            let span = inner
                .values()
                .next()
                .map(candidate_type_span)
                .unwrap_or(Span::SYNTHETIC);
            diagnostics.push(Diagnostic::error(
                "E118",
                format!(
                    "channel {:?}: source {:?} not declared in pipeline (known: {})",
                    channel_name,
                    src_name,
                    declared_sources.join(", "),
                ),
                LabeledSpan::primary(span, "source override declared here"),
            ));
            continue;
        }
        let mut resolved_inner = IndexMap::new();
        for (var_name, decl) in inner {
            if let Some(value) = validate_and_coerce(
                channel_name,
                "source",
                var_name,
                decl,
                declared.get(var_name).copied(),
                Some(VarScope::Source),
                diagnostics,
            ) {
                resolved_inner.insert(var_name.clone(), value);
            }
        }
        if !resolved_inner.is_empty() {
            out.insert(src_name.clone(), resolved_inner);
        }
    }
    out
}

/// Single per-entry validator: reserved-name guard (when
/// `reserved_scope` is `Some`), type-equality check on override
/// (E116), default coercion. Push diagnostics on failure; return
/// `None` so the caller skips the entry.
fn validate_and_coerce(
    channel_name: &str,
    scope_label: &str,
    var_name: &str,
    decl: &ChannelVarValue,
    declared_type: Option<ScopedVarType>,
    reserved_scope: Option<VarScope>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Value> {
    if let Some(scope) = reserved_scope
        && reserved_names_for(scope).contains(&var_name)
    {
        diagnostics.push(Diagnostic::error(
            "E117",
            format!(
                "channel {:?}: var ${}.{} shadows reserved system field",
                channel_name, scope_label, var_name,
            ),
            LabeledSpan::primary(candidate_type_span(decl), "reserved variable name"),
        ));
        return None;
    }

    if let Some(declared) = declared_type
        && declared != decl.var_type
    {
        diagnostics.push(Diagnostic::error(
            "E116",
            format!(
                "channel {:?}: var ${}.{} override type mismatch — declared {:?}, override declared {:?}",
                channel_name, scope_label, var_name, declared, decl.var_type,
            ),
            LabeledSpan::primary(candidate_type_span(decl), "type declared here"),
        ));
        return None;
    }

    let default = decl.default.as_ref()?;

    let where_label = format!("channel {channel_name:?} vars.{scope_label}");
    if let Err(e) = check_scoped_var_default(&where_label, var_name, decl.var_type, default) {
        diagnostics.push(Diagnostic::error(
            "E116",
            format!(
                "channel {:?}: var ${}.{} default does not match type {:?}: {e}",
                channel_name, scope_label, var_name, decl.var_type,
            ),
            LabeledSpan::primary(candidate_value_span(decl), "default declared here"),
        ));
        return None;
    }

    Some(coerce_scoped_var_default(decl.var_type, default))
}

fn candidate_type_span(candidate: &ChannelVarValue) -> Span {
    Span::line_only(candidate.type_span.line() as u32)
}

fn candidate_value_span(candidate: &ChannelVarValue) -> Span {
    Span::line_only(
        candidate
            .default_span
            .or(candidate.fixed_span)
            .unwrap_or(candidate.type_span)
            .line() as u32,
    )
}
