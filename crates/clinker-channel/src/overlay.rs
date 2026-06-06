//! Channel overlay merge.
//!
//! Applies a [`ChannelBinding`] to a [`CompiledPlan`], overlaying config
//! values as `ChannelDefault` or `ChannelFixed` provenance layers on the
//! plan's [`ProvenanceDb`](clinker_plan::config::composition::ProvenanceDb),
//! and resolving channel-supplied var
//! overrides/adds for the four scoped registries (`$vars.*`,
//! `$pipeline.*`, `$source.*`, `$record.*`) against the pipeline's
//! declarations. Stamps a [`ChannelIdentity`] for cache-keying.

use indexmap::IndexMap;

use clinker_core_types::Span;
use clinker_core_types::{Diagnostic, LabeledSpan};
use clinker_plan::config::composition::LayerKind;
use clinker_plan::config::pipeline_node::{PipelineNode, VarScope};
use clinker_plan::config::{
    PipelineConfig, ScopedVarDecl, ScopedVarType, check_scoped_var_default,
    coerce_scoped_var_default, reserved_names_for,
};
use clinker_plan::plan::{ChannelIdentity, CompiledPlan};
use clinker_record::Value;

use crate::binding::{ChannelBinding, ChannelTarget, DottedPath};

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
}

/// Apply a channel binding to a compiled plan.
///
/// Performs three things:
///
/// 1. Overlays `config.default` / `config.fixed` onto the plan's
///    [`ProvenanceDb`](clinker_plan::config::composition::ProvenanceDb) as
///    `ChannelDefault` / `ChannelFixed` layers
///    (W103 for keys not matched in the plan).
/// 2. Resolves the channel's `vars:` block against the pipeline's
///    declared registries — typecheck on override (E107), reserved
///    name guard (E110), unknown source-node guard (E111),
///    composition-target guard (E109).
/// 3. Stamps `ChannelIdentity` on the plan.
///
/// Returns the typed var maps even when diagnostics include warnings;
/// callers should refuse to execute if any `Severity::Error` is present.
pub fn apply_channel_overlay(
    plan: &mut CompiledPlan,
    binding: &ChannelBinding,
    config: &PipelineConfig,
) -> ChannelOverlayResult {
    let mut result = ChannelOverlayResult::default();

    apply_config_layer(
        plan,
        &binding.config_default,
        LayerKind::ChannelDefault,
        &binding.name,
        &mut result.diagnostics,
    );
    apply_config_layer(
        plan,
        &binding.config_fixed,
        LayerKind::ChannelFixed,
        &binding.name,
        &mut result.diagnostics,
    );

    let composition_target = matches!(binding.target, ChannelTarget::Composition(_));
    let has_var_overrides = !binding.vars_static.is_empty()
        || !binding.vars_pipeline.is_empty()
        || !binding.vars_source.is_empty()
        || !binding.vars_record.is_empty();

    if composition_target && has_var_overrides {
        let target_path = match &binding.target {
            ChannelTarget::Composition(p) => p.display().to_string(),
            ChannelTarget::Pipeline(_) => unreachable!(),
        };
        result.diagnostics.push(Diagnostic::error(
            "E109",
            format!(
                "channel {:?}: var overrides not supported on composition channels (target: {})",
                binding.name, target_path,
            ),
            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
        ));
    } else {
        result.static_vars = resolve_static_overrides(
            &binding.name,
            &binding.vars_static,
            config,
            &mut result.diagnostics,
        );
        result.pipeline_vars = resolve_scoped_overrides(
            &binding.name,
            &binding.vars_pipeline,
            config,
            VarScope::Pipeline,
            &mut result.diagnostics,
        );
        result.record_vars = resolve_scoped_overrides(
            &binding.name,
            &binding.vars_record,
            config,
            VarScope::Record,
            &mut result.diagnostics,
        );
        result.source_vars = resolve_source_overrides(
            &binding.name,
            &binding.vars_source,
            config,
            &mut result.diagnostics,
        );
    }

    plan.set_channel_identity(ChannelIdentity {
        name: binding.name.clone(),
        content_hash: binding.channel_hash,
    });

    result
}

fn apply_config_layer(
    plan: &mut CompiledPlan,
    config: &IndexMap<DottedPath, serde_json::Value>,
    kind: LayerKind,
    channel_name: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let provenance = plan.provenance_mut();

    for (dotted_path, value) in config {
        let (node_name, param_name) = match dotted_path.segments() {
            (Some(alias), param) => (alias, param),
            (None, param) => ("", param),
        };

        match provenance.get_mut(node_name, param_name) {
            Some(resolved) => {
                resolved.apply_layer(value.clone(), kind, Span::SYNTHETIC);
            }
            None => {
                diagnostics.push(Diagnostic::warning(
                    "W103",
                    format!(
                        "channel {:?}: config key {:?} does not match any \
                         composition parameter in the compiled plan",
                        channel_name,
                        dotted_path.as_str(),
                    ),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
            }
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
    overrides: &IndexMap<String, ScopedVarDecl>,
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
    overrides: &IndexMap<String, ScopedVarDecl>,
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
/// the source-node name (must exist in the pipeline; E111 otherwise);
/// inner dimension follows the same rules as
/// [`resolve_scoped_overrides`] for `Source` scope.
fn resolve_source_overrides(
    channel_name: &str,
    overrides: &IndexMap<String, IndexMap<String, ScopedVarDecl>>,
    config: &PipelineConfig,
    diagnostics: &mut Vec<Diagnostic>,
) -> IndexMap<String, IndexMap<String, Value>> {
    let declared_sources = declared_source_node_names(config);
    let declared = declared_scoped_vars(config, VarScope::Source);
    let mut out: IndexMap<String, IndexMap<String, Value>> = IndexMap::new();
    for (src_name, inner) in overrides {
        if !declared_sources.iter().any(|n| n == src_name) {
            diagnostics.push(Diagnostic::error(
                "E111",
                format!(
                    "channel {:?}: source {:?} not declared in pipeline (known: {})",
                    channel_name,
                    src_name,
                    declared_sources.join(", "),
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
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
/// (E107), default coercion. Push diagnostics on failure; return
/// `None` so the caller skips the entry.
fn validate_and_coerce(
    channel_name: &str,
    scope_label: &str,
    var_name: &str,
    decl: &ScopedVarDecl,
    declared_type: Option<ScopedVarType>,
    reserved_scope: Option<VarScope>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Value> {
    if let Some(scope) = reserved_scope
        && reserved_names_for(scope).contains(&var_name)
    {
        diagnostics.push(Diagnostic::error(
            "E110",
            format!(
                "channel {:?}: var ${}.{} shadows reserved system field",
                channel_name, scope_label, var_name,
            ),
            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
        ));
        return None;
    }

    if let Some(declared) = declared_type
        && declared != decl.var_type
    {
        diagnostics.push(Diagnostic::error(
            "E107",
            format!(
                "channel {:?}: var ${}.{} override type mismatch — declared {:?}, override declared {:?}",
                channel_name, scope_label, var_name, declared, decl.var_type,
            ),
            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
        ));
        return None;
    }

    let default = decl.default.as_ref()?;

    let where_label = format!("channel {channel_name:?} vars.{scope_label}");
    if let Err(e) = check_scoped_var_default(&where_label, var_name, decl.var_type, default) {
        diagnostics.push(Diagnostic::error(
            "E107",
            format!(
                "channel {:?}: var ${}.{} default does not match type {:?}: {e}",
                channel_name, scope_label, var_name, decl.var_type,
            ),
            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
        ));
        return None;
    }

    Some(coerce_scoped_var_default(decl.var_type, default))
}
