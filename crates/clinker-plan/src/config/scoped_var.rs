//! Scoped-variable declarations and the default-collection / registry helpers.

use super::*;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// One static-config variable's declared type and optional default value.
///
/// `default` must match `var_type` — enforced by [`validate_static_vars`].
/// Static-config vars are read via `$vars.<key>`; producer-written scoped
/// state (`$pipeline.*` / `$source.*` / `$record.*`) is declared on
/// Transforms via their `declares:` block instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedVarDecl {
    #[serde(rename = "type")]
    pub var_type: ScopedVarType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

/// Scoped-variable primitive type set.
///
/// Mirrors the CXL primitive types the typecheck pass uses to validate
/// `$pipeline.<key>` / `$source.<key>` / `$record.<key>` reads. `Date`
/// and `DateTime` accept ISO-8601 string defaults (parsed at the eval
/// boundary, not at YAML load).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScopedVarType {
    String,
    Int,
    Float,
    Bool,
    Date,
    DateTime,
}

impl From<ScopedVarType> for cxl::resolve::ScopedVarType {
    fn from(t: ScopedVarType) -> Self {
        match t {
            ScopedVarType::String => cxl::resolve::ScopedVarType::String,
            ScopedVarType::Int => cxl::resolve::ScopedVarType::Int,
            ScopedVarType::Float => cxl::resolve::ScopedVarType::Float,
            ScopedVarType::Bool => cxl::resolve::ScopedVarType::Bool,
            ScopedVarType::Date => cxl::resolve::ScopedVarType::Date,
            ScopedVarType::DateTime => cxl::resolve::ScopedVarType::DateTime,
        }
    }
}

/// Build the resolver-side scoped-vars registry from:
/// (a) the flat top-level `vars:` block — frozen `$vars.<key>` config,
///     channel-overridable;
/// (b) every Transform's `config.declares:` entries — producer-declared
///     scoped state addressable as `$pipeline.<key>` / `$source.<key>` /
///     `$record.<key>`. The writer-site declaration is the sole authority.
pub fn build_scoped_vars_registry(
    vars: Option<&IndexMap<String, ScopedVarDecl>>,
    nodes: &[crate::yaml::Spanned<crate::config::pipeline_node::PipelineNode>],
) -> cxl::resolve::ScopedVarsRegistry {
    use crate::config::pipeline_node::{PipelineNode, VarScope};
    let mut reg = cxl::resolve::ScopedVarsRegistry {
        pipeline: indexmap::IndexMap::new(),
        source: indexmap::IndexMap::new(),
        record: indexmap::IndexMap::new(),
        // Top-level pipelines never have hidden vars — those are
        // populated only when entering a composition body via
        // `build_body_scoped_vars` to support the E173 diagnostic.
        hidden_pipeline: indexmap::IndexMap::new(),
        hidden_source: indexmap::IndexMap::new(),
        hidden_record: indexmap::IndexMap::new(),
        static_vars: vars
            .map(|sv| {
                sv.iter()
                    .map(|(k, d)| (k.clone(), d.var_type.into()))
                    .collect()
            })
            .unwrap_or_default(),
        // `$config.*` is composition-body-only; a top-level pipeline
        // declares no config schema, so both tiers stay empty and the
        // resolver rejects any `$config.*` read here.
        config: indexmap::IndexMap::new(),
        config_fold: indexmap::IndexMap::new(),
    };
    for spanned in nodes {
        if let PipelineNode::Transform { config, .. } = &spanned.value {
            for entry in &config.declares {
                let target = match entry.scope {
                    VarScope::Pipeline => &mut reg.pipeline,
                    VarScope::Source => &mut reg.source,
                    VarScope::Record => &mut reg.record,
                };
                target.insert(entry.name.clone(), entry.var_type.into());
            }
        }
    }
    reg
}

/// Typecheck a serde-json default value against a declared scoped-var
/// type. Used at config-load time for pipeline `vars:` and Transform
/// `declares:` defaults, and at channel-bind time for channel var
/// overrides — same invariant in every caller.
pub fn check_scoped_var_default(
    where_label: &str,
    name: &str,
    var_type: ScopedVarType,
    default: &serde_json::Value,
) -> Result<(), ConfigError> {
    let mismatch = || {
        ConfigError::Validation(format!(
            "{where_label}.{name}: default value does not match declared type {var_type:?}",
        ))
    };
    match (var_type, default) {
        (_, serde_json::Value::Null) => Err(ConfigError::Validation(format!(
            "{where_label}.{name}: default value cannot be null",
        ))),
        (
            ScopedVarType::String | ScopedVarType::Date | ScopedVarType::DateTime,
            serde_json::Value::String(_),
        ) => Ok(()),
        (ScopedVarType::Int, serde_json::Value::Number(n)) if n.is_i64() => Ok(()),
        (ScopedVarType::Float, serde_json::Value::Number(_)) => Ok(()),
        (ScopedVarType::Bool, serde_json::Value::Bool(_)) => Ok(()),
        _ => Err(mismatch()),
    }
}

/// Convert the flat `vars:` block into the runtime `$vars.*` value map.
/// Entries without a `default` resolve to `Value::Null` at read time
/// (the eval arm does the unwrap_or). Channels override these values
/// once at pipeline start (Stage 7 wiring).
pub fn convert_vars(
    vars: &IndexMap<String, ScopedVarDecl>,
) -> IndexMap<String, clinker_record::Value> {
    vars.iter()
        .filter_map(|(name, decl)| {
            decl.default.as_ref().map(|default| {
                (
                    name.clone(),
                    coerce_scoped_var_default(decl.var_type, default),
                )
            })
        })
        .collect()
}

/// Walk every Transform's `declares:` entries and collect declared
/// defaults for variables whose `scope: pipeline`. Used at executor
/// init to seed `StableEvalContext.pipeline_vars` before the runtime
/// walk begins; reads of a declared `$pipeline.<key>` that fire
/// before the writer (or whose writer never fires for a given record)
/// resolve to the default rather than `Null`.
pub fn collect_pipeline_var_defaults(
    nodes: &[crate::yaml::Spanned<crate::config::pipeline_node::PipelineNode>],
) -> IndexMap<String, clinker_record::Value> {
    collect_scoped_var_defaults(nodes, pipeline_node::VarScope::Pipeline)
}

/// Walk every Transform's `declares:` entries and collect declared
/// defaults for variables whose scope matches `wanted`. Pipeline,
/// Source, and Record scopes share the same shape — pull them through
/// the same helper rather than duplicating the loop body. Source and
/// record defaults use this map as their pre-seed at executor init
/// when channel-supplied overrides are absent.
fn collect_scoped_var_defaults(
    nodes: &[crate::yaml::Spanned<crate::config::pipeline_node::PipelineNode>],
    wanted: pipeline_node::VarScope,
) -> IndexMap<String, clinker_record::Value> {
    use crate::config::pipeline_node::PipelineNode;
    let mut out = IndexMap::new();
    for spanned in nodes {
        let PipelineNode::Transform { config, .. } = &spanned.value else {
            continue;
        };
        for entry in &config.declares {
            if entry.scope != wanted {
                continue;
            }
            if let Some(default) = &entry.default {
                out.insert(
                    entry.name.clone(),
                    coerce_scoped_var_default(entry.var_type, default),
                );
            }
        }
    }
    out
}

/// Walk every Transform's `declares:` entries and collect declared
/// defaults for variables whose `scope: source`. Channels overlay
/// per-source-name overrides on top of this baseline.
pub fn collect_source_var_defaults(
    nodes: &[crate::yaml::Spanned<crate::config::pipeline_node::PipelineNode>],
) -> IndexMap<String, clinker_record::Value> {
    collect_scoped_var_defaults(nodes, pipeline_node::VarScope::Source)
}

/// Walk every Transform's `declares:` entries and collect declared
/// defaults for variables whose `scope: record`. Channels overlay
/// channel-wide defaults on top; the executor pre-seeds these into
/// each record's `record_vars` map at materialization.
pub fn collect_record_var_defaults(
    nodes: &[crate::yaml::Spanned<crate::config::pipeline_node::PipelineNode>],
) -> IndexMap<String, clinker_record::Value> {
    collect_scoped_var_defaults(nodes, pipeline_node::VarScope::Record)
}

/// Walk the config's declared scopes ({Pipeline, Source, Record}) and
/// reject duplicate names within each flat shared registry. Source-scope
/// uniqueness applies globally (the runtime keys source-scope state by
/// `Arc<str>` source-file but the declared-name namespace is shared).
///
/// A duplicate name implies ambiguity for downstream channel-overlay
/// resolution and for the runtime read namespace itself — flat shared
/// namespaces fail-fast (Beam, Flink, Kafka Streams, Dagster, Cargo,
/// Rust statics, post-fix dbt). Authors who want shared state declare
/// once and reference everywhere; clinker has no nested-scope construct
/// for `$pipeline.*`/`$source.*`/`$record.*` that would justify
/// shadowing semantics.
pub(crate) fn validate_unique_scoped_declarations(
    config: &PipelineConfig,
) -> Result<(), ConfigError> {
    use crate::config::pipeline_node::{PipelineNode, VarScope};
    for scope in [VarScope::Pipeline, VarScope::Source, VarScope::Record] {
        let mut first_seen: HashMap<&str, &str> = HashMap::new();
        for spanned in &config.nodes {
            let PipelineNode::Transform {
                header,
                config: body,
            } = &spanned.value
            else {
                continue;
            };
            for entry in &body.declares {
                if entry.scope != scope {
                    continue;
                }
                if let Some(prior) = first_seen.insert(entry.name.as_str(), header.name.as_str())
                    && prior != header.name.as_str()
                {
                    let scope_label = match scope {
                        VarScope::Pipeline => "pipeline",
                        VarScope::Source => "source",
                        VarScope::Record => "record",
                    };
                    return Err(ConfigError::Validation(format!(
                        "duplicate ${scope_label} declaration: '{name}' is declared in transforms '{prior}' and '{current}' — the {scope_label}-scope namespace is flat and shared, declare once and reference",
                        name = entry.name,
                        current = header.name,
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Coerce a serde-json default value into a typed `clinker_record::Value`
/// according to the declared scoped-var type. Caller MUST have run
/// [`check_scoped_var_default`] first; this function panics on any
/// (type, value) combo that validator would have rejected.
pub fn coerce_scoped_var_default(
    var_type: ScopedVarType,
    default: &serde_json::Value,
) -> clinker_record::Value {
    use clinker_record::Value;
    match (var_type, default) {
        (ScopedVarType::Bool, serde_json::Value::Bool(b)) => Value::Bool(*b),
        (ScopedVarType::Int, serde_json::Value::Number(n)) => {
            Value::Integer(n.as_i64().unwrap_or(0))
        }
        (ScopedVarType::Float, serde_json::Value::Number(n)) => {
            Value::Float(n.as_f64().unwrap_or(0.0))
        }
        (
            ScopedVarType::String | ScopedVarType::Date | ScopedVarType::DateTime,
            serde_json::Value::String(s),
        ) => Value::String(s.as_str().into()),
        _ => unreachable!("check_scoped_var_default should reject mismatched defaults"),
    }
}
