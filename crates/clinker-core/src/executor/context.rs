//! Pipeline-stable evaluation-context construction: the once-per-run
//! `StableEvalContext` and the static/source-scoped var maps it carries.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clinker_record::{PipelineCounters, Value};
use indexmap::IndexMap;

use crate::config::PipelineConfig;
use cxl::eval::{StableEvalContext, WallClock};

/// Build the pipeline-stable evaluation context.
///
/// Called ONCE per pipeline run at the top of `execute_dag_branching`. The
/// returned `StableEvalContext` is reused (via borrow) at every per-record
/// dispatch site, killing the prior `String::clone` + `IndexMap::clone`
/// per-record allocation profile. `pipeline_start_time` must be frozen at
/// pipeline start so `$pipeline.start_time` is deterministic within a run.
/// The `now` keyword uses `ctx.stable.clock.now()` (wall-clock) and is
/// intentionally non-deterministic.
pub(super) fn build_stable_eval_context(
    config: &PipelineConfig,
    pipeline_start_time: chrono::NaiveDateTime,
    execution_id: &str,
    batch_id: &str,
    pipeline_vars: &IndexMap<String, Value>,
    static_vars_overrides: &IndexMap<String, Value>,
) -> StableEvalContext {
    // Seed pipeline-scope vars with declared defaults from every
    // Transform's `declares:` entries; runtime injections (channel
    // overrides, test seeds) overlay on top.
    let mut seeded = crate::config::collect_pipeline_var_defaults(&config.nodes);
    for (k, v) in pipeline_vars {
        seeded.insert(k.clone(), v.clone());
    }
    StableEvalContext {
        clock: Box::new(WallClock),
        pipeline_start_time,
        pipeline_name: Arc::from(config.pipeline.name.as_str()),
        pipeline_execution_id: Arc::from(execution_id),
        pipeline_batch_id: Arc::from(batch_id),
        pipeline_counters: PipelineCounters::default(),
        pipeline_vars: Arc::new(std::sync::RwLock::new(seeded)),
        source_vars: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        source_input_arcs: Arc::new(compute_source_input_arcs(config)),
        static_vars: Arc::new(build_static_vars(config, static_vars_overrides)),
    }
}

/// Build the `$vars.*` runtime value map from the pipeline's `vars:`
/// block, with `overrides` (channel-supplied) layered on top. Channel
/// adds extend the map; channel overrides replace.
fn build_static_vars(
    config: &PipelineConfig,
    overrides: &IndexMap<String, Value>,
) -> IndexMap<String, Value> {
    let mut out = config
        .pipeline
        .vars
        .as_ref()
        .map(crate::config::convert_vars)
        .unwrap_or_default();
    for (k, v) in overrides {
        out.insert(k.clone(), v.clone());
    }
    out
}

/// Walk the YAML config to map every `Merge` / `Combine` named input to
/// the source-file `Arc<str>`s of the upstream `Source` node(s) it
/// transitively reads from. Used by `Expr::QualifiedSourceAccess` eval
/// to look up `source_vars` entries written by upstream source-scope
/// Transform writers (qualified post-merge `$source.<input>.<key>` reads).
///
/// For Merge, each entry in `inputs:` becomes its own input name (the
/// referenced node name itself, since Merge does not rename). For
/// Combine, the IndexMap key is the input name. Walk-back follows the
/// consumer-side `input:` field through Transform/Aggregate/Route nodes
/// until a Source is reached; nested Merges fan out, collecting every
/// reachable Source's path Arc.
fn compute_source_input_arcs(
    config: &PipelineConfig,
) -> std::collections::HashMap<String, Vec<Arc<str>>> {
    use crate::config::pipeline_node::PipelineNode;
    let mut by_name: HashMap<&str, &PipelineNode> = HashMap::new();
    for node in &config.nodes {
        by_name.insert(node.value.name(), &node.value);
    }
    let mut out: std::collections::HashMap<String, Vec<Arc<str>>> =
        std::collections::HashMap::new();
    for node in &config.nodes {
        match &node.value {
            PipelineNode::Combine { header, .. } => {
                for (input_name, upstream) in header.input.iter() {
                    let arcs = arcs_reachable_from(&by_name, upstream.value.name());
                    if !arcs.is_empty() {
                        out.entry(input_name.clone()).or_default().extend(arcs);
                    }
                }
            }
            PipelineNode::Merge { header, .. } => {
                for upstream in &header.inputs {
                    let upstream_name = upstream.value.name();
                    let arcs = arcs_reachable_from(&by_name, upstream_name);
                    if !arcs.is_empty() {
                        out.entry(upstream_name.to_string())
                            .or_default()
                            .extend(arcs);
                    }
                }
            }
            _ => {}
        }
    }
    for arcs in out.values_mut() {
        arcs.sort();
        arcs.dedup();
    }
    out
}

/// Collect every source-file `Arc<str>` reachable upstream from `start`
/// by walking the consumer-side `input:` chain back to its Source roots,
/// fanning out through nested Merge / Combine inputs.
fn arcs_reachable_from(
    by_name: &HashMap<&str, &crate::config::pipeline_node::PipelineNode>,
    start: &str,
) -> Vec<Arc<str>> {
    use crate::config::pipeline_node::PipelineNode;
    let mut out = Vec::new();
    let mut stack = vec![start.to_string()];
    let mut seen: HashSet<String> = HashSet::new();
    while let Some(name) = stack.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        let Some(node) = by_name.get(name.as_str()) else {
            continue;
        };
        match node {
            PipelineNode::Source { config: body, .. } => {
                let path = body.source.path_str();
                if !path.is_empty() {
                    out.push(Arc::from(path));
                }
            }
            PipelineNode::Merge { header, .. } => {
                for upstream in &header.inputs {
                    stack.push(upstream.value.name().to_string());
                }
            }
            PipelineNode::Combine { header, .. } => {
                for upstream in header.input.values() {
                    stack.push(upstream.value.name().to_string());
                }
            }
            other => {
                if let Some(input) = other.input_node_name() {
                    stack.push(input.to_string());
                }
            }
        }
    }
    out
}
