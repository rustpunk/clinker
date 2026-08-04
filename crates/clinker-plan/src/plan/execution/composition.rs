//! Composition-body window resolution and relaxed-CK retraction-flag passes.

use super::*;

use std::collections::{BTreeSet, HashMap, HashSet};

use petgraph::graph::{DiGraph, NodeIndex};

use crate::config::{SortField, SourceConfig};

use cxl::ast::Statement;
use cxl::typecheck::pass::TypedProgram;

/// Bind every reachable composition-body window to a scope-local runtime key.
///
/// The walk follows actual Composition call edges from the top-level DAG into
/// nested bodies. Each window roots in its own mini-DAG, including windows fed
/// by a synthetic input-port Source. No parent-DAG slot is encoded or inferred.
pub(crate) fn resolve_composition_body_windows(
    parent_dag: &ExecutionPlanDag,
    artifacts: &mut crate::plan::bind_schema::CompileArtifacts,
    diags: &mut Vec<clinker_core_types::Diagnostic>,
) {
    use crate::plan::index::{
        IndexSpec, PlanIndexRoot, RawIndexRequest, deduplicate_indices, find_index_for,
    };
    use crate::plan::{BodyWindowBinding, WindowRuntimeKey};

    let body_ids = reachable_body_order(parent_dag, artifacts, diags);
    for body_id in body_ids {
        let Some(body) = artifacts.composition_bodies.get(&body_id) else {
            continue;
        };

        // Per-Transform spec construction. Walk in declaration order
        // (HashMap iteration order is undefined but stable per
        // process; we collect names sorted for determinism into the
        // explain output downstream).
        let mut window_names: Vec<&str> = body
            .body_window_configs
            .keys()
            .map(|s| s.as_str())
            .collect();
        window_names.sort_unstable();

        let mut raw_requests: Vec<(RawIndexRequest, NodeIndex)> = Vec::new();

        for transform_name in &window_names {
            let Some(wc) = body.body_window_configs.get(*transform_name) else {
                continue;
            };
            let Some(&transform_idx) = body.name_to_idx.get(*transform_name) else {
                continue;
            };

            let mut arena_fields: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for gb in &wc.group_by {
                arena_fields.insert(gb.clone());
            }
            for sf in &wc.sort_by {
                arena_fields.insert(sf.field.clone());
            }
            // Pull every field a body Transform references through
            // window builtins or bare FieldRefs — e.g.
            // `$window.sum(amount)` adds `amount`, and `emit x = y` adds
            // `y`. The analyzer is the same one consumed at top-level
            // lowering (`config/mod.rs`); body Transforms register their
            // typed programs in `artifacts.typed` under their own
            // `PlanNodeId`, so resolve by the body node's id here (read off
            // the body mini-DAG at `transform_idx`).
            if let Some(typed) = artifacts.typed_get(body.graph[transform_idx].id()) {
                let analysis = cxl::analyzer::analyze_transform(transform_name, typed);
                for f in &analysis.accessed_fields {
                    arena_fields.insert(f.clone());
                }
            }
            let mut arena_fields_vec: Vec<String> = arena_fields.into_iter().collect();
            arena_fields_vec.sort();

            // Body-internal first non-pass-through ancestor.
            let predecessors: Vec<NodeIndex> = body
                .graph
                .neighbors_directed(transform_idx, petgraph::Direction::Incoming)
                .collect();
            let pred_idx = match predecessors.as_slice() {
                [only] => *only,
                [] => {
                    diags.push(body_window_diag(
                        "E160",
                        &body.graph[transform_idx],
                        format!(
                            "composition body windowed transform {transform_name:?} has no input root"
                        ),
                    ));
                    continue;
                }
                _ => {
                    diags.push(body_window_diag(
                        "E161",
                        &body.graph[transform_idx],
                        format!(
                            "composition body windowed transform {transform_name:?} has ambiguous input roots"
                        ),
                    ));
                    continue;
                }
            };
            let rooted_idx = match body_window_root(&body.graph, pred_idx) {
                Ok(idx) => idx,
                Err((code, detail)) => {
                    diags.push(body_window_diag(code, &body.graph[transform_idx], detail));
                    continue;
                }
            };
            let rooted_node = &body.graph[rooted_idx];

            if wc.on.is_some()
                || wc
                    .source
                    .as_deref()
                    .is_some_and(|source| source != rooted_node.name())
            {
                diags.push(body_window_diag(
                    "E165",
                    &body.graph[transform_idx],
                    format!(
                        "composition body windowed transform {transform_name:?} uses a cross-input/source lookup; body windows must read their owning input root"
                    ),
                ));
                continue;
            }

            let root: PlanIndexRoot = match rooted_node {
                PlanNode::Source { name, resolved, .. } => {
                    if resolved.is_none() && !body.input_port_rows.contains_key(name) {
                        diags.push(body_window_diag(
                            "E167",
                            &body.graph[transform_idx],
                            format!(
                                "composition body windowed transform {transform_name:?} resolves to undeclared input port {name:?}"
                            ),
                        ));
                        continue;
                    }
                    let Some(anchor_schema) = rooted_node.stored_output_schema().cloned() else {
                        diags.push(body_window_diag(
                            "E168",
                            &body.graph[transform_idx],
                            format!(
                                "composition body windowed transform {transform_name:?} has an input root with no output schema"
                            ),
                        ));
                        continue;
                    };
                    PlanIndexRoot::Node {
                        upstream: rooted_idx,
                        anchor_schema,
                    }
                }
                PlanNode::Merge { .. } => {
                    diags.push(body_window_diag(
                        "E166",
                        &body.graph[transform_idx],
                        format!(
                            "composition body windowed transform {transform_name:?} \
                             is rooted at a Merge node; Merge concatenates streams \
                             without a single producer identity, so a window cannot \
                             anchor to it"
                        ),
                    ));
                    continue;
                }
                other => {
                    let Some(anchor_schema) = other.stored_output_schema().cloned() else {
                        diags.push(body_window_diag(
                            "E168",
                            &body.graph[transform_idx],
                            format!(
                                "composition body windowed transform {transform_name:?} has an input root with no output schema"
                            ),
                        ));
                        continue;
                    };
                    let mut schema_error = false;
                    for f in &arena_fields_vec {
                        if !anchor_schema.contains(f.as_str()) {
                            diags.push(body_window_diag(
                                "E168",
                                &body.graph[transform_idx],
                                format!(
                                    "composition body windowed transform {transform_name:?} \
                                     references field {f:?} that the upstream operator {:?} \
                                     does not emit; a node-rooted window can only see \
                                     columns produced by its rooted operator",
                                    other.name()
                                ),
                            ));
                            schema_error = true;
                        }
                    }
                    if schema_error {
                        continue;
                    }
                    PlanIndexRoot::Node {
                        upstream: rooted_idx,
                        anchor_schema,
                    }
                }
            };

            // Body Sources do not declare top-level sort_order today;
            // node-rooted / parent-node-rooted arenas sort partitions
            // post-build at the upstream-arm exit anyway. Treat body
            // windows as `already_sorted = false` uniformly.
            let already_sorted = false;

            let req = RawIndexRequest {
                root,
                group_by: wc.group_by.clone(),
                sort_by: wc.sort_by.clone(),
                arena_fields: arena_fields_vec,
                already_sorted,
                // `transform_index` indexes into a per-body Vec; the
                // body has no top-level "transform list" alongside it,
                // so we record the body NodeIndex's underlying integer
                // for traceability. The dedup pass uses (root,
                // group_by, sort_by) regardless.
                transform_index: transform_idx.index(),
                requires_buffer_recompute: false,
            };
            raw_requests.push((req, transform_idx));
        }

        let request_only: Vec<RawIndexRequest> =
            raw_requests.iter().map(|(r, _)| r.clone()).collect();
        let body_indices: Vec<IndexSpec> = deduplicate_indices(request_only);

        let body_scope = body.body_scope;
        let binding_data: Vec<_> = raw_requests
            .iter()
            .filter_map(|(req, transform_idx)| {
                find_index_for(&body_indices, &req.root, &req.group_by, &req.sort_by).map(|index| {
                    let window = body.graph[*transform_idx].id();
                    let input_root = match req.root {
                        PlanIndexRoot::Node { upstream, .. } => body.graph[upstream].id(),
                    };
                    (
                        window,
                        BodyWindowBinding {
                            key: WindowRuntimeKey {
                                body_scope,
                                window,
                                input_root,
                            },
                            index,
                        },
                        *transform_idx,
                    )
                })
            })
            .collect();

        let Some(body_mut) = artifacts.composition_bodies.get_mut(&body_id) else {
            continue;
        };
        body_mut.window_bindings.clear();
        for (window, binding, transform_idx) in binding_data {
            if let PlanNode::Transform {
                window_index,
                partition_lookup,
                ..
            } = &mut body_mut.graph[transform_idx]
            {
                *window_index = Some(binding.index);
                *partition_lookup = Some(PartitionLookupKind::SameSource);
            }
            body_mut.window_bindings.insert(window, binding);
        }
        body_mut.body_indices_to_build = body_indices;
    }
}

fn body_window_diag(
    code: &'static str,
    node: &PlanNode,
    message: String,
) -> clinker_core_types::Diagnostic {
    clinker_core_types::Diagnostic::error(
        code,
        message,
        clinker_core_types::LabeledSpan::primary(node.span(), "window declared here"),
    )
}

fn body_window_root(
    graph: &DiGraph<PlanNode, PlanEdge>,
    start: NodeIndex,
) -> Result<NodeIndex, (&'static str, String)> {
    let mut current = start;
    let mut visited = HashSet::new();
    loop {
        if !visited.insert(current) {
            return Err((
                "E162",
                format!(
                    "composition body window input path cycles at {:?}",
                    graph[current].name()
                ),
            ));
        }
        if !matches!(
            graph[current],
            PlanNode::Sort { .. } | PlanNode::Route { .. }
        ) {
            return Ok(current);
        }
        let incoming: Vec<_> = graph
            .neighbors_directed(current, petgraph::Direction::Incoming)
            .collect();
        match incoming.as_slice() {
            [only] => current = *only,
            [] => {
                return Err((
                    "E160",
                    format!(
                        "composition body window input path stops at {:?} without a producer",
                        graph[current].name()
                    ),
                ));
            }
            _ => {
                return Err((
                    "E161",
                    format!(
                        "composition body window input path reaches ambiguous producer {:?}",
                        graph[current].name()
                    ),
                ));
            }
        }
    }
}

fn reachable_body_order(
    parent_dag: &ExecutionPlanDag,
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
    diags: &mut Vec<clinker_core_types::Diagnostic>,
) -> Vec<crate::plan::CompositionBodyId> {
    struct Traversal<'a> {
        owner_by_body:
            &'a mut HashMap<crate::plan::CompositionBodyId, Option<crate::plan::BodyScopeId>>,
        visiting: &'a mut HashSet<crate::plan::CompositionBodyId>,
        visited: &'a mut HashSet<crate::plan::CompositionBodyId>,
        order: &'a mut Vec<crate::plan::CompositionBodyId>,
        diags: &'a mut Vec<clinker_core_types::Diagnostic>,
    }

    fn visit(
        body_id: crate::plan::CompositionBodyId,
        owner: Option<crate::plan::BodyScopeId>,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        traversal: &mut Traversal<'_>,
    ) {
        if let Some(previous_owner) = traversal.owner_by_body.insert(body_id, owner)
            && previous_owner != owner
        {
            traversal.diags.push(clinker_core_types::Diagnostic::error(
                "E163",
                format!(
                    "composition body {:?} is referenced outside its owning call scope",
                    body_id
                ),
                clinker_core_types::LabeledSpan::primary(
                    clinker_core_types::span::Span::SYNTHETIC,
                    "body ownership is not unique",
                ),
            ));
            return;
        }
        if traversal.visited.contains(&body_id) {
            return;
        }
        if !traversal.visiting.insert(body_id) {
            traversal.diags.push(clinker_core_types::Diagnostic::error(
                "E162",
                format!("composition body call graph cycles through {:?}", body_id),
                clinker_core_types::LabeledSpan::primary(
                    clinker_core_types::span::Span::SYNTHETIC,
                    "cyclic body ownership",
                ),
            ));
            return;
        }
        let Some(body) = artifacts.composition_bodies.get(&body_id) else {
            traversal.diags.push(clinker_core_types::Diagnostic::error(
                "E163",
                format!("composition call references missing body {:?}", body_id),
                clinker_core_types::LabeledSpan::primary(
                    clinker_core_types::span::Span::SYNTHETIC,
                    "missing owned body",
                ),
            ));
            traversal.visiting.remove(&body_id);
            return;
        };
        traversal.order.push(body_id);
        for idx in &body.topo_order {
            if let PlanNode::Composition { body: child, .. } = body.graph[*idx] {
                visit(child, Some(body.body_scope), artifacts, traversal);
            }
        }
        traversal.visiting.remove(&body_id);
        traversal.visited.insert(body_id);
    }

    let mut owner_by_body = HashMap::new();
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    let mut order = Vec::new();
    {
        let mut traversal = Traversal {
            owner_by_body: &mut owner_by_body,
            visiting: &mut visiting,
            visited: &mut visited,
            order: &mut order,
            diags,
        };
        for idx in &parent_dag.topo_order {
            if let PlanNode::Composition { body, .. } = parent_dag.graph[*idx] {
                visit(body, None, artifacts, &mut traversal);
            }
        }
    }
    for body_id in artifacts.composition_bodies.keys() {
        if !visited.contains(body_id) {
            diags.push(clinker_core_types::Diagnostic::error(
                "E163",
                format!(
                    "composition body {:?} is outside the reachable owner tree",
                    body_id
                ),
                clinker_core_types::LabeledSpan::primary(
                    clinker_core_types::span::Span::SYNTHETIC,
                    "unowned composition body",
                ),
            ));
        }
    }
    order
}

/// On-disk byte seed for a file-backed Source's `predicted_peak_bytes`.
///
/// Resolves each matcher against `anchor` (the pipeline file's directory) so
/// the size is independent of the process CWD, and absolute paths resolve
/// as-is — matching the source-discovery resolver.
///
/// Coverage:
/// - **`path:`** (single file) — `Some(len)`.
/// - **`paths:`** (explicit list) — `Some(sum)` of every readable listed file.
///   The explicit list carries no glob/exclude/min-size discovery filters, so
///   summing the listed sizes is exact. An unreadable entry contributes
///   nothing; the sum still seeds the others.
/// - **`glob:` / `regex:`** (multi-file matchers) — `Some(sum)` of the matched
///   files' sizes, computed by running the same [`discover`] resolver the
///   staging and ingest paths use. Reusing that one resolver (its `exclude`
///   list, `min_size`/`max_size`, `modified_after`/`before`, `take`, and sort
///   filters) means the seed names exactly the bytes the run will read, with
///   no second implementation to drift. An empty match seeds `Some(0)`, which
///   the caller renders as `unknown` like any other zero seed.
///
/// Returns `None` — the "unknown" seed the caller writes as `0` — for an
/// absent matcher, a `path:` whose `std::fs::metadata` fails, an empty
/// `paths:` list, or a discovery failure on a `glob:`/`regex:` matcher
/// (invalid pattern, a no-match under `on_no_match: error`, or a walk I/O
/// error). A discovery failure is reported as unknown rather than `0` so a
/// broken matcher does not masquerade as a zero-byte input; the run's own
/// discovery surfaces the same error at startup.
pub(crate) fn source_seed_bytes(source: &SourceConfig, anchor: &std::path::Path) -> Option<u64> {
    let resolve = |p: &str| -> std::path::PathBuf {
        let p = std::path::Path::new(p);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            anchor.join(p)
        }
    };

    // An explicit `paths:` list has no discovery filters, so the sum of the
    // listed files' sizes is an exact seed. Sum the readable ones; an
    // unreadable entry contributes 0 rather than poisoning the whole estimate.
    if let Some(paths) = source.paths.as_ref() {
        if paths.is_empty() {
            return None;
        }
        let total = paths.iter().fold(0u64, |acc, p| {
            let len = std::fs::metadata(resolve(p)).map(|m| m.len()).unwrap_or(0);
            acc.saturating_add(len)
        });
        return Some(total);
    }

    // `glob` / `regex` fan out through the discovery resolver and its filters.
    // Run that one resolver and sum the matched files' already-stat'd sizes so
    // the estimate equals the bytes the run reads — never a second, divergent
    // re-implementation of the filter pipeline. A discovery error is unknown
    // (`None`), not a misleading `0`.
    if source.glob.is_some() || source.regex.is_some() {
        return match crate::config::discovery::discover(source, anchor) {
            Ok(outcome) => Some(
                outcome
                    .files()
                    .iter()
                    .fold(0u64, |acc, f| acc.saturating_add(f.size)),
            ),
            Err(_) => None,
        };
    }

    let path = source.path.as_deref()?;
    std::fs::metadata(resolve(path)).ok().map(|m| m.len())
}

/// Idempotency predicate for enforcer-sort insertion.
///
/// Returns true iff `declared` (the upstream source's actual ordering) is a
/// strict prefix of, or equal to, `required` viewed the other way around: the
/// required ordering must be a prefix of the declared ordering. Element-wise
/// equality is on `(field, order, null_order)`. Mirrors DataFusion's
/// `extract_common_sort_prefix` semantics.
///
/// An empty `required` is always satisfied. An empty `declared` only satisfies
/// an empty `required`.
pub fn source_ordering_satisfies(declared: &[SortField], required: &[SortField]) -> bool {
    if required.len() > declared.len() {
        return false;
    }
    declared
        .iter()
        .zip(required.iter())
        .all(|(d, r)| d.field == r.field && d.order == r.order && d.null_order == r.null_order)
}

/// Extract the set of record-field names a CXL transform writes.
///
/// Walks the `TypedProgram`'s top-level statements and collects the names of
/// every `emit name = ...` whose target is the record. `let` statements bind
/// locals only and are ignored; `filter`, `distinct`, `trace`, and bare
/// expression statements do not write to fields.
///
/// Consumed by `compute_node_properties` to populate the
/// `DestroyedByTransformWriteSet` provenance variant. The write set
/// lives directly on `PlanNode::Transform` so the property pass never
/// has to reach into executor-private types.
pub(crate) fn extract_write_set(typed: &TypedProgram) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    cxl::ast::for_each_field_emit(&typed.program.statements, &mut |name, _| {
        set.insert(name.to_string());
    });
    set
}

/// True when an aggregate's `group_by` omits at least one field of the
/// parent's visible CK set.
///
/// Selects between the strict-collateral two-phase commit (returns
/// `false`) and the relaxed lattice + five-phase retraction protocol
/// (returns `true`). Aggregates whose parent ck_set is empty always
/// return `false` — there is no CK to test against, so retraction is
/// not in play.
///
/// `parent_ck_set` is the lattice value computed at the aggregate's
/// upstream node (typed-stable across composition descent). `group_by`
/// is the aggregate's user-declared (or auto-extension-rewritten)
/// group-by list. Field-name comparison is strict equality; the
/// auto-extension pass appends `$ck.<field>` shadow columns whenever
/// the user already lists the corresponding bare field, so the
/// bare-name check below sees the post-extension shape and stays
/// stable across the rewrite.
pub fn group_by_omits_any_ck_field(group_by: &[String], parent_ck_set: &BTreeSet<String>) -> bool {
    parent_ck_set
        .iter()
        .any(|f| !group_by.iter().any(|g| g.as_str() == f.as_str()))
}

/// Walk the top-level DAG and re-stamp each aggregate's
/// `requires_lineage` / `requires_buffer_mode` flags from the lattice.
///
/// Lowering stamps the strict default; this pass flips the flags via
/// `set_retraction_flags(true)` for any aggregate whose `group_by`
/// does not cover its parent's `ck_set`. Runs after
/// `compute_node_properties`, so every aggregate's parent has a
/// populated lattice entry.
pub(crate) fn apply_retraction_flags(dag: &mut ExecutionPlanDag) {
    use std::sync::Arc;

    let plan: Vec<(petgraph::graph::NodeIndex, bool)> = dag
        .graph
        .node_indices()
        .filter_map(|idx| {
            let PlanNode::Aggregation { config, .. } = &dag.graph[idx] else {
                return None;
            };
            let parent_ck = dag
                .graph
                .neighbors_directed(idx, petgraph::Direction::Incoming)
                .next()
                .and_then(|p| dag.node_properties.get(&p))
                .map(|p| p.ck_set.clone())
                .unwrap_or_default();
            // Time-windowed aggregates stay on the strict-collateral
            // path: relaxed-CK retraction over multi-window emissions
            // is unsupported. The dispatch arm reads the same fact
            // (`config.time_window.is_some()`) to skip the relaxed
            // finalize path entirely.
            let is_relaxed = config.time_window.is_none()
                && group_by_omits_any_ck_field(&config.group_by, &parent_ck);
            Some((idx, is_relaxed))
        })
        .collect();

    for (idx, is_relaxed) in plan {
        if let PlanNode::Aggregation { compiled, .. } = &mut dag.graph[idx] {
            Arc::make_mut(compiled).set_retraction_flags(is_relaxed);
        }
    }
}

/// Body-graph variant. Body mini-DAGs don't carry a `node_properties`
/// side table, so the parent's CK set is derived inline by walking
/// the upstream node's `output_schema` for `$ck.<field>` columns.
pub(crate) fn apply_retraction_flags_in_body(body: &mut crate::plan::composition_body::BoundBody) {
    use clinker_record::FieldMetadata;
    use std::sync::Arc;

    let plan: Vec<(petgraph::graph::NodeIndex, bool)> = body
        .graph
        .node_indices()
        .filter_map(|idx| {
            let PlanNode::Aggregation { config, .. } = &body.graph[idx] else {
                return None;
            };
            let mut ck: BTreeSet<String> = BTreeSet::new();
            let mut cursor = idx;
            while let Some(upstream) = body
                .graph
                .neighbors_directed(cursor, petgraph::Direction::Incoming)
                .next()
            {
                if let Some(schema) = body.graph[upstream].stored_output_schema() {
                    for (i, col) in schema.columns().iter().enumerate() {
                        if matches!(
                            schema.field_metadata(i),
                            Some(FieldMetadata::SourceCorrelation { .. }),
                        ) && let Some(field) = col.strip_prefix("$ck.")
                        {
                            ck.insert(field.to_string());
                        }
                    }
                    break;
                }
                cursor = upstream;
            }
            let is_relaxed =
                config.time_window.is_none() && group_by_omits_any_ck_field(&config.group_by, &ck);
            Some((idx, is_relaxed))
        })
        .collect();

    for (idx, is_relaxed) in plan {
        if let PlanNode::Aggregation { compiled, .. } = &mut body.graph[idx] {
            Arc::make_mut(compiled).set_retraction_flags(is_relaxed);
        }
    }
}

/// Shared core for the buffer-recompute auto-flip walk over any
/// `(graph, indices_to_build)` pair.
///
/// `ck_at` returns the CK set visible at a given node — the top-level
/// dispatch reads `node_properties.ck_set`; the body dispatch derives it
/// inline by walking the nearest upstream `output_schema` for
/// `FieldMetadata::SourceCorrelation` columns.
///
/// The walk is the unified rule from
/// [`ExecutionPlanDag::derive_window_buffer_recompute_flags`]: when at
/// least one aggregate's `group_by` omits a parent-CK field (relaxed
/// retraction protocol fires), every windowed Transform whose
/// `partition_by` does not cover the visible CK set flips to
/// `requires_buffer_recompute = true`.
pub(crate) fn derive_window_buffer_recompute_flags_for_graph<F>(
    graph: &DiGraph<PlanNode, PlanEdge>,
    indices_to_build: &mut [crate::plan::index::IndexSpec],
    mut ck_at: F,
) where
    F: FnMut(NodeIndex) -> BTreeSet<String>,
{
    // Lattice-driven enabler: at least one aggregate whose parent's
    // `ck_set` is NOT a subset of `group_by`. Without one, the
    // retraction protocol does not fire and no window needs buffer
    // mode.
    let has_relaxed_aggregate = graph.node_indices().any(|idx| {
        let PlanNode::Aggregation { config, .. } = &graph[idx] else {
            return false;
        };
        let parent_ck = graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .next()
            .map(&mut ck_at)
            .unwrap_or_default();
        group_by_omits_any_ck_field(&config.group_by, &parent_ck)
    });
    if !has_relaxed_aggregate {
        return;
    }

    let mut to_flag: Vec<usize> = Vec::new();
    for idx in graph.node_indices() {
        if let PlanNode::Transform {
            window_index: Some(idx_num),
            ..
        } = &graph[idx]
        {
            let idx_num = *idx_num;
            let Some(spec) = indices_to_build.get(idx_num) else {
                continue;
            };
            let partition_set: BTreeSet<&str> = spec.group_by.iter().map(String::as_str).collect();
            let ck_set = ck_at(idx);
            let ck_outside_partition = ck_set.iter().any(|f| !partition_set.contains(f.as_str()));
            if ck_outside_partition {
                to_flag.push(idx_num);
            }
        }
    }
    for idx_num in to_flag {
        if let Some(spec) = indices_to_build.get_mut(idx_num) {
            spec.requires_buffer_recompute = true;
        }
    }
}

/// Body-graph variant of `derive_window_buffer_recompute_flags`.
///
/// Body mini-DAGs do not carry a `node_properties` side table, so the
/// CK set visible at any body node is derived inline by walking the
/// nearest upstream `output_schema` for `FieldMetadata::SourceCorrelation`
/// columns — the same shape `apply_retraction_flags_in_body` uses to
/// derive the relaxed-aggregate trigger. Composition-body windows
/// downstream of a body-internal relaxed-CK aggregate flip into
/// buffer-recompute mode the same way top-level windows do, so the
/// commit-phase recompute path can rerun the window over
/// `partition − retracted_rows`.
pub(crate) fn derive_window_buffer_recompute_flags_in_body(
    body: &mut crate::plan::composition_body::BoundBody,
) {
    use clinker_record::FieldMetadata;

    let ck_at = |start: NodeIndex| -> BTreeSet<String> {
        let mut ck: BTreeSet<String> = BTreeSet::new();
        let mut cursor = start;
        loop {
            if let Some(schema) = body.graph[cursor].stored_output_schema() {
                for (i, col) in schema.columns().iter().enumerate() {
                    if matches!(
                        schema.field_metadata(i),
                        Some(FieldMetadata::SourceCorrelation { .. }),
                    ) && let Some(field) = col.strip_prefix("$ck.")
                    {
                        ck.insert(field.to_string());
                    }
                }
                return ck;
            }
            match body
                .graph
                .neighbors_directed(cursor, petgraph::Direction::Incoming)
                .next()
            {
                Some(upstream) => cursor = upstream,
                None => return ck,
            }
        }
    };

    derive_window_buffer_recompute_flags_for_graph(
        &body.graph,
        &mut body.body_indices_to_build,
        ck_at,
    );
}

/// Detect whether a CXL transform contains any `distinct` statement.
///
/// Sibling of [`extract_write_set`] — sourced from the same `TypedProgram`
/// during plan compilation. Persisted as `has_distinct` on
/// [`PlanNode::Transform`] so the property pass can emit
/// [`OrderingProvenance::DestroyedByDistinct`] without reaching into
/// executor-private types.
pub(crate) fn extract_has_distinct(typed: &TypedProgram) -> bool {
    typed
        .program
        .statements
        .iter()
        .any(|s| matches!(s, Statement::Distinct { .. }))
}
