//! Composition-body window resolution and relaxed-CK retraction-flag passes.

use super::*;

use std::collections::BTreeSet;

use petgraph::graph::{DiGraph, NodeIndex};

use crate::config::{SortField, SourceConfig};

use cxl::ast::Statement;
use cxl::typecheck::pass::TypedProgram;

/// Resolve composition-body analytic-window IndexSpecs against the
/// fully-built parent DAG.
///
/// Body lowering (`bind_composition`) runs before the parent DAG's
/// NodeIndex space is allocated, so it cannot stamp `window_index`
/// onto body Transform nodes whose first non-pass-through ancestor
/// reaches the body's `input:` port — that port resolves to a
/// parent-DAG operator only after Stage 5 has built the parent's
/// `name_to_idx`. This pass closes that gap: per body, per Transform
/// carrying a captured `body_window_configs` entry, walk
/// `first_non_passthrough_ancestor` through the body graph, classify
/// the rooting (`Source` / `Node` / `ParentNode`), construct the
/// `RawIndexRequest` set, deduplicate it, and backfill `window_index`
/// onto the body Transform.
///
/// Cross-body recursion (composition-of-composition) is handled by
/// running the pass per-body; nested bodies' parent context is the
/// enclosing body's mini-DAG plus any ancestor port resolutions
/// already baked into the encoding scope's index list.
pub(crate) fn resolve_composition_body_windows(
    parent_dag: &ExecutionPlanDag,
    artifacts: &mut crate::plan::bind_schema::CompileArtifacts,
    diags: &mut Vec<clinker_core_types::Diagnostic>,
) {
    use crate::plan::index::{
        IndexSpec, PlanIndexRoot, RawIndexRequest, deduplicate_indices, find_index_for,
    };
    use clinker_core_types::span::Span as PlanSpan;
    use clinker_core_types::{Diagnostic, LabeledSpan};

    // Two-step ownership shuffle: (1) compute every body's
    // (idx_specs, window_index_assignments) without holding a mutable
    // borrow on `artifacts`, then (2) write the results back.
    // `artifacts.composition_bodies` holds `BoundBody` values keyed
    // by `CompositionBodyId`; the immutable borrow during compute
    // also covers parent_dag access.
    let body_ids: Vec<crate::plan::composition_body::CompositionBodyId> =
        artifacts.composition_bodies.keys().copied().collect();

    for body_id in body_ids {
        let Some(body) = artifacts.composition_bodies.get(&body_id) else {
            continue;
        };

        // Build a parent-DAG name → NodeIndex map once per body so
        // later port resolutions don't re-walk the parent graph for
        // every window. Top-level lookups walk parent_dag; nested-
        // body lookups would need the enclosing body's mini-DAG, but
        // for this pass the parent context is always the top-level
        // DAG — nested ParentNode rooting through multiple body
        // layers is an extension that the dispatcher's
        // `current_body_node_input_refs` plumbing already handles via
        // active_stack walks; the spec list emitted here points at the
        // most-immediate parent-DAG operator. Top-level pipelines
        // dominate the production geometry so this is the load-
        // bearing case.
        let mut parent_name_to_idx: std::collections::HashMap<&str, NodeIndex> =
            std::collections::HashMap::new();
        for idx in parent_dag.graph.node_indices() {
            parent_name_to_idx.insert(parent_dag.graph[idx].name(), idx);
        }

        // Find this body's call-site composition node in the parent
        // DAG. Multi-call-site signatures (the same .comp.yaml
        // referenced by N composition nodes) bind to N distinct
        // `CompositionBodyId`s — `composition_body_assignments` keys
        // by composition node name, not body file path, so each
        // body has exactly one composition node.
        let mut composition_idx: Option<NodeIndex> = None;
        for (comp_name, &assigned_id) in &artifacts.composition_body_assignments {
            if assigned_id == body_id
                && let Some(&idx) = parent_name_to_idx.get(comp_name.as_str())
            {
                composition_idx = Some(idx);
                break;
            }
        }
        let Some(composition_idx) = composition_idx else {
            // No call site — body is dead code. Skip.
            continue;
        };

        // Per-port → parent-DAG NodeIndex via the composition's
        // incoming port-tagged edges.
        let mut port_to_parent_idx: std::collections::HashMap<&str, NodeIndex> =
            std::collections::HashMap::new();
        use petgraph::visit::EdgeRef;
        for edge in parent_dag
            .graph
            .edges_directed(composition_idx, petgraph::Direction::Incoming)
        {
            if let Some(port_name) = edge.weight().port.as_deref() {
                port_to_parent_idx.insert(port_name, edge.source());
            }
        }

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
            // typed programs in `artifacts.typed` under their own `Body`
            // scope, so resolve by `(Body(body_id), name)` here.
            if let Some(typed) = artifacts.typed_get(
                crate::plan::bind_schema::NodeScope::Body(body_id),
                transform_name,
            ) {
                let analysis = cxl::analyzer::analyze_transform(transform_name, typed);
                for f in &analysis.accessed_fields {
                    arena_fields.insert(f.clone());
                }
            }
            let mut arena_fields_vec: Vec<String> = arena_fields.into_iter().collect();
            arena_fields_vec.sort();

            // Body-internal first non-pass-through ancestor.
            let pred_idx = match body
                .graph
                .neighbors_directed(transform_idx, petgraph::Direction::Incoming)
                .next()
            {
                Some(p) => p,
                None => continue,
            };
            let rooted_idx = first_non_passthrough_ancestor(&body.graph, pred_idx);
            let rooted_node = &body.graph[rooted_idx];

            // Classify the rooting:
            // - Body Source whose name matches a port → ParentNode.
            // - Body Source not a port (declared inside body) →
            //   Node{ body_source_idx, .. } anchored at the body's
            //   own Source NodeIndex (the body's Source dispatch arm
            //   populates the slot at its emit, the same way every
            //   top-level Source-anchored window now does).
            // - Other body operator → Node{ body_upstream, .. }.
            let root: PlanIndexRoot = match rooted_node {
                PlanNode::Source { name, .. } => {
                    if let Some(&parent_upstream) = port_to_parent_idx.get(name.as_str()) {
                        let anchor_schema = match parent_dag.graph[parent_upstream]
                            .stored_output_schema()
                            .cloned()
                        {
                            Some(s) => s,
                            None => {
                                diags.push(Diagnostic::error(
                                    "E003",
                                    format!(
                                        "composition body windowed transform {transform_name:?} \
                                         resolves through input port {name:?} to parent operator {:?} \
                                         which has no output schema",
                                        parent_dag.graph[parent_upstream].name()
                                    ),
                                    LabeledSpan::primary(PlanSpan::SYNTHETIC, String::new()),
                                ));
                                continue;
                            }
                        };
                        PlanIndexRoot::ParentNode {
                            upstream: parent_upstream,
                            anchor_schema,
                        }
                    } else {
                        // Body-declared Source — rare but legal. Anchor
                        // at the body Source's NodeIndex; its dispatch
                        // arm finalizes the arena at emit.
                        let Some(anchor_schema) = rooted_node.stored_output_schema().cloned()
                        else {
                            diags.push(Diagnostic::error(
                                "E003",
                                format!(
                                    "composition body windowed transform {transform_name:?} \
                                     rooted at body Source {name:?} which has no output schema"
                                ),
                                LabeledSpan::primary(PlanSpan::SYNTHETIC, String::new()),
                            ));
                            continue;
                        };
                        PlanIndexRoot::Node {
                            upstream: rooted_idx,
                            anchor_schema,
                        }
                    }
                }
                PlanNode::Merge { .. } => {
                    diags.push(Diagnostic::error(
                        "E150d",
                        format!(
                            "composition body windowed transform {transform_name:?} \
                             is rooted at a Merge node; Merge concatenates streams \
                             without a single producer identity, so a window cannot \
                             anchor to it"
                        ),
                        LabeledSpan::primary(PlanSpan::SYNTHETIC, String::new()),
                    ));
                    continue;
                }
                other => {
                    let Some(anchor_schema) = other.stored_output_schema().cloned() else {
                        continue;
                    };
                    // E150b — every arena field must be present in the
                    // upstream operator's output schema. The top-level
                    // lowering enforces this at `config/mod.rs`; body
                    // windows enforce it here so body-internal
                    // post-aggregate windows that reference a column
                    // the aggregate did not emit fail at compile rather
                    // than silently reading Null at runtime.
                    let mut e150b_fired = false;
                    for f in &arena_fields_vec {
                        if !anchor_schema.contains(f.as_str()) {
                            diags.push(Diagnostic::error(
                                "E150b",
                                format!(
                                    "composition body windowed transform {transform_name:?} \
                                     references field {f:?} that the upstream operator {:?} \
                                     does not emit; a node-rooted window can only see \
                                     columns produced by its rooted operator",
                                    other.name()
                                ),
                                LabeledSpan::primary(PlanSpan::SYNTHETIC, String::new()),
                            ));
                            e150b_fired = true;
                        }
                    }
                    if e150b_fired {
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

        // Backfill `window_index` onto each body Transform node. Re-
        // borrow `body` mutably now that we've finished consulting it
        // immutably. Only mutate `body.graph`, `body.body_indices_to_build`.
        let Some(body_mut) = artifacts.composition_bodies.get_mut(&body_id) else {
            continue;
        };
        for (req, transform_idx) in &raw_requests {
            let new_window_index =
                find_index_for(&body_indices, &req.root, &req.group_by, &req.sort_by);
            if let PlanNode::Transform {
                window_index,
                partition_lookup,
                ..
            } = &mut body_mut.graph[*transform_idx]
            {
                *window_index = new_window_index;
                // Body-internal partition lookup: cross-source
                // (`wc.source: <other>`) is not a body-level surface
                // today, so every body window resolves through the
                // same-source path. Stamping `SameSource` keeps
                // downstream consumers that branch on
                // `partition_lookup` uniform with the top-level
                // lowering arm in `lower_node_to_plan_node`.
                *partition_lookup = Some(PartitionLookupKind::SameSource);
            }
        }
        body_mut.body_indices_to_build = body_indices;
    }
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
