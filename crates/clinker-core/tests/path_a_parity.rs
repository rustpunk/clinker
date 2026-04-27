//! Gate tests — assert that `PipelineConfig::compile` (the canonical
//! compile path) produces a fully-enriched `ExecutionPlanDag`.
//!
//! These tests lock in the invariants of the canonical path: one
//! compile path, one enrichment pipeline, one DAG. They fire if someone
//! ever regresses the canonical path to a minimal shape (empty
//! `source_dag`, empty `indices_to_build`, empty `node_properties`,
//! hardcoded `ParallelismClass::Stateless`, `None`-returning Aggregate
//! arm, etc.).
//!
//! Fixtures are the existing `examples/pipelines/tests/baseline/`
//! corpus (already used by `pre_lift_baselines.rs`) plus one combine
//! fixture from `crates/clinker-core/tests/fixtures/combine/`.

use std::path::PathBuf;

use clinker_core::config::{CompileContext, PipelineConfig, parse_config};
use clinker_core::plan::execution::{
    ExecutionPlanDag, NodeExecutionReqs, ParallelismClass, PlanNode,
};

fn workspace_root() -> PathBuf {
    // Crate manifest dir is `<workspace>/crates/clinker-core`; pop twice.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn compile_fixture(relative_yaml_path: &str) -> clinker_core::plan::CompiledPlan {
    compile_fixture_with(relative_yaml_path, |_| {})
}

fn compile_fixture_with(
    relative_yaml_path: &str,
    tweak: impl FnOnce(&mut CompileContext),
) -> clinker_core::plan::CompiledPlan {
    let root = workspace_root();
    let full = root.join(relative_yaml_path);
    let text =
        std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("read {}: {e}", full.display()));
    let config: PipelineConfig =
        parse_config(&text).unwrap_or_else(|e| panic!("parse_config {}: {e:?}", full.display()));
    let pipeline_dir = full.parent().map(PathBuf::from).unwrap_or(root.clone());
    let mut ctx = CompileContext::with_pipeline_dir(root, pipeline_dir);
    tweak(&mut ctx);
    config
        .compile(&ctx)
        .unwrap_or_else(|diags| panic!("compile {}: {diags:?}", full.display()))
}

// ───────────────────── Aggregate lowering ─────────────────────

#[test]
fn test_path_a_aggregate_lowered_no_w100() {
    // aggregate_windowed.yaml has a user-declared Aggregate. A
    // previous minimal compile path returned `None + W100` here; the
    // enriched path lowers it to `PlanNode::Aggregation`.
    let plan = compile_fixture("examples/pipelines/tests/baseline/aggregate_windowed.yaml");
    let dag = plan.dag();
    let has_aggregation = dag
        .graph
        .node_weights()
        .any(|n| matches!(n, PlanNode::Aggregation { .. }));
    assert!(
        has_aggregation,
        "Path A must lower PipelineNode::Aggregate into PlanNode::Aggregation (no W100 stub)"
    );
}

// ──────────────────── Transform enrichment ─────────────────────

#[test]
fn test_path_a_transform_has_derived_fields() {
    // csv_transform_sink.yaml has a simple stateless Transform. A
    // previous minimal compile path hardcoded every Transform to
    // `ParallelismClass::Stateless` + `NodeExecutionReqs::Streaming`
    // regardless of analyzer output; now the fields are derived from
    // the analyzer. `Stateless` + `Streaming` is still correct for
    // this fixture, but the derivation path must be exercised.
    let plan = compile_fixture("examples/pipelines/tests/baseline/csv_transform_sink.yaml");
    let dag = plan.dag();
    let transform = dag
        .graph
        .node_weights()
        .find_map(|n| match n {
            PlanNode::Transform {
                parallelism_class,
                execution_reqs,
                ..
            } => Some((*parallelism_class, execution_reqs.clone())),
            _ => None,
        })
        .expect("transform present");
    assert!(
        matches!(transform.0, ParallelismClass::Stateless),
        "stateless CXL must derive ParallelismClass::Stateless, got {:?}",
        transform.0
    );
    assert!(
        matches!(transform.1, NodeExecutionReqs::Streaming),
        "no-window transform must derive NodeExecutionReqs::Streaming, got {:?}",
        transform.1
    );
}

#[test]
fn test_path_a_transform_write_set_non_empty_for_emitter() {
    // csv_transform_sink.yaml emits several fields. `write_set`
    // extracted from the TypedProgram must be populated.
    let plan = compile_fixture("examples/pipelines/tests/baseline/csv_transform_sink.yaml");
    let dag = plan.dag();
    let write_set_populated = dag.graph.node_weights().any(|n| match n {
        PlanNode::Transform { write_set, .. } => !write_set.is_empty(),
        _ => false,
    });
    assert!(
        write_set_populated,
        "at least one Transform must have a non-empty write_set after enrichment"
    );
}

// ───────────────────── Enrichment outputs ─────────────────────

#[test]
fn test_path_a_source_dag_populated() {
    let plan = compile_fixture("examples/pipelines/tests/baseline/csv_transform_sink.yaml");
    let dag = plan.dag();
    assert!(
        !dag.source_dag.is_empty(),
        "source_dag must be populated by build_source_dag; was empty"
    );
}

#[test]
fn test_path_a_output_projections_populated() {
    let plan = compile_fixture("examples/pipelines/tests/baseline/csv_transform_sink.yaml");
    let dag = plan.dag();
    assert!(
        !dag.output_projections.is_empty(),
        "output_projections must be derived from output_configs; was empty"
    );
}

#[test]
fn test_path_a_parallelism_profile_populated() {
    let plan = compile_fixture("examples/pipelines/tests/baseline/csv_transform_sink.yaml");
    let dag = plan.dag();
    assert!(
        !dag.parallelism.per_transform.is_empty(),
        "parallelism.per_transform must cover every Transform; was empty"
    );
}

#[test]
fn test_path_a_node_properties_populated_for_every_node() {
    // compute_node_properties walks the graph topologically and
    // populates an entry per node.
    let plan = compile_fixture("examples/pipelines/tests/baseline/csv_transform_sink.yaml");
    let dag = plan.dag();
    assert_eq!(
        dag.node_properties.len(),
        dag.graph.node_count(),
        "every DAG node must have a NodeProperties entry after compute_node_properties"
    );
}

// ───────────────────── User-declared Merge ─────────────────────

#[test]
fn test_path_a_preserves_user_declared_merge() {
    // Under the unified taxonomy a user-declared `- type: merge` with
    // `inputs: [a, b]` lowers to a single `PlanNode::Merge` with N
    // fan-in edges — distinct from the old synthetic-Merge-per-transform
    // shape that a prior compile path emitted.
    let yaml = r#"
pipeline:
  name: merge_parity

nodes:
  - type: source
    name: east
    config:
      name: east
      type: csv
      path: /tmp/east.csv
      schema:
        - { name: id, type: string }

  - type: source
    name: west
    config:
      name: west
      type: csv
      path: /tmp/west.csv
      schema:
        - { name: id, type: string }

  - type: merge
    name: combined
    inputs: [east, west]

  - type: transform
    name: passthrough
    input: combined
    config:
      cxl: |
        emit id = id

  - type: output
    name: out
    input: passthrough
    config:
      name: out
      type: csv
      path: /tmp/out.csv
"#;
    let mut ctx = CompileContext::default();
    ctx.allow_absolute_paths = true;
    let config: PipelineConfig =
        parse_config(yaml).unwrap_or_else(|e| panic!("parse_config: {e:?}"));
    let plan = config
        .compile(&ctx)
        .unwrap_or_else(|diags| panic!("compile: {diags:?}"));
    let dag = plan.dag();
    let merge_count = dag
        .graph
        .node_weights()
        .filter(|n| matches!(n, PlanNode::Merge { .. }))
        .count();
    assert_eq!(
        merge_count, 1,
        "exactly one user-declared Merge expected; got {merge_count}"
    );
    // Two incoming edges on the Merge node.
    let merge_idx = dag
        .graph
        .node_indices()
        .find(|&i| matches!(&dag.graph[i], PlanNode::Merge { .. }))
        .expect("merge index");
    let incoming = dag
        .graph
        .neighbors_directed(merge_idx, petgraph::Direction::Incoming)
        .count();
    assert_eq!(
        incoming, 2,
        "user-declared Merge must have 2 fan-in edges; got {incoming}"
    );
}

// ───────────────────── Combine strategy post-pass ─────────────

#[test]
fn test_path_a_combine_strategies_selected_on_enriched_dag() {
    // After select_combine_strategies runs, every PlanNode::Combine has
    // a populated `driving_input` (empty string = pre-pass placeholder).
    // The pass runs AFTER compute_node_properties, so this test also
    // serves as a lock against regressing the pass order — moving
    // select_combine_strategies above the enrichment pipeline would
    // leave it observing an un-enriched DAG.
    let plan = compile_fixture_with(
        "crates/clinker-core/tests/fixtures/combine/combine_empty_build.yaml",
        |c| c.allow_absolute_paths = true,
    );
    let dag = plan.dag();
    let combine = dag
        .graph
        .node_weights()
        .find_map(|n| match n {
            PlanNode::Combine { driving_input, .. } => Some(driving_input.clone()),
            _ => None,
        })
        .expect("combine node present");
    assert!(
        !combine.is_empty(),
        "select_combine_strategies must stamp driving_input; was empty (pre-pass placeholder)"
    );
    // node_properties must be populated — asserts enrichment ran
    // before the combine post-pass, matching D4.
    assert_eq!(
        dag.node_properties.len(),
        dag.graph.node_count(),
        "combine post-pass must run after compute_node_properties"
    );
}

// ───────────────────── Canonical DAG shape snapshots ─────────────────────

/// Canonical textual rendering of an `ExecutionPlanDag`: one line per
/// node in topological order, each line `[type] name <- pred1, pred2`.
/// Predecessors are sorted alphabetically so the output is stable across
/// runs regardless of petgraph's internal iteration order. This is a
/// lossy projection — it captures the topology (node set + edges) but
/// deliberately omits enrichment-derived fields (parallelism, indices,
/// node_properties) that other tests in this file already assert.
fn canonical_dag_shape(dag: &ExecutionPlanDag) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for &idx in &dag.topo_order {
        let node = &dag.graph[idx];
        let mut preds: Vec<&str> = dag
            .graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .map(|p| dag.graph[p].name())
            .collect();
        preds.sort();
        if preds.is_empty() {
            writeln!(out, "[{}] {}", node.type_tag(), node.name()).unwrap();
        } else {
            writeln!(
                out,
                "[{}] {} <- {}",
                node.type_tag(),
                node.name(),
                preds.join(", ")
            )
            .unwrap();
        }
    }
    out
}

/// Canonical DAG shape for a representative corpus of pipelines.
///
/// Existing pre-lift baselines capture the `--explain` projection of
/// each pipeline, which elides the raw topology behind a human-readable
/// formatter. This snapshot captures the topology itself — node set,
/// edges, topological order — so structural changes to the compile
/// pipeline fail this test even when the explain formatter masks them.
#[test]
fn test_canonical_dag_shape_baselines() {
    let cases = &[
        (
            "csv_transform_sink",
            "examples/pipelines/tests/baseline/csv_transform_sink.yaml",
        ),
        (
            "route_fanout",
            "examples/pipelines/tests/baseline/route_fanout.yaml",
        ),
        (
            "aggregate_windowed",
            "examples/pipelines/tests/baseline/aggregate_windowed.yaml",
        ),
        (
            "combine_two_input_equi",
            "crates/clinker-core/tests/fixtures/combine/two_input_equi.yaml",
        ),
        (
            "combine_two_input_mixed",
            "crates/clinker-core/tests/fixtures/combine/two_input_mixed.yaml",
        ),
    ];
    for (name, path) in cases {
        let plan = compile_fixture_with(path, |c| c.allow_absolute_paths = true);
        let shape = canonical_dag_shape(plan.dag());
        insta::assert_snapshot!(format!("canonical_dag_{name}"), shape);
    }
}

// ───────────────────── Multi-consumer Merge fan-out ─────────────────────

/// A user-declared Merge with multiple downstream consumers compiles to
/// a single `PlanNode::Merge` with one outgoing edge per consumer — not
/// a per-consumer synthetic Merge fan-out. This guards against a
/// prior compile path that emitted `merge_<transform>` synthetic Merge
/// nodes per downstream Transform. Under the unified taxonomy the
/// user's Merge is first-class; downstream consumers reference it by
/// name.
///
/// Scope: compile-only. The executor's fan-out-by-clone is exercised
/// end-to-end by other tests that share a Transform predecessor across
/// multiple Outputs; a shared-Merge runtime assertion would require
/// CSV writers to derive their output schema from the Merge's union
/// inputs, which is not currently handled without an intervening
/// projection. Covering that path is a runtime-correctness task
/// independent of this structural gate.
#[test]
fn test_multi_consumer_merge_compiles_to_single_node() {
    let yaml = r#"
pipeline:
  name: merge_fanout
nodes:
  - type: source
    name: east
    config:
      name: east
      type: csv
      path: east.csv
      schema:
        - { name: id, type: string }
  - type: source
    name: west
    config:
      name: west
      type: csv
      path: west.csv
      schema:
        - { name: id, type: string }
  - type: merge
    name: combined
    inputs: [east, west]
  - type: transform
    name: passthrough_a
    input: combined
    config:
      cxl: |
        emit id = id
  - type: transform
    name: passthrough_b
    input: combined
    config:
      cxl: |
        emit id = id
  - type: output
    name: out_a
    input: passthrough_a
    config:
      name: out_a
      type: csv
      path: out_a.csv
  - type: output
    name: out_b
    input: passthrough_b
    config:
      name: out_b
      type: csv
      path: out_b.csv
"#;
    let config: PipelineConfig =
        parse_config(yaml).unwrap_or_else(|e| panic!("parse_config: {e:?}"));
    let mut ctx = CompileContext::default();
    ctx.allow_absolute_paths = true;
    let plan = config
        .compile(&ctx)
        .unwrap_or_else(|diags| panic!("compile merge_fanout: {diags:?}"));
    let dag = plan.dag();

    // Exactly one Merge in the compiled DAG (no per-consumer synthesis).
    let merge_count = dag
        .graph
        .node_weights()
        .filter(|n| matches!(n, PlanNode::Merge { .. }))
        .count();
    assert_eq!(
        merge_count, 1,
        "expected exactly one Merge node (shared between both consumers); got {merge_count}"
    );

    // The Merge fans out to both consumers via two outgoing edges.
    let merge_idx = dag
        .graph
        .node_indices()
        .find(|&i| matches!(&dag.graph[i], PlanNode::Merge { .. }))
        .expect("merge node present");
    let outgoing: Vec<&str> = dag
        .graph
        .neighbors_directed(merge_idx, petgraph::Direction::Outgoing)
        .map(|c| dag.graph[c].name())
        .collect();
    assert_eq!(
        outgoing.len(),
        2,
        "Merge must have 2 outgoing edges (one per consumer); got {outgoing:?}"
    );
    assert!(
        outgoing.contains(&"passthrough_a"),
        "Merge outgoing must include passthrough_a; got {outgoing:?}"
    );
    assert!(
        outgoing.contains(&"passthrough_b"),
        "Merge outgoing must include passthrough_b; got {outgoing:?}"
    );
}
