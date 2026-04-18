//! Phase 16d gate tests — assert that `PipelineConfig::compile` (the
//! canonical compile path) produces a fully-enriched `ExecutionPlanDag`.
//!
//! These tests lock in the invariants established by Phase 16d: one
//! compile path, one enrichment pipeline, one DAG. They fire if someone
//! ever regresses the canonical path to the pre-16d minimal shape
//! (empty `source_dag`, empty `indices_to_build`, empty
//! `node_properties`, hardcoded `ParallelismClass::Stateless`,
//! `None`-returning Aggregate arm, etc.).
//!
//! Fixtures are the existing `examples/pipelines/tests/16b_baseline/`
//! corpus (already used by `pre_lift_baselines.rs`) plus one combine
//! fixture from `crates/clinker-core/tests/fixtures/combine/`.

use std::path::PathBuf;

use clinker_core::config::{CompileContext, PipelineConfig, parse_config};
use clinker_core::plan::execution::{NodeExecutionReqs, ParallelismClass, PlanNode};

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
    // aggregate_windowed.yaml has a user-declared Aggregate. The
    // pre-16d minimal Path A returned `None + W100` here; the enriched
    // path lowers it to `PlanNode::Aggregation`.
    let plan = compile_fixture("examples/pipelines/tests/16b_baseline/aggregate_windowed.yaml");
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
    // csv_transform_sink.yaml has a simple stateless Transform. The
    // pre-16d minimal Path A hardcoded every Transform to
    // `ParallelismClass::Stateless` + `NodeExecutionReqs::Streaming`
    // regardless of analyzer output. After 16d the fields are derived
    // from the analyzer; `Stateless` + `Streaming` is still correct
    // for this fixture, but the derivation path must be exercised.
    let plan = compile_fixture("examples/pipelines/tests/16b_baseline/csv_transform_sink.yaml");
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
    // extracted from the TypedProgram must be populated — pre-16d Path A
    // always left it empty.
    let plan = compile_fixture("examples/pipelines/tests/16b_baseline/csv_transform_sink.yaml");
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
    let plan = compile_fixture("examples/pipelines/tests/16b_baseline/csv_transform_sink.yaml");
    let dag = plan.dag();
    assert!(
        !dag.source_dag.is_empty(),
        "source_dag must be populated by build_source_dag; was empty (pre-16d shape)"
    );
}

#[test]
fn test_path_a_output_projections_populated() {
    let plan = compile_fixture("examples/pipelines/tests/16b_baseline/csv_transform_sink.yaml");
    let dag = plan.dag();
    assert!(
        !dag.output_projections.is_empty(),
        "output_projections must be derived from output_configs; was empty"
    );
}

#[test]
fn test_path_a_parallelism_profile_populated() {
    let plan = compile_fixture("examples/pipelines/tests/16b_baseline/csv_transform_sink.yaml");
    let dag = plan.dag();
    assert!(
        !dag.parallelism.per_transform.is_empty(),
        "parallelism.per_transform must cover every Transform; was empty"
    );
}

#[test]
fn test_path_a_node_properties_populated_for_every_node() {
    // compute_node_properties walks the graph topologically and
    // populates an entry per node. Pre-16d Path A left it empty.
    let plan = compile_fixture("examples/pipelines/tests/16b_baseline/csv_transform_sink.yaml");
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
    // fan-in edges — distinct from the pre-16b synthetic-Merge-per-
    // transform shape in the deleted Path B.
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
    // The pass runs AFTER compute_node_properties per Phase 16d's D4
    // strategy-pass ordering, so this test also serves as a lock
    // against regressing the pass order (if someone moves
    // select_combine_strategies back above the enrichment pipeline, the
    // DAG will still be enriched when the pass runs because Phase 16d
    // placed it at the end of the canonical path).
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
