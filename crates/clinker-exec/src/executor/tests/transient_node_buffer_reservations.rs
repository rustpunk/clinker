//! End-to-end accounting checks for transient node-buffer clones.
//!
//! These use the crate-private arbitrator-injection entry point so the
//! reservation gate and registry cleanup can be observed without relying on
//! process RSS. A fixed consumer near a 100 GiB hard limit makes clone
//! rejection deterministic: the producer slot fits, while one additional
//! clone exceeds the limit by exactly one byte.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

const HARD_LIMIT: u64 = 100 * 1024 * 1024 * 1024;

struct PinnedUsage(u64);

impl crate::pipeline::memory::MemoryConsumer for PinnedUsage {
    fn current_usage(&self) -> u64 {
        self.0
    }

    fn spill_priority(&self) -> i32 {
        i32::MAX
    }

    fn try_spill(
        &self,
        target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        Err(crate::pipeline::memory::ConsumerSpillError::BelowTarget {
            target: target_bytes,
            freed: 0,
        })
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

fn quiet_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        0.80,
        0.70,
        Box::new(crate::pipeline::memory::NoOpPolicy),
    ))
}

fn clone_rejection_arbitrator(
    row_bytes: u64,
) -> (
    Arc<crate::pipeline::memory::MemoryArbitrator>,
    crate::pipeline::memory::ConsumerId,
    u64,
) {
    let arbitrator = quiet_arbitrator();
    let pinned_bytes = HARD_LIMIT - 2 * row_bytes + 1;
    let id = arbitrator.register_consumer(Arc::new(PinnedUsage(pinned_bytes)));
    (arbitrator, id, pinned_bytes)
}

fn canonicalization_overlap_rejection_arbitrator(
    row_bytes: u64,
) -> (
    Arc<crate::pipeline::memory::MemoryArbitrator>,
    crate::pipeline::memory::ConsumerId,
    u64,
) {
    let arbitrator = quiet_arbitrator();
    let pinned_bytes = HARD_LIMIT - 2 * row_bytes + 1;
    let id = arbitrator.register_consumer(Arc::new(PinnedUsage(pinned_bytes)));
    (arbitrator, id, pinned_bytes)
}

fn row_bytes_for_node(yaml: &str, node_name: &str) -> u64 {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let compiled = config
        .compile(&fixture_compile_context())
        .expect("compile pipeline");
    let dag = compiled.dag();
    let idx = dag
        .graph
        .node_indices()
        .find(|idx| dag.graph[*idx].name() == node_name)
        .expect("named node exists");
    crate::executor::node_buffer::record_byte_cost(
        dag.graph[idx].output_schema_in(dag).column_count(),
    )
}

fn fixture_compile_context() -> clinker_plan::config::CompileContext {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures");
    clinker_plan::config::CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"))
}

fn readers(entries: &[(&str, &str)]) -> crate::executor::SourceReaders {
    entries
        .iter()
        .map(|(name, csv)| {
            (
                (*name).to_string(),
                crate::executor::single_file_reader(
                    format!("{name}.csv"),
                    Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
                ),
            )
        })
        .collect()
}

fn writers(
    names: &[&str],
) -> (
    HashMap<String, Box<dyn std::io::Write + Send>>,
    HashMap<String, SharedBuffer>,
) {
    let buffers: HashMap<_, _> = names
        .iter()
        .map(|name| ((*name).to_string(), SharedBuffer::new()))
        .collect();
    let writers = buffers
        .iter()
        .map(|(name, buffer)| {
            (
                name.clone(),
                Box::new(buffer.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    (writers, buffers)
}

fn run(
    yaml: &str,
    source_csv: &[(&str, &str)],
    output_names: &[&str],
    arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>,
) -> (
    Result<ExecutionReport, PipelineError>,
    HashMap<String, SharedBuffer>,
) {
    run_with_params(
        yaml,
        source_csv,
        output_names,
        arbitrator,
        &PipelineRunParams {
            execution_id: "transient-node-buffer-reservation".to_string(),
            batch_id: "batch-0".to_string(),
            ..Default::default()
        },
    )
}

fn run_with_params(
    yaml: &str,
    source_csv: &[(&str, &str)],
    output_names: &[&str],
    arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>,
    params: &PipelineRunParams,
) -> (
    Result<ExecutionReport, PipelineError>,
    HashMap<String, SharedBuffer>,
) {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let (writers, buffers) = writers(output_names);
    let result = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers(source_csv),
        writers.into(),
        params,
        fixture_compile_context(),
        arbitrator,
    );
    (result, buffers)
}

const COMPOSITION_PASSTHROUGH: &str = r#"
pipeline:
  name: transient_clone_composition_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema: [ { name: id, type: string } ]
  - type: output
    name: sibling
    input: src
    config: { name: sibling, type: csv, path: sibling.csv }
  - type: composition
    name: passthrough_call
    input: src
    use: ../compositions/passthrough_check.comp.yaml
    inputs:
      data: src
  - type: output
    name: out
    input: passthrough_call
    config: { name: out, type: csv, path: out.csv }
"#;

#[test]
fn composition_clone_rejects_before_allocation_and_restores_baseline() {
    let row_bytes = row_bytes_for_node(COMPOSITION_PASSTHROUGH, "src");
    let (arbitrator, baseline_id, baseline_usage) = clone_rejection_arbitrator(row_bytes);
    let (result, _) = run(
        COMPOSITION_PASSTHROUGH,
        &[("src", "id\n123\n")],
        &["out", "sibling"],
        Arc::clone(&arbitrator),
    );

    match result.expect_err("the composition input clone must cross the hard limit") {
        PipelineError::MemoryBudgetExceeded {
            node,
            used,
            limit,
            source,
            ..
        } => {
            assert_eq!(node, "passthrough_call");
            assert_eq!(used, HARD_LIMIT + 1);
            assert_eq!(limit, HARD_LIMIT);
            assert_eq!(source, clinker_plan::BudgetCategory::NodeBuffer);
        }
        other => panic!("expected bare composition-site E310 NodeBuffer; got {other:?}"),
    }
    assert_eq!(arbitrator.consumer_count(), 1);
    assert_eq!(arbitrator.sum_consumer_usage(), baseline_usage);
    arbitrator.unregister_consumer(baseline_id);
}

const SHARED_PREDECESSOR: &str = r#"
pipeline:
  name: transient_clone_shared_predecessor_gate
nodes:
  - type: source
    name: a
    config: { name: a, type: csv, path: a.csv, schema: [ { name: k, type: string }, { name: v, type: int } ] }
  - type: source
    name: b
    config: { name: b, type: csv, path: b.csv, schema: [ { name: k, type: string }, { name: v, type: int } ] }
  - type: source
    name: c
    config: { name: c, type: csv, path: c.csv, schema: [ { name: k, type: string }, { name: v, type: int } ] }
  - type: transform
    name: shared
    input: a
    config:
      cxl: |
        emit k = k
        emit v = v
  - type: merge
    name: m1
    inputs: [shared, b]
  - type: merge
    name: m2
    inputs: [shared, c]
  - type: output
    name: out1
    input: m1
    config: { name: out1, type: csv, path: out1.csv }
  - type: output
    name: out2
    input: m2
    config: { name: out2, type: csv, path: out2.csv }
"#;

#[test]
fn shared_transform_clone_rejects_before_allocation_and_restores_baseline() {
    let row_bytes = row_bytes_for_node(SHARED_PREDECESSOR, "shared");
    let (arbitrator, baseline_id, baseline_usage) = clone_rejection_arbitrator(row_bytes);
    let (result, _) = run(
        SHARED_PREDECESSOR,
        &[("a", "k,v\nx,15\n"), ("b", "k,v\n"), ("c", "k,v\n")],
        &["out1", "out2"],
        Arc::clone(&arbitrator),
    );

    match result.expect_err("the first shared Transform clone must cross the hard limit") {
        PipelineError::MemoryBudgetExceeded {
            node,
            used,
            limit,
            source,
            ..
        } => {
            assert!(node == "m1" || node == "m2", "unexpected clone site {node}");
            assert_eq!(used, HARD_LIMIT + 1);
            assert_eq!(limit, HARD_LIMIT);
            assert_eq!(source, clinker_plan::BudgetCategory::NodeBuffer);
        }
        other => panic!("expected shared-Transform E310 NodeBuffer; got {other:?}"),
    }
    assert_eq!(arbitrator.consumer_count(), 1);
    assert_eq!(arbitrator.sum_consumer_usage(), baseline_usage);
    arbitrator.unregister_consumer(baseline_id);
}

const SHARED_OUTPUT_PREDECESSOR: &str = r#"
pipeline:
  name: transient_clone_shared_output_predecessor_gate
nodes:
  - type: source
    name: src
    config: { name: src, type: csv, path: src.csv, schema: [ { name: id, type: string } ] }
  - type: transform
    name: prepared
    input: src
    config:
      cxl: |
        emit marker = "ready"
  - type: output
    name: alpha
    input: prepared
    config: { name: alpha, type: csv, path: alpha.csv }
  - type: output
    name: beta
    input: prepared
    config: { name: beta, type: csv, path: beta.csv }
"#;

#[test]
fn shared_output_clone_rejects_before_allocation_and_restores_baseline() {
    let row_bytes = row_bytes_for_node(SHARED_OUTPUT_PREDECESSOR, "prepared");
    let (arbitrator, baseline_id, baseline_usage) = clone_rejection_arbitrator(row_bytes);
    let (result, _) = run(
        SHARED_OUTPUT_PREDECESSOR,
        &[("src", "id\n123\n")],
        &["alpha", "beta"],
        Arc::clone(&arbitrator),
    );

    match result.expect_err("the first shared Output clone must cross the hard limit") {
        PipelineError::MemoryBudgetExceeded {
            node,
            used,
            limit,
            source,
            ..
        } => {
            assert!(
                node == "alpha" || node == "beta",
                "unexpected clone site {node}"
            );
            assert_eq!(used, HARD_LIMIT + 1);
            assert_eq!(limit, HARD_LIMIT);
            assert_eq!(source, clinker_plan::BudgetCategory::NodeBuffer);
        }
        other => panic!("expected shared-Output E310 NodeBuffer; got {other:?}"),
    }
    assert_eq!(arbitrator.consumer_count(), 1);
    assert_eq!(arbitrator.sum_consumer_usage(), baseline_usage);
    arbitrator.unregister_consumer(baseline_id);
}

const DIRECT_SOURCE_COMPOSITION: &str = r#"
pipeline:
  name: transient_clone_composition_lifecycle
nodes:
  - type: source
    name: src
    config: { name: src, type: csv, path: src.csv, schema: [ { name: a, type: int } ] }
  - type: composition
    name: doubled_call
    input: src
    use: ../compositions/exec_transform_check.comp.yaml
    inputs:
      inp: src
  - type: output
    name: composition_out
    input: doubled_call
    config: { name: composition_out, type: csv, path: composition.csv, include_unmapped: true }
"#;

#[test]
fn direct_source_composition_transfer_restores_colliding_parent_registration() {
    let config =
        clinker_plan::config::parse_config(DIRECT_SOURCE_COMPOSITION).expect("parse pipeline YAML");
    let compiled = config
        .compile(&fixture_compile_context())
        .expect("compile pipeline");
    let parent_source_idx = compiled
        .dag()
        .graph
        .node_indices()
        .find(|idx| compiled.dag().graph[*idx].name() == "src")
        .expect("parent Source exists");
    let body_id = compiled
        .dag()
        .graph
        .node_indices()
        .find_map(|idx| match &compiled.dag().graph[idx] {
            clinker_plan::plan::execution::PlanNode::Composition { name, body, .. }
                if name == "doubled_call" =>
            {
                Some(*body)
            }
            _ => None,
        })
        .expect("composition body id exists");
    let body = compiled
        .composition_bodies()
        .get(&body_id)
        .expect("bound composition body exists");
    let body_port_source_idx = *body
        .port_name_to_node_idx
        .get("inp")
        .expect("body input port Source exists");
    assert!(matches!(
        body.graph[body_port_source_idx],
        clinker_plan::plan::execution::PlanNode::Source { .. }
    ));
    assert_eq!(
        parent_source_idx, body_port_source_idx,
        "this regression must exercise colliding parent/body NodeIndex spaces"
    );

    let arbitrator = quiet_arbitrator();
    let (result, outputs) = run(
        DIRECT_SOURCE_COMPOSITION,
        &[("src", "a\n5\n")],
        &["composition_out"],
        Arc::clone(&arbitrator),
    );
    result.expect("direct Source-to-Composition pipeline must run");

    assert!(outputs["composition_out"].as_string().contains("10"));
    assert_eq!(
        arbitrator.consumer_count(),
        0,
        "body transfer and parent restoration must leave no registration"
    );
    assert_eq!(arbitrator.sum_consumer_usage(), 0);
}

const DIRECT_SOURCE_FUSION_NAME_COLLISION: &str = r#"
pipeline:
  name: direct_source_fusion_name_collision
nodes:
  - type: source
    name: body_feed
    config: { name: body_feed, type: csv, path: body-feed.csv, schema: [ { name: id, type: int } ] }
  - type: composition
    name: body_call
    input: body_feed
    use: ../compositions/source_boundary_collision.comp.yaml
    inputs:
      collision: body_feed
  - type: output
    name: body_out
    input: body_call
    config: { name: body_out, type: csv, path: body-out.csv, include_unmapped: false }
  - type: source
    name: collision
    config: { name: collision, type: csv, path: collision.csv, schema: [ { name: id, type: int } ] }
  - type: transform
    name: top_transform
    input: collision
    config:
      declares:
        - { name: boundary, scope: record, type: string }
      cxl: |
        emit id = id
        emit origin = "top-transform"
        emit $record.boundary = $record.boundary
  - type: output
    name: top_out
    input: top_transform
    config: { name: top_out, type: csv, path: top-out.csv, include_unmapped: false }
"#;

#[test]
fn direct_source_composition_seed_precedes_fused_name_and_releases_registration() {
    let config = clinker_plan::config::parse_config(DIRECT_SOURCE_FUSION_NAME_COLLISION)
        .expect("parse pipeline YAML");
    let compiled = config
        .compile(&fixture_compile_context())
        .expect("compile pipeline");
    let dag = compiled.dag();
    let parent_source_idx = dag
        .graph
        .node_indices()
        .find(|idx| dag.graph[*idx].name() == "body_feed")
        .expect("parent Source exists");
    let body_id = dag
        .graph
        .node_indices()
        .find_map(|idx| match &dag.graph[idx] {
            clinker_plan::plan::execution::PlanNode::Composition { name, body, .. }
                if name == "body_call" =>
            {
                Some(*body)
            }
            _ => None,
        })
        .expect("composition body id exists");
    let body = compiled
        .composition_bodies()
        .get(&body_id)
        .expect("bound composition body exists");
    let body_port_source_idx = *body
        .port_name_to_node_idx
        .get("collision")
        .expect("body input port Source exists");
    assert_eq!(
        parent_source_idx, body_port_source_idx,
        "this regression must exercise colliding parent/body NodeIndex spaces"
    );

    let merge_fused =
        clinker_plan::plan::execution::compute_merge_interleave_fused_sources(dag, &config);
    let init_phase = clinker_plan::plan::execution::compute_init_phase_node_set(dag);
    let (transform_sources, _) = clinker_plan::plan::execution::compute_transform_fused_sources(
        dag,
        &merge_fused,
        &init_phase,
    );
    assert!(transform_sources.contains("collision"));

    let arbitrator = quiet_arbitrator();
    let mut params = PipelineRunParams {
        execution_id: "direct-source-fusion-name-collision".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };
    params
        .record_vars
        .insert("boundary".to_string(), clinker_record::Value::from("body"));
    let (result, outputs) = run_with_params(
        DIRECT_SOURCE_FUSION_NAME_COLLISION,
        &[("body_feed", "id\n7\n"), ("collision", "id\n9\n")],
        &["body_out", "top_out"],
        Arc::clone(&arbitrator),
        &params,
    );
    result.expect("direct Source-to-Composition collision pipeline must run");

    assert_eq!(outputs["body_out"].as_string(), "id,boundary\n7,body\n");
    assert_eq!(
        outputs["top_out"].as_string(),
        "id,origin\n9,top-transform\n"
    );
    assert_eq!(arbitrator.consumer_count(), 0);
    assert_eq!(arbitrator.sum_consumer_usage(), 0);
}

#[test]
fn composition_source_canonicalization_overlap_rejects_and_releases_transfer() {
    let row_bytes = row_bytes_for_node(DIRECT_SOURCE_COMPOSITION, "src");
    let (arbitrator, baseline_id, baseline_usage) =
        canonicalization_overlap_rejection_arbitrator(row_bytes);
    let (result, _) = run(
        DIRECT_SOURCE_COMPOSITION,
        &[("src", "a\n5\n")],
        &["composition_out"],
        Arc::clone(&arbitrator),
    );

    match result.expect_err("body Source rematerialization must cross the hard limit") {
        PipelineError::CompositionBodyError {
            composition_name,
            inner,
        } => {
            assert_eq!(composition_name, "doubled_call");
            match *inner {
                PipelineError::MemoryBudgetExceeded {
                    used,
                    limit,
                    source,
                    ..
                } => {
                    assert_eq!(used, HARD_LIMIT + 1);
                    assert_eq!(limit, HARD_LIMIT);
                    assert_eq!(source, clinker_plan::BudgetCategory::NodeBuffer);
                }
                other => panic!("expected inner E310 NodeBuffer; got {other:?}"),
            }
        }
        other => panic!("expected composition-wrapped overlap E310; got {other:?}"),
    }
    assert_eq!(arbitrator.consumer_count(), 1);
    assert_eq!(arbitrator.sum_consumer_usage(), baseline_usage);
    arbitrator.unregister_consumer(baseline_id);
}

const FAILING_DIRECT_SOURCE_COMPOSITION: &str = r#"
pipeline:
  name: transient_clone_composition_error_cleanup
error_handling:
  strategy: fail_fast
nodes:
  - type: source
    name: src
    config: { name: src, type: csv, path: src.csv, schema: [ { name: a, type: string } ] }
  - type: composition
    name: failing_call
    input: src
    use: ../compositions/exec_runtime_error.comp.yaml
    inputs:
      inp: src
  - type: output
    name: composition_out
    input: failing_call
    config: { name: composition_out, type: csv, path: composition.csv }
"#;

#[test]
fn direct_source_composition_body_error_restores_parent_registration() {
    let arbitrator = quiet_arbitrator();
    let (result, _) = run(
        FAILING_DIRECT_SOURCE_COMPOSITION,
        &[("src", "a\nnot-an-integer\n")],
        &["composition_out"],
        Arc::clone(&arbitrator),
    );

    assert!(
        matches!(
            result,
            Err(PipelineError::CompositionBodyError {
                composition_name,
                ..
            }) if composition_name == "failing_call"
        ),
        "the runtime error must retain its composition wrapper"
    );
    assert_eq!(
        arbitrator.consumer_count(),
        0,
        "body-error restoration and top-level teardown must release all registrations"
    );
    assert_eq!(arbitrator.sum_consumer_usage(), 0);
}

const HARVEST_ERROR_COMPOSITION: &str = r#"
pipeline:
  name: transient_clone_composition_harvest_cleanup
nodes:
  - type: source
    name: src
    config: { name: src, type: csv, path: src.csv, schema: [ { name: a, type: int } ] }
  - type: composition
    name: harvest_call
    input: src
    use: ../compositions/transient_clone_harvest_error.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: harvest_call
    config: { name: out, type: csv, path: out.csv }
"#;

#[test]
fn composition_harvest_error_restores_parent_registration_before_propagation() {
    let arbitrator = quiet_arbitrator();
    let (result, _) = run(
        HARVEST_ERROR_COMPOSITION,
        &[("src", "a\n5\n")],
        &["out"],
        Arc::clone(&arbitrator),
    );

    let error = result.expect_err("a port-qualified Route terminal has no bare harvest slot");
    assert!(
        matches!(error, PipelineError::Internal { ref node, .. } if node == "harvest_call"),
        "the body output-harvest error must surface at the composition call-site; got {error:?}"
    );
    assert_eq!(
        arbitrator.consumer_count(),
        0,
        "harvest-error restoration and top-level teardown must release all registrations"
    );
    assert_eq!(arbitrator.sum_consumer_usage(), 0);
}
