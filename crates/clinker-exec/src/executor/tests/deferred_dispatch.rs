//! Deferred-dispatch white-box tests (inline half).
//!
//! These read crate-private executor seams the public API does not
//! surface — `ExecutionPlanDag::deferred_region_at_producer` /
//! `deferred_region_at`, the `BoundBody::deferred_regions` map, the
//! pre-seeded `MemoryArbitrator` run entry, and the
//! `commit::with_test_loop_cap` thread-local — so they cannot live in the
//! `tests/` directory. The integration half (commit-pass emission,
//! harvest chains, output-fanout invariant) runs through the public entry
//! point in `crates/clinker-exec/tests/deferred_dispatch.rs`.
//!
//! Companion plan-time tests live in `plan::deferred_region`.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::{BTreeSet, HashMap};

const DEFERRED_PIPELINE: &str = r#"
pipeline:
  name: deferred_smoke
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: order_id
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: src
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount)
- type: transform
  name: scaled
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit scaled = total * 2
- type: output
  name: out
  input: scaled
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;

/// Plan-side: the planner registers a deferred region whose producer is
/// the relaxed Aggregate and whose `buffer_schema` covers exactly the
/// columns the deferred Transform reaches via `Expr::support_into`.
/// `members` includes the Transform; `outputs` includes the
/// correlation-buffered Output. The region's NodeIndex membership
/// is reachable from `dag.deferred_region_at` for every participant.
#[test]
fn relaxed_aggregate_seeds_a_deferred_region_with_downstream_members() {
    use clinker_plan::config::{CompileContext, parse_config};
    use clinker_plan::plan::execution::PlanNode;

    let config = parse_config(DEFERRED_PIPELINE).expect("parse");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile must succeed for the deferred-region smoke pipeline")
        .dag()
        .clone();

    let agg_idx = plan
        .graph
        .node_indices()
        .find(|&i| matches!(&plan.graph[i], PlanNode::Aggregation { .. }))
        .expect("Aggregate node must be present");
    let transform_idx = plan
        .graph
        .node_indices()
        .find(|&i| matches!(&plan.graph[i], PlanNode::Transform { name, .. } if name == "scaled"))
        .expect("Transform 'scaled' must be present");
    let output_idx = plan
        .graph
        .node_indices()
        .find(|&i| matches!(&plan.graph[i], PlanNode::Output { name, .. } if name == "out"))
        .expect("Output 'out' must be present");

    let region = plan
        .deferred_region_at_producer(agg_idx)
        .expect("Aggregate must seed a deferred region");
    assert!(
        region.members.contains(&transform_idx),
        "deferred region members must include the downstream Transform"
    );
    assert!(
        region.outputs.contains(&output_idx),
        "deferred region outputs must include the correlation-buffered Output"
    );

    // `is_deferred_consumer` flips true for member + output (so their
    // arms short-circuit) but stays false for the producer (which still
    // runs its aggregation kernel, then projects to buffer_schema).
    assert!(
        plan.is_deferred_consumer(transform_idx),
        "Transform should be flagged as deferred consumer"
    );
    assert!(
        plan.is_deferred_consumer(output_idx),
        "Output should be flagged as deferred consumer"
    );
    assert!(
        !plan.is_deferred_consumer(agg_idx),
        "Aggregate (the producer) must NOT be flagged as deferred consumer"
    );

    // The buffer_schema is the column-pruned union the deferred
    // operators reach. The Transform reads `department` and `total`,
    // so both must appear — and the schema is ordered to match the
    // producer's `output_schema` so a downstream operator's
    // `check_input_schema` does not trip on column-list reorder.
    let schema_set: BTreeSet<&str> = region.buffer_schema.iter().map(String::as_str).collect();
    assert!(
        schema_set.contains("department"),
        "buffer_schema must retain 'department': {:?}",
        region.buffer_schema
    );
    assert!(
        schema_set.contains("total"),
        "buffer_schema must retain 'total': {:?}",
        region.buffer_schema
    );
    // Producer-order match: every consecutive pair in `buffer_schema`
    // appears in the same relative order in the producer's
    // `output_schema`. Reordering would trip `check_input_schema` at
    // the first downstream consumer.
    let producer_schema = plan.graph[agg_idx]
        .stored_output_schema()
        .expect("producer aggregate has stored_output_schema");
    let producer_cols: Vec<&str> = producer_schema
        .columns()
        .iter()
        .map(|c| c.as_ref())
        .collect();
    let buffer_positions: Vec<usize> = region
        .buffer_schema
        .iter()
        .map(|col| {
            producer_cols
                .iter()
                .position(|p| *p == col.as_str())
                .unwrap_or_else(|| {
                    panic!("buffer_schema column {col:?} not in producer output_schema")
                })
        })
        .collect();
    let mut sorted_positions = buffer_positions.clone();
    sorted_positions.sort();
    assert_eq!(
        buffer_positions, sorted_positions,
        "buffer_schema must preserve the producer's emit order; got {:?} (positions in producer schema {:?})",
        region.buffer_schema, buffer_positions
    );
}

/// Whole-process RSS over the hard limit aborts a Combine that lives in
/// a deferred region with the `BudgetCategory::Arena` admission-failure
/// shape.
///
/// Sibling to [`memory_budget_overflow_on_deferred_buffer_raises_e310`],
/// which drives the per-arena logical charge with a tight byte budget.
/// This one drives the RSS-based `should_abort` gate instead: it seeds
/// the pipeline-scoped arbitrator's `peak_rss` above the hard limit and
/// runs the combine-in-deferred-region topology through the executor's
/// arbitrator-injection seam. The Combine's build phase polls
/// `should_abort` and surfaces `MemoryBudgetExceeded { source: Arena }`
/// naming the Combine, where `used` is the observed peak RSS and `limit`
/// is the hard limit.
///
/// Seeding `peak_rss` rather than setting a tight YAML budget is the
/// only deterministic lever under pull-mode: at `cargo test` time the
/// test process's own RSS dwarfs any tight budget, so a budget-driven
/// abort would race the framework footprint. A 100 GiB hard limit keeps
/// the seeded value dominant in the `fetch_max` fold inside `observe()`.
#[test]
fn memory_budget_overflow_on_region_input_buffer_raises_e310() {
    let yaml = r#"
pipeline:
  name: region_input_buffer_overshoot
error_handling:
  strategy: continue
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    correlation_key: order_id
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: orders
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount)
- type: transform
  name: probe_xform
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
- type: source
  name: dept_lookup
  config:
    name: dept_lookup
    path: dept_lookup.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: budget, type: int }
- type: combine
  name: enriched
  input:
    p: probe_xform
    b: dept_lookup
  config:
    where: 'p.department == b.department'
    match: first
    on_miss: skip
    cxl: |
      emit department = p.department
      emit total = p.total
      emit budget = b.budget
    propagate_ck: driver
- type: transform
  name: tail
  input: enriched
  config:
    cxl: |
      emit department = department
      emit total = total
      emit budget = budget
- type: output
  name: out
  input: tail
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let orders_csv = "order_id,department,amount\no1,HR,10\no2,HR,20\no3,ENG,100\no4,ENG,200\n";
    let lookup_csv = "department,budget\nHR,100\nENG,500\n";

    let config = clinker_plan::config::parse_config(yaml).expect("parse");
    let readers: crate::executor::SourceReaders = HashMap::from([
        (
            "orders".to_string(),
            crate::executor::single_file_reader(
                "orders.csv",
                Box::new(std::io::Cursor::new(orders_csv.as_bytes().to_vec())),
            ),
        ),
        (
            "dept_lookup".to_string(),
            crate::executor::single_file_reader(
                "dept_lookup.csv",
                Box::new(std::io::Cursor::new(lookup_csv.as_bytes().to_vec())),
            ),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "region-input-buffer-overshoot".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };

    // 100 GiB hard limit so the seeded peak RSS stays dominant; seed
    // just above the hard limit so `should_abort` trips on the first
    // poll inside the Combine build.
    const HARD_LIMIT: u64 = 100 * 1024 * 1024 * 1024;
    let arbitrator = std::sync::Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        0.80,
        Box::new(crate::pipeline::memory::Priority),
    ));
    arbitrator.set_peak_rss_for_test(HARD_LIMIT + 4096);

    let err = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &params,
        clinker_plan::config::CompileContext::default(),
        arbitrator,
    )
    .expect_err("peak RSS above the hard limit must abort the deferred-region Combine");

    match err {
        clinker_plan::error::PipelineError::MemoryBudgetExceeded {
            source: clinker_plan::BudgetCategory::Arena,
            used,
            limit,
            ..
        } => {
            assert!(
                used > limit,
                "reported used ({used}) must exceed the hard limit ({limit}) at abort",
            );
        }
        other => panic!(
            "RSS overshoot in a deferred-region Combine must carry \
             MemoryBudgetExceeded (BudgetCategory::Arena); got: {other:?}"
        ),
    }
}

/// Cascading-retraction loop iteration cap protects against a planner
/// bug where the structural termination invariant breaks. The cap is
/// `node_count + |source_rows| + 1`; each iteration must add at least
/// one source row to the trigger set, and source rows are bounded, so
/// the loop terminates cleanly in well-formed pipelines. The cap is
/// the defensive backstop for a hypothetical planner regression that
/// drives the loop monotonically past that bound; tripping it panics
/// with the documented message so the regression surfaces loudly
/// instead of hanging.
///
/// Production callers cannot trigger the cap because
/// `RetractScope::expand_with_dlq_events` enforces strict monotonicity
/// on `seen_source_rows` (a `BTreeSet`); reaching the panic path
/// requires forcing a smaller-than-structural cap. The
/// [`crate::executor::commit::with_test_loop_cap`] thread-local
/// override exists exactly to exercise this defensive contract under
/// a synthetic small cap; production code never reads it.
#[test]
#[should_panic(expected = "cascading-retraction loop exceeded")]
fn cascading_retraction_loop_cap_panics_with_documented_message() {
    use crate::executor::commit::with_test_loop_cap;
    // Same shape as the convergence test in
    // `correlated_post_aggregate_retract::cascading_retraction_loop_converges_and_excludes_failing_partition`
    // — relaxed Aggregate + downstream Transform that fails on HR's
    // aggregate emit. Forcing cap=0 means the orchestrator's defensive
    // check fires before the first iteration body runs, exercising the
    // panic-message contract without needing to provoke a real planner
    // regression.
    let yaml = r#"
pipeline:
  name: loop_cap_defensive
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: order_id
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: src
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount)
- type: transform
  name: ratio
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit ratio = 1 / (total - 60)
- type: output
  name: out
  input: ratio
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,HR,30
";
    let primary = "src".to_string();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
        ..Default::default()
    };
    let config = clinker_plan::config::parse_config(yaml).expect("parse");
    with_test_loop_cap(0, || {
        let _ =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params);
    });
}

/// Composition body containing a relaxed-CK Aggregate end-to-end:
/// the body's Aggregate runs at body-executor entry, parks its narrow
/// emit into the body-local `node_buffers[producer]`, and the
/// orchestrator's commit-time deferred dispatcher recurses into the
/// body via `recurse_into_body` to drive the body's deferred-region
/// member arms on post-recompute data. A failure on the body's
/// downstream Transform routes through the SHARED `correlation_buffers`
/// (parent-scope); the HR contributing source rows surface as a DLQ
/// trigger after the cascading-retraction loop converges.
///
/// The body's commit-pass output-port records propagate through the
/// body→parent harvest in `recurse_into_body`: the post-recompute
/// ratio row for ENG is drained from the body's output-port slot,
/// seeded into the parent's `node_buffers[composition_idx]`, and the
/// parent's continuation (the Output node) runs through the same
/// `dispatch_plan_node` arms with `in_deferred_dispatch=true`. The
/// surviving (non-DLQ'd) row therefore reaches the parent Output;
/// HR's failed contribution does not.
#[test]
fn composition_body_relaxed_aggregate_runs_under_commit_time_dispatch() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("relaxed_ratio.comp.yaml"),
        r#"_compose:
  name: relaxed_ratio
  inputs:
    inp:
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: ratio
  config_schema: {}

nodes:
  - type: aggregate
    name: dept_totals
    input: inp
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)
  - type: transform
    name: ratio
    input: dept_totals
    config:
      cxl: |
        emit department = department
        emit total = total
        emit ratio = 1 / (total - 60)
"#,
    )
    .expect("write composition fixture");
    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: composition_relaxed_runtime
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: composition
    name: body
    input: src
    use: ../compositions/relaxed_ratio.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: body
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,HR,30
o4,ENG,100
o5,ENG,200
o6,ENG,300
";
    let config = clinker_plan::config::parse_config(yaml).expect("parse");
    let ctx = clinker_plan::config::CompileContext::with_pipeline_dir(
        workspace.path(),
        std::path::PathBuf::from("pipelines"),
    );
    let compiled = config
        .compile(&ctx)
        .expect("composition pipeline must compile");

    // Sanity-pin: the body-internal relaxed Aggregate was detected and
    // its body-local deferred region was registered. Without this the
    // orchestrator's `is_relaxed_pipeline` check (which now inspects
    // bodies) returns false and the commit-time recurse never fires.
    let bodies: Vec<_> = compiled.artifacts().composition_bodies.values().collect();
    assert_eq!(
        bodies.len(),
        1,
        "exactly one composition body must be bound"
    );
    assert!(
        !bodies[0].deferred_regions.is_empty(),
        "body must carry at least one deferred region (rooted at the body's relaxed Aggregate)"
    );

    let primary = "src".to_string();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
        ..Default::default()
    };
    // Run via the standard `run_with_readers_writers` entry point;
    // composition resolution flows through the same `_compose:` loader
    // the CompileContext-backed compile uses internally because the
    // pipeline file's `use:` is relative to the pipelines dir.
    // Use the in-context entry point so the temp workspace root
    // threads through compile-time composition resolution without
    // touching CWD — `CompileContext::default()` reads CWD at call
    // time, which is not safe under cargo's default parallel test
    // runner.
    let report = PipelineExecutor::run_with_readers_writers_in_context(
        &config,
        readers,
        writers.into(),
        &params,
        ctx,
    )
    .expect("composition pipeline must run without error");

    assert!(
        report.counters.dlq_count >= 1,
        "the body Transform's /0 lands in the DLQ via the shared \
         correlation_buffers + body-scoped detect/recompute path; got {}",
        report.counters.dlq_count
    );
    let hr_dlq = report.dlq_entries.iter().find(|e| {
        e.original_record
            .values()
            .iter()
            .any(|v| matches!(v, clinker_record::Value::String(s) if s.as_ref() == "HR"))
    });
    assert!(
        hr_dlq.is_some(),
        "DLQ must carry an entry whose original_record references HR; got: {:?}",
        report.dlq_entries
    );

    // Body→parent harvest + continuation dispatch: ENG's surviving
    // (non-DLQ) row must reach the parent Output through the new
    // commit-pass harvest path. The body's commit-time deferred
    // dispatcher emits the post-recompute ratio row through the body's
    // output port; `recurse_into_body` harvests that row into the
    // parent's `node_buffers[composition_idx]` slot and drives the
    // parent continuation (just the Output node here) on the same
    // commit pass.
    let written = buf.as_string();
    let body_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    assert!(
        body_lines.len() >= 2,
        "expected header + at least ENG row in writer output, got {body_lines:?}"
    );
    let rows: Vec<&str> = body_lines.iter().skip(1).copied().collect();
    let eng = rows
        .iter()
        .find(|r| r.contains("ENG"))
        .copied()
        .unwrap_or_else(|| {
            panic!("ENG row must reach parent Output via harvest path; got rows {rows:?}")
        });
    assert!(
        eng.contains("600"),
        "ENG row must carry the body Aggregate's post-recompute total=600; got {eng:?}"
    );
    assert!(
        !rows.iter().any(|r| r.contains("HR")),
        "HR was DLQ'd by the body Transform's /0 — it must NOT appear in the writer output; got rows {rows:?}"
    );
}

/// Recursive composition: outer body wraps inner body whose internal
/// Aggregate is relaxed-CK and whose downstream Transform fails on
/// the HR aggregate emit. The orchestrator's commit-time deferred
/// dispatcher walks parent-graph regions (none), then iterates every
/// parent-graph Composition node and recurses into bound bodies that
/// (transitively) carry a deferred region. The outer body has no
/// deferred region of its own but its inner body does, so the outer
/// recurse fires and propagates into the inner body's
/// `recurse_into_body`, where the inner's commit-pass dispatch fires
/// and its body Transform's /0 surfaces in the DLQ.
///
/// The body→parent harvest then chains across both nesting levels:
/// the inner body's commit-pass output port records flow into the
/// outer body's `node_buffers[inner_composition_idx]`; the outer
/// body's continuation walk is empty here (the outer body is a pure
/// passthrough wrapping the inner Composition), but the outer body's
/// own output port surfaces those same records back to the parent's
/// `node_buffers[outer_composition_idx]` via a second harvest, and
/// the parent's continuation (just the Output node) runs over them.
/// ENG's surviving row therefore reaches the parent Output; HR does
/// not.
#[test]
fn recursive_composition_inner_body_relaxed_aggregate_dispatches_at_commit() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");

    // Inner body: relaxed Aggregate + downstream Transform that throws
    // /0 on HR's emit.
    std::fs::write(
        comp_dir.join("inner_relaxed.comp.yaml"),
        r#"_compose:
  name: inner_relaxed
  inputs:
    inp:
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: ratio
  config_schema: {}

nodes:
  - type: aggregate
    name: dept_totals
    input: inp
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)
  - type: transform
    name: ratio
    input: dept_totals
    config:
      cxl: |
        emit department = department
        emit total = total
        emit ratio = 1 / (total - 60)
"#,
    )
    .expect("write inner");

    // Outer body wraps the inner.
    std::fs::write(
        comp_dir.join("outer_wrap.comp.yaml"),
        r#"_compose:
  name: outer_wrap
  inputs:
    inp:
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: inner_call
  config_schema: {}

nodes:
  - type: composition
    name: inner_call
    input: inp
    use: ./inner_relaxed.comp.yaml
    inputs:
      inp: inp
"#,
    )
    .expect("write outer");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: recursive_composition_runtime
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: composition
    name: outer
    input: src
    use: ../compositions/outer_wrap.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: outer
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,HR,30
o4,ENG,100
o5,ENG,200
o6,ENG,300
";
    let config = clinker_plan::config::parse_config(yaml).expect("parse");
    let ctx = clinker_plan::config::CompileContext::with_pipeline_dir(
        workspace.path(),
        std::path::PathBuf::from("pipelines"),
    );
    let compiled = config
        .compile(&ctx)
        .expect("recursive composition pipeline must compile");

    // Sanity-pin: exactly two bodies are bound; the inner one carries
    // the deferred region rooted at its `dept_totals` Aggregate.
    let bodies: Vec<_> = compiled.artifacts().composition_bodies.values().collect();
    assert_eq!(bodies.len(), 2, "inner and outer bodies must both bind");
    assert!(
        bodies.iter().any(|b| !b.deferred_regions.is_empty()),
        "at least one body (inner) must carry a deferred region; got {:?}",
        bodies
            .iter()
            .map(|b| b.deferred_regions.is_empty())
            .collect::<Vec<_>>()
    );

    let primary = "src".to_string();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_with_readers_writers_in_context(
        &config,
        readers,
        writers.into(),
        &params,
        ctx,
    )
    .expect("recursive composition pipeline must run without error");

    // The inner body's commit-pass deferred dispatch must surface the
    // /0 trigger from the inner Transform. `recurse_into_body` for the
    // outer body recurses through the parent's dispatcher; the inner
    // composition node lives at the OUTER body's NodeIndex space, and
    // its body recursion fires via the outer body's
    // `composition_members` walk inside `dispatch_deferred_inner`.
    assert!(
        report.counters.dlq_count >= 1,
        "the inner body Transform's /0 must reach the DLQ via the \
         recursive commit-time dispatch path; got {}",
        report.counters.dlq_count
    );
    let hr_dlq = report.dlq_entries.iter().find(|e| {
        e.original_record
            .values()
            .iter()
            .any(|v| matches!(v, clinker_record::Value::String(s) if s.as_ref() == "HR"))
    });
    assert!(
        hr_dlq.is_some(),
        "DLQ must carry the HR-tagged trigger from the inner body's \
         deferred region; got: {:?}",
        report.dlq_entries
    );

    // Body→parent harvest chains across both nesting levels: the
    // inner body's post-recompute ratio row for ENG flows through
    // the inner output port into the outer body's
    // `node_buffers[inner_comp_idx]`, then through the outer output
    // port into the parent's `node_buffers[outer_comp_idx]`, and the
    // parent's continuation drives the Output node over it. ENG's
    // surviving row must land in the writer; HR was DLQ'd by the
    // inner body Transform's /0 and must NOT appear.
    let written = buf.as_string();
    let body_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    assert!(
        body_lines.len() >= 2,
        "expected header + at least ENG row in writer output, got {body_lines:?}"
    );
    let rows: Vec<&str> = body_lines.iter().skip(1).copied().collect();
    let eng = rows
        .iter()
        .find(|r| r.contains("ENG"))
        .copied()
        .unwrap_or_else(|| {
            panic!("ENG row must reach parent Output via two-level harvest; got rows {rows:?}")
        });
    assert!(
        eng.contains("600"),
        "ENG row must carry the inner Aggregate's post-recompute total=600; got {eng:?}"
    );
    assert!(
        !rows.iter().any(|r| r.contains("HR")),
        "HR was DLQ'd by the inner body Transform's /0 — it must NOT appear in the writer output; got rows {rows:?}"
    );
}
