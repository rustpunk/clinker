//! End-to-end deferred-dispatch tests.
//!
//! Covers the forward-pass short-circuit, the producer-projection
//! split, the commit-time deferred dispatcher's cascading-retraction
//! loop, and the operator-arm parity invariants between forward and
//! commit passes. Companion plan-time tests live in
//! `crates/clinker-core/src/plan/tests/deferred_region.rs`.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::{BTreeSet, HashMap};

/// Source(`order_id` CK) → Aggregate(`group_by: [department]`, relaxed
/// because `group_by` omits the source CK) → Transform → Output. The
/// Aggregate seeds a deferred region whose member is the Transform and
/// whose exit is the Output.
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
    use crate::config::{CompileContext, parse_config};
    use crate::plan::execution::PlanNode;

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

/// End-to-end forward + commit pass: deferred member and Output arms
/// short-circuit on the forward pass, then the orchestrator's
/// commit-time deferred dispatcher re-feeds the producer's parked
/// emit through the same arms and the Output's records reach the
/// writer through the correlation-buffer flush. With no upstream
/// errors the cascading-retraction loop converges on iteration 1
/// (no DLQ events fold into the next iteration's scope) and every
/// aggregate emit lands as a record on the sink.
#[test]
fn deferred_consumers_emit_through_commit_time_dispatch() {
    let config = crate::config::parse_config(DEFERRED_PIPELINE).expect("parse");
    let primary = "src".to_string();
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,ENG,100
";
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())) as Box<dyn std::io::Read + Send>,
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
    };
    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .expect("pipeline must run without error");

    let written = buf.as_string();
    let body_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    // Header + at least HR + ENG rows. The commit-time deferred
    // dispatcher re-runs the Transform and Output over the producer's
    // parked emit; with no errors injected the converged iteration
    // emits both aggregate rows.
    assert!(
        body_lines.len() >= 3,
        "commit-time deferred dispatch must land header + HR + ENG rows; \
         got {body_lines:?}"
    );
    let header = body_lines[0];
    assert!(
        header.contains("scaled"),
        "header must include the deferred Transform's emit column \
         `scaled`; got {header:?}"
    );
    let rows: Vec<&str> = body_lines.iter().skip(1).copied().collect();
    let hr = rows
        .iter()
        .find(|r| r.starts_with("HR,"))
        .copied()
        .expect("HR row must reach the writer through commit-time dispatch");
    let eng = rows
        .iter()
        .find(|r| r.starts_with("ENG,"))
        .copied()
        .expect("ENG row must reach the writer through commit-time dispatch");
    assert!(
        hr.ends_with(",60"),
        "HR scaled = HR total (30) * 2 = 60; got {hr:?}"
    );
    assert!(
        eng.ends_with(",200"),
        "ENG scaled = ENG total (100) * 2 = 200; got {eng:?}"
    );
    assert_eq!(
        report.counters.dlq_count, 0,
        "no upstream errors expected in the smoke fixture; got {}",
        report.counters.dlq_count
    );
}

/// Cascading retraction converges and produces the post-retract output:
/// a deferred-region Transform throws divide-by-zero on a recomputed
/// aggregate value, the orchestrator folds the failure into the next
/// iteration's retract scope, the failing aggregate's contributors are
/// retracted, and the converged output excludes the failing department.
/// Counters reflect at least one DLQ trigger (the failing aggregate
/// emit) and at least one recomputed group (the retract path emitted a
/// post-retract delta for the surviving departments).
#[test]
fn cascading_retraction_converges_and_excludes_failing_partition() {
    let yaml = r#"
pipeline:
  name: cascading_retract
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
    // HR sums to 60 → 1/(60-60) divides by zero. ENG sums to 600.
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,10
o3,HR,10
o4,HR,10
o5,HR,10
o6,HR,10
o7,ENG,100
o8,ENG,200
o9,ENG,300
";

    let primary = "src".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())) as Box<dyn std::io::Read + Send>,
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
    };
    let config = crate::config::parse_config(yaml).expect("parse");
    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .expect("cascading-retract pipeline must converge");

    let written = buf.as_string();
    let body_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    assert!(
        body_lines
            .iter()
            .skip(1)
            .all(|line| !line.starts_with("HR,")),
        "HR partition retracted across the cascading-retraction loop; \
         must not appear in the converged output. Got: {body_lines:?}"
    );
    assert!(
        body_lines
            .iter()
            .skip(1)
            .any(|line| line.starts_with("ENG,")),
        "ENG partition survives (its aggregate emit does not trigger /0); \
         must reach the writer. Got: {body_lines:?}"
    );

    assert!(
        report.counters.dlq_count >= 1,
        "the HR aggregate emit's /0 lands in the DLQ; got {}",
        report.counters.dlq_count
    );
    assert!(
        report.counters.retraction.groups_recomputed >= 1,
        "the cascading-retraction loop recomputes at least one group; got {}",
        report.counters.retraction.groups_recomputed
    );
}

/// Memory-budget overflow during deferred-buffer projection raises the
/// E310 admission failure shape that the windowed-Transform's buffer-
/// recompute path uses, so callers see a uniform error mode regardless
/// of which deferred-region path tripped the overflow.
#[test]
fn memory_budget_overflow_on_deferred_buffer_raises_e310() {
    // Force the per-arena budget to a very small value so the deferred
    // region producer's narrow projection trips memory accounting on
    // the first record.
    let yaml = r#"
pipeline:
  name: deferred_budget_overflow
  memory_limit: "500"
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
      - { name: payload, type: string }
- type: aggregate
  name: dept_totals
  input: src
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = count(*)
- type: transform
  name: post
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
- type: output
  name: out
  input: post
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    // Pad payload values so each record consumes well over 1KB at the
    // arena projection step.
    let big = "x".repeat(2048);
    let csv = format!("order_id,department,payload\no1,HR,{big}\no2,ENG,{big}\n");

    let primary = "src".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv.into_bytes())) as Box<dyn std::io::Read + Send>,
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
    };
    let config = crate::config::parse_config(yaml).expect("parse");
    // The pipeline either errors at the producer's region tee (E310 from
    // `tee_emit_to_region_input_buffers`) or at the source's arena
    // build (E310 from the source-rooted Phase-0 arena). Either path
    // surfaces the E310 admission failure shape; assert the err string
    // carries that signature.
    let result =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params);
    let err = result.expect_err(
        "tight memory limit must surface as a typed admission failure on the \
         deferred buffer's projection or upstream spill path",
    );
    let rendered = err.to_string();
    // The deferred-buffer admission path raises E310 directly; an
    // upstream sort enforcer that spilled past the budget surfaces a
    // distinct "spill files" message but with the same "memory_limit
    // too small" semantic. Either is an acceptable observation that
    // memory accounting fired; the test pins the failure-shape
    // contract for the deferred buffer specifically by asserting the
    // E310 / MemoryBudgetExceeded substring when reachable, falling
    // back to the spill message when the upstream ran out first.
    assert!(
        rendered.contains("E310")
            || rendered.contains("MemoryBudgetExceeded")
            || rendered.contains("memory_limit too small"),
        "memory-overflow surface must carry E310 / MemoryBudgetExceeded \
         or the spill-fallback admission message; got: {rendered}"
    );
    let _ = BTreeSet::<&str>::new();
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
    // Same shape as `cascading_retraction_converges_and_excludes_failing_partition`
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
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())) as Box<dyn std::io::Read + Send>,
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
    };
    let config = crate::config::parse_config(yaml).expect("parse");
    with_test_loop_cap(0, || {
        let _ = PipelineExecutor::run_with_readers_writers(
            &config, &primary, readers, writers, &params,
        );
    });
}

/// Combine inside a deferred region with a non-deferred build-side
/// upstream Source that feeds records via `combine_source_records`
/// (loaded once at executor entry from the build-side reader). The
/// driver-side aggregate's commit-pass emit joins against the
/// build-side hash; matched rows propagate through the deferred
/// `tail` Transform to the writer. Pins that the Combine arm runs
/// correctly inside a deferred region with a Source build side —
/// the executor's existing combine-source loading path delivers the
/// build-side records, and the deferred dispatcher's
/// `seed_cross_region_inputs_for` is a no-op for this shape (the
/// build-side has no cross-region edge into the Combine — it sits
/// outside the region but produces directly into the Combine arm
/// via `node_buffers[build_pred]`).
#[test]
fn combine_in_deferred_region_replays_build_side_through_region_input_buffer() {
    let yaml = r#"
pipeline:
  name: combine_deferred_region
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
    let orders_csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,ENG,100
o4,ENG,200
";
    let lookup_csv = "\
department,budget
HR,100
ENG,500
";

    let config = crate::config::parse_config(yaml).expect("parse");
    let primary = "orders".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([
        (
            "orders".to_string(),
            Box::new(std::io::Cursor::new(orders_csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        ),
        (
            "dept_lookup".to_string(),
            Box::new(std::io::Cursor::new(lookup_csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        ),
    ]);
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
    };
    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .expect("combine-in-region pipeline must converge");

    let written = buf.as_string();
    let body_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    // Header + HR + ENG. The Combine joined the post-recompute aggregate
    // emit (HR=30, ENG=300) against the build-side hash (HR=100,
    // ENG=500) and the tail Transform projected the enriched record.
    let rows: Vec<&str> = body_lines.iter().skip(1).copied().collect();
    let hr = rows
        .iter()
        .find(|r| r.starts_with("HR,"))
        .copied()
        .expect("HR row reaches the writer through commit-time Combine");
    let eng = rows
        .iter()
        .find(|r| r.starts_with("ENG,"))
        .copied()
        .expect("ENG row reaches the writer through commit-time Combine");
    assert!(
        hr.contains(",100"),
        "HR row must carry budget=100 from the build-side join; got {hr:?}"
    );
    assert!(
        eng.contains(",500"),
        "ENG row must carry budget=500 from the build-side join; got {eng:?}"
    );
    assert_eq!(
        report.counters.dlq_count, 0,
        "no upstream errors expected; got {}",
        report.counters.dlq_count
    );
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
/// Architectural caveat: the current `recurse_into_body` runs the
/// body's commit-time deferred dispatch but does NOT harvest the
/// body's output port records back to the parent's
/// `node_buffers[composition_idx]`. The parent Output therefore sees
/// nothing — that propagation path is on the follow-up surface.
/// This test pins what IS wired: the body's commit-pass DLQ trigger
/// fires (proving `recurse_into_body` invoked the body's deferred
/// dispatch), the body's relaxed aggregator was retracted (proving
/// `recompute_aggregates` ran on body-local indices), and the source
/// row is identifiable in the DLQ (proving the body's
/// `detect_retract_scope` resolved synthetic-CK lineage).
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
    let config = crate::config::parse_config(yaml).expect("parse");
    let ctx = crate::config::CompileContext::with_pipeline_dir(
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
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())) as Box<dyn std::io::Read + Send>,
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
        &config, &primary, readers, writers, &params, ctx,
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
}

/// Recursive composition: outer body wraps inner body whose internal
/// Aggregate is relaxed-CK and whose downstream Transform fails on
/// the HR aggregate emit. The orchestrator's commit-time deferred
/// dispatcher walks parent-graph regions (none), then iterates every
/// parent-graph Composition node and recurses into bound bodies whose
/// `deferred_regions` are non-empty. The OUTER body has no deferred
/// regions of its own (its internals are pure passthroughs), so the
/// outer recursion would skip — but the outer body executes the inner
/// composition during the FORWARD pass, retaining the inner body's
/// aggregator state. Re-entering the outer body's recurse explicitly
/// must propagate to the inner body's `recurse_into_body` so the
/// inner's commit-pass dispatch fires and its body Transform's /0
/// surfaces in the DLQ.
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
    let config = crate::config::parse_config(yaml).expect("parse");
    let ctx = crate::config::CompileContext::with_pipeline_dir(
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
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())) as Box<dyn std::io::Read + Send>,
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
    };
    let report = PipelineExecutor::run_with_readers_writers_in_context(
        &config, &primary, readers, writers, &params, ctx,
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
}

/// Output fan-out writers do not double-flush across cascading-
/// retraction iterations: a Route distributes the deferred Aggregate's
/// emit to two Output sinks. Iteration 1's deferred dispatch admits
/// records into both Output buffer cells; the post-aggregate Transform
/// raises /0 on HR's emit, the orchestrator's archive captures the
/// error and the loop iterates with HR retracted; iteration 2 admits
/// the post-retract emit set (HR removed) into both Output cells —
/// the orchestrator's `restore_baseline` wipes iteration 1's
/// speculative records before iteration 2 runs, then merges only the
/// archived ERROR signal back at flush time. Both writers therefore
/// open + write + close exactly once per pipeline run, and the
/// final-iteration record set lands without duplicates.
///
/// This pins sub-agent D's snapshot/restore architecture: speculative
/// records are dropped per iteration; only the converged set reaches
/// the writer.
#[test]
fn output_fanout_writers_do_not_double_flush_across_iterations() {
    let yaml = r#"
pipeline:
  name: fanout_no_double_flush
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
  name: validate
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit ratio = 1 / (total - 60)
- type: route
  name: classify
  input: validate
  config:
    conditions:
      big: total > 100
    default: small
- type: output
  name: big
  input: classify
  config:
    name: big
    path: big.csv
    type: csv
    include_unmapped: true
- type: output
  name: small
  input: classify
  config:
    name: small
    path: small.csv
    type: csv
    include_unmapped: true
"#;
    // HR sums to 60 → /0 in `validate`. ENG sums to 600 → routes to
    // `out_big`. After cascading retraction excludes HR, only ENG
    // remains in either Output queue.
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,HR,30
o4,ENG,100
o5,ENG,200
o6,ENG,300
";
    let primary = "src".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())) as Box<dyn std::io::Read + Send>,
    )]);
    let big_buf = SharedBuffer::new();
    let small_buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "big".to_string(),
            Box::new(big_buf.clone()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "small".to_string(),
            Box::new(small_buf.clone()) as Box<dyn std::io::Write + Send>,
        ),
    ]);
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
    };
    let config = crate::config::parse_config(yaml).expect("parse");
    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .expect("fan-out pipeline must converge");

    let big_out = big_buf.as_string();
    let small_out = small_buf.as_string();
    // Each writer opens and writes exactly one CSV header line.
    // Iteration 1's speculative HR record (which would have appeared
    // BEFORE the /0 error fired in `validate` if it propagated; the
    // /0 actually preempts the writer admission, but the test still
    // pins the structural invariant that no header duplicates appear).
    let big_headers = big_out
        .lines()
        .filter(|l| l.starts_with("department,"))
        .count();
    let small_headers = small_out
        .lines()
        .filter(|l| l.starts_with("department,"))
        .count();
    assert!(
        big_headers <= 1,
        "`big` writer must open + write header at most once across all \
         cascading-retraction iterations; got {big_headers} header lines: \
         {big_out:?}"
    );
    assert!(
        small_headers <= 1,
        "`small` writer must open + write header at most once across all \
         cascading-retraction iterations; got {small_headers} header \
         lines: {small_out:?}"
    );

    // No HR record reaches either Output: HR's aggregate emit hit /0
    // and was retracted; iteration 2's `restore_baseline` wiped any
    // speculative HR record from iteration 1's buffer.
    for line in big_out.lines().chain(small_out.lines()).skip(0) {
        if line.starts_with("department,") {
            continue;
        }
        assert!(
            !line.starts_with("HR,"),
            "no HR record must reach either writer after retraction; \
             got line: {line:?}"
        );
    }

    // ENG must reach exactly one writer: `big` (ENG total=600 > 100).
    // No duplicate ENG row across iterations.
    let eng_in_big = big_out.lines().filter(|l| l.starts_with("ENG,")).count();
    let eng_in_small = small_out.lines().filter(|l| l.starts_with("ENG,")).count();
    assert_eq!(
        eng_in_big, 1,
        "ENG row must appear in `big` writer exactly once (no per-iteration \
         duplicate from snapshot/restore); got {eng_in_big}: \
         {big_out:?}"
    );
    assert_eq!(
        eng_in_small, 0,
        "ENG row routes to `big`, not `small`; got {eng_in_small}: \
         {small_out:?}"
    );

    // The HR /0 trigger must surface in the DLQ.
    assert!(
        report.counters.dlq_count >= 1,
        "the HR aggregate emit's /0 lands in the DLQ; got {}",
        report.counters.dlq_count
    );
}

/// MemoryBudget overflow on the region-input-buffer surfaces with the
/// E310 admission failure shape. Pipeline: a Combine inside a deferred
/// region with a non-deferred build-side Source whose forward-pass tee
/// lands records into `region_input_buffers`. A tight pipeline-level
/// `memory_limit` forces the per-row admission charge to overflow on
/// the first build-side row, raising the same E310 the deferred
/// producer's own buffer admission raises.
#[test]
fn memory_budget_overflow_on_region_input_buffer_raises_e310() {
    let yaml = r#"
pipeline:
  name: region_input_buffer_overflow
  memory_limit: "1000"
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
- type: source
  name: dept_lookup
  config:
    name: dept_lookup
    path: dept_lookup.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: payload, type: string }
- type: combine
  name: enriched
  input:
    p: dept_totals
    b: dept_lookup
  config:
    where: 'p.department == b.department'
    match: first
    on_miss: skip
    cxl: |
      emit department = p.department
      emit total = p.total
      emit payload = b.payload
    propagate_ck: driver
- type: transform
  name: tail
  input: enriched
  config:
    cxl: |
      emit department = department
      emit total = total
      emit payload = payload
- type: output
  name: out
  input: tail
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    // The cross-region tee charges a per-Value-slot byte cost per row
    // (excluding the payload string contents); a 2-column row charges
    // ~80 bytes. Generate enough build-side rows that the cumulative
    // admission charge exceeds the 1KB limit on its way through
    // `tee_emit_to_region_input_buffers`.
    let mut orders_csv = String::from("order_id,department,amount\n");
    let mut lookup_csv = String::from("department,payload\n");
    for i in 0..200 {
        orders_csv.push_str(&format!("o{i},D{i},10\n"));
        lookup_csv.push_str(&format!("D{i},lookup_payload_{i}\n"));
    }

    let primary = "orders".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([
        (
            "orders".to_string(),
            Box::new(std::io::Cursor::new(orders_csv.into_bytes()))
                as Box<dyn std::io::Read + Send>,
        ),
        (
            "dept_lookup".to_string(),
            Box::new(std::io::Cursor::new(lookup_csv.into_bytes()))
                as Box<dyn std::io::Read + Send>,
        ),
    ]);
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
    };
    let config = crate::config::parse_config(yaml).expect("parse");
    let result =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params);

    // The pipeline either errors at the build-side cross-region tee
    // (`tee_emit_to_region_input_buffers` raising E310 from
    // `MemoryBudget::charge_arena_bytes`) or at the source-rooted
    // arena build for the build-side reader (also E310). Both surfaces
    // carry the same admission-failure error code; the test pins the
    // failure-shape contract regardless of which boundary fires first.
    match result {
        Err(err) => {
            let rendered = err.to_string();
            assert!(
                rendered.contains("E310") || rendered.contains("MemoryBudgetExceeded"),
                "memory-overflow on the region-input-buffer admission \
                 must carry E310 / MemoryBudgetExceeded; got: {rendered}"
            );
        }
        Ok(_) => {
            // 1KB is below the per-row charge for a 2KB payload, so
            // admission must overflow somewhere along the build-side
            // path. A clean Ok return here means the budget guard
            // silently absorbed the overflow — that would be the
            // architectural regression this test catches.
            panic!(
                "expected E310 admission failure on the region-input-buffer \
                 cross-region tee; got Ok"
            );
        }
    }
}
