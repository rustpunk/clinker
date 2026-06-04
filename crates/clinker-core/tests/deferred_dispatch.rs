//! End-to-end deferred-dispatch tests (integration half).
//!
//! Covers the commit-time deferred dispatcher's behavior through the
//! public executor entry point: commit-pass emission, the disk-spill-quota
//! E310 surface on a deferred buffer, build-side replay through the
//! region-input buffer, composition-body harvest chains, and the
//! output-fanout double-flush invariant. The white-box half (deferred-
//! region plan structure, RSS-overshoot abort, the cascading-retraction
//! loop cap) reads crate-private executor seams and stays inline in
//! `executor/tests/deferred_dispatch.rs`.

mod common;

use std::collections::{BTreeSet, HashMap};
use std::io::Write;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, PipelineConfig};
use clinker_core::error::PipelineError;
use clinker_core::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders};

/// Run a parsed `config` resolving source/composition paths against an
/// explicit compile anchor (a temp workspace root), returning the full
/// [`ExecutionReport`]. The composition tests need a non-CWD anchor so the
/// `use:`-relative `_compose:` loader resolves bodies without mutating CWD
/// (unsafe under cargo's parallel runner). Forwards to the public
/// in-context `&CompiledPlan` entry point.
fn run_in_context(
    config: &PipelineConfig,
    readers: SourceReaders,
    writers: HashMap<String, Box<dyn Write + Send>>,
    params: &PipelineRunParams,
    ctx: CompileContext,
) -> Result<ExecutionReport, PipelineError> {
    let plan = config
        .compile(&ctx)
        .expect("integration-test pipeline must compile");
    PipelineExecutor::run_plan_with_readers_writers_in_context(&plan, readers, writers, params, ctx)
}

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
    let config = clinker_core::config::parse_config(DEFERRED_PIPELINE).expect("parse");
    let primary = "src".to_string();
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,ENG,100
";
    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        clinker_core::executor::single_file_reader(
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
    let report = common::run_config(&config, readers, writers, &params)
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
  memory: { limit: "500" }
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
    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
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
    let config = clinker_core::config::parse_config(yaml).expect("parse");
    // The pipeline either errors at the producer's region tee (E310 from
    // `tee_emit_to_region_input_buffers`) or at the source's arena
    // build (E310 from the source-rooted Phase-0 arena). Either path
    // surfaces the E310 admission failure shape; assert the err string
    // carries that signature.
    let result = common::run_config(&config, readers, writers, &params);
    let err = result.expect_err(
        "tight memory limit must surface as a typed admission failure on the \
         deferred buffer's projection or upstream spill path",
    );
    // The deferred-buffer admission path raises a typed
    // MemoryBudgetExceeded directly; an upstream sort enforcer that
    // spilled past the budget surfaces a distinct "spill files"
    // Compilation message with the same "memory.limit too small"
    // semantic. Either is an acceptable observation that memory
    // accounting fired; the test pins the failure-shape contract for
    // the deferred buffer by destructuring on MemoryBudgetExceeded
    // when reachable and on the spill-fallback Compilation arm
    // otherwise.
    match &err {
        clinker_core::error::PipelineError::MemoryBudgetExceeded {
            source: clinker_core::pipeline::memory::BudgetCategory::Arena,
            ..
        } => {}
        clinker_core::error::PipelineError::Compilation { messages, .. }
            if messages
                .iter()
                .any(|m| m.contains("memory.limit too small")) => {}
        clinker_core::error::PipelineError::Io(io_err)
            if io_err.to_string().contains("memory.limit too small") => {}
        other => panic!(
            "memory-overflow surface must carry MemoryBudgetExceeded \
             (BudgetCategory::Arena) or the spill-fallback admission \
             message; got: {other:?}"
        ),
    }
    let _ = BTreeSet::<&str>::new();
}

/// Combine inside a deferred region with a non-deferred build-side
/// Source. The Source arm tees its emit into
/// `region_input_buffers[(None, edge_idx)]` for the edge crossing
/// into the deferred Combine; at commit time the dispatcher's
/// deferred subdag walk drives Combine against the post-recompute
/// driver-side aggregate emit and the cross-region build-side
/// records, and the joined records flow through the deferred `tail`
/// Transform to the writer.
///
/// The fixture verifies the happy path end-to-end (both join keys
/// match the build-side hash, both flow through the deferred member
/// arm and reach the writer with the enriched columns) AND pins the
/// orchestrator-loop convergence shape: with no DLQ events surfaced
/// during commit-pass dispatch, `expand_with_dlq_events` returns an
/// empty delta on the first check, the orchestrator breaks out of
/// the loop, and `iterations == 1`. A regression that would re-add
/// already-retracted source rows on every cascade pass would inflate
/// this counter, so a tight equality is the cleanest pin.
///
/// Build-side rows that hit DLQ during the forward pass require
/// either a Transform-mediated build chain (which breaks
/// `source_records` lookup since Combine's build input must resolve
/// to a Source directly) or source-level validation (not yet
/// available on `Source` nodes). The contract that an at-commit
/// Combine rebuild reads from the cross-region buffer rather than
/// re-reading the source IS pinned indirectly: a per-iteration
/// re-load would shift source row numbers and change the
/// `iterations` count under cascading retraction; today's
/// architecture iterates exactly once on this happy fixture.
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

    let config = clinker_core::config::parse_config(yaml).expect("parse");
    let _primary = "orders".to_string();
    let readers: clinker_core::executor::SourceReaders = HashMap::from([
        (
            "orders".to_string(),
            clinker_core::executor::single_file_reader(
                "test.csv",
                Box::new(std::io::Cursor::new(orders_csv.as_bytes().to_vec())),
            ),
        ),
        (
            "dept_lookup".to_string(),
            clinker_core::executor::single_file_reader(
                "test.csv",
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
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = common::run_config(&config, readers, writers, &params)
        .expect("combine-in-region pipeline must converge");

    let written = buf.as_string();
    let body_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
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
        "HR row must carry budget=100 from the cross-region build-side \
         buffer; got {hr:?}"
    );
    assert!(
        eng.contains(",500"),
        "ENG row must carry budget=500 from the cross-region build-side \
         buffer; got {eng:?}"
    );
    assert_eq!(
        report.counters.dlq_count, 0,
        "no errors expected on the happy fixture; got {}",
        report.counters.dlq_count
    );
    // Convergence pin: the orchestrator iterates exactly once. A
    // regression re-adding already-retracted source rows on each
    // cascade pass would inflate this counter.
    assert_eq!(
        report.counters.retraction.iterations, 1,
        "happy-path Combine-in-region must converge on iteration 1; \
         got {}",
        report.counters.retraction.iterations
    );
}

/// Two-level nested composition wrapping a relaxed-CK Aggregate at the
/// inner body's terminal output port, with a parent-side Transform that
/// mutates the body Aggregate's emit. The commit-pass harvest chain
/// runs end-to-end:
///
/// 1. Inner body's commit-pass dispatch re-finalizes the relaxed
///    Aggregate; `emit_post_recompute` projects the post-recompute
///    rows to the deferred region's `buffer_schema`. The body's
///    output port is the Aggregate itself, so the cross-scope
///    continuation-demand seed in `pass_b_body` is what keeps
///    `(department, total)` in `buffer_schema` — the body has no
///    member operator widening the schema on its own.
/// 2. Those rows are harvested into the outer body's
///    `node_buffers[inner_comp_idx]`.
/// 3. The outer body has no continuation members beyond the inner
///    Composition (it's a wrapper), so its harvest re-surfaces the
///    same rows through the outer output port.
/// 4. The parent harvest seeds `node_buffers[outer_comp_idx]` and the
///    parent's continuation Transform `bump` runs on the same commit
///    pass with `in_deferred_dispatch=true`, applying `+1` to `total`.
/// 5. The parent Output writes the mutated rows.
///
/// Asserts both surviving rows reach the writer with the parent
/// Transform's mutation applied; no DLQ entries because the mutation
/// is total.
#[test]
fn composition_body_harvest_runs_parent_continuation_on_post_recompute_data() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");

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
    out: dept_totals
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
"#,
    )
    .expect("write inner");

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
  name: composition_harvest_continuation
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
  - type: transform
    name: bump
    input: outer
    config:
      cxl: |
        emit department = department
        emit total = total
        emit total_plus_one = total + 1
  - type: output
    name: out
    input: bump
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
    let config = clinker_core::config::parse_config(yaml).expect("parse");
    let ctx = clinker_core::config::CompileContext::with_pipeline_dir(
        workspace.path(),
        std::path::PathBuf::from("pipelines"),
    );
    let _compiled = config
        .compile(&ctx)
        .expect("nested composition + parent continuation must compile");

    let primary = "src".to_string();
    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        clinker_core::executor::single_file_reader(
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
    let report = run_in_context(&config, readers, writers, &params, ctx)
        .expect("nested composition + parent continuation must run without error");

    assert_eq!(
        report.counters.dlq_count, 0,
        "no DLQ entries expected — `bump` is a total CXL projection over the body emits; got {}",
        report.counters.dlq_count
    );

    let written = buf.as_string();
    let body_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        body_lines.len(),
        3,
        "expected header + HR row + ENG row, got {body_lines:?}"
    );
    let rows: Vec<&str> = body_lines.iter().skip(1).copied().collect();
    let hr = rows
        .iter()
        .find(|r| r.contains("HR"))
        .copied()
        .unwrap_or_else(|| panic!("HR row must reach parent Output; got rows {rows:?}"));
    assert!(
        hr.contains("60") && hr.contains("61"),
        "HR row must carry total=60 (sum of HR amounts) and total_plus_one=61 from the parent `bump` Transform; got {hr:?}"
    );
    let eng = rows
        .iter()
        .find(|r| r.contains("ENG"))
        .copied()
        .unwrap_or_else(|| panic!("ENG row must reach parent Output; got rows {rows:?}"));
    assert!(
        eng.contains("600") && eng.contains("601"),
        "ENG row must carry total=600 (sum of ENG amounts) and total_plus_one=601 from the parent `bump` Transform; got {eng:?}"
    );
}

/// Same nested-composition geometry as the harvest happy-path test, but
/// the parent-side Transform's CXL trips /0 on HR's body emit. The
/// cascading-retraction loop must:
///
/// 1. Run the body's commit-pass dispatch and harvest both
///    `(HR, total=60)` and `(ENG, total=600)` rows through the chain
///    into `node_buffers[outer_comp_idx]`.
/// 2. Run the parent continuation Transform `bump`; its `1 / (total - 60)`
///    raises /0 on HR.
/// 3. Surface that error as a DLQ event whose lineage resolves through
///    the unified cross-scope `aggregate_idx_by_name` to the body's
///    `dept_totals` aggregate.
/// 4. Iterate: the next `detect_retract_scope` folds the contributing
///    HR source rows back into the body aggregator's input, recomputes
///    without HR, harvests the surviving ENG row through the chain,
///    runs `bump` cleanly on it, and the parent Output writes only the
///    ENG row.
///
/// Pins the cap-widening fix for cross-boundary lineage: the loop's
/// termination cap must include body node counts, otherwise the
/// retraction can panic with the documented cap-exceeded message.
#[test]
fn composition_body_harvest_with_cascading_retraction_preserves_mutation() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");

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
    out: dept_totals
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
"#,
    )
    .expect("write inner");

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
  name: composition_harvest_cascading_retract
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
  - type: transform
    name: bump
    input: outer
    config:
      cxl: |
        emit department = department
        emit total = total
        emit safe_ratio = 1 / (total - 60)
  - type: output
    name: out
    input: bump
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
    let config = clinker_core::config::parse_config(yaml).expect("parse");
    let ctx = clinker_core::config::CompileContext::with_pipeline_dir(
        workspace.path(),
        std::path::PathBuf::from("pipelines"),
    );
    let _compiled = config
        .compile(&ctx)
        .expect("nested composition + cascading-retract pipeline must compile");

    let primary = "src".to_string();
    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        clinker_core::executor::single_file_reader(
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
    let report = run_in_context(&config, readers, writers, &params, ctx)
        .expect("nested composition + cascading retract must run without error");

    // The parent Transform's /0 on HR routes the error into the
    // correlation buffers; cascading retraction folds HR's source rows
    // back to the body aggregator. The DLQ surfaces at least one HR
    // entry; mirror the existing tests' >=1 convention rather than
    // asserting an exact count, since the precise number depends on
    // whether the orchestrator hands the retraction lineage one
    // composite trigger or one per contributing source row.
    assert!(
        report.counters.dlq_count >= 1,
        "the parent Transform's /0 on HR's body emit must surface in the DLQ via the cross-scope retraction path; got {}",
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

    let written = buf.as_string();
    let body_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    assert!(
        body_lines.len() >= 2,
        "expected header + at least the surviving ENG row in writer output, got {body_lines:?}"
    );
    let rows: Vec<&str> = body_lines.iter().skip(1).copied().collect();
    let eng = rows
        .iter()
        .find(|r| r.contains("ENG"))
        .copied()
        .unwrap_or_else(|| {
            panic!("ENG row must reach parent Output via harvest + parent continuation after HR is retracted; got rows {rows:?}")
        });
    // 1 / (600 - 60) = 1 / 540 = 0 in CXL integer division (matches the
    // ratio expressions in the existing relaxed_ratio.comp.yaml fixtures
    // earlier in this file).
    assert!(
        eng.contains("600") && eng.contains(",0"),
        "ENG row must carry total=600 and safe_ratio=0 (integer division 1/540); got {eng:?}"
    );
    assert!(
        !rows.iter().any(|r| r.contains("HR")),
        "HR was DLQ'd by the parent `bump` /0 — it must NOT appear in the writer output; got rows {rows:?}"
    );
}

/// Bare-Aggregate-at-output-port body harvest: the composition body
/// holds a single relaxed-CK Aggregate node which is itself the body's
/// terminal output port. No in-body Transform sits between the
/// Aggregate and the port. The plan-time deferred-region detector
/// must seed the body's `buffer_schema` with the columns the parent's
/// continuation Transform reads, so the harvest carries a record
/// shape the parent's stamped `expected_input_schema` accepts. The
/// parent's continuation Transform mutates `total` by `+1`; the
/// writer must capture the mutated rows. Pins the cross-scope demand
/// path end-to-end without an in-body identity Transform softening
/// the geometry.
#[test]
fn body_with_aggregate_at_output_port_harvests_through_parent_continuation() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");

    std::fs::write(
        comp_dir.join("bare_agg.comp.yaml"),
        r#"_compose:
  name: bare_agg
  inputs:
    inp:
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  outputs:
    out: dept_totals
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
"#,
    )
    .expect("write bare_agg comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: bare_agg_harvest
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
    use: ../compositions/bare_agg.comp.yaml
    inputs:
      inp: src
  - type: transform
    name: bump
    input: body
    config:
      cxl: |
        emit department = department
        emit total = total
        emit total_plus_one = total + 1
  - type: output
    name: out
    input: bump
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
    let config = clinker_core::config::parse_config(yaml).expect("parse");
    let ctx = clinker_core::config::CompileContext::with_pipeline_dir(
        workspace.path(),
        std::path::PathBuf::from("pipelines"),
    );
    let _compiled = config
        .compile(&ctx)
        .expect("bare-Aggregate body + parent continuation must compile");

    let primary = "src".to_string();
    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        clinker_core::executor::single_file_reader(
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
    let report = run_in_context(&config, readers, writers, &params, ctx)
        .expect("bare-Aggregate body + parent continuation must run without error");

    assert_eq!(
        report.counters.dlq_count, 0,
        "no DLQ entries expected — `bump` is a total CXL projection over the body emits; got {}",
        report.counters.dlq_count
    );

    let written = buf.as_string();
    let body_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        body_lines.len(),
        3,
        "expected header + HR row + ENG row, got {body_lines:?}"
    );
    let rows: Vec<&str> = body_lines.iter().skip(1).copied().collect();
    let hr = rows
        .iter()
        .find(|r| r.contains("HR"))
        .copied()
        .unwrap_or_else(|| panic!("HR row must reach parent Output; got rows {rows:?}"));
    assert!(
        hr.contains("60") && hr.contains("61"),
        "HR row must carry total=60 (sum of HR amounts) and total_plus_one=61 from the parent `bump` Transform; got {hr:?}"
    );
    let eng = rows
        .iter()
        .find(|r| r.contains("ENG"))
        .copied()
        .unwrap_or_else(|| panic!("ENG row must reach parent Output; got rows {rows:?}"));
    assert!(
        eng.contains("600") && eng.contains("601"),
        "ENG row must carry total=600 (sum of ENG amounts) and total_plus_one=601 from the parent `bump` Transform; got {eng:?}"
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
    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
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
        ..Default::default()
    };
    let config = clinker_core::config::parse_config(yaml).expect("parse");
    let report = common::run_config(&config, readers, writers, &params)
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
    for line in big_out.lines().chain(small_out.lines()) {
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
