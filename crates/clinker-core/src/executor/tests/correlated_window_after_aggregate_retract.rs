//! `Source (CK) → Aggregate (relaxed) → Window → Output` compiles
//! without E150 and produces correct windowed values at runtime.
//!
//! The synthetic `$ck.aggregate.<name>` column the relaxed aggregate
//! emits joins the post-aggregate `ck_set`. The downstream Transform's
//! analytic-window declares `partition_by` over user fields only — the
//! synthetic name is never listed in any window's `partition_by`. So
//! `derive_window_buffer_recompute_flags` flips the window's
//! `requires_buffer_recompute` flag (the `ck_outside_partition`
//! predicate fires on the synthetic name), and the E150 gate at
//! `config/mod.rs` lifts.
//!
//! At runtime the windowed Transform reads its arena+index pair from
//! the upstream Aggregate's emit buffer (the `IndexSpec.root` is a
//! `PlanIndexRoot::Node` rooted at the Aggregate), so window builtins
//! see aggregate-emitted columns directly. The companion file
//! `correlated_post_aggregate_retract.rs` covers the no-window case
//! for the same Aggregate (relaxed) lattice.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

fn run_pipeline(
    yaml: &str,
    csv_input: &str,
) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
    let config = crate::config::parse_config(yaml).unwrap();
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: config
            .pipeline
            .vars
            .as_ref()
            .map(|v| crate::config::convert_pipeline_vars(v))
            .unwrap_or_default(),
        shutdown_token: None,
    };

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)?;
    Ok((report.counters, report.dlq_entries, buf.as_string()))
}

const D7_PIPELINE: &str = r#"
pipeline:
  name: agg_then_window_d7
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
    cxl: 'emit department = department

      emit total = sum(amount)

      '
- type: transform
  name: running
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: running
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

/// Compile-time D-7 unblock: `Source (CK) → Aggregate (relaxed) →
/// Transform with analytic_window → Output` no longer hits E150.
///
/// Three load-bearing assertions:
///
/// 1. `compile()` succeeds — no E150 diagnostic on the windowed
///    Transform.
/// 2. The Transform's `IndexSpec` has `requires_buffer_recompute = true`,
///    set by `derive_window_buffer_recompute_flags` because the
///    synthetic `$ck.aggregate.dept_totals` in the parent ck_set is
///    not listed in the window's `partition_by`.
/// 3. The aggregate's `node_properties.ck_set` carries
///    `$ck.aggregate.dept_totals`. This is the source of the
///    auto-flip — the synthetic name is what falls outside every
///    window's partition.
#[test]
fn aggregate_relaxed_then_window_compiles_without_e150() {
    use crate::config::{CompileContext, parse_config};
    use crate::plan::execution::PlanNode;

    let config = parse_config(D7_PIPELINE).expect("parse");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile must succeed; E150 must lift via buffer-recompute auto-flip")
        .dag()
        .clone();

    let mut window_idx_num: Option<usize> = None;
    for idx in plan.graph.node_indices() {
        if let PlanNode::Transform {
            name, window_index, ..
        } = &plan.graph[idx]
            && name == "running"
        {
            window_idx_num = *window_index;
        }
    }
    let idx_num = window_idx_num
        .expect("Transform 'running' must carry an analytic_window with a populated window_index");
    let spec = plan
        .indices_to_build
        .get(idx_num)
        .expect("indices_to_build must hold the running window's IndexSpec");
    assert!(
        spec.requires_buffer_recompute,
        "derive_window_buffer_recompute_flags must auto-flip the post-aggregate \
         window: synthetic $ck.aggregate.dept_totals lives in the parent ck_set \
         but is never listed in any window's partition_by, so ck_outside_partition \
         fires and the flag flips."
    );

    let mut agg_synthetic_present = false;
    for idx in plan.graph.node_indices() {
        if plan.graph[idx].name() != "dept_totals" {
            continue;
        }
        let props = plan
            .node_properties
            .get(&idx)
            .expect("dept_totals must have node_properties");
        agg_synthetic_present = props
            .ck_set
            .iter()
            .any(|f| f == "$ck.aggregate.dept_totals");
    }
    assert!(
        agg_synthetic_present,
        "relaxed aggregate must carry $ck.aggregate.dept_totals in its ck_set; \
         this is the source of the auto-flip"
    );
}

/// Runnable end-to-end: the post-aggregate-window geometry executes
/// and produces correct `running_total` values. The aggregate emits
/// one row per department; the window's `partition_by: [department]`
/// gives each emitted row its own size-1 partition, so
/// `running_total == total` for every output row.
#[test]
fn aggregate_relaxed_then_window_runs_without_panic() {
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,30
O4,ENG,100
O5,ENG,200
O6,ENG,300
";
    let (counters, dlq, output) = run_pipeline(D7_PIPELINE, csv)
        .expect("post-aggregate-window pipeline must execute without error");
    assert_eq!(
        counters.dlq_count, 0,
        "no failure injected; pipeline must produce zero DLQ entries"
    );
    assert!(dlq.is_empty(), "no failure injected; DLQ must stay empty");

    // Parse the writer output; HR's total = 60, ENG's total = 600.
    // running_total over a 1-row partition equals total.
    let mut by_dept: std::collections::HashMap<String, std::collections::HashMap<String, String>> =
        std::collections::HashMap::new();
    let mut lines = output.lines();
    let header_line = lines.next().expect("header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let dept_idx = headers
        .iter()
        .position(|h| *h == "department")
        .expect("department column present");
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let v: Vec<&str> = line.split(',').collect();
        let row: std::collections::HashMap<String, String> = headers
            .iter()
            .zip(v.iter())
            .map(|(h, val)| ((*h).to_string(), (*val).to_string()))
            .collect();
        by_dept.insert(v[dept_idx].to_string(), row);
    }
    let hr = by_dept
        .get("HR")
        .expect("HR partition row reaches the writer");
    assert_eq!(hr.get("total").map(String::as_str), Some("60"));
    assert_eq!(
        hr.get("running_total").map(String::as_str),
        Some("60"),
        "HR running_total over a 1-row partition must equal HR total"
    );
    let eng = by_dept
        .get("ENG")
        .expect("ENG partition row reaches the writer");
    assert_eq!(eng.get("total").map(String::as_str), Some("600"));
    assert_eq!(
        eng.get("running_total").map(String::as_str),
        Some("600"),
        "ENG running_total over a 1-row partition must equal ENG total"
    );
}

/// Buffer-recompute under post-aggregate window: a downstream Transform
/// fails on the HR aggregate row; the orchestrator's recompute pass
/// reruns the affected window partition over `partition − retracted_rows`,
/// and the failing record's department is excluded from the writer
/// output. The CK fan-out resolution that decides which OTHER rows
/// (driver/collateral) reach the writer is exercised end-to-end by
/// `examples/pipelines/retract-demo` (covered by `retract_demo_smoke`);
/// this test pins the post-aggregate single-row exclusion contract.
#[test]
fn aggregate_relaxed_then_window_buffer_recompute_excludes_retracted_partition() {
    let yaml = r#"
pipeline:
  name: agg_then_window_buffer_recompute
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
    cxl: 'emit department = department

      emit total = sum(amount)

      '
- type: transform
  name: running
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
      emit ratio = 1 / (total - 60)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: running
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,30
O4,ENG,100
O5,ENG,200
O6,ENG,300
";
    let (counters, dlq, output) =
        run_pipeline(yaml, csv).expect("buffer-recompute pipeline must execute");

    // HR's aggregate output has total=60; the ratio's `1/(60-60)` divides
    // by zero. The orchestrator routes the failure through the
    // synthetic-CK fan-out path; HR is excluded.
    assert_eq!(
        counters.dlq_count, 1,
        "exactly one row (HR aggregate output) hits /0"
    );
    assert_eq!(dlq.len(), 1);

    // The DLQ trigger is the HR aggregate row carrying the synthetic
    // CK column.
    let trigger = &dlq[0];
    assert!(trigger.trigger, "the single DLQ entry is the trigger");
    assert!(
        trigger
            .original_record
            .values()
            .iter()
            .any(|v| matches!(v, clinker_record::Value::String(s) if s.as_ref() == "HR")),
        "trigger record must be the HR aggregate output row"
    );
    assert_eq!(trigger.error_message, "division by zero");
    assert!(
        trigger
            .original_record
            .schema()
            .contains("$ck.aggregate.dept_totals"),
        "trigger record must carry the synthetic CK column the relaxed \
         aggregate stamped at finalize"
    );

    // The synthetic CK lives outside partition_by, so the
    // orchestrator routed through the deferred-region commit pass.
    // Surviving lines (if any) MUST NOT include HR — its single-row
    // partition is empty after retraction.
    for line in output.lines().skip(1) {
        assert!(
            !line.starts_with("HR,"),
            "HR row must NOT reach the writer (DLQ'd; partition has \
             no surviving rows after retraction); got line: {line:?}"
        );
    }

    // The deferred-region commit pass runs `running` (the windowed
    // Transform) at least once per iteration: iteration 1 dispatches
    // every aggregate emit including HR's failing row, iteration 2
    // dispatches the post-retract emit set with HR removed. The
    // counter therefore exceeds zero and the determinism guarantee is
    // pinned by `post_aggregate_recompute_determinism`. The exclusion
    // shape the test pins (HR absent from writer, single DLQ trigger
    // carrying the synthetic CK) is independent of the per-iteration
    // dispatch count.
    assert!(
        counters.retraction.partitions_dispatched > 0,
        "the deferred-region commit pass must dispatch the windowed \
         Transform at least once across iterations; got {}",
        counters.retraction.partitions_dispatched
    );
}
