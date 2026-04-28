//! D-7 unblock: `Source (CK) → Aggregate (relaxed) → Window → Output`
//! compiles without E150.
//!
//! The synthetic `$ck.aggregate.<name>` column the relaxed aggregate
//! emits joins the post-aggregate `ck_set`. The downstream Transform's
//! analytic-window declares `partition_by` over user fields only — the
//! synthetic name is never listed in any window's `partition_by`. So
//! `derive_window_buffer_recompute_flags` flips the window's
//! `requires_buffer_recompute` flag (the `ck_outside_partition`
//! predicate fires on the synthetic name), and the E150 gate at
//! `config/mod.rs` lifts (the gate's comment notes "Buffer-recompute
//! mode lifts the E150 restriction"). The previously-rejected geometry
//! now compiles and produces a runnable plan.
//!
//! The companion file `correlated_post_aggregate_retract.rs` covers
//! the post-aggregate fan-out semantics for `Aggregate (relaxed) →
//! Transform → Output` (no analytic-window between the aggregate and
//! the failing Transform), which is the path runtime fully supports
//! today.
//!
//! Runtime windowed evaluation against post-aggregate input — the
//! analytic-window machinery's `SecondaryIndex` builds over the source
//! arena, so `$window.*` builtins evaluated at a Transform downstream
//! of an Aggregate read source-row fields, not aggregate-output fields
//! — is a separate workstream. Compile-time D-7 unblock (this commit)
//! is independent of that workstream: the planner's lattice walk and
//! the E150 gate behave correctly regardless of whether the runtime
//! supports the geometry yet.

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
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec()))
            as Box<dyn std::io::Read + Send>,
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

/// Runnable end-to-end: the D-7 geometry executes without panic, no
/// spurious DLQ entries, and emits a row per partition. The values
/// of `$window.*` builtins evaluated against post-aggregate input are
/// not the focus — that geometry's runtime semantics are a separate
/// workstream — but the buffer-recompute admission path, the
/// dispatcher, and the commit phase must all traverse cleanly.
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
    let (counters, dlq, output) =
        run_pipeline(D7_PIPELINE, csv).expect("D-7 pipeline must execute without error");
    assert_eq!(
        counters.dlq_count, 0,
        "no failure injected; pipeline must produce zero DLQ entries"
    );
    assert!(dlq.is_empty(), "no failure injected; DLQ must stay empty");
    assert!(
        output.contains("HR") && output.contains("ENG"),
        "both partitions must reach the writer; got:\n{output}"
    );
}
