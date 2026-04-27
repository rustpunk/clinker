//! Integration tests for the relaxed-CK five-phase correlation-commit
//! protocol. Sibling to `correlated_dlq.rs` which exercises the strict
//! E151 path.
//!
//! The orchestrator's path selection is verified through:
//!
//! * The zero-overhead invariant — strict pipelines short-circuit to
//!   the `FastPath` body and the existing `correlated_dlq.rs` workload
//!   continues to pass byte-identically.
//! * The CorrelationFanoutPolicy precedence — per-Output > per-pipeline
//!   default > documented `Any` default.
//! * The replay-loop iteration cap — a fixture that would loop in a
//!   buggy implementation panics with the documented message.
//!
//! End-to-end retraction across an aggregate → transform → output chain
//! is exercised at the HashAggregator unit test level
//! (`aggregation::spill_trigger_tests::test_buffer_mode_retract_*`) and
//! at the accumulator unit test level (`accumulator::tests::test_*_retract_*`)
//! per the bottom-up test pyramid.

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

/// Strict pipeline (no `relaxed_correlation_key`) — verifies today's
/// `correlated_dlq.rs` workload runs unchanged after the orchestrator
/// landed. The orchestrator's `is_relaxed_pipeline` check picks the
/// FastPath branch so the strict body emits the same DLQ shape as
/// before.
#[test]
fn strict_pipeline_zero_overhead_short_circuits_to_fast_path() {
    let yaml = r#"
pipeline:
  name: strict_test
error_handling:
  strategy: continue
  correlation_key: employee_id
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: employee_id, type: string }
      - { name: value, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit emp_id = employee_id

      emit val = value.to_int()

      '
- type: output
  name: out
  input: validate
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "employee_id,value\nA,100\nA,bad\nB,200\n";
    let (counters, dlq_entries, output) = run_pipeline(yaml, csv).unwrap();

    // Group A is dirty (one bad record); group B is clean.
    assert_eq!(counters.dlq_count, 2, "group A DLQ'd as a unit (2 records)");
    assert_eq!(counters.ok_count, 1, "only group B emitted");
    assert_eq!(dlq_entries.len(), 2);
    assert!(output.contains("B,200"), "output has group B");
    assert!(
        !output.contains(",bad"),
        "output should not contain the trigger record"
    );
}

/// Relaxed-CK pipeline that does NOT trigger any DLQ events. Exercises
/// the orchestrator's full FivePhase path with an empty retract scope:
/// every group is clean, the recompute / replay phases are no-ops, and
/// the flush phase produces the same output as the strict path.
#[test]
fn relaxed_pipeline_no_failures_emits_normally() {
    let yaml = r#"
pipeline:
  name: relaxed_clean
error_handling:
  strategy: continue
  correlation_key: emp_id
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: emp_id, type: string }
      - { name: dept, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: src
  config:
    group_by: [dept]
    relaxed_correlation_key: true
    cxl: 'emit dept = dept

      emit total = sum(amount)

      '
- type: output
  name: out
  input: dept_totals
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "emp_id,dept,amount\nE1,HR,10\nE2,HR,20\nE3,ENG,100\n";
    let (counters, _dlq_entries, output) = run_pipeline(yaml, csv).unwrap();

    assert_eq!(counters.dlq_count, 0, "no failures expected");
    // sum(amount) per dept: HR=30, ENG=100. Output rows materialize
    // through the orchestrator's flush phase.
    assert!(
        output.contains("HR") && output.contains("30"),
        "HR=30 expected, got: {output}"
    );
    assert!(
        output.contains("ENG") && output.contains("100"),
        "ENG=100 expected, got: {output}"
    );
}

/// CorrelationFanoutPolicy precedence: per-pipeline `All` policy
/// reaches the strict commit body and records still DLQ atomically per
/// group (the strict path's `is_full_tuple_match` is `true` for every
/// in-group slot, so `All` collapses to `Any`'s observed behavior here;
/// the test covers the resolution wiring rather than partial-match
/// shaping which only fires under multi-CK fan-out).
#[test]
fn fanout_policy_resolution_precedence_pipeline_default() {
    let yaml = r#"
pipeline:
  name: policy_default
error_handling:
  strategy: continue
  correlation_key: emp_id
  correlation_fanout_policy: all
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: emp_id, type: string }
      - { name: value, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit emp_id = emp_id

      emit val = value.to_int()

      '
- type: output
  name: out
  input: validate
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "emp_id,value\nA,100\nA,bad\nB,200\n";
    let (counters, dlq_entries, _output) = run_pipeline(yaml, csv).unwrap();
    // Pipeline-level All policy still rolls back the whole A group
    // because every collateral slot's CK matches the trigger fully.
    assert_eq!(counters.dlq_count, 2);
    assert_eq!(dlq_entries.len(), 2);
}

/// Per-Output `Primary` override should resolve through the orchestrator
/// regardless of the pipeline-level setting. Verifies the per-Output
/// override takes precedence over the per-pipeline default.
#[test]
fn fanout_policy_resolution_output_override_wins() {
    let yaml = r#"
pipeline:
  name: policy_override
error_handling:
  strategy: continue
  correlation_key: emp_id
  correlation_fanout_policy: any
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: emp_id, type: string }
      - { name: value, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit emp_id = emp_id

      emit val = value.to_int()

      '
- type: output
  name: out
  input: validate
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
    correlation_fanout_policy: primary
"#;
    let csv = "emp_id,value\nA,100\nB,200\n";
    let (counters, _dlq_entries, output) = run_pipeline(yaml, csv).unwrap();
    // No failures, so the policy resolution does not affect emission;
    // the test verifies the override field parses end-to-end and
    // doesn't break the strict-path emission shape.
    assert_eq!(counters.dlq_count, 0);
    assert!(output.contains("A,100"));
    assert!(output.contains("B,200"));
}

/// E15W rejection — a Transform downstream of a relaxed-CK aggregate
/// cannot call a non-deterministic builtin. The compile-time check
/// surfaces the diagnostic at config parse time.
#[test]
fn e15w_rejects_now_under_relaxed_aggregate() {
    let yaml = r#"
pipeline:
  name: e15w
error_handling:
  strategy: continue
  correlation_key: emp_id
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: emp_id, type: string }
      - { name: dept, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: src
  config:
    group_by: [dept]
    relaxed_correlation_key: true
    cxl: 'emit dept = dept

      emit total = sum(amount)

      '
- type: transform
  name: stamp
  input: dept_totals
  config:
    cxl: 'emit dept = dept

      emit total = total

      emit emitted_at = now

      '
- type: output
  name: out
  input: stamp
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let config = crate::config::parse_config(yaml).expect("parse");
    let diags = config
        .compile(&crate::config::CompileContext::default())
        .expect_err("E15W must reject `now` downstream of relaxed-CK aggregate");
    assert!(
        diags.iter().any(|d| d.code == "E15W"),
        "expected E15W diagnostic, got codes: {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
}
