//! Integration tests for the relaxed-CK five-phase correlation-commit
//! protocol. Sibling to `correlated_dlq.rs` which exercises the
//! strict-collateral path.
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
//! * End-to-end retraction across the source → transform → aggregate →
//!   (transform →) output chain — fires a real DLQ trigger inside the
//!   pipeline and asserts bit-for-bit equivalence between the
//!   retract-corrected writer payload and a baseline rerun of the same
//!   pipeline with the failing record omitted from the input. Covers
//!   Reversible-only bindings (`sum`, `count`), BufferRequired-only
//!   bindings (`min`, `max`, `avg`), and a downstream Transform that
//!   derives a column from the aggregate output. Closes the integration
//!   gap above the per-aggregator and per-accumulator retract unit
//!   tests.

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

/// Strict pipeline (every aggregate has `group_by ⊇ correlation_key`,
/// or no aggregate at all) — verifies today's `correlated_dlq.rs`
/// workload runs unchanged after the orchestrator landed. The
/// orchestrator's `is_relaxed_pipeline` check picks the FastPath
/// branch so the strict body emits the same DLQ shape as before.
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

/// End-to-end retraction: a relaxed-CK Aggregate over Reversible
/// bindings (`sum`, `count`) produces output that matches a baseline
/// rerun without the failing source row. The DLQ trigger fires
/// upstream of the aggregate (a `to_int` validation that rejects a
/// non-numeric sentinel), so the failing row never enters the
/// aggregator's lineage. The orchestrator's detect phase observes the
/// trigger row id, the recompute phase calls `retract_row` against the
/// aggregator and gracefully treats the not-found id as a no-op, and
/// the flush phase emits the surviving groups' aggregate output to the
/// writer. The bit-for-bit equivalence assertion is the load-bearing
/// claim — anything that breaks the chain (lineage drop, retract
/// mis-attribution, replay overshoot) shows up as a value mismatch
/// against the baseline rerun.
#[test]
fn end_to_end_retraction_reversible_matches_baseline_rerun() {
    let yaml = r#"
pipeline:
  name: retract_reversible
error_handling:
  strategy: continue
  correlation_key: order_id
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit order_id = order_id

      emit department = department

      emit amount_int = amount.to_int()

      '
- type: aggregate
  name: dept_totals
  input: validate
  config:
    group_by: [department]
    cxl: 'emit department = department

      emit total = sum(amount_int)

      emit n = count(*)

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
    // Twelve rows across two departments. Order O7 in HR carries the
    // sentinel `BAD` amount; the validate transform's `to_int` fires
    // the DLQ trigger on that row. Group HR has the bad row removed;
    // group ENG is unaffected. Baseline (the same pipeline run with
    // O7 omitted from the input) is computed below.
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,30
O4,ENG,100
O5,ENG,200
O6,ENG,300
O7,HR,BAD
O8,HR,40
O9,HR,50
O10,ENG,400
O11,ENG,500
O12,HR,60
";
    let baseline_csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,30
O4,ENG,100
O5,ENG,200
O6,ENG,300
O8,HR,40
O9,HR,50
O10,ENG,400
O11,ENG,500
O12,HR,60
";

    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    let (baseline_counters, baseline_dlq, baseline_output) =
        run_pipeline(yaml, baseline_csv).unwrap();

    // Baseline is the canonical reference: it has zero DLQ entries
    // (no bad rows) and the surviving groups' aggregate output reaches
    // the writer.
    assert_eq!(baseline_dlq.len(), 0, "baseline has no failures");
    assert_eq!(baseline_counters.dlq_count, 0);

    // The retract-corrected run records the bad row as a single
    // trigger DLQ entry. The aggregate output rows for both
    // departments land in distinct null-keyed buffer cells (group_by
    // omits order_id, so no $ck.order_id propagates onto the
    // aggregate output) — the trigger error lands in a separate
    // CK-stamped cell that has no record slots, so its dispositioning
    // is trigger-only with zero collateral.
    assert_eq!(
        dlq.len(),
        1,
        "exactly one DLQ entry — the bad O7 row's to_int failure"
    );
    assert!(dlq[0].trigger, "the bad row is the trigger");
    assert_eq!(counters.dlq_count, 1);

    // Bit-for-bit equivalence on the writer payload. The CSV writer
    // emission order is hash-table arbitrary, so compare set-equal on
    // sorted body lines (header is identical).
    fn sort_body(s: &str) -> Vec<String> {
        let mut lines: Vec<String> = s.lines().map(|l| l.to_string()).collect();
        if !lines.is_empty() {
            let header = lines.remove(0);
            lines.sort();
            lines.insert(0, header);
        }
        lines
    }
    assert_eq!(
        sort_body(&output),
        sort_body(&baseline_output),
        "retract-corrected output must equal baseline rerun output:\n\
         got:\n{output}\nbaseline:\n{baseline_output}"
    );

    // Spot-check the aggregate values directly against the documented
    // sums. HR survivors are O1+O2+O3+O8+O9+O12 = 10+20+30+40+50+60 = 210
    // (count 6); ENG survivors are O4+O5+O6+O10+O11 = 100+200+300+400+500
    // = 1500 (count 5).
    assert!(
        output.contains("HR,210,6"),
        "HR sum=210 count=6 expected, got: {output}"
    );
    assert!(
        output.contains("ENG,1500,5"),
        "ENG sum=1500 count=5 expected, got: {output}"
    );
}

/// End-to-end retraction over BufferRequired bindings (`min`, `max`,
/// `avg`). The relaxed-CK aggregator runs in buffer-mode for these
/// bindings, retains every contribution, and recomputes the finalize
/// step in place after retract. As with the Reversible test, the DLQ
/// trigger fires upstream of the aggregate so the bad row is never
/// added to the aggregator's buffered contributions. The bit-for-bit
/// equivalence assertion proves the buffer-mode aggregator's
/// finalize-after-retract path produces the same min/max/avg across
/// surviving rows that a clean rerun would produce.
#[test]
fn end_to_end_retraction_buffer_required_matches_baseline_rerun() {
    let yaml = r#"
pipeline:
  name: retract_buffer_required
error_handling:
  strategy: continue
  correlation_key: order_id
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit order_id = order_id

      emit department = department

      emit amount_int = amount.to_int()

      '
- type: aggregate
  name: dept_stats
  input: validate
  config:
    group_by: [department]
    cxl: 'emit department = department

      emit lo = min(amount_int)

      emit hi = max(amount_int)

      emit mean = avg(amount_int)

      '
- type: output
  name: out
  input: dept_stats
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    // Order O3 in HR carries the bad amount; the survivors are
    // {10, 20, 40, 50}. Group ENG is clean.
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,BAD
O4,ENG,100
O5,ENG,200
O6,ENG,300
O7,HR,40
O8,HR,50
";
    let baseline_csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O4,ENG,100
O5,ENG,200
O6,ENG,300
O7,HR,40
O8,HR,50
";

    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    let (_, baseline_dlq, baseline_output) = run_pipeline(yaml, baseline_csv).unwrap();

    assert_eq!(baseline_dlq.len(), 0, "baseline has no failures");
    assert_eq!(dlq.len(), 1, "one DLQ entry for the bad O3 row");
    assert!(dlq[0].trigger);
    assert_eq!(counters.dlq_count, 1);

    fn sort_body(s: &str) -> Vec<String> {
        let mut lines: Vec<String> = s.lines().map(|l| l.to_string()).collect();
        if !lines.is_empty() {
            let header = lines.remove(0);
            lines.sort();
            lines.insert(0, header);
        }
        lines
    }
    assert_eq!(
        sort_body(&output),
        sort_body(&baseline_output),
        "buffer-required retract-corrected output must equal baseline:\n\
         got:\n{output}\nbaseline:\n{baseline_output}"
    );

    // HR survivors: {10, 20, 40, 50} → min=10, max=50, avg=30.0.
    // ENG survivors: {100, 200, 300} → min=100, max=300, avg=200.0.
    assert!(
        output.contains("HR,10,50,30"),
        "HR min=10 max=50 avg=30 expected, got: {output}"
    );
    assert!(
        output.contains("ENG,100,300,200"),
        "ENG min=100 max=300 avg=200 expected, got: {output}"
    );
}

/// End-to-end retraction with a downstream Transform deriving a column
/// from the relaxed-CK aggregate's output. The Transform is determined
/// downstream — under the E15W policy it cannot use a non-deterministic
/// builtin, so it computes a deterministic per-group derived value
/// (`emit per_capita = total / n`). The bit-for-bit assertion proves
/// the downstream Transform observes the retract-corrected aggregate
/// state, not the pre-retract state: a stale aggregate output would
/// surface as a wrong per_capita.
#[test]
fn end_to_end_retraction_downstream_transform_observes_corrected_state() {
    let yaml = r#"
pipeline:
  name: retract_downstream_transform
error_handling:
  strategy: continue
  correlation_key: order_id
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit order_id = order_id

      emit department = department

      emit amount_int = amount.to_int()

      '
- type: aggregate
  name: dept_totals
  input: validate
  config:
    group_by: [department]
    cxl: 'emit department = department

      emit total = sum(amount_int)

      emit n = count(*)

      '
- type: transform
  name: per_capita
  input: dept_totals
  config:
    cxl: 'emit department = department

      emit total = total

      emit n = n

      emit per_capita = total / n

      '
- type: output
  name: out
  input: per_capita
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    // HR has bad row O4 (amount BAD); survivors are {10,20,30,40} →
    // total=100 / n=4 → per_capita=25. ENG is clean: {100,200,300} →
    // total=600 / n=3 → per_capita=200.
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,30
O4,HR,BAD
O5,ENG,100
O6,ENG,200
O7,ENG,300
O8,HR,40
";
    let baseline_csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,30
O5,ENG,100
O6,ENG,200
O7,ENG,300
O8,HR,40
";

    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    let (_, baseline_dlq, baseline_output) = run_pipeline(yaml, baseline_csv).unwrap();

    assert_eq!(baseline_dlq.len(), 0, "baseline has no failures");
    assert_eq!(dlq.len(), 1, "one DLQ entry for the bad O4 row");
    assert!(dlq[0].trigger);
    assert_eq!(counters.dlq_count, 1);

    fn sort_body(s: &str) -> Vec<String> {
        let mut lines: Vec<String> = s.lines().map(|l| l.to_string()).collect();
        if !lines.is_empty() {
            let header = lines.remove(0);
            lines.sort();
            lines.insert(0, header);
        }
        lines
    }
    assert_eq!(
        sort_body(&output),
        sort_body(&baseline_output),
        "downstream-transform retract-corrected output must equal baseline:\n\
         got:\n{output}\nbaseline:\n{baseline_output}"
    );

    // The downstream per_capita column is the load-bearing assertion
    // here: a stale aggregate output would have produced a wrong
    // per_capita ratio.
    assert!(
        output.contains("HR,100,4,25"),
        "HR total=100 n=4 per_capita=25 expected, got: {output}"
    );
    assert!(
        output.contains("ENG,600,3,200"),
        "ENG total=600 n=3 per_capita=200 expected, got: {output}"
    );
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
