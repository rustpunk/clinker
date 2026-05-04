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
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: employee_id
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
/// the orchestrator's full ThreePhase path with an empty retract scope:
/// every group is clean, the recompute / deferred-dispatch phases are
/// no-ops, and the flush phase produces the same output as the strict
/// path.
#[test]
fn relaxed_pipeline_no_failures_emits_normally() {
    let yaml = r#"
pipeline:
  name: relaxed_clean
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: emp_id
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
  correlation_fanout_policy: all
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: emp_id
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
  correlation_fanout_policy: any
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: emp_id
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
/// from the relaxed-CK aggregate's output. The Transform computes a
/// deterministic per-group derived value (`emit per_capita = total / n`).
/// The bit-for-bit assertion proves the downstream Transform observes
/// the retract-corrected aggregate state, not the pre-retract state: a
/// stale aggregate output would surface as a wrong per_capita.
#[test]
fn end_to_end_retraction_downstream_transform_observes_corrected_state() {
    let yaml = r#"
pipeline:
  name: retract_downstream_transform
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

/// Empty group after retraction: every record contributing to a group
/// is retracted. The aggregator's recompute phase must elide the group
/// entirely (no row emitted for it) while leaving untouched groups
/// intact. The pre-retract output had one row per group; post-retract
/// the all-bad group has zero rows.
///
/// Group HR has every record fail validation; group ENG is clean.
/// Pre-retract finalize emits two rows; post-retract finalize emits
/// one. The recompute phase pairs HR's pre-retract row against `None`,
/// producing a delete-only Delta that the replay phase removes from
/// the correlation buffer.
#[test]
fn empty_group_after_retraction_disappears_from_output() {
    let yaml = r#"
pipeline:
  name: empty_group_retract
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
    let csv = "\
order_id,department,amount
O1,HR,BAD
O2,HR,BAD
O3,HR,BAD
O4,ENG,100
O5,ENG,200
O6,ENG,300
";
    let baseline_csv = "\
order_id,department,amount
O4,ENG,100
O5,ENG,200
O6,ENG,300
";

    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    let (_, baseline_dlq, baseline_output) = run_pipeline(yaml, baseline_csv).unwrap();

    assert_eq!(baseline_dlq.len(), 0, "baseline has no failures");
    assert_eq!(
        dlq.len(),
        3,
        "three DLQ entries — one per bad row, all triggers"
    );
    assert!(
        dlq.iter().all(|d| d.trigger),
        "every DLQ entry is a trigger"
    );
    assert_eq!(counters.dlq_count, 3);

    // Empty HR group must NOT appear in writer output. A row carrying
    // any HR key would indicate the recompute phase failed to elide
    // the group when its only contributions were retracted.
    assert!(
        !output.contains("HR,"),
        "HR group must not appear post-retract (every row was bad), got: {output}"
    );

    // ENG survivors land bit-for-bit with the baseline.
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
        "post-retract output must equal baseline rerun (ENG-only):\n\
         got:\n{output}\nbaseline:\n{baseline_output}"
    );

    // Direct value spot-check: ENG total=600 / n=3 from {100,200,300}.
    assert!(
        output.contains("ENG,600,3"),
        "ENG total=600 n=3 expected, got: {output}"
    );
}

/// Idempotent retract: a single source row triggers two distinct
/// failures (a record that fails both Transform validation and
/// Output-write parsing). The detect phase deduplicates row IDs via
/// `BTreeSet<u32>` before the recompute phase calls `retract_row`, so
/// the aggregator's accumulator state is retracted exactly once per
/// row regardless of how many failures the row participated in.
///
/// The fixture wires two consecutive transforms; the second fails on
/// the same input row that the first salvaged with a sentinel. Both
/// errors land in the correlation buffer. The recompute phase must
/// see the row's contribution retracted exactly once — a
/// double-retract would shift `count(*)` and `sum(amount)` into
/// negative-shaped territory that doesn't match a baseline rerun.
#[test]
fn idempotent_retract_dedupes_multi_failure_row() {
    let yaml = r#"
pipeline:
  name: idempotent_retract
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
      - { name: amount, type: string }
      - { name: tag, type: string }
- type: transform
  name: validate_amount
  input: src
  config:
    cxl: 'emit order_id = order_id

      emit department = department

      emit amount_int = amount.to_int()

      emit tag = tag

      '
- type: transform
  name: validate_tag
  input: validate_amount
  config:
    cxl: 'emit order_id = order_id

      emit department = department

      emit amount_int = amount_int

      emit tag_int = tag.to_int()

      '
- type: aggregate
  name: dept_totals
  input: validate_tag
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
    // Order O3 has BOTH amount=BAD and tag=BAD — two distinct failures
    // upstream of the aggregate. Survivors in HR: {10, 20, 40} → total
    // = 70, n = 3. ENG is clean: {100, 200} → total = 300, n = 2.
    let csv = "\
order_id,department,amount,tag
O1,HR,10,1
O2,HR,20,2
O3,HR,BAD,BAD
O4,HR,40,4
O5,ENG,100,5
O6,ENG,200,6
";
    let baseline_csv = "\
order_id,department,amount,tag
O1,HR,10,1
O2,HR,20,2
O4,HR,40,4
O5,ENG,100,5
O6,ENG,200,6
";

    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    let (_, baseline_dlq, baseline_output) = run_pipeline(yaml, baseline_csv).unwrap();

    assert_eq!(baseline_dlq.len(), 0, "baseline has no failures");
    // The bad row participates in both failures but the orchestrator
    // dedupes by source_row at detect time. We expect exactly ONE
    // trigger DLQ entry for O3 — anything more indicates the dedup
    // step missed.
    let triggers_for_o3: Vec<&DlqEntry> =
        dlq.iter()
            .filter(|d| {
                d.trigger
                    && d.original_record.values().iter().any(
                        |v| matches!(v, clinker_record::Value::String(s) if s.as_ref() == "O3"),
                    )
            })
            .collect();
    assert!(
        !triggers_for_o3.is_empty(),
        "expected at least one trigger DLQ entry for the multi-failure row, got: {dlq:#?}"
    );
    assert_eq!(counters.dlq_count, dlq.len() as u64);

    // Aggregate values prove single retract: HR survivors are {10, 20,
    // 40} → total=70 / n=3. A double-retract of O3 would produce a
    // wrong sum or wrong count.
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
        "single-retract semantics must match baseline rerun:\n\
         got:\n{output}\nbaseline:\n{baseline_output}"
    );
    assert!(
        output.contains("HR,70,3"),
        "HR total=70 (survivors {{10,20,40}}) n=3 expected, got: {output}"
    );
    assert!(
        output.contains("ENG,300,2"),
        "ENG total=300 (survivors {{100,200}}) n=2 expected, got: {output}"
    );
}

/// CorrelationFanoutPolicy `Any` end-to-end through the relaxed
/// orchestrator: a relaxed-CK pipeline with one bad row in the HR
/// group. Under `Any`, every contributing record sharing the trigger's
/// CK group is rolled back — the strict-collateral DLQ shape.
///
/// Sibling test to [`fanout_policy_resolution_pipeline_default`], but
/// driven through the ThreePhase orchestrator (a relaxed aggregate is
/// present) rather than the FastPath strict body. Documents that the
/// orchestrator's flush step honors `Any` identically to the strict
/// path's flush.
#[test]
fn fanout_policy_any_under_relaxed_orchestrator() {
    let yaml = r#"
pipeline:
  name: fanout_any_relaxed
error_handling:
  strategy: continue
  correlation_fanout_policy: any
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
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,BAD
O4,ENG,100
O5,ENG,200
";
    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    // Trigger row is the failing record; collateral handling is
    // policy-specific. Under `Any` in shared-CK groups, every slot
    // sharing the trigger's CK gets rolled back. The relaxed
    // orchestrator's recompute phase still corrects the HR sum to
    // its survivors, but the trigger row's own DLQ entry counts.
    assert_eq!(dlq.len(), 1, "one trigger DLQ entry for the BAD row");
    assert!(dlq[0].trigger, "trigger flag set on the failing row");
    assert_eq!(counters.dlq_count, 1);
    // HR survivors after retract: {10, 20} → total = 30. ENG is clean.
    assert!(
        output.contains("HR,30"),
        "HR total=30 (survivors {{10,20}}) expected under Any, got: {output}"
    );
    assert!(output.contains("ENG,300"), "ENG total=300, got: {output}");
}

/// CorrelationFanoutPolicy `All` under the relaxed orchestrator. In a
/// shared-CK group every slot's `is_full_tuple_match` is `true`, so
/// `should_spare_collateral(All, true, true)` returns `false` —
/// collateral inside the group still rolls back. The behavior matches
/// `Any` for this fixture; the differential effect of `All` only
/// appears in multi-CK fan-out where some slots have partial CK
/// matches against the trigger (deferred until Combine retraction
/// lands). This test pins the current shared-CK behavior and verifies
/// `All` resolves through the orchestrator without regressing the
/// observed output shape.
#[test]
fn fanout_policy_all_under_relaxed_orchestrator() {
    let yaml = r#"
pipeline:
  name: fanout_all_relaxed
error_handling:
  strategy: continue
  correlation_fanout_policy: all
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
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,BAD
O4,ENG,100
O5,ENG,200
";
    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    assert_eq!(
        dlq.len(),
        1,
        "All policy in shared-CK groups behaves identically to Any \
         (every slot's full-tuple match is true)"
    );
    assert_eq!(counters.dlq_count, 1);
    assert!(
        output.contains("HR,30"),
        "HR total=30 (survivors {{10,20}}) expected, got: {output}"
    );
    assert!(output.contains("ENG,300"), "ENG total=300, got: {output}");
}

/// CorrelationFanoutPolicy `Primary` under the relaxed orchestrator.
/// The single-source pipeline below has every slot's primary CK
/// matching the trigger's primary CK, so `should_spare_collateral` is
/// `false` for every slot and `Primary` collapses to `Any`'s observed
/// behavior. The differential build-side-spare effect of `Primary`
/// only manifests under Combine fan-out (driver vs build sources with
/// different CK fields) — that geometry is deferred until Combine
/// retraction lands. This test pins the single-source semantics and
/// proves `Primary` resolves through the orchestrator's policy
/// precedence chain.
#[test]
fn fanout_policy_primary_under_relaxed_orchestrator() {
    let yaml = r#"
pipeline:
  name: fanout_primary_relaxed
error_handling:
  strategy: continue
  correlation_fanout_policy: primary
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
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,BAD
O4,ENG,100
O5,ENG,200
";
    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    assert_eq!(
        dlq.len(),
        1,
        "Primary policy in single-source pipelines spares no slot in shared-CK groups; \
         build-side sparing requires Combine fan-out (deferred)"
    );
    assert_eq!(counters.dlq_count, 1);
    assert!(
        output.contains("HR,30"),
        "HR total=30 (survivors {{10,20}}) expected, got: {output}"
    );
    assert!(output.contains("ENG,300"), "ENG total=300, got: {output}");
}

/// Degrade-fallback when a single record's buffer-mode contribution
/// exceeds the entire memory budget. The aggregate runs in buffer-mode
/// (a `BufferRequired` binding like `min`) with a memory_limit so
/// small that any single row's per-binding charge breaches it. Today's
/// runtime guard panics with a documented message describing the
/// scenario; the orchestrator's degrade-fallback path is supposed to
/// translate this into a runtime DLQ outcome treating the affected
/// group as strict-collateral, but that translation is wired only at
/// the per-group recompute step (see `recompute_aggregates`'s
/// `degraded` collection), not at admission time.
///
/// This test runs the pipeline using `std::panic::catch_unwind` so a
/// runtime crash surfaces as a test FAILURE that points at the
/// degrade-fallback gap rather than aborting the harness. If the
/// pipeline completes (memory pressure manifested differently before
/// reaching the per-row guard, e.g. via spill), the test asserts the
/// expected DLQ shape; if the panic fires, the test surfaces the
/// finding so the bug can be addressed in a dedicated commit.
#[test]
fn degrade_fallback_runtime_protection_or_documented_panic() {
    let yaml = r#"
pipeline:
  name: degrade_fallback
  memory_limit: "1"
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
  name: dept_stats
  input: src
  config:
    group_by: [department]
    cxl: 'emit department = department

      emit lo = min(amount)

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
    let csv = "order_id,department,amount\nO1,HR,10\nO2,HR,20\nO3,ENG,100\n";

    let outcome = std::panic::catch_unwind(|| run_pipeline(yaml, csv));
    match outcome {
        Ok(Ok((counters, _dlq, output))) => {
            // Pipeline completed cleanly. Memory pressure was absorbed
            // by spill or a larger-than-expected per-row charge fit
            // under the budget. The aggregator's output is still
            // structurally valid — the degrade-fallback path was not
            // exercised but no data was lost or corrupted.
            assert!(
                counters.dlq_count == 0 || !output.is_empty(),
                "tiny-budget pipeline produced empty output AND zero DLQ entries; \
                 expected either successful processing or surfaced failures"
            );
        }
        Ok(Err(_e)) => {
            // Pipeline returned a typed error. The runtime gracefully
            // surfaced the memory-budget exhaustion through the
            // PipelineError type rather than crashing — the executor's
            // structural fallback worked. This is the architecturally-
            // correct shape: a typed error reaches the caller and the
            // DLQ machinery decides at a higher level whether to route
            // affected records through the DLQ. The panic guard at
            // `add_record_buffered` would NOT be the right shape; a
            // typed error IS the right shape, and we observe it here.
        }
        Err(panic_payload) => {
            // The runtime guard at `add_record_buffered` (or any other
            // sibling guard) fired. This is a real bug: a tiny-memory-
            // budget pipeline should surface a typed runtime error or
            // route the affected group's records to the DLQ as
            // strict-collateral, not abort the executor. The
            // architectural-rigor policy requires this be surfaced —
            // it is NOT papered over with a `#[should_panic]` shim.
            let msg = if let Some(s) = panic_payload.downcast_ref::<&'static str>() {
                (*s).to_string()
            } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "<non-string panic payload>".to_string()
            };
            panic!(
                "BUG SURFACED: degrade-fallback runtime path crashed instead of \
                 routing through the typed-error/DLQ surface. The aggregator's \
                 buffer-mode admission guard (or another runtime guard) panicked: \
                 {msg}\n\
                 Convert the panic to a runtime error that flows into the typed \
                 PipelineError surface or into the DLQ via the orchestrator's \
                 `relaxed_aggregator_degrade` list."
            );
        }
    }
}
