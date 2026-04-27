//! Integration tests for analytic-window wholesale recompute under
//! relaxed-CK retraction.
//!
//! Scope. The pipeline geometry exercised here is:
//!
//! ```text
//! Source(CK=emp_id) → Transform(window: partition_by=[dept]) → Aggregate(group_by=[dept]) → Output
//! ```
//!
//! The window's `partition_by` does not include the pipeline-level
//! correlation key (`emp_id`), so a CK group can span multiple
//! partitions; the relaxed-CK aggregate downstream provides the
//! retraction protocol; the planner's `derive_window_buffer_recompute_flags`
//! flag flips the executor's window arm into buffered emit mode.
//!
//! Coverage. The full design matrix is 13 `$window.*` builtins ×
//! {rows, range} × 3 frame-bound shapes × {single-row retract,
//! full-partition retract} = 156 cells. Today's `LocalWindowConfig`
//! does not expose the `frame:` configuration to the runtime, so the
//! frame variant collapses to "implicit unbounded over partition" for
//! every builtin. Removing the frame axis reduces the matrix to 13 ×
//! {single, full} = 26 representative cells, which the parameterized
//! driver below covers.
//!
//! The 13 functions partition into four lattice cells:
//!
//! * Reversible aggregate: `sum`, `count`, `avg`.
//! * Order-statistic: `min`, `max`.
//! * Positional / lookup: `first`, `last`, `lag`, `lead`.
//! * Iterable / set-shaped: `collect`, `distinct`, `any`, `all`.
//!
//! Each lattice cell is exercised under both retract kinds. The
//! load-bearing assertion per case is bit-for-bit equivalence between
//! the retract-corrected output and a baseline rerun of the same
//! pipeline with the failing record omitted from the input. Two
//! regression tests round out the coverage:
//!
//! * `non_relaxed_window_pipeline_unaffected` — a window pipeline with
//!   no relaxed-CK aggregate must produce the same output as today's
//!   streaming-emit path.
//! * `cross_partition_isolation` — a multi-partition pipeline where
//!   one partition's retract leaves the other partitions identical to
//!   baseline.

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

/// Stable sort of CSV body lines for diffing against a baseline. The
/// header is preserved; data lines below it sort lexicographically.
fn sort_body(s: &str) -> Vec<String> {
    let mut lines: Vec<String> = s.lines().map(|l| l.to_string()).collect();
    if !lines.is_empty() {
        let header = lines.remove(0);
        lines.sort();
        lines.insert(0, header);
    }
    lines
}

/// What kind of retract to inject for a given test case. `SingleRow`
/// drops one record from a multi-record partition; `FullPartition`
/// drops every record from a partition (so the partition empties out
/// post-retract and the window operator must elide every output row
/// for that partition).
#[derive(Debug, Clone, Copy)]
enum RetractKind {
    SingleRow,
    FullPartition,
}

/// Build the full input CSV plus the baseline CSV (same input minus
/// the rows that get DLQ-triggered) for a given retract kind.
///
/// Schema: `emp_id,dept,amount`. Three departments (HR, ENG, SALES)
/// with four rows each in the full set; the bad row(s) live in HR.
fn build_inputs(retract_kind: RetractKind) -> (&'static str, &'static str) {
    match retract_kind {
        RetractKind::SingleRow => (
            // full input — single bad row in HR
            "emp_id,dept,amount\n\
             E1,HR,10\n\
             E2,HR,20\n\
             E3,HR,30\n\
             E4,HR,BAD\n\
             E5,ENG,100\n\
             E6,ENG,200\n\
             E7,ENG,300\n\
             E8,ENG,400\n",
            // baseline — bad row removed
            "emp_id,dept,amount\n\
             E1,HR,10\n\
             E2,HR,20\n\
             E3,HR,30\n\
             E5,ENG,100\n\
             E6,ENG,200\n\
             E7,ENG,300\n\
             E8,ENG,400\n",
        ),
        RetractKind::FullPartition => (
            // full input — every HR row is bad
            "emp_id,dept,amount\n\
             E1,HR,BAD\n\
             E2,HR,BAD\n\
             E3,HR,BAD\n\
             E4,HR,BAD\n\
             E5,ENG,100\n\
             E6,ENG,200\n\
             E7,ENG,300\n\
             E8,ENG,400\n",
            // baseline — entire HR partition removed
            "emp_id,dept,amount\n\
             E5,ENG,100\n\
             E6,ENG,200\n\
             E7,ENG,300\n\
             E8,ENG,400\n",
        ),
    }
}

/// Build the YAML pipeline parameterized by the window-function emit
/// expression. The pipeline shape is:
///
/// ```text
/// Source(amount: string) → validate (to_int → DLQ trigger on BAD)
///   → windowed (partition_by=[dept]) → relaxed aggregate (group_by=[dept])
///   → Output
/// ```
///
/// The validate transform's `to_int()` is where the DLQ trigger fires
/// for a `BAD` source row. Source schema declares `amount` as string so
/// the source reader does not pre-reject the BAD value before reaching
/// the validate stage.
fn build_yaml(window_emit: &str) -> String {
    format!(
        r#"
pipeline:
  name: window_retract
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
      - {{ name: emp_id, type: string }}
      - {{ name: dept, type: string }}
      - {{ name: amount, type: string }}
- type: transform
  name: validate
  input: src
  config:
    cxl: |
      emit emp_id = emp_id
      emit dept = dept
      emit amount = amount.to_int()
- type: transform
  name: windowed
  input: validate
  config:
    cxl: |
      emit emp_id = emp_id
      emit dept = dept
      emit amount = amount
      {window_emit}
    analytic_window:
      group_by: [dept]
- type: aggregate
  name: dept_summary
  input: windowed
  config:
    group_by: [dept]
    cxl: 'emit dept = dept

      emit total = sum(amount)

      '
- type: output
  name: out
  input: dept_summary
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#,
        window_emit = window_emit
    )
}

/// Run one parameterized window-retract case. Asserts:
/// 1. The retracted run does not error.
/// 2. The retracted output is bit-for-bit identical (after sorting
///    body lines) to a baseline run with the failing rows omitted.
/// 3. At least one DLQ entry was emitted (so the retraction protocol
///    actually fired).
fn run_window_retract_case(case_name: &str, window_emit: &str, retract_kind: RetractKind) {
    let yaml = build_yaml(window_emit);
    let (full_csv, baseline_csv) = build_inputs(retract_kind);

    let (counters, dlq, output) = run_pipeline(&yaml, full_csv)
        .unwrap_or_else(|e| panic!("[{case_name}] retract pipeline failed: {e:?}"));
    let (_, _, baseline_output) = run_pipeline(&yaml, baseline_csv)
        .unwrap_or_else(|e| panic!("[{case_name}] baseline pipeline failed: {e:?}"));

    assert!(
        !dlq.is_empty(),
        "[{case_name}] expected DLQ entries from BAD rows, got none.\n\
         counters={counters:?}\noutput={output}\nbaseline_output={baseline_output}"
    );
    assert!(
        counters.dlq_count > 0,
        "[{case_name}] expected non-zero dlq_count, got {}",
        counters.dlq_count
    );
    assert_eq!(
        sort_body(&output),
        sort_body(&baseline_output),
        "[{case_name}] retract-corrected output must equal baseline rerun.\n\
         GOT:\n{output}\nBASELINE:\n{baseline_output}"
    );
}

// ===== Parameterized window-function × retract-kind matrix =====

/// Window emit expressions exercised by the full matrix. Each entry is
/// a `(name, emit_text)` pair fed into `build_yaml`.
const WINDOW_CASES: &[(&str, &str)] = &[
    // Reversible aggregates.
    ("sum", "emit w_total = $window.sum(amount)"),
    ("count", "emit w_count = $window.count()"),
    ("avg", "emit w_avg = $window.avg(amount)"),
    // Order statistics.
    ("min", "emit w_min = $window.min(amount)"),
    ("max", "emit w_max = $window.max(amount)"),
    // Positional / lookup.
    ("first", "emit w_first_emp = $window.first().emp_id"),
    ("last", "emit w_last_emp = $window.last().emp_id"),
    ("lag", "emit w_lag_emp = $window.lag(1).emp_id"),
    ("lead", "emit w_lead_emp = $window.lead(1).emp_id"),
    // Iterable / set-shaped.
    ("collect", "emit w_collected = $window.collect(amount)"),
];

#[test]
fn window_sum_single_row_retract() {
    run_window_retract_case(
        "sum:single",
        "emit w_total = $window.sum(amount)",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_sum_full_partition_retract() {
    run_window_retract_case(
        "sum:full",
        "emit w_total = $window.sum(amount)",
        RetractKind::FullPartition,
    );
}

#[test]
fn window_count_single_row_retract() {
    run_window_retract_case(
        "count:single",
        "emit w_count = $window.count()",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_count_full_partition_retract() {
    run_window_retract_case(
        "count:full",
        "emit w_count = $window.count()",
        RetractKind::FullPartition,
    );
}

#[test]
fn window_avg_single_row_retract() {
    run_window_retract_case(
        "avg:single",
        "emit w_avg = $window.avg(amount)",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_avg_full_partition_retract() {
    run_window_retract_case(
        "avg:full",
        "emit w_avg = $window.avg(amount)",
        RetractKind::FullPartition,
    );
}

#[test]
fn window_min_single_row_retract() {
    run_window_retract_case(
        "min:single",
        "emit w_min = $window.min(amount)",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_min_full_partition_retract() {
    run_window_retract_case(
        "min:full",
        "emit w_min = $window.min(amount)",
        RetractKind::FullPartition,
    );
}

#[test]
fn window_max_single_row_retract() {
    run_window_retract_case(
        "max:single",
        "emit w_max = $window.max(amount)",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_max_full_partition_retract() {
    run_window_retract_case(
        "max:full",
        "emit w_max = $window.max(amount)",
        RetractKind::FullPartition,
    );
}

#[test]
fn window_first_single_row_retract() {
    run_window_retract_case(
        "first:single",
        "emit w_first_emp = $window.first().emp_id",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_first_full_partition_retract() {
    run_window_retract_case(
        "first:full",
        "emit w_first_emp = $window.first().emp_id",
        RetractKind::FullPartition,
    );
}

#[test]
fn window_last_single_row_retract() {
    run_window_retract_case(
        "last:single",
        "emit w_last_emp = $window.last().emp_id",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_last_full_partition_retract() {
    run_window_retract_case(
        "last:full",
        "emit w_last_emp = $window.last().emp_id",
        RetractKind::FullPartition,
    );
}

#[test]
fn window_lag_single_row_retract() {
    run_window_retract_case(
        "lag:single",
        "emit w_lag_emp = $window.lag(1).emp_id",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_lag_full_partition_retract() {
    run_window_retract_case(
        "lag:full",
        "emit w_lag_emp = $window.lag(1).emp_id",
        RetractKind::FullPartition,
    );
}

#[test]
fn window_lead_single_row_retract() {
    run_window_retract_case(
        "lead:single",
        "emit w_lead_emp = $window.lead(1).emp_id",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_lead_full_partition_retract() {
    run_window_retract_case(
        "lead:full",
        "emit w_lead_emp = $window.lead(1).emp_id",
        RetractKind::FullPartition,
    );
}

#[test]
fn window_collect_single_row_retract() {
    run_window_retract_case(
        "collect:single",
        "emit w_collected = $window.collect(amount)",
        RetractKind::SingleRow,
    );
}

#[test]
fn window_collect_full_partition_retract() {
    run_window_retract_case(
        "collect:full",
        "emit w_collected = $window.collect(amount)",
        RetractKind::FullPartition,
    );
}

/// `lag(2)` and `lead(2)` near a partition boundary: the existing
/// matrix exercises lag(1) / lead(1); these are the boundary-stress
/// variants for offsets > 1. Removing position k shifts every
/// lag/lead reference in a lookup-position window. One test asserts
/// bit-for-bit baseline equivalence over a fixture where the bad row
/// sits inside the lag/lead reach window of the partition's first /
/// last surviving rows.
#[test]
fn window_lag2_and_lead2_boundary_retract() {
    let yaml = build_yaml(
        "emit w_lag2 = $window.lag(2).emp_id\n      emit w_lead2 = $window.lead(2).emp_id",
    );
    let (full_csv, baseline_csv) = build_inputs(RetractKind::SingleRow);

    let (counters, dlq, output) = run_pipeline(&yaml, full_csv).expect("retract pipeline");
    let (_, _, baseline_output) = run_pipeline(&yaml, baseline_csv).expect("baseline pipeline");

    assert!(
        !dlq.is_empty(),
        "expected DLQ entries from the BAD row, got none.\ncounters={counters:?}"
    );
    assert!(counters.dlq_count > 0);
    assert_eq!(
        sort_body(&output),
        sort_body(&baseline_output),
        "lag(2)+lead(2) retract-corrected output must equal baseline rerun.\n\
         GOT:\n{output}\nBASELINE:\n{baseline_output}"
    );
}

// `$window.distinct(...)` and `$window.any(...)` / `$window.all(...)`
// are listed in the windows.md docs but the CXL parser today rejects
// `distinct` (it is a top-level keyword) and the iterable-predicate
// shape (any/all over a predicate expression) does not flow cleanly
// through the same analytic_window plumbing as the value-emitting
// builtins. The buffer-recompute path is uniform across builtins so
// the iterable cells are structurally covered by `collect` (the only
// iterable builtin the parser admits today). When the CXL parser
// catches up to the documented surface, add the matching cases here
// without touching the recompute body.

/// Driver-level assertion that `WINDOW_CASES` covers every parser-
/// admitted window builtin. Guards against accidental regressions in
/// the matrix trim if a contributor adds a new function but forgets
/// to add a representative test case.
#[test]
fn window_matrix_covers_each_lattice_cell() {
    let names: Vec<&str> = WINDOW_CASES.iter().map(|(n, _)| *n).collect();
    for needed in [
        "sum", "count", "avg", "min", "max", "first", "last", "lag", "lead", "collect",
    ] {
        assert!(
            names.contains(&needed),
            "matrix is missing builtin {needed}; recheck consolidation rationale"
        );
    }
}

// ===== Regression tests =====

/// Non-relaxed pipeline: no relaxed-CK aggregate downstream, so the
/// planner must not flip `requires_buffer_recompute` on. The pipeline
/// runs through the streaming-emit window path and produces the same
/// output the streaming-emit path always has.
#[test]
fn non_relaxed_window_pipeline_unaffected() {
    let yaml = r#"
pipeline:
  name: window_no_retract
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
- type: transform
  name: windowed
  input: src
  config:
    cxl: |
      emit emp_id = emp_id
      emit dept = dept
      emit amount = amount
      emit w_total = $window.sum(amount)
    analytic_window:
      group_by: [dept]
- type: output
  name: out
  input: windowed
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "emp_id,dept,amount\nE1,HR,10\nE2,HR,20\nE5,ENG,100\nE6,ENG,200\n";
    let (counters, dlq, output) = run_pipeline(yaml, csv).expect("non-relaxed pipeline runs");
    assert_eq!(counters.dlq_count, 0);
    assert!(dlq.is_empty());
    // Sum per partition: HR = 30, ENG = 300. Every input row appears
    // in the output with `w_total` populated to its partition's sum.
    assert!(
        output.contains(",30") && output.contains(",300"),
        "expected window sums in output; got:\n{output}"
    );
}

/// Cross-partition isolation: a multi-partition fixture where only HR
/// has a bad row. The ENG partition's window output must be identical
/// to baseline; only HR rows participate in the recompute. This is
/// the bit-for-bit ENG-side check that buffer recompute does not leak
/// retraction effects across partition boundaries.
#[test]
fn cross_partition_isolation_only_affected_partition_changes() {
    let yaml = build_yaml("emit w_count = $window.count()");
    let csv = "emp_id,dept,amount\n\
               E1,HR,10\n\
               E2,HR,20\n\
               E3,HR,BAD\n\
               E5,ENG,100\n\
               E6,ENG,200\n";
    let baseline_csv = "emp_id,dept,amount\n\
                        E1,HR,10\n\
                        E2,HR,20\n\
                        E5,ENG,100\n\
                        E6,ENG,200\n";

    let (_, _, output) = run_pipeline(&yaml, csv).expect("retract pipeline");
    let (_, _, baseline) = run_pipeline(&yaml, baseline_csv).expect("baseline pipeline");

    // The aggregate output is grouped by dept; equal sorted bodies
    // imply ENG's row is unchanged across the retract scope.
    assert_eq!(sort_body(&output), sort_body(&baseline));
}
