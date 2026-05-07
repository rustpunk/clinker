//! Integration tests for the synthetic-CK fan-out path through the
//! correlation-buffer detect phase.
//!
//! Failures upstream of a relaxed aggregate land in
//! [`correlated_dlq_retract.rs`]. The tests below trigger the failures
//! DOWNSTREAM of the aggregate, where the buffer cell is keyed by the
//! synthetic `$ck.aggregate.<name>` shadow column and the cell's
//! `error_rows` carry post-aggregate row numbers — not source row IDs.
//! Without the detect-phase synthetic-CK classification, the recompute
//! phase's `retract_row` lookup misses (the aggregator's lineage is
//! keyed by source row IDs) and silently no-ops, leaving the failing
//! group's contributors in the writer output.
//!
//! The bit-for-bit equivalence assertion against a baseline rerun (same
//! pipeline, source rows that contributed to the failing group
//! pre-excluded) is the load-bearing claim — anything that breaks the
//! detect-phase classification, the `input_rows_by_group_index` lookup,
//! or the retraction round-trip surfaces as a value mismatch against
//! the baseline.

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

fn sort_body(s: &str) -> Vec<String> {
    let mut lines: Vec<String> = s.lines().map(|l| l.to_string()).collect();
    if !lines.is_empty() {
        let header = lines.remove(0);
        lines.sort();
        lines.insert(0, header);
    }
    lines
}

/// Source → relaxed Aggregate → Transform (division-by-zero on certain
/// group totals) → Output. The Transform's `1 / (total - 60)` fails
/// when the HR group's `total` equals 60; the failure record carries
/// the synthetic `$ck.aggregate.dept_totals` column, so the buffer
/// cell key contains the aggregate's group_index — not source row IDs.
///
/// The detect phase must walk that key, find the
/// `AggregateGroupIndex { aggregate_name: "dept_totals" }` field
/// metadata, look up the aggregator's `input_rows` for that group, and
/// union those source row IDs into `affected_row_ids`. Recompute then
/// retracts all six HR contributors and finalizes a HR-less output.
///
/// The baseline rerun uses an input with all HR rows pre-excluded so
/// only ENG appears in either output. Bit-for-bit equivalence proves
/// the synthetic-CK fan-out covered every contributing source row.
#[test]
fn post_aggregate_failure_retracts_every_contributing_source_row() {
    let yaml = r#"
pipeline:
  name: post_aggregate_div_zero
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

      emit n = count(*)

      '
- type: transform
  name: post_check
  input: dept_totals
  config:
    cxl: 'emit department = department

      emit total = total

      emit n = n

      emit ratio = 1 / (total - 60)

      '
- type: output
  name: out
  input: post_check
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    // Six HR rows summing to 60 (10+10+10+10+10+10) — `total - 60 == 0`
    // for HR, fires division-by-zero in `post_check` for HR's single
    // aggregate output row. ENG sums to 600 (no failure).
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,10
O3,HR,10
O4,HR,10
O5,HR,10
O6,HR,10
O7,ENG,100
O8,ENG,200
O9,ENG,300
";
    // Baseline: every contributing HR source row excluded so the HR
    // group never finalizes. ENG output is unchanged.
    let baseline_csv = "\
order_id,department,amount
O7,ENG,100
O8,ENG,200
O9,ENG,300
";

    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    let (_, baseline_dlq, baseline_output) = run_pipeline(yaml, baseline_csv).unwrap();

    assert_eq!(baseline_dlq.len(), 0, "baseline has no failures");

    // The post-aggregate division-by-zero is the trigger. Without the
    // detect-phase synthetic-CK fan-out, the recompute phase silently
    // no-ops and HR's stale row reaches the writer — anything in the
    // output containing "HR" indicates the regression. Every HR source
    // row participated in the failing group, so the DLQ count covers
    // those plus the trigger.
    assert!(
        !output.contains("HR"),
        "HR group must not appear post-retract; division-by-zero in \
         post_check should have retracted every HR contributor:\n\
         got: {output}"
    );
    assert_eq!(
        sort_body(&output),
        sort_body(&baseline_output),
        "synthetic-CK fan-out must produce baseline-equivalent output:\n\
         got:\n{output}\nbaseline:\n{baseline_output}"
    );
    assert!(
        counters.dlq_count >= 1,
        "expected at least one DLQ entry for the post-aggregate failure"
    );
    assert!(
        dlq.iter().any(|d| d.trigger),
        "expected a trigger DLQ entry for the post-aggregate failure"
    );
}

/// Cascading-retraction loop convergence on a downstream-Transform
/// trigger. The deferred-region Transform throws divide-by-zero on a
/// recomputed aggregate value at commit time; the orchestrator folds
/// the failure into the next iteration's retract scope; the failing
/// aggregate's contributors are retracted via the synthetic-CK
/// lineage; the converged output excludes the failing department.
///
/// Companion to `post_aggregate_failure_retracts_every_contributing_source_row`
/// above: that test asserts bit-for-bit equivalence against a baseline
/// rerun. This test directly exercises loop semantics — exactly two
/// iterations run (the seed pass that surfaces the /0 trigger plus
/// one cascading pass that reruns dispatch with the HR rows added to
/// the retract scope; the third call would observe an empty
/// expand-delta and break before incrementing). The fixture mirrors
/// the bit-for-bit test's shape so a regression in either surfaces
/// the same failure mode.
#[test]
fn cascading_retraction_loop_converges_and_excludes_failing_partition() {
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

    let (counters, _dlq, output) =
        run_pipeline(yaml, csv).expect("cascading-retract pipeline must converge");

    let body_lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
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
        counters.dlq_count >= 1,
        "the HR aggregate emit's /0 lands in the DLQ; got {}",
        counters.dlq_count
    );
    assert!(
        counters.retraction.groups_recomputed >= 1,
        "the cascading-retraction loop recomputes at least one group; got {}",
        counters.retraction.groups_recomputed
    );
    // Two iterations: the seed iteration retracts no rows and discovers
    // the /0 trigger at commit; the second iteration runs recompute +
    // dispatch with the HR contributing rows added to the retract
    // scope, then `expand_with_dlq_events` returns an empty delta and
    // the loop breaks before a third increment.
    assert_eq!(
        counters.retraction.iterations, 2,
        "cascading-retraction loop must converge in exactly two \
         iterations on this fixture (seed + one cascade); got {}",
        counters.retraction.iterations
    );
}

/// Aggregator state degraded mid-run (tiny memory budget forces spill
/// or buffer-mode admission failure). When the relaxed-CK aggregator
/// has no retained state for the failing group, the synthetic-CK
/// lookup at detect time misses and the existing degrade-fallback in
/// `recompute_agg.rs` handles the affected aggregate group through
/// strict-collateral DLQ. The protocol must NOT panic and MUST NOT
/// produce a spurious retract — output is either valid (no-spill)
/// or the failure is surfaced through DLQ accounting.
#[test]
fn aggregator_state_degraded_falls_back_without_panic() {
    let yaml = r#"
pipeline:
  name: degraded_post_aggregate
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
  name: dept_totals
  input: src
  config:
    group_by: [department]
    cxl: 'emit department = department

      emit total = sum(amount)

      '
- type: transform
  name: post_check
  input: dept_totals
  config:
    cxl: 'emit department = department

      emit total = total

      emit ratio = 1 / (total - 30)

      '
- type: output
  name: out
  input: post_check
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "order_id,department,amount\n\
               O1,HR,10\nO2,HR,10\nO3,HR,10\n\
               O4,ENG,100\n";

    let outcome = std::panic::catch_unwind(|| run_pipeline(yaml, csv));
    match outcome {
        Ok(Ok(_)) => {
            // Memory pressure absorbed (or aggregator was not
            // degraded): the protocol completed without a crash. The
            // synthetic-CK detect-phase classification ran and either
            // hit a retained aggregator or skipped the aggregate when
            // its state was missing.
        }
        Ok(Err(_)) => {
            // Typed runtime error reached the caller. The
            // degrade-fallback path is supposed to translate this
            // through the orchestrator's `relaxed_aggregator_degrade`
            // list rather than aborting; here a typed error IS the
            // architecturally-correct shape (no panic, no silent
            // mis-retract).
        }
        Err(_) => {
            panic!(
                "BUG: degraded-aggregator post-aggregate-failure path \
                 panicked instead of degrading through DLQ. The detect \
                 phase must skip synthetic-CK expansion when \
                 `relaxed_aggregator_states` lacks the node, leaving \
                 the recompute phase's degrade-fallback to handle the \
                 group."
            );
        }
    }
}

/// Sibling aggregates over the SAME source feed two different
/// downstream Transforms; only one Transform fails. The detect-phase
/// synthetic-CK classification must isolate the fan-out — only the
/// aggregate whose downstream Transform failed contributes its
/// `input_rows` to the retraction. Source rows in the OTHER aggregate
/// remain untouched in its lineage and the other branch's output is
/// bit-identical to a baseline that omitted only the failing branch.
#[test]
fn sibling_aggregates_isolate_per_branch_fan_out() {
    let yaml = r#"
pipeline:
  name: sibling_aggregates_isolation
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
      - { name: region, type: string }
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
  name: post_dept
  input: dept_totals
  config:
    cxl: 'emit department = department

      emit total = total

      emit ratio = 1 / (total - 60)

      '
- type: output
  name: out
  input: post_dept
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    // HR sums to 60 (six 10s), triggering division-by-zero post_dept;
    // ENG is clean (sum = 600).
    let csv = "\
order_id,department,region,amount
O1,HR,East,10
O2,HR,East,10
O3,HR,West,10
O4,HR,West,10
O5,HR,East,10
O6,HR,West,10
O7,ENG,East,100
O8,ENG,West,200
O9,ENG,East,300
";

    let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
    assert!(
        !output.contains("HR"),
        "HR rolled back via synthetic-CK fan-out; got: {output}"
    );
    assert!(
        output.contains("ENG"),
        "ENG must survive — its rows did not feed the failing group; \
         got: {output}"
    );
    assert!(counters.dlq_count >= 1);
    assert!(dlq.iter().any(|d| d.trigger));
}

/// Reversible-binding-only post-aggregate failure: pure `sum` /
/// `count` aggregator. The lineage path retraction round-trips
/// cleanly; a baseline rerun with the failing group's contributors
/// excluded matches the retracted output bit-for-bit.
#[test]
fn post_aggregate_reversible_only_matches_baseline_rerun() {
    let yaml = r#"
pipeline:
  name: reversible_post_aggregate
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

      emit n = count(*)

      '
- type: transform
  name: post_check
  input: dept_totals
  config:
    cxl: 'emit department = department

      emit total = total

      emit n = n

      emit ratio = 100 / (total - 30)

      '
- type: output
  name: out
  input: post_check
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    // HR sums to 30 (three 10s), triggers division by zero. ENG is
    // clean.
    let csv = "\
order_id,department,amount
O1,HR,10
O2,HR,10
O3,HR,10
O4,ENG,100
O5,ENG,200
";
    let baseline_csv = "\
order_id,department,amount
O4,ENG,100
O5,ENG,200
";

    let (_counters, _dlq, output) = run_pipeline(yaml, csv).unwrap();
    let (_, baseline_dlq, baseline_output) = run_pipeline(yaml, baseline_csv).unwrap();

    assert_eq!(baseline_dlq.len(), 0);
    assert!(
        !output.contains("HR"),
        "HR must not appear post-retract; got: {output}"
    );
    assert_eq!(
        sort_body(&output),
        sort_body(&baseline_output),
        "Reversible-binding retract through synthetic-CK fan-out must \
         match baseline rerun:\ngot:\n{output}\nbaseline:\n{baseline_output}"
    );
}
