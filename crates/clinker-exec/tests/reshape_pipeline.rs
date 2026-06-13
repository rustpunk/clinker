//! Reshape node integration tests.
//!
//! Exercises the per-correlation-group synthesis and trigger-row mutation
//! arm end-to-end against in-memory CSV readers/writers: group synthesis,
//! trigger-row mutation, the `$meta.*` audit stamps, the no-cascade
//! contract, and mutation-conflict routing to the dead-letter queue with a
//! whole-group rollback.

mod common;

use std::collections::HashMap;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{DlqEntry, PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;
use clinker_record::PipelineCounters;

/// Run a single-source → reshape → single-output pipeline over `csv_input`,
/// returning the run counters, DLQ entries, and the CSV output string.
fn run_reshape(
    yaml: &str,
    csv_input: &str,
) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
    let report = run_reshape_report(yaml, csv_input)?;
    Ok((report.counters, report.dlq_entries, report.output))
}

/// The full observable surface of a Reshape run: counters, DLQ entries, the
/// rendered CSV output, and the run's cumulative on-disk spill volume. Spill
/// tests assert `cumulative_spill_bytes > 0` to prove the disk path fired.
#[derive(Debug)]
struct ReshapeReport {
    counters: PipelineCounters,
    dlq_entries: Vec<DlqEntry>,
    output: String,
    cumulative_spill_bytes: u64,
}

/// Run a Reshape pipeline and surface the full [`ReshapeReport`], including
/// the cumulative spill volume the memory-pressure tests gate on.
fn run_reshape_report(yaml: &str, csv_input: &str) -> Result<ReshapeReport, PipelineError> {
    let config = clinker_plan::config::parse_config(yaml).expect("fixture pipeline must parse");
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        primary,
        clinker_exec::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report = common::run_config(&config, readers, writers, &params)?;
    Ok(ReshapeReport {
        counters: report.counters,
        dlq_entries: report.dlq_entries,
        output: buf.as_string(),
        cumulative_spill_bytes: report.cumulative_spill_bytes,
    })
}

/// SCD-Type-2 shape: per employee, a row whose `plan_start - plan_end`
/// exceeds the one-year boundary (here 365) triggers a fix of its end date
/// and the synthesis of a new boundary row.
const SCD_PIPELINE: &str = r#"
pipeline:
  name: reshape_scd
error_handling:
  strategy: continue
nodes:
  - type: source
    name: plans
    config:
      name: plans
      type: csv
      path: test.csv
      schema:
        - { name: employee_id, type: string }
        - { name: plan_start, type: int }
        - { name: plan_end, type: int }
        - { name: status, type: string }
  - type: reshape
    name: backfill
    input: plans
    config:
      partition_by: [employee_id]
      order_by:
        - { field: plan_start, order: asc }
      rules:
        - name: fix_long_plan
          when: "plan_start - plan_end > 365"
          mutate:
            set:
              plan_end: "plan_start"
          synthesize:
            copy_from: trigger
            overrides:
              status: "'synthesized'"
  - type: output
    name: out
    input: backfill
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn scd_type2_mutates_trigger_and_synthesizes_row() {
    // Employee A has one long-gap row (1000 - 100 = 900 > 365) and one
    // normal row. Employee B has only a normal row.
    let csv = "employee_id,plan_start,plan_end,status\n\
               A,100,90,baseline\n\
               A,1000,100,baseline\n\
               B,200,150,baseline\n";
    let (counters, dlq, output) = run_reshape(SCD_PIPELINE, csv).unwrap();

    assert!(dlq.is_empty(), "no DLQ entries expected, got {dlq:?}");
    // `ok_count` counts distinct SOURCE rows reaching output; the
    // synthesized row borrows its trigger's identity, so all 3 source
    // rows count clean.
    assert_eq!(counters.ok_count, 3, "all 3 source rows emit clean");

    // The trigger row's end date is fixed to its start (1000), and a
    // synthesized row carries status=synthesized. The output carries a
    // header line plus 4 data rows (3 originals + 1 synthesized).
    let data_lines: Vec<&str> = output.lines().skip(1).filter(|l| !l.is_empty()).collect();
    assert_eq!(
        data_lines.len(),
        4,
        "3 originals + 1 synthesized data row: {output}"
    );
    let lines: Vec<&str> = output.lines().collect();
    assert!(
        lines.iter().any(|l| l.contains("1000,1000")),
        "trigger row end date should be fixed to start: {output}"
    );
    assert!(
        lines.iter().any(|l| l.contains("synthesized")),
        "a synthesized row should be present: {output}"
    );
    // Audit columns stay out of the default writer surface.
    assert!(
        !output.contains("$meta."),
        "$meta.* audit columns must not leak into default output: {output}"
    );
    // The untriggered rows are unchanged.
    assert!(lines.iter().any(|l| l.contains("A,100,90,baseline")));
    assert!(lines.iter().any(|l| l.contains("B,200,150,baseline")));
}

#[test]
fn scd_idempotent_rerun_is_stable() {
    // After the first reshape, the synthesized row plus the fixed trigger
    // exist. Feeding the post-reshape state back in must not re-trigger:
    // the fixed trigger now has plan_start - plan_end == 0, and the
    // synthesized row likewise. Only genuinely long-gap rows fire.
    let already_fixed = "employee_id,plan_start,plan_end,status\n\
                         A,100,90,baseline\n\
                         A,1000,1000,baseline\n\
                         A,1000,1000,synthesized\n";
    let (_, dlq, output) = run_reshape(SCD_PIPELINE, already_fixed).unwrap();
    assert!(dlq.is_empty());
    // No new synthesized row beyond the one already present.
    let synth_count = output.lines().filter(|l| l.contains("synthesized")).count();
    assert_eq!(
        synth_count, 1,
        "no rule should fire on an already-fixed group: {output}"
    );
}

/// Two rules whose selectors overlap on content and both write the same
/// field. Static overlap cannot prove disjointness, so the collision
/// surfaces at runtime as a mutation conflict.
const CONFLICT_PIPELINE: &str = r#"
pipeline:
  name: reshape_conflict
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: test.csv
      schema:
        - { name: gid, type: string }
        - { name: amount, type: int }
        - { name: tier, type: string }
  - type: reshape
    name: classify
    input: rows
    config:
      partition_by: [gid]
      rules:
        - name: bump_high
          when: "amount > 50"
          mutate:
            set:
              tier: "'high'"
        - name: bump_big
          when: "amount > 80"
          mutate:
            set:
              tier: "'big'"
  - type: output
    name: out
    input: classify
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn mutation_conflict_dlqs_whole_group() {
    // Group X: amount 100 fires BOTH rules on the same row → both write
    // `tier` → MutationConflict → whole group rolls back. Group Y: amount
    // 60 fires only `bump_high` → clean, emits.
    let csv = "gid,amount,tier\n\
               X,100,base\n\
               X,30,base\n\
               Y,60,base\n";
    let (counters, dlq, output) = run_reshape(CONFLICT_PIPELINE, csv).unwrap();

    assert_eq!(dlq.len(), 1, "exactly one MutationConflict DLQ entry");
    assert_eq!(
        dlq[0].category,
        clinker_core_types::dlq::DlqErrorCategory::MutationConflict
    );
    assert_eq!(
        dlq[0].stage.as_deref(),
        Some("reshape:classify:bump_high+bump_big"),
        "stage label names the colliding rule pair"
    );

    // Group X produced no output (whole-group rollback); group Y emitted.
    assert!(
        !output.contains("X,"),
        "conflicting group X must not reach output: {output}"
    );
    assert!(
        output.contains("Y,60,high"),
        "group Y emitted clean: {output}"
    );
    assert_eq!(counters.ok_count, 1, "only group Y's single row emitted");
}

#[test]
fn no_cascade_rules_see_original_state() {
    // `bump_high` rewrites tier to 'high' for amount>50; a second rule
    // gated on tier=='base' must STILL fire on the same row, because rules
    // observe the original group snapshot — not bump_high's effect. The
    // two rules write different fields, so there is no conflict.
    let yaml = r#"
pipeline:
  name: reshape_no_cascade
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: test.csv
      schema:
        - { name: gid, type: string }
        - { name: amount, type: int }
        - { name: tier, type: string }
        - { name: note, type: string }
  - type: reshape
    name: classify
    input: rows
    config:
      partition_by: [gid]
      rules:
        - name: set_tier
          when: "amount > 50"
          mutate:
            set:
              tier: "'high'"
        - name: note_base
          when: "tier == 'base'"
          mutate:
            set:
              note: "'was_base'"
  - type: output
    name: out
    input: classify
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let csv = "gid,amount,tier,note\nX,100,base,\n";
    let (_, dlq, output) = run_reshape(yaml, csv).unwrap();
    assert!(dlq.is_empty(), "different fields → no conflict: {dlq:?}");
    // Both rules fired against the original snapshot: tier=high AND
    // note=was_base on the same row.
    assert!(
        output.contains("high") && output.contains("was_base"),
        "both rules apply against the original group state: {output}"
    );
}

/// A `synthesize` rule with `copy_from: none` that overrides every user
/// column. The synthesized row is born from an all-null base, so every
/// output value comes from an `overrides` expression. This exercises the
/// `CopyFrom::None` dispatch path end-to-end — the path the compile-only
/// `copy_from: none` validation tests never reach.
const COPY_FROM_NONE_PIPELINE: &str = r#"
pipeline:
  name: reshape_copy_from_none
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: test.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
        - { name: label, type: string }
  - type: reshape
    name: emit_summary
    input: rows
    config:
      partition_by: [id]
      rules:
        - name: summarize_big
          when: "amount > 100"
          synthesize:
            copy_from: none
            overrides:
              id: "id"
              amount: "amount * 2"
              label: "'synthetic-summary'"
  - type: output
    name: out
    input: emit_summary
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn copy_from_none_synthesizes_fully_overridden_row() {
    // Group A's amount=150 (>100) fires the rule and synthesizes a brand-new
    // row from an all-null base, every column supplied by an override:
    // id=A (the trigger's), amount=300 (150*2), label='synthetic-summary'.
    // Group B's amount=50 does not fire. Before the sizing fix, building the
    // synthesized Record from an empty values Vec against the wider output
    // schema (3 user cols + 3 `$meta.*` audit cols) tripped the
    // `Record::new` length `debug_assert`, panicking this debug-build run.
    let csv = "id,amount,label\n\
               A,150,original\n\
               B,50,original\n";
    let (counters, dlq, output) = run_reshape(COPY_FROM_NONE_PIPELINE, csv).unwrap();

    assert!(dlq.is_empty(), "no DLQ entries expected, got {dlq:?}");

    let lines: Vec<&str> = output.lines().collect();
    // The two originals pass through plus exactly one synthesized row.
    let data_lines: Vec<&str> = lines
        .iter()
        .skip(1)
        .filter(|l| !l.is_empty())
        .copied()
        .collect();
    assert_eq!(
        data_lines.len(),
        3,
        "2 originals + 1 synthesized data row: {output}"
    );

    // The synthesized row carries every overridden value (none null): the
    // trigger's id, the doubled amount, and the literal label.
    assert!(
        lines.iter().any(|l| l == &"A,300,synthetic-summary"),
        "fully-overridden synthesized row must be present and non-null: {output}"
    );

    // Originals are untouched, and no audit column leaks into output.
    assert!(lines.iter().any(|l| l == &"A,150,original"));
    assert!(lines.iter().any(|l| l == &"B,50,original"));
    assert!(
        !output.contains("$meta."),
        "$meta.* audit columns must not leak into default output: {output}"
    );

    // `ok_count` counts distinct source rows reaching output; the synthesized
    // row borrows its trigger's identity, so both source rows count clean.
    assert_eq!(counters.ok_count, 2, "both source rows emit clean");
}

/// SCD pipeline parameterized on the pipeline-level `memory.limit`, with the
/// `spill` backpressure policy so a sub-baseline limit forces the disk path
/// instead of being rejected as unsatisfiable (a producer-pausing policy
/// rejects a limit below baseline RSS up front). The rule both mutates the
/// trigger row and synthesizes a `copy_from: none` row, so the spill
/// round-trip exercises the synthesized-row schema-width path at runtime.
fn scd_spill_pipeline(memory_limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: reshape_scd_spill
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: continue
nodes:
  - type: source
    name: plans
    config:
      name: plans
      type: csv
      path: test.csv
      schema:
        - {{ name: employee_id, type: string }}
        - {{ name: plan_start, type: int }}
        - {{ name: plan_end, type: int }}
        - {{ name: status, type: string }}
  - type: reshape
    name: backfill
    input: plans
    config:
      partition_by: [employee_id]
      order_by:
        - {{ field: plan_start, order: asc }}
      rules:
        - name: fix_long_plan
          when: "plan_start - plan_end > 365"
          mutate:
            set:
              plan_end: "plan_start"
          synthesize:
            copy_from: none
            overrides:
              employee_id: "employee_id"
              plan_start: "plan_start"
              plan_end: "plan_start"
              status: "'synthesized'"
  - type: output
    name: out
    input: backfill
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// SCD spill pipeline with NO `order_by`, so within-group emit order is the
/// records' arrival order. Used to prove a spilled group reloads in arrival
/// order: with an `order_by`, a stable re-sort would mask a reorder, but with
/// no `order_by` the within-group order is exactly the input order and any
/// spill-induced reordering shows up directly in the output.
fn scd_spill_pipeline_no_order(memory_limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: reshape_scd_spill_no_order
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: continue
nodes:
  - type: source
    name: plans
    config:
      name: plans
      type: csv
      path: test.csv
      schema:
        - {{ name: employee_id, type: string }}
        - {{ name: plan_start, type: int }}
        - {{ name: plan_end, type: int }}
        - {{ name: status, type: string }}
  - type: reshape
    name: backfill
    input: plans
    config:
      partition_by: [employee_id]
      rules:
        - name: fix_long_plan
          when: "plan_start - plan_end > 365"
          mutate:
            set:
              plan_end: "plan_start"
  - type: output
    name: out
    input: backfill
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// Build a wide SCD input: `groups` employees, each with `rows_per_group`
/// plan rows, the last of which trips the long-gap rule. Returns the CSV and
/// the count of rows expected to trigger (one per group).
fn scd_input(groups: usize, rows_per_group: usize) -> (String, usize) {
    let mut csv = String::from("employee_id,plan_start,plan_end,status\n");
    for g in 0..groups {
        for r in 0..rows_per_group {
            let employee = format!("employee-{g:05}");
            if r == rows_per_group - 1 {
                // Long-gap trigger row: plan_start - plan_end = 900 > 365.
                csv.push_str(&format!("{employee},1000,100,baseline-{r}\n"));
            } else {
                // Normal row: small gap, no trigger.
                csv.push_str(&format!("{employee},{},{},baseline-{r}\n", r * 10, r * 10));
            }
        }
    }
    (csv, groups)
}

#[test]
fn reshape_spills_under_memory_pressure() {
    // A 4K budget against a baseline RSS in the megabytes forces the disk
    // path from the first admitted record. The synthesized `copy_from: none`
    // row makes the spill round-trip exercise the synthesized-row
    // schema-width path. The output must be byte-identical to the same input
    // run with an ample budget (spill is a memory strategy, never a data
    // transform) AND the run must report on-disk spill volume.
    let (csv, trigger_groups) = scd_input(200, 8);

    let spilled = run_reshape_report(&scd_spill_pipeline("4K"), &csv).unwrap();
    let in_memory = run_reshape_report(&scd_spill_pipeline("512M"), &csv).unwrap();

    assert!(
        spilled.dlq_entries.is_empty(),
        "spilling run produces no DLQ entries: {:?}",
        spilled.dlq_entries
    );
    assert!(
        spilled.cumulative_spill_bytes > 0,
        "the 4K budget must force the disk spill path"
    );
    assert_eq!(
        in_memory.cumulative_spill_bytes, 0,
        "the 512M budget keeps everything in memory"
    );

    // Spill is transparent: identical output regardless of budget. Compare
    // sorted line sets so any reordering across the spill round-trip is
    // caught as a difference.
    let mut spilled_lines: Vec<&str> = spilled.output.lines().collect();
    let mut memory_lines: Vec<&str> = in_memory.output.lines().collect();
    spilled_lines.sort_unstable();
    memory_lines.sort_unstable();
    assert_eq!(
        spilled_lines, memory_lines,
        "spilled output must be byte-identical to the in-memory run"
    );

    // Every trigger group synthesized exactly one row.
    let synth_count = spilled
        .output
        .lines()
        .filter(|l| l.contains("synthesized"))
        .count();
    assert_eq!(
        synth_count, trigger_groups,
        "one synthesized row per trigger group survives the spill round-trip"
    );
    assert!(
        !spilled.output.contains("$meta."),
        "audit columns must not leak even across spill"
    );
}

#[test]
fn reshape_spill_preserves_within_group_arrival_order() {
    // A spilled group must emit in arrival order, identical to a resident
    // group — output must not depend on the memory budget. With NO `order_by`,
    // within-group order IS arrival order, so a spill-induced reorder shows up
    // directly in the output (an `order_by` would stably re-sort and mask it).
    // One employee, 50 rows whose `status` carries the arrival index, none
    // triggering, so the output is the input rows in order. An 18K budget
    // (≈14.4K soft) sits just below the single group's ≈15.6K footprint, so
    // the group exceeds the soft threshold and partition-spills across buckets
    // (the case that can reorder), yet still fits the finalize reload under
    // the 18K hard limit. A 512M budget keeps it resident for the baseline.
    let mut csv = String::from("employee_id,plan_start,plan_end,status\n");
    for r in 0..50u32 {
        // Small gaps (plan_start == plan_end) so no row triggers; `status`
        // pins each row's arrival index for an order-sensitive comparison.
        csv.push_str(&format!("E,{},{},seq-{r:04}\n", r * 10, r * 10));
    }

    let spilled = run_reshape_report(&scd_spill_pipeline_no_order("18K"), &csv).unwrap();
    let in_memory = run_reshape_report(&scd_spill_pipeline_no_order("512M"), &csv).unwrap();

    assert!(spilled.dlq_entries.is_empty(), "no DLQ entries under spill");
    assert!(
        spilled.cumulative_spill_bytes > 0,
        "the single group must partition-spill under the 4K budget"
    );
    assert_eq!(
        in_memory.cumulative_spill_bytes, 0,
        "the 512M budget keeps the group resident"
    );
    // Row-for-row identity (NOT a sorted-set comparison): a reorder across the
    // spill round-trip is a hard failure here.
    assert_eq!(
        spilled.output, in_memory.output,
        "a spilled group must emit byte-identical to the resident group"
    );
    // And the order is the input order: seq-0000, seq-0001, … seq-0049.
    let seqs: Vec<&str> = spilled
        .output
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .map(|l| l.rsplit(',').next().unwrap())
        .collect();
    let expected: Vec<String> = (0..50).map(|r| format!("seq-{r:04}")).collect();
    assert_eq!(
        seqs, expected,
        "the spilled group must emit in arrival order"
    );
}

#[test]
fn reshape_skew_single_giant_group() {
    // One partition dwarfs the rest: employee-00000 holds 600 rows while 50
    // others hold a handful each. The giant group exceeds the soft threshold,
    // so it must spill INCREMENTALLY (sliced by arrival hash) rather than be
    // buffered whole — and the resident peak must stay bounded near the budget
    // even though the giant group's total is several times larger. The budget
    // is sized so the giant group still fits the finalize reload (it is not a
    // fail-loud case — see `reshape_giant_group_exceeds_budget_fails_loud`).
    let mut csv = String::from("employee_id,plan_start,plan_end,status\n");
    for r in 0..599 {
        csv.push_str(&format!("employee-00000,{},{},base\n", r * 10, r * 10));
    }
    csv.push_str("employee-00000,1000,100,base\n");
    for g in 1..=50 {
        csv.push_str(&format!("employee-{g:05},10,10,base\n"));
        csv.push_str(&format!("employee-{g:05},20,20,base\n"));
    }

    // 224K hard / ≈179K soft. The giant group (~187 KB) exceeds the soft
    // threshold — so it partition-spills incrementally rather than being
    // buffered whole — yet still fits the finalize reload under the 224K hard
    // limit (this is the success path, not the fail-loud case). The small
    // groups stay resident under the budget.
    let report = run_reshape_report(&scd_spill_pipeline("224K"), &csv).unwrap();
    assert!(report.dlq_entries.is_empty(), "no DLQ entries under skew");

    // The giant group went to disk: spill fired and evicted real volume. A
    // single group that fits finalize can only spill its overflow above the
    // soft threshold (≈ group − soft), so the volume is modest by design — the
    // point is that eviction happened incrementally, not that the whole group
    // was written. The bounded resident peak and arrival-order preservation
    // under partition slicing are asserted directly against the buffer in the
    // `spill_stops_at_soft_threshold_without_draining_all_groups` and
    // `partition_spill_preserves_arrival_order` unit tests, which can observe
    // the per-group resident bytes the run-level metric cannot isolate.
    assert!(
        report.cumulative_spill_bytes > 0,
        "the giant group must trip the soft threshold and evict its overflow to disk"
    );

    // Giant group: 600 originals + 1 synthesized. Small groups: 100 rows.
    let data_lines: Vec<&str> = report
        .output
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(
        data_lines.len(),
        701,
        "600 + 1 synthesized (giant) + 100 (small) rows survive: got {}",
        data_lines.len()
    );
    let synth_count = report
        .output
        .lines()
        .filter(|l| l.contains("synthesized"))
        .count();
    assert_eq!(
        synth_count, 1,
        "only the giant group's trigger synthesizes a row"
    );
    assert!(
        report
            .output
            .lines()
            .any(|l| l == "employee-00001,10,10,base"),
        "small groups pass through untouched while the giant group spills"
    );
}

#[test]
fn reshape_giant_group_exceeds_budget_fails_loud() {
    // The no-cascade contract requires the WHOLE correlation group resident at
    // finalize to observe the rules. A single group larger than the memory
    // budget therefore has no in-budget representation: skew slicing bounds the
    // ingest peak, but the finalize reload still needs the whole group. Rather
    // than OOM, the run must fail loud with a clear diagnostic naming the
    // single-group limitation.
    let (csv, _) = scd_input(1, 400); // one ~125 KB group

    let err = run_reshape_report(&scd_spill_pipeline("8K"), &csv)
        .expect_err("a single group larger than the budget must fail loud, not OOM");
    let rendered = format!("{err:?}");
    assert!(
        rendered.contains("single Reshape correlation group"),
        "the diagnostic must name the giant-group limitation: {rendered}"
    );
    assert!(
        rendered.contains("memory budget") && rendered.contains("no-cascade"),
        "the diagnostic must explain why one group must fit the budget: {rendered}"
    );
}

/// Workspace root, derived from the crate manifest, so the example fixture
/// is located independently of the harness cwd.
fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("pipelines")
}

#[test]
fn scd_type2_e2e_with_spill() {
    // Run the runnable `examples/pipelines/scd_type2.yaml` end-to-end. Its
    // pipeline-level `memory.limit: 16K` with the `spill` policy forces the
    // disk path on this small fixture, so the example doubles as the
    // bounded-memory smoke test: the mutate+synthesize output must be correct
    // AND the run must report on-disk spill volume.
    let yaml = std::fs::read_to_string(examples_dir().join("scd_type2.yaml"))
        .expect("read scd_type2.yaml example");
    let csv = std::fs::read_to_string(examples_dir().join("data").join("scd_plans.csv"))
        .expect("read scd_plans.csv example data");

    let config = parse_config(&yaml).expect("example pipeline must parse");
    // The example's `path:` fields are relative `./data` / `./output`
    // strings; anchor compile-time path validation at the examples dir. The
    // writer is an in-memory buffer, so no output file is opened.
    let ctx = CompileContext::new(examples_dir());
    let plan = config.compile(&ctx).expect("example pipeline must compile");

    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "plans".to_string(),
        clinker_exec::executor::single_file_reader(
            "scd_plans.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "scd-e2e".to_string(),
        batch_id: "scd-e2e".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("example pipeline run");
    let output = buf.as_string();

    assert!(
        report.dlq_entries.is_empty(),
        "example run produces no DLQ entries: {:?}",
        report.dlq_entries
    );
    assert!(
        report.cumulative_spill_bytes > 0,
        "the example's 16K budget forces the disk spill path"
    );

    // Three employees have over-long windows (E001, E002, E004), so three
    // synthesized rows appear, each marked `status=synthesized`.
    let synth_count = output.lines().filter(|l| l.contains("synthesized")).count();
    assert_eq!(
        synth_count, 3,
        "one synthesized continuation row per over-long window: {output}"
    );
    // The trigger rows have their window closed at the start boundary.
    assert!(
        output.lines().any(|l| l.contains("E001,1000,1000")),
        "E001's over-long window is closed at its start: {output}"
    );
    assert!(
        !output.contains("$meta."),
        "audit columns must not leak into default output: {output}"
    );
}
