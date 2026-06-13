//! Reshape node integration tests.
//!
//! Exercises the per-correlation-group synthesis and trigger-row mutation
//! arm end-to-end against in-memory CSV readers/writers: group synthesis,
//! trigger-row mutation, the `$meta.*` audit stamps, the no-cascade
//! contract, and mutation-conflict routing to the dead-letter queue with a
//! whole-group rollback.

mod common;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{DlqEntry, PipelineRunParams};
use clinker_plan::error::PipelineError;
use clinker_record::PipelineCounters;

/// Run a single-source → reshape → single-output pipeline over `csv_input`,
/// returning the run counters, DLQ entries, and the CSV output string.
fn run_reshape(
    yaml: &str,
    csv_input: &str,
) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
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
    Ok((report.counters, report.dlq_entries, buf.as_string()))
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
