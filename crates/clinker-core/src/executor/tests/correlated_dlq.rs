//! Correlation-key DLQ tests for the deferred-commit design.
//!
//! Locks the load-bearing semantics — group identity fixed at ingest,
//! per-record null rejection, root-cause vs collateral marking, group
//! size overflow disposition — plus the multi-output failure-domain
//! (Case A.1/A.2/A.3), in-pipeline branching (B.2), and group-identity
//! preservation (F.1, F.2) cases that motivated the redesign.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

fn run_correlated_pipeline(
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

fn base_yaml(correlation_key: &str) -> String {
    format!(
        r#"
pipeline:
  name: correlated_test
error_handling:
  strategy: continue
  correlation_key: {0}
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - {{ name: employee_id, type: string }}
      - {{ name: value, type: string }}
      - {{ name: dept, type: string }}

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
"#,
        correlation_key
    )
}

#[test]
fn one_fail_dlqs_whole_group() {
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\nA,100\nA,bad\nA,300\nB,400\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(&yaml, csv).unwrap();

    assert_eq!(
        counters.dlq_count, 3,
        "all 3 records in group A should be DLQ'd"
    );
    assert_eq!(counters.ok_count, 1, "only group B emitted");
    assert_eq!(dlq_entries.len(), 3);
    assert!(output.contains("B"), "output should contain group B");
    assert!(
        !output.contains(",bad"),
        "output should not contain bad record"
    );
}

#[test]
fn good_groups_emit_bad_groups_dlq() {
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\nA,100\nA,200\nB,bad\nB,300\nC,500\nC,600\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(&yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 4, "groups A and C emitted (4 records)");
    assert_eq!(counters.dlq_count, 2, "group B DLQ'd (2 records)");
    assert_eq!(dlq_entries.len(), 2);
    assert!(output.contains("A,100"), "output has group A");
    assert!(output.contains("C,500"), "output has group C");
}

#[test]
fn trigger_marks_root_cause_only() {
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\nA,100\nA,bad\nA,300\n";
    let (_counters, dlq_entries, _output) = run_correlated_pipeline(&yaml, csv).unwrap();

    assert_eq!(dlq_entries.len(), 3);

    let triggers: Vec<bool> = dlq_entries.iter().map(|e| e.trigger).collect();
    let root_causes = triggers.iter().filter(|&&t| t).count();
    let collaterals = triggers.iter().filter(|&&t| !t).count();

    assert_eq!(root_causes, 1, "exactly one root cause");
    assert_eq!(collaterals, 2, "two collateral records");

    let root = dlq_entries.iter().find(|e| e.trigger).unwrap();
    assert!(
        root.error_message.contains("convert") || root.error_message.contains("Int"),
        "root cause should mention conversion failure: {}",
        root.error_message
    );
}

#[test]
fn collateral_carries_correlated_category() {
    // Per the deferred-commit design, collateral entries carry the
    // dedicated `Correlated` category (was `ValidationFailure` in the
    // deleted impl).
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\nA,100\nA,bad\n";
    let (_counters, dlq_entries, _output) = run_correlated_pipeline(&yaml, csv).unwrap();

    let collateral = dlq_entries.iter().find(|e| !e.trigger).unwrap();
    assert_eq!(
        collateral.category,
        crate::dlq::DlqErrorCategory::Correlated,
        "collateral should carry the Correlated category"
    );
    assert!(
        collateral
            .error_message
            .contains("correlated with failure in group"),
        "collateral message should mention correlation: {}",
        collateral.error_message
    );
}

#[test]
fn null_key_records_are_per_record_groups() {
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\n,100\n,bad\n,300\nA,400\n";
    let (counters, dlq_entries, _output) = run_correlated_pipeline(&yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 3, "three good records emitted");
    assert_eq!(counters.dlq_count, 1, "only the bad null-key record DLQ'd");
    assert_eq!(dlq_entries.len(), 1);
    assert!(dlq_entries[0].trigger, "individual rejection is root cause");
}

#[test]
fn compound_key_groups_dlq_atomically() {
    let yaml = r#"
pipeline:
  name: compound_key_test
error_handling:
  strategy: continue
  correlation_key:
  - employee_id
  - dept
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: employee_id, type: string }
      - { name: dept, type: string }
      - { name: value, type: string }

- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit emp = employee_id

      emit d = dept

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
    let csv = "employee_id,dept,value\nA,HR,100\nA,HR,bad\nA,ENG,200\nB,HR,300\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(yaml, csv).unwrap();

    assert_eq!(counters.dlq_count, 2, "group (A,HR) DLQ'd");
    assert_eq!(counters.ok_count, 2, "groups (A,ENG) and (B,HR) emitted");
    assert_eq!(dlq_entries.len(), 2);
    assert!(output.contains("A,ENG"), "output has (A,ENG)");
    assert!(output.contains("B,HR"), "output has (B,HR)");
}

#[test]
fn empty_input_zero_dlq_zero_emit() {
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(&yaml, csv).unwrap();

    assert_eq!(counters.total_count, 0);
    assert_eq!(counters.dlq_count, 0);
    assert_eq!(counters.ok_count, 0);
    assert!(dlq_entries.is_empty());
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.len() <= 1, "empty or header-only output");
}

#[test]
fn group_overflow_emits_root_cause_and_collaterals() {
    let yaml = r#"
pipeline:
  name: buffer_overflow_test
error_handling:
  strategy: continue
  correlation_key: employee_id
  max_group_buffer: 3
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
    cxl: 'emit emp = employee_id

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
    // Group A has 5 records — exceeds max_group_buffer of 3. Per the
    // deferred-commit design, every record of the overflowing group
    // becomes a DLQ entry: one root-cause with category=GroupSizeExceeded,
    // the rest collaterals with category=Correlated.
    let csv = "employee_id,value\nA,100\nA,200\nA,300\nA,400\nA,500\nB,600\n";
    let (counters, dlq_entries, _output) = run_correlated_pipeline(yaml, csv).unwrap();

    assert_eq!(
        counters.dlq_count, 5,
        "all 5 records of group A DLQ'd post-overflow"
    );
    assert_eq!(counters.ok_count, 1, "only group B emitted");

    let triggers = dlq_entries.iter().filter(|e| e.trigger).count();
    assert_eq!(triggers, 1, "exactly one root-cause entry");
    let trigger_entry = dlq_entries.iter().find(|e| e.trigger).unwrap();
    assert_eq!(
        trigger_entry.category,
        crate::dlq::DlqErrorCategory::GroupSizeExceeded,
    );
    let collateral_count = dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(collateral_count, 4);
    for collateral in dlq_entries.iter().filter(|e| !e.trigger) {
        assert_eq!(
            collateral.category,
            crate::dlq::DlqErrorCategory::Correlated,
        );
    }
}

#[test]
fn multi_output_route_dlqs_group_across_branches() {
    // A.1: multi-output via inclusive Route. A transform error on a
    // record reachable from any output branch DLQs the whole
    // correlation group across both outputs.
    let yaml = r#"
pipeline:
  name: multi_output_correlation
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
    cxl: 'emit emp = employee_id

      emit val = value.to_int()

      '
- type: route
  name: split
  input: validate
  config:
    mode: inclusive
    conditions:
      a: 'emp != ""'
      b: 'emp != ""'
    default: a

- type: output
  name: out_a
  input: split.a
  config:
    name: out_a
    path: out_a.csv
    type: csv
    include_unmapped: true

- type: output
  name: out_b
  input: split.b
  config:
    name: out_b
    path: out_b.csv
    type: csv
    include_unmapped: true
"#;
    // Group A: 1 bad among 3 → all 3 records DLQ'd. Each record
    // reaches BOTH outputs (inclusive Route) so without correlation
    // grouping we'd see double-count; with correlation grouping the
    // DLQ entry count is one per source row, not per (row, output).
    let csv = "employee_id,value\nA,100\nA,bad\nA,300\nB,400\n";
    let config = crate::config::parse_config(yaml).unwrap();
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
    };
    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())) as Box<dyn std::io::Read + Send>,
    )]);
    let buf_a = SharedBuffer::new();
    let buf_b = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "out_a".to_string(),
            Box::new(buf_a.clone()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "out_b".to_string(),
            Box::new(buf_b.clone()) as Box<dyn std::io::Write + Send>,
        ),
    ]);
    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .unwrap();

    let out_a = buf_a.as_string();
    let out_b = buf_b.as_string();

    // 3 source rows DLQ'd (one per row, dedup across outputs)
    assert_eq!(report.counters.dlq_count, 3, "group A DLQ'd whole");
    // Group B's 1 record reached both outputs → records_written = 2
    assert_eq!(
        report.counters.records_written, 2,
        "group B written to both outputs"
    );
    assert!(out_a.contains("B"), "out_a should contain group B");
    assert!(out_b.contains("B"), "out_b should contain group B");
    assert!(!out_a.contains(",bad"), "bad row must not reach out_a");
    assert!(!out_b.contains(",bad"), "bad row must not reach out_b");
}

#[test]
fn correlation_key_with_arena_rejected_at_compile_time() {
    // P.1: E150 — `correlation_key` plus any `analytic_window` (arena)
    // rejected at compile time.
    let yaml = r#"
pipeline:
  name: corr_with_arena
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
    sort_order:
    - employee_id
    schema:
      - { name: employee_id, type: string }
      - { name: value, type: int }

- type: transform
  name: with_window
  input: src
  config:
    analytic_window:
      group_by: [employee_id]
    cxl: |
      emit emp = employee_id
      emit s = $window.sum(value)
- type: output
  name: out
  input: with_window
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = crate::config::parse_config(yaml).unwrap();
    let result = config.compile(&crate::config::CompileContext::default());
    let diags = result.expect_err("E150 should reject correlation_key + arena");
    assert!(
        diags.iter().any(|d| d.code == "E150"),
        "expected an E150 diagnostic, got: {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
}

#[test]
fn correlation_key_aggregate_group_by_must_be_superset() {
    // P.2: E151 — Aggregate group_by must include every
    // correlation_key field.
    let yaml = r#"
pipeline:
  name: corr_with_agg
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
    sort_order:
    - employee_id
    schema:
      - { name: employee_id, type: string }
      - { name: dept, type: string }
      - { name: value, type: int }

- type: aggregate
  name: agg
  input: src
  config:
    group_by:
    - dept
    cxl: 'emit total = sum(value)

      '
- type: output
  name: out
  input: agg
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = crate::config::parse_config(yaml).unwrap();
    let result = config.compile(&crate::config::CompileContext::default());
    let diags = result.expect_err("E151 should reject correlation_key not in group_by");
    assert!(
        diags.iter().any(|d| d.code == "E151"),
        "expected an E151 diagnostic, got: {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
}

#[test]
fn aggregate_output_inherits_correlation_meta() {
    // An aggregate that satisfies E151's group_by-superset invariant
    // must have its emitted rows participate in correlation rollback.
    // When a transform error fails one record in group A, the
    // surviving A records still flow into the aggregator and produce
    // one (A, total) output row — that aggregate row is the ONLY
    // thing the Output arm sees for group A, so it must inherit the
    // correlation meta and route into cell [A] alongside the trigger
    // error. The whole group then DLQs uniformly: one trigger for
    // the bad source row plus one collateral for the aggregate
    // output row. Group B is clean and its aggregate row flushes to
    // the writer.
    //
    // Mechanism: `MetadataCommonTracker` in HashAggregator /
    // StreamingAggregator observes every input record's meta and at
    // finalize() copies non-conflicting common pairs onto each
    // emitted row. Under E151's group_by ⊇ correlation_key.fields()
    // invariant every input record in a group shares the same
    // `__cxl_correlation_key` meta, so the tracker forwards it to
    // the aggregate output row.
    //
    // Without meta inheritance, the aggregate row for A would land
    // in a distinct null-keyed buffer cell, group A's writer-side
    // buffer would be "clean" (only the trigger event in cell [A]
    // with no record slots), and group A's aggregate output would
    // be wrongly emitted to the writer.
    let yaml = r#"
pipeline:
  name: aggregate_correlation_meta
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
    cxl: 'emit employee_id = employee_id

      emit val = value.to_int()

      '
- type: aggregate
  name: agg
  input: validate
  config:
    group_by:
    - employee_id
    cxl: 'emit employee_id = employee_id

      emit total = sum(val)

      '
- type: output
  name: out
  input: agg
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "employee_id,value\nA,1\nA,bad\nA,3\nB,7\nB,11\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(yaml, csv).unwrap();

    assert_eq!(
        counters.dlq_count, 2,
        "group A: 1 trigger (bad row) + 1 collateral (aggregate output)"
    );
    assert_eq!(counters.ok_count, 1, "only group B's aggregate row emitted");

    let triggers = dlq_entries.iter().filter(|e| e.trigger).count();
    let collaterals = dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(triggers, 1, "exactly one trigger for the bad source row");
    assert_eq!(
        collaterals, 1,
        "exactly one collateral for the aggregate output row"
    );

    let collateral = dlq_entries.iter().find(|e| !e.trigger).unwrap();
    assert_eq!(
        collateral.category,
        crate::dlq::DlqErrorCategory::Correlated,
        "aggregate-output collateral carries the Correlated category"
    );

    assert!(
        output.contains("B,18"),
        "output should contain group B's aggregate row: {output}"
    );
    assert!(
        !output.contains("A,4"),
        "output must NOT contain group A's aggregate row (group A failed): {output}"
    );
}

#[test]
fn group_identity_fixed_at_ingest_when_transform_rewrites_key() {
    // F.1: a Transform that rewrites the correlation_key field must
    // not change a row's group identity. Group identity is captured at
    // Source ingest (via `__cxl_correlation_key` meta) and survives
    // every downstream Transform.
    let yaml = r#"
pipeline:
  name: rewrite_key_test
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
  name: rewrite
  input: src
  config:
    cxl: 'emit employee_id = "REWRITTEN"

      emit val = value.to_int()

      '
- type: output
  name: out
  input: rewrite
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    // Group A: 2 good, 1 bad → the rewrite would change the field
    // value to "REWRITTEN" but group identity is the original "A".
    // Group B: all good. If grouping used the rewritten value, every
    // record would coalesce into one giant group called "REWRITTEN"
    // and the bad record would DLQ everyone (4 records); the
    // ingest-time-identity contract says only the original-A group's
    // 3 records get DLQ'd.
    let csv = "employee_id,value\nA,1\nA,bad\nA,3\nB,4\n";
    let (counters, _dlq_entries, _output) = run_correlated_pipeline(yaml, csv).unwrap();

    assert_eq!(
        counters.dlq_count, 3,
        "only the original-A group (3 records) DLQ'd"
    );
    assert_eq!(counters.ok_count, 1, "the original-B group emitted");
}

#[test]
fn combine_output_inherits_driver_correlation_meta() {
    // A combine's output rows must inherit the driver record's
    // correlation meta so they participate in the same buffer cell as
    // any driver-side trigger error from the same correlation group.
    // The driver side carries the authoritative group identity;
    // build-side rows are consumed by the hash table and never reach
    // the writer directly.
    //
    // Mechanism: the combine arm emits each combined record by either
    // (a) cloning the driver record and overlaying the body's emitted
    // fields, or (b) widening the driver record to the combine's
    // output schema via `widen_record_to_schema`, which copies
    // `iter_meta()` from the driver onto the widened record. Either
    // way the driver's `__cxl_correlation_key` meta survives onto the
    // combined output.
    //
    // Test pipeline: orders (driver) → validate (transform that fails
    // on a bad amount) → combine with departments (build) on
    // employee_id → output. correlation_key=employee_id stamped at
    // both sources. Group A's bad row triggers a buffer error; the
    // surviving A driver records produce combined rows that must DLQ
    // as Correlated collaterals. Group B's clean driver record
    // produces a combined row that flushes to the writer.
    let yaml = r#"
pipeline:
  name: combine_correlation_meta
error_handling:
  strategy: continue
  correlation_key: employee_id
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    schema:
      - { name: employee_id, type: string }
      - { name: amount, type: string }

- type: source
  name: departments
  config:
    name: departments
    path: departments.csv
    type: csv
    schema:
      - { name: employee_id, type: string }
      - { name: dept, type: string }

- type: transform
  name: validate
  input: orders
  config:
    cxl: 'emit employee_id = employee_id

      emit amount_int = amount.to_int()

      '
- type: combine
  name: enriched
  input:
    o: validate
    d: departments
  config:
    where: 'o.employee_id == d.employee_id'
    match: first
    on_miss: skip
    cxl: |
      emit employee_id = o.employee_id
      emit amount_int = o.amount_int
      emit dept = d.dept

- type: output
  name: out
  input: enriched
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: false
"#;
    let orders_csv = "employee_id,amount\nA,1\nA,bad\nA,3\nB,7\n";
    let departments_csv = "employee_id,dept\nA,HR\nB,ENG\n";

    let config = crate::config::parse_config(yaml).unwrap();
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
    };

    let primary = "orders".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([
        (
            "orders".to_string(),
            Box::new(std::io::Cursor::new(orders_csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        ),
        (
            "departments".to_string(),
            Box::new(std::io::Cursor::new(departments_csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        ),
    ]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .unwrap();
    let output = buf.as_string();

    // Group A driver records: A,1 (clean) and A,3 (clean) survive
    // validate; A,bad triggers the buffer error. The 2 surviving
    // driver records produce 2 combined output rows (each matched
    // with department HR). Both DLQ as collaterals; the bad row
    // DLQs as the trigger. Total = 3.
    assert_eq!(
        report.counters.dlq_count, 3,
        "group A: 1 trigger (bad row) + 2 collaterals (combined output rows for A,1 and A,3)"
    );
    assert_eq!(
        report.counters.ok_count, 1,
        "group B's combined row (B,7,ENG) is the only emission"
    );

    let triggers = report.dlq_entries.iter().filter(|e| e.trigger).count();
    let collaterals = report.dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(triggers, 1, "one trigger for the bad source row");
    assert_eq!(
        collaterals, 2,
        "two collaterals for the surviving driver-side combined output rows"
    );

    for c in report.dlq_entries.iter().filter(|e| !e.trigger) {
        assert_eq!(
            c.category,
            crate::dlq::DlqErrorCategory::Correlated,
            "combined-output collateral carries the Correlated category"
        );
    }

    assert!(
        output.contains("B,7,ENG"),
        "output should contain group B's combined row: {output}"
    );
    assert!(
        !output.contains("A,1") && !output.contains("A,3"),
        "output must NOT contain any group A combined row (group A failed): {output}"
    );
}

#[test]
fn combine_chain_output_inherits_driver_correlation_meta() {
    // Three-input combine decomposed into a left-deep chain of two
    // binary steps. Step 0 (synthetic, body-less) joins the driver
    // with the first build input and emits an intermediate record
    // that becomes step 1's driver. Step 1 (final, body-bearing)
    // joins that intermediate with the second build input and emits
    // the user-projected output row.
    //
    // The intermediate record must carry the original driver's
    // correlation meta so that step 1's widen path can re-emit it
    // onto the final output row. If step 0 drops meta, step 1's
    // driver has none, the final widen has nothing to copy, and the
    // output rows fall into null buffer cells disconnected from any
    // upstream trigger error.
    //
    // Test pipeline: orders (driver, with department_id correlation
    // key) → validate (transform that fails on a bad amount) →
    // 3-input combine (orders ⨝ products on product_id, then ⨝
    // categories on category_id) → output. correlation_key is
    // department_id so the trigger error from one bad record in
    // department A propagates to invalidate the surviving
    // department-A combined output rows.
    let yaml = r#"
pipeline:
  name: combine_chain_correlation_meta
error_handling:
  strategy: continue
  correlation_key: department_id
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department_id, type: string }
      - { name: product_id, type: string }
      - { name: amount, type: string }

- type: source
  name: products
  config:
    name: products
    path: products.csv
    type: csv
    schema:
      - { name: product_id, type: string }
      - { name: name, type: string }
      - { name: category_id, type: string }

- type: source
  name: categories
  config:
    name: categories
    path: categories.csv
    type: csv
    schema:
      - { name: category_id, type: string }
      - { name: category_name, type: string }

- type: transform
  name: validate
  input: orders
  config:
    cxl: |
      emit order_id = order_id
      emit department_id = department_id
      emit product_id = product_id
      emit amount_int = amount.to_int()

- type: combine
  name: enriched
  input:
    o: validate
    p: products
    c: categories
  config:
    where: "o.product_id == p.product_id and p.category_id == c.category_id"
    match: first
    on_miss: skip
    cxl: |
      emit order_id = o.order_id
      emit department_id = o.department_id
      emit product_name = p.name
      emit category_name = c.category_name
      emit amount_int = o.amount_int

- type: output
  name: out
  input: enriched
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: false
"#;
    let orders_csv = "order_id,department_id,product_id,amount\nO1,A,P1,100\nO2,A,P1,bad\nO3,A,P2,300\nO4,B,P2,400\n";
    let products_csv = "product_id,name,category_id\nP1,Widget,C1\nP2,Gadget,C2\n";
    let categories_csv = "category_id,category_name\nC1,Hardware\nC2,Software\n";

    let config = crate::config::parse_config(yaml).unwrap();
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
    };

    let primary = "orders".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([
        (
            "orders".to_string(),
            Box::new(std::io::Cursor::new(orders_csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        ),
        (
            "products".to_string(),
            Box::new(std::io::Cursor::new(products_csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        ),
        (
            "categories".to_string(),
            Box::new(std::io::Cursor::new(categories_csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        ),
    ]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .unwrap();
    let output = buf.as_string();

    // Department A: O1 and O3 succeed validate; O2 fails (trigger).
    // Both surviving driver rows produce one combined output row each
    // through the 2-step chain. With correct meta inheritance both
    // combined rows DLQ as collaterals. Trigger + 2 collaterals = 3.
    // Department B: O4 succeeds and produces one clean combined row
    // that flushes to the writer.
    assert_eq!(
        report.counters.dlq_count, 3,
        "department A: 1 trigger (bad row) + 2 collaterals (chain output rows for O1 and O3)"
    );
    assert_eq!(
        report.counters.ok_count, 1,
        "department B's combined row (O4) is the only emission"
    );

    let triggers = report.dlq_entries.iter().filter(|e| e.trigger).count();
    let collaterals = report.dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(triggers, 1, "one trigger for the bad source row");
    assert_eq!(
        collaterals, 2,
        "two collaterals for the surviving driver-side chain output rows"
    );

    assert!(
        output.contains("O4"),
        "output should contain department B's combined row: {output}"
    );
    assert!(
        !output.contains("O1") && !output.contains("O3"),
        "output must NOT contain department A combined rows (department A failed): {output}"
    );
}

#[test]
fn route_eval_error_dlqs_whole_group() {
    // A Route condition that errors at evaluation time (e.g., a CXL
    // expression that fails to convert) is routed to the correlation
    // buffer as a trigger event for the failing record's group, just
    // like a Transform error. Surviving records of the same group
    // that succeed routing then DLQ as collaterals at commit time.
    //
    // Test pipeline: source → route(condition contains
    // amount.to_int() > 0) → output. The bad row fails conversion
    // inside the route condition, surfacing a route eval error;
    // surviving group-A rows route cleanly to the "good" arm, but
    // the buffer rolls back the whole group at commit.
    let yaml = r#"
pipeline:
  name: route_eval_error_test
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
      - { name: amount, type: string }

- type: route
  name: split
  input: src
  config:
    mode: exclusive
    conditions:
      good: 'amount.to_int() > 0'
    default: good

- type: output
  name: out
  input: split.good
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "employee_id,amount\nA,1\nA,bad\nA,3\nB,4\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(yaml, csv).unwrap();

    assert_eq!(
        counters.dlq_count, 3,
        "group A: 1 trigger (bad route eval) + 2 collaterals (A,1 and A,3)"
    );
    assert_eq!(counters.ok_count, 1, "only B,4 emitted");

    let triggers = dlq_entries.iter().filter(|e| e.trigger).count();
    assert_eq!(triggers, 1, "one trigger for the route eval failure");

    assert!(output.contains("B,4"), "output should contain B: {output}");
    assert!(
        !output.contains("A,1") && !output.contains("A,3"),
        "output must NOT contain any group A rows: {output}"
    );
}

#[test]
fn combine_iejoin_output_inherits_driver_correlation_meta() {
    // The IEJoin combine strategy (selected for mixed equi+range
    // predicates) must inherit driver meta the same way the
    // HashBuildProbe path does. The kernel emits combined records via
    // its local `widen_record_to_schema` helper (pipeline/iejoin.rs),
    // which already copies `iter_meta()` from the driver record onto
    // the widened output. This test locks that behavior so a future
    // refactor that swaps the helper for a bare `Record::new(...)`
    // would surface here.
    //
    // Test pipeline: orders (driver) → validate (transform that fails
    // on a bad amount) → combine(orders ⨝ sessions on employee_id +
    // event-time-within-session) → output. The mixed equi+range
    // predicate triggers the HashPartitionIEJoin strategy.
    let yaml = r#"
pipeline:
  name: combine_iejoin_correlation_meta
error_handling:
  strategy: continue
  correlation_key: employee_id
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    schema:
      - { name: employee_id, type: string }
      - { name: amount, type: string }
      - { name: event_time, type: int }

- type: source
  name: sessions
  config:
    name: sessions
    path: sessions.csv
    type: csv
    schema:
      - { name: employee_id, type: string }
      - { name: session_start, type: int }
      - { name: session_end, type: int }

- type: transform
  name: validate
  input: orders
  config:
    cxl: |
      emit employee_id = employee_id
      emit amount_int = amount.to_int()
      emit event_time = event_time

- type: combine
  name: enriched
  input:
    o: validate
    s: sessions
  config:
    where: 'o.employee_id == s.employee_id and o.event_time >= s.session_start and o.event_time <= s.session_end'
    match: first
    on_miss: skip
    cxl: |
      emit employee_id = o.employee_id
      emit amount_int = o.amount_int
      emit event_time = o.event_time
      emit session_start = s.session_start

- type: output
  name: out
  input: enriched
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: false
"#;
    let orders_csv = "employee_id,amount,event_time\nA,1,5\nA,bad,10\nA,3,15\nB,4,25\n";
    let sessions_csv = "employee_id,session_start,session_end\nA,0,20\nB,20,30\n";

    let config = crate::config::parse_config(yaml).unwrap();
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
    };

    let primary = "orders".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([
        (
            "orders".to_string(),
            Box::new(std::io::Cursor::new(orders_csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        ),
        (
            "sessions".to_string(),
            Box::new(std::io::Cursor::new(sessions_csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        ),
    ]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .unwrap();
    let output = buf.as_string();

    assert_eq!(
        report.counters.dlq_count, 3,
        "group A: 1 trigger (bad row) + 2 collaterals (IEJoin output rows)"
    );
    assert_eq!(
        report.counters.ok_count, 1,
        "only group B's combined row emitted"
    );

    let triggers = report.dlq_entries.iter().filter(|e| e.trigger).count();
    let collaterals = report.dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(triggers, 1, "one trigger for the bad source row");
    assert_eq!(collaterals, 2, "two collaterals for IEJoin output rows");

    assert!(output.contains("B,4"), "output should contain B: {output}");
    assert!(
        !output.contains("A,1") && !output.contains("A,3"),
        "output must NOT contain group A rows: {output}"
    );
}
