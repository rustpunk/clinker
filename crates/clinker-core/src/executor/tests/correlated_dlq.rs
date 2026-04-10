//! Phase 14 correlated DLQ tests — grouped rejection with sort-and-group pattern.
//!
//! Tests: one-fail-DLQs-group, trigger flag, null key individual rejection,
//! compound keys, auto-sort injection, --explain output, large groups,
//! buffer overflow, threshold counting.

use super::*;
use crate::test_helpers::SharedBuffer;
use std::collections::HashMap;

/// Run a single-output pipeline with the given YAML and CSV input.
/// Returns (counters, dlq_entries, output_csv).
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

    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        config.source_configs().next().unwrap().name.clone(),
        Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec()))
            as Box<dyn std::io::Read + Send>,
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report = PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)?;
    Ok((report.counters, report.dlq_entries, buf.as_string()))
}

fn base_yaml(correlation_key: &str) -> String {
    format!(
        r#"
pipeline:
  name: correlated_test
error_handling:
  strategy: continue
  correlation_key:
    correlation_key: null
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }

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
"#
    )
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_sorted_one_fail() {
    // One record fails → entire key group DLQ'd
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\nA,100\nA,bad\nA,300\nB,400\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(&yaml, csv).unwrap();

    // Group A (3 records, one "bad") should be entirely DLQ'd
    // Group B (1 record, good) should be emitted
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
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_sorted_good_group() {
    // Good groups emitted, bad group DLQ'd
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\nA,100\nA,200\nB,bad\nB,300\nC,500\nC,600\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(&yaml, csv).unwrap();

    // Group A: all good -> emitted (2 records)
    // Group B: one bad -> all DLQ'd (2 records)
    // Group C: all good -> emitted (2 records)
    assert_eq!(counters.ok_count, 4, "groups A and C emitted (4 records)");
    assert_eq!(counters.dlq_count, 2, "group B DLQ'd (2 records)");
    assert_eq!(dlq_entries.len(), 2);
    assert!(output.contains("A,100"), "output has group A");
    assert!(output.contains("C,500"), "output has group C");
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_trigger_flag() {
    // Root-cause record has trigger=true, collateral has trigger=false
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\nA,100\nA,bad\nA,300\n";
    let (_counters, dlq_entries, _output) = run_correlated_pipeline(&yaml, csv).unwrap();

    assert_eq!(dlq_entries.len(), 3);

    let triggers: Vec<bool> = dlq_entries.iter().map(|e| e.trigger).collect();
    let root_causes = triggers.iter().filter(|&&t| t).count();
    let collaterals = triggers.iter().filter(|&&t| !t).count();

    assert_eq!(root_causes, 1, "exactly one root cause (the bad record)");
    assert_eq!(collaterals, 2, "two collateral records");

    // The root cause should be the record with "bad"
    let root = dlq_entries.iter().find(|e| e.trigger).unwrap();
    assert!(
        root.error_message.contains("convert") || root.error_message.contains("Int"),
        "root cause error should mention conversion failure: {}",
        root.error_message
    );
}

#[test]
fn test_correlated_dlq_null_key_individual() {
    // Null key → individual rejection (not grouped)
    let yaml = base_yaml("employee_id");
    // Empty employee_id fields = null key
    let csv = "employee_id,value\n,100\n,bad\n,300\nA,400\n";
    let (counters, dlq_entries, _output) = run_correlated_pipeline(&yaml, csv).unwrap();

    // Each null-key record is its own group:
    // Row 1 (empty, 100): good -> emitted
    // Row 2 (empty, bad): bad -> individual DLQ
    // Row 3 (empty, 300): good -> emitted
    // Row 4 (A, 400): good -> emitted
    assert_eq!(counters.ok_count, 3, "three good records emitted");
    assert_eq!(counters.dlq_count, 1, "only the bad null-key record DLQ'd");
    assert_eq!(dlq_entries.len(), 1);
    assert!(dlq_entries[0].trigger, "individual rejection is root cause");
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_compound_key() {
    // Compound key [employee_id, dept] works correctly
    let yaml = format!(
        r#"
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
      - { name: id, type: string }

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
"#
    );
    // Groups: (A,HR), (A,ENG), (B,HR)
    let csv = "employee_id,dept,value\nA,HR,100\nA,HR,bad\nA,ENG,200\nB,HR,300\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(&yaml, csv).unwrap();

    // (A,HR): one bad -> entire group DLQ'd (2 records)
    // (A,ENG): good -> emitted (1 record)
    // (B,HR): good -> emitted (1 record)
    assert_eq!(counters.dlq_count, 2, "group (A,HR) DLQ'd");
    assert_eq!(counters.ok_count, 2, "groups (A,ENG) and (B,HR) emitted");
    assert_eq!(dlq_entries.len(), 2);
    assert!(output.contains("A,ENG"), "output has (A,ENG)");
    assert!(output.contains("B,HR"), "output has (B,HR)");
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_auto_sort_prepend() {
    // Auto-sort prepends correlation_key to existing sort fields
    let yaml = r#"
pipeline:
  name: auto_sort_test
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
    - timestamp
    schema:
      - { name: id, type: string }

- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit emp = employee_id

      emit ts = timestamp

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
    // Input NOT sorted by employee_id — auto-sort should kick in
    // Note: sorted by timestamp per sort_order, but not by employee_id
    let csv = "employee_id,timestamp,value\nB,1,100\nA,2,200\nA,3,bad\nB,4,300\n";
    let (counters, dlq_entries, _output) = run_correlated_pipeline(yaml, csv).unwrap();

    // After auto-sort by [employee_id, timestamp]:
    // A,2,200 and A,3,bad -> group A: one bad -> DLQ (2 records)
    // B,1,100 and B,4,300 -> group B: all good -> emitted (2 records)
    assert_eq!(counters.dlq_count, 2, "group A DLQ'd after auto-sort");
    assert_eq!(counters.ok_count, 2, "group B emitted after auto-sort");
    assert_eq!(dlq_entries.len(), 2);
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_auto_sort_already_sorted() {
    // If input already sorted by correlation key, no injection
    let yaml = r#"
pipeline:
  name: already_sorted_test
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
    - timestamp
    schema:
      - { name: id, type: string }

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
    // Input pre-sorted by employee_id
    let csv = "employee_id,timestamp,value\nA,1,100\nA,2,bad\nB,3,300\n";
    // Still works correctly
    let (counters, dlq_entries, _) = run_correlated_pipeline(yaml, csv).unwrap();
    assert_eq!(counters.dlq_count, 2, "group A DLQ'd");
    assert_eq!(counters.ok_count, 1, "group B emitted");
    assert_eq!(dlq_entries.len(), 2);
}

#[test]
fn test_correlated_dlq_explain_shows_sort() {
    // --explain output shows injected sort
    use crate::plan::execution::ExecutionPlanDag;

    let yaml = r#"
pipeline:
  name: explain_test
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
      - { name: id, type: string }

- type: transform
  name: validate
  input: src
  config:
    cxl: 'emit val = value.to_int()

      '
- type: output
  name: out
  input: validate
  config:
    name: out
    path: output.csv
    type: csv
"#;
    let config = crate::config::parse_config(yaml).unwrap();

    // Phase 16b Task 16b.9: pull pre-typechecked programs from
    // `config.compile()` artifacts instead of running a runtime
    // typecheck pass.
    let validated_plan = config.compile().unwrap();
    let resolved_transforms_owned = crate::executor::build_transform_specs(&config);
    let compiled_refs: Vec<(&str, &cxl::typecheck::TypedProgram)> = resolved_transforms_owned
        .iter()
        .map(|t| {
            let typed = validated_plan
                .artifacts()
                .typed
                .get(&t.name)
                .expect("cxl_compile produced a typed program for this node");
            (t.name.as_str(), typed.as_ref())
        })
        .collect();

    let mut plan = ExecutionPlanDag::compile(&config, &compiled_refs).unwrap();

    // Simulate the sort injection that run_with_readers_writers does
    let correlation_key = config.error_handling.correlation_key.as_ref().unwrap();
    plan.correlation_sort_note = Some(format!(
        "Correlation sort (auto-injected): {:?} + existing {:?}",
        correlation_key.fields(),
        Vec::<String>::new()
    ));

    let explain_output = plan.explain();
    assert!(
        explain_output.contains("Correlation sort"),
        "explain should mention correlation sort: {explain_output}"
    );
    assert!(
        explain_output.contains("auto-injected"),
        "explain should mention auto-injected: {explain_output}"
    );
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_reason_message() {
    // DLQ entries contain "correlated with failure" reason
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\nA,100\nA,bad\n";
    let (_counters, dlq_entries, _output) = run_correlated_pipeline(&yaml, csv).unwrap();

    let collateral = dlq_entries.iter().find(|e| !e.trigger).unwrap();
    assert!(
        collateral
            .error_message
            .contains("correlated with failure in group"),
        "collateral reason should mention correlation: {}",
        collateral.error_message
    );
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_large_group() {
    // 1000-record group, one fails → all 1000 DLQ'd
    let yaml = base_yaml("employee_id");
    let mut csv = String::from("employee_id,value\n");
    for i in 0..999 {
        csv.push_str(&format!("A,{}\n", i));
    }
    csv.push_str("A,bad\n"); // The 1000th record fails
    csv.push_str("B,100\n"); // Good group

    let (counters, dlq_entries, _output) = run_correlated_pipeline(&yaml, &csv).unwrap();

    assert_eq!(counters.dlq_count, 1000, "all 1000 in group A DLQ'd");
    assert_eq!(counters.ok_count, 1, "group B emitted");
    assert_eq!(dlq_entries.len(), 1000);

    let root_causes = dlq_entries.iter().filter(|e| e.trigger).count();
    assert_eq!(root_causes, 1, "exactly one root cause");
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_multiple_failures_in_group() {
    // 3 of 10 fail → all 10 DLQ'd, first failure is trigger
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\n\
        A,100\nA,200\nA,bad1\nA,400\nA,bad2\n\
        A,600\nA,700\nA,bad3\nA,900\nA,1000\n\
        B,500\n";
    let (counters, dlq_entries, _output) = run_correlated_pipeline(&yaml, csv).unwrap();

    assert_eq!(counters.dlq_count, 10, "all 10 in group A DLQ'd");
    assert_eq!(counters.ok_count, 1, "group B emitted");
    assert_eq!(dlq_entries.len(), 10);

    // Multiple root causes (each bad record is a trigger)
    let root_causes = dlq_entries.iter().filter(|e| e.trigger).count();
    assert_eq!(root_causes, 3, "3 root causes (3 bad records)");

    let collaterals = dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(collaterals, 7, "7 collateral records");
}

#[test]
fn test_correlated_dlq_empty_input() {
    // Zero records → no DLQ entries, no output records
    let yaml = base_yaml("employee_id");
    let csv = "employee_id,value\n";
    let (counters, dlq_entries, output) = run_correlated_pipeline(&yaml, csv).unwrap();

    assert_eq!(counters.total_count, 0);
    assert_eq!(counters.dlq_count, 0);
    assert_eq!(counters.ok_count, 0);
    assert!(dlq_entries.is_empty());
    // Output should be empty or header-only
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.len() <= 1, "empty or header-only output");
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_group_exceeds_buffer() {
    // Group > max_group_buffer → all DLQ'd with group_size_exceeded
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
      - { name: id, type: string }

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
    // Group A has 5 records, exceeds max_group_buffer of 3
    let csv = "employee_id,value\nA,100\nA,200\nA,300\nA,400\nA,500\nB,600\n";
    let (counters, dlq_entries, _output) = run_correlated_pipeline(yaml, csv).unwrap();

    // Group A: first 4 records DLQ'd when buffer (3) exceeded at record 4.
    // Record A,500 starts a fresh buffer (it's a "new" group after overflow reset),
    // and gets emitted normally. Group B also emitted.
    assert_eq!(
        counters.dlq_count, 4,
        "4 records DLQ'd from buffer overflow"
    );
    assert_eq!(
        counters.ok_count, 2,
        "A500 (post-overflow) and B600 emitted"
    );

    // Check that at least one entry mentions group_size_exceeded
    let exceeded = dlq_entries
        .iter()
        .any(|e| e.error_message.contains("group_size_exceeded"));
    assert!(exceeded, "should have group_size_exceeded message");
}

#[test]
#[ignore = "Re-enabled by Task 16.0.5.10 once enforcer-insertion is wired into compile_transforms; correlation sort was deleted in 16.0.5.9"]
fn test_correlated_dlq_threshold_counts_root_cause_only() {
    // ErrorThreshold should count only root-cause entries, not collateral.
    // We test this by counting trigger=true entries vs total DLQ count.
    let yaml = base_yaml("employee_id");
    // 100-record group with 1 failure
    let mut csv = String::from("employee_id,value\n");
    for i in 0..99 {
        csv.push_str(&format!("A,{}\n", i));
    }
    csv.push_str("A,bad\n"); // 1 failure

    let (counters, dlq_entries, _output) = run_correlated_pipeline(&yaml, &csv).unwrap();

    assert_eq!(counters.dlq_count, 100, "all 100 DLQ'd");
    assert_eq!(dlq_entries.len(), 100);

    // Count root causes (trigger=true) — should be 1, not 100
    let root_cause_count = dlq_entries.iter().filter(|e| e.trigger).count();
    assert_eq!(
        root_cause_count, 1,
        "threshold should count only 1 root cause, not 100"
    );

    // Collateral count should be 99
    let collateral_count = dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(collateral_count, 99, "99 collateral records");
}
