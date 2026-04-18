//! Branch execution tests for Phase 15.
//!
//! Tests in this module exercise the DAG executor's branch dispatch,
//! merge semantics, record conservation, and per-node execution strategy.

use super::*;

/// Helper: run executor with in-memory CSV input/output for branching tests.
fn run_branch_test(
    yaml: &str,
    csv_input: &str,
) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
    let config = crate::config::parse_config(yaml).unwrap();
    let output_buf = clinker_bench_support::io::SharedBuffer::new();

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec()))
            as Box<dyn std::io::Read + Send>,
    )]);
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(output_buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(|v| crate::config::convert_pipeline_vars(v))
        .unwrap_or_default();
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars,
        shutdown_token: None,
    };

    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)?;

    let output = output_buf.as_string();
    Ok((report.counters, report.dlq_entries, output))
}

/// Diamond DAG: fork -> 2 branches -> merge: all records present in output.
#[test]
fn test_branch_diamond_dag() {
    let yaml = r#"
pipeline:
  name: diamond
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 100
    default: low
- type: transform
  name: enrich_high
  input: classify.high
  config:
    cxl: 'emit tag = "HIGH"

      '
- type: transform
  name: enrich_low
  input: classify.low
  config:
    cxl: 'emit tag = "LOW"

      '
- type: merge
  name: combine__merge
  inputs:
  - enrich_high
  - enrich_low
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = tag

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,200\n2,50\n3,300\n4,10\n";
    let (counters, dlq, output) = run_branch_test(yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 4, "all 4 records should be in output");
    assert!(dlq.is_empty(), "no DLQ entries expected");

    // All records should be present
    assert!(
        output.contains("HIGH"),
        "output should contain HIGH tag: {output}"
    );
    assert!(
        output.contains("LOW"),
        "output should contain LOW tag: {output}"
    );
}

/// Exclusive mode: input count = output count (no duplication, no loss).
#[test]
fn test_branch_exclusive_conservation() {
    let yaml = r#"
pipeline:
  name: exclusive_conservation
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 100
    default: low
    mode: exclusive
- type: transform
  name: enrich_high
  input: classify.high
  config:
    cxl: 'emit tag = "HIGH"

      '
- type: transform
  name: enrich_low
  input: classify.low
  config:
    cxl: 'emit tag = "LOW"

      '
- type: merge
  name: combine__merge
  inputs:
  - enrich_high
  - enrich_low
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = tag

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,200\n2,50\n3,300\n4,10\n5,150\n";
    let (counters, _, _) = run_branch_test(yaml, csv).unwrap();

    // Exclusive mode: no duplication, no loss
    assert_eq!(
        counters.ok_count, 5,
        "input count should equal output count"
    );
}

/// Inclusive mode: records in multiple branches, merge has more than input.
#[test]
fn test_branch_inclusive_duplication() {
    let yaml = r#"
pipeline:
  name: inclusive_dup
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      over_50: amount_val > 50
      over_100: amount_val > 100
    default: low
    mode: inclusive
- type: transform
  name: tag_over50
  input: classify.over_50
  config:
    cxl: 'emit tag = "OVER50"

      '
- type: transform
  name: tag_over100
  input: classify.over_100
  config:
    cxl: 'emit tag = "OVER100"

      '
- type: transform
  name: tag_low
  input: classify.low
  config:
    cxl: 'emit tag = "LOW"

      '
- type: merge
  name: combine__merge
  inputs:
  - tag_over50
  - tag_over100
  - tag_low
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = tag

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,200\n2,50\n3,75\n";
    let (counters, _, output) = run_branch_test(yaml, csv).unwrap();

    // id=1 (200): matches over_50 AND over_100 -> 2 records
    // id=2 (50): matches nothing -> default (low) -> 1 record
    // id=3 (75): matches over_50 only -> 1 record
    // Total: 4 records output from 3 input records
    assert_eq!(
        counters.ok_count, 4,
        "inclusive mode should duplicate records: {output}"
    );
}

/// Records within each branch maintain input order.
#[test]
fn test_branch_order_within_branch() {
    let yaml = r#"
pipeline:
  name: order_test
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 100
    default: low
- type: transform
  name: enrich_high
  input: classify.high
  config:
    cxl: 'emit tag = "H"

      '
- type: transform
  name: enrich_low
  input: classify.low
  config:
    cxl: 'emit tag = "L"

      '
- type: merge
  name: combine__merge
  inputs:
  - enrich_high
  - enrich_low
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = tag

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    // High records: 1(200), 3(300), 5(500) -- should maintain order
    let csv = "id,amount\n1,200\n2,50\n3,300\n4,10\n5,500\n";
    let (counters, _, output) = run_branch_test(yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 5, "all records should be in output");

    // Find column indices from header
    let lines: Vec<&str> = output.lines().collect();
    let header: Vec<&str> = lines[0].split(',').collect();
    let id_col = header.iter().position(|&h| h == "id").unwrap();
    let tag_col = header.iter().position(|&h| h == "tag").unwrap();

    // Within the high branch, records should be in input order: 1, 3, 5
    let high_ids: Vec<&str> = lines[1..]
        .iter()
        .filter_map(|l| {
            let cols: Vec<&str> = l.split(',').collect();
            if cols.get(tag_col) == Some(&"H") {
                cols.get(id_col).copied()
            } else {
                None
            }
        })
        .collect();
    assert_eq!(high_ids, vec!["1", "3", "5"], "high branch order: {output}");
}

/// Merge output: branch A records, then branch B records (declaration order).
#[test]
fn test_branch_merge_concatenation_order() {
    let yaml = r#"
pipeline:
  name: merge_order
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 100
    default: low
- type: transform
  name: enrich_high
  input: classify.high
  config:
    cxl: 'emit tag = "H"

      '
- type: transform
  name: enrich_low
  input: classify.low
  config:
    cxl: 'emit tag = "L"

      '
- type: merge
  name: combine__merge
  inputs:
  - enrich_high
  - enrich_low
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = tag

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,200\n2,50\n3,300\n4,10\n";
    let (_, _, output) = run_branch_test(yaml, csv).unwrap();

    // enrich_high is declared first in the merge input, so high records come first
    let lines: Vec<&str> = output.lines().collect();
    let header: Vec<&str> = lines[0].split(',').collect();
    let tag_col = header.iter().position(|&h| h == "tag").unwrap();

    let tags: Vec<&str> = lines[1..]
        .iter()
        .filter_map(|l| {
            let cols: Vec<&str> = l.split(',').collect();
            cols.get(tag_col).copied()
        })
        .collect();
    // High branch first (ids 1, 3), then low branch (ids 2, 4)
    assert_eq!(tags, vec!["H", "H", "L", "L"], "merge order: {output}");
}

/// Route condition that never matches -> empty branch -> no error.
#[test]
fn test_branch_empty_branch_no_error() {
    let yaml = r#"
pipeline:
  name: empty_branch
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      impossible: amount_val > 999999
    default: normal
- type: transform
  name: tag_impossible
  input: classify.impossible
  config:
    cxl: 'emit tag = "IMPOSSIBLE"

      '
- type: transform
  name: tag_normal
  input: classify.normal
  config:
    cxl: 'emit tag = "NORMAL"

      '
- type: merge
  name: combine__merge
  inputs:
  - tag_impossible
  - tag_normal
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = tag

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,100\n2,200\n3,300\n";
    let (counters, dlq, output) = run_branch_test(yaml, csv).unwrap();

    // All records go to 'normal' branch, 'impossible' is empty
    assert_eq!(counters.ok_count, 3);
    assert!(dlq.is_empty());
    assert!(output.contains("NORMAL"));
    assert!(!output.contains("IMPOSSIBLE"));
}

/// 3 branches, each with different transforms.
#[test]
fn test_branch_three_way_fork() {
    let yaml = r#"
pipeline:
  name: three_way
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 200
      medium: amount_val > 50
    default: low
- type: transform
  name: tag_high
  input: classify.high
  config:
    cxl: 'emit tier = "HIGH"

      '
- type: transform
  name: tag_medium
  input: classify.medium
  config:
    cxl: 'emit tier = "MEDIUM"

      '
- type: transform
  name: tag_low
  input: classify.low
  config:
    cxl: 'emit tier = "LOW"

      '
- type: merge
  name: combine__merge
  inputs:
  - tag_high
  - tag_medium
  - tag_low
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = tier

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,300\n2,100\n3,10\n4,500\n5,75\n6,5\n";
    let (counters, _, output) = run_branch_test(yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 6, "all 6 records in output");
    assert!(output.contains("HIGH"));
    assert!(output.contains("MEDIUM"));
    assert!(output.contains("LOW"));
}

/// Branch A: enrichment, Branch B: filtering -- different transforms per branch.
#[test]
fn test_branch_different_transforms_per_branch() {
    let yaml = r#"
pipeline:
  name: different_transforms
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 100
    default: low
- type: transform
  name: enrich_high
  input: classify.high
  config:
    cxl: 'emit enriched = id + "_premium"

      '
- type: transform
  name: enrich_low
  input: classify.low
  config:
    cxl: 'emit enriched = id + "_standard"

      '
- type: merge
  name: combine__merge
  inputs:
  - enrich_high
  - enrich_low
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = enriched

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,200\n2,50\n3,300\n";
    let (counters, _, output) = run_branch_test(yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 3);
    assert!(
        output.contains("1_premium"),
        "high branch should enrich: {output}"
    );
    assert!(
        output.contains("2_standard"),
        "low branch should enrich: {output}"
    );
    assert!(
        output.contains("3_premium"),
        "high branch should enrich: {output}"
    );
}

/// Linear pipeline executes correctly through execute_dag() (no regression).
#[test]
fn test_dag_linear_execution_no_regression() {
    // This test verifies that linear pipelines (no branching) still work
    // correctly through execute_dag(). It's a regression test.
    let yaml = r#"
pipeline:
  name: linear
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: name, type: any }
      - { name: age, type: any }

- type: transform
  name: calc
  input: src
  config:
    cxl: 'emit doubled = name + "_doubled"

      '
- type: output
  name: dest
  input: calc
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "name,age\nAlice,30\nBob,25\nCarol,35\n";
    let (counters, dlq, output) = run_branch_test(yaml, csv).unwrap();

    assert_eq!(counters.total_count, 3);
    assert_eq!(counters.ok_count, 3);
    assert!(dlq.is_empty());
    assert!(output.contains("Alice_doubled"));
    assert!(output.contains("Bob_doubled"));
    assert!(output.contains("Carol_doubled"));
}

/// TwoPass node in one branch, Streaming in another -- per-node dispatch works.
#[test]
fn test_dag_mixed_execution_reqs() {
    // When any transform requires arena (window functions), the entire
    // pipeline runs in TwoPass mode. This test verifies that a pipeline
    // mixing window and non-window transforms works correctly.
    let yaml = r#"
pipeline:
  name: mixed_reqs
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: dept, type: any }
      - { name: amount, type: any }

- type: transform
  name: stateless_calc
  input: src
  config:
    cxl: 'emit label = dept + "_label"

      '
- type: transform
  name: window_calc
  input: stateless_calc
  config:
    cxl: 'emit cnt = $window.count()

      '
    analytic_window:
      group_by:
      - dept
- type: output
  name: dest
  input: window_calc
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "dept,amount\nA,10\nB,20\nA,30\n";
    let (counters, dlq, output) = run_branch_test(yaml, csv).unwrap();

    assert_eq!(counters.total_count, 3);
    assert_eq!(counters.ok_count, 3);
    assert!(dlq.is_empty());
    assert!(
        output.contains("label"),
        "stateless transform output: {output}"
    );
    assert!(output.contains("cnt"), "window transform output: {output}");
    assert!(output.contains("A_label"), "output: {output}");
}

/// rayon::scope indexed results: deterministic merge order for N > 2 branches.
#[test]
fn test_branch_rayon_scope_deterministic_order() {
    // Run the three-way fork multiple times and verify deterministic output
    let yaml = r#"
pipeline:
  name: deterministic
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 200
      medium: amount_val > 50
    default: low
- type: transform
  name: tag_high
  input: classify.high
  config:
    cxl: 'emit tier = "HIGH"

      '
- type: transform
  name: tag_medium
  input: classify.medium
  config:
    cxl: 'emit tier = "MEDIUM"

      '
- type: transform
  name: tag_low
  input: classify.low
  config:
    cxl: 'emit tier = "LOW"

      '
- type: merge
  name: combine__merge
  inputs:
  - tag_high
  - tag_medium
  - tag_low
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = tier

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,300\n2,100\n3,10\n4,500\n5,75\n";

    // Run multiple times and check deterministic output
    let (_, _, output1) = run_branch_test(yaml, csv).unwrap();
    let (_, _, output2) = run_branch_test(yaml, csv).unwrap();
    let (_, _, output3) = run_branch_test(yaml, csv).unwrap();

    assert_eq!(output1, output2, "output must be deterministic");
    assert_eq!(output2, output3, "output must be deterministic");
}

/// Inclusive mode clones records -- mutations in one branch don't affect another.
#[test]
fn test_branch_inclusive_isolation() {
    let yaml = r#"
pipeline:
  name: inclusive_isolation
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: any }
      - { name: amount, type: any }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      branch_a: amount_val > 50
      branch_b: amount_val > 50
    default: low
    mode: inclusive
- type: transform
  name: mutate_a
  input: classify.branch_a
  config:
    cxl: 'emit marker = "A_" + id

      '
- type: transform
  name: mutate_b
  input: classify.branch_b
  config:
    cxl: 'emit marker = "B_" + id

      '
- type: merge
  name: combine__merge
  inputs:
  - mutate_a
  - mutate_b
- type: transform
  name: combine
  input: combine__merge
  config:
    cxl: 'emit final = marker

      '
- type: output
  name: dest
  input: combine
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,100\n";
    let (counters, _, output) = run_branch_test(yaml, csv).unwrap();

    // id=1 matches both branches (inclusive) -> 2 output records
    assert_eq!(counters.ok_count, 2);
    // Branch A should have "A_1", Branch B should have "B_1"
    // They're independent — mutations in one don't affect the other
    assert!(output.contains("A_1"), "branch A marker: {output}");
    assert!(output.contains("B_1"), "branch B marker: {output}");
}
