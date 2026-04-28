//! Post-aggregate analytic-window correctness suite.
//!
//! Pins the value-correctness contract for the
//! `Source → Aggregate → Transform(window)` geometry. Every test asserts
//! the windowed Transform's emit values against arithmetic ground truth
//! computed independently of the planner — the previous source-rooted
//! geometry would silently see `Value::Null` from `arena.resolve_field`
//! when the windowed expression referenced an aggregate-emitted column,
//! and the test surface that hides Null arithmetic (substring matches,
//! "is Some" assertions) was the failure mode this rip exists to
//! eliminate.

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
            .map(crate::config::convert_pipeline_vars)
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

/// Parse a CSV string into (header, rows-keyed-by-first-column).
/// `key_col` names the column whose value identifies a row in the
/// returned map; subsequent columns are stored as `(name, value)` pairs
/// so test assertions can look up `running_total` etc. by department.
fn csv_rows_by_key(csv: &str, key_col: &str) -> HashMap<String, HashMap<String, String>> {
    let mut lines = csv.lines();
    let header_line = lines.next().expect("csv must have header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let key_idx = headers
        .iter()
        .position(|h| *h == key_col)
        .unwrap_or_else(|| panic!("key column {key_col:?} not found in headers {headers:?}"));
    let mut rows: HashMap<String, HashMap<String, String>> = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let values: Vec<&str> = line.split(',').collect();
        let key = values[key_idx].to_string();
        let row: HashMap<String, String> = headers
            .iter()
            .zip(values.iter())
            .map(|(h, v)| ((*h).to_string(), (*v).to_string()))
            .collect();
        rows.insert(key, row);
    }
    rows
}

// ── #1: $window.sum over post-aggregate ────────────────────────────

const SUM_PIPELINE: &str = r#"
pipeline:
  name: post_agg_window_sum
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
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
  name: running
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: running
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

/// Single-row-per-partition: `running_total == total`.
///
/// The aggregate emits one row per department; the windowed Transform's
/// `partition_by: [department]` gives each emitted row its own partition
/// of size 1. `$window.sum(total)` over a single-row partition equals
/// `total`. A node-rooted arena resolves `total` via the upstream
/// aggregate's emit buffer; the previous source-rooted geometry would
/// see Null because `total` is not a source column.
#[test]
fn post_aggregate_window_sum_equals_total_for_single_row_partition() {
    let csv = "\
department,amount
HR,10
HR,20
HR,30
ENG,100
ENG,200
ENG,300
";
    let (counters, dlq, output) = run_pipeline(SUM_PIPELINE, csv).expect("pipeline must execute");
    assert_eq!(counters.dlq_count, 0, "no failure injected");
    assert!(dlq.is_empty(), "no failure injected");

    let rows = csv_rows_by_key(&output, "department");
    let hr = rows.get("HR").expect("HR row must be present");
    assert_eq!(hr.get("total").unwrap(), "60", "HR total = 10+20+30");
    assert_eq!(
        hr.get("running_total").unwrap(),
        "60",
        "HR running_total over a 1-row partition equals HR total"
    );
    let eng = rows.get("ENG").expect("ENG row must be present");
    assert_eq!(eng.get("total").unwrap(), "600", "ENG total = 100+200+300");
    assert_eq!(
        eng.get("running_total").unwrap(),
        "600",
        "ENG running_total over a 1-row partition equals ENG total"
    );
}

// ── #2: $window.sum across multiple aggregate-emitted rows in a partition

const MULTI_ROW_PIPELINE: &str = r#"
pipeline:
  name: post_agg_multi_row
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: region, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_region
  input: src
  config:
    group_by: [department, region]
    cxl: |
      emit department = department
      emit region = region
      emit total = sum(amount)
- type: transform
  name: dept_sums
  input: dept_region
  config:
    cxl: |
      emit department = department
      emit region = region
      emit total = total
      emit dept_total = $window.sum(total)
    analytic_window:
      group_by: [department]
      sort_by:
        - field: region
          order: asc
- type: output
  name: out
  input: dept_sums
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

/// Multi-row-per-partition: `$window.sum(total)` over a partition with
/// multiple aggregate-emitted rows yields the sum across the partition.
/// Two aggregate group keys (`department`, `region`) → window
/// `partition_by: [department]`. Every row in HR sees the HR sum;
/// every row in ENG sees the ENG sum.
#[test]
fn post_aggregate_window_sum_across_multiple_emit_rows_per_partition() {
    // HR/east: 10+20=30, HR/west: 5  → HR partition total = 35
    // ENG/east: 100+200=300, ENG/west: 50, ENG/north: 7+3=10 → ENG = 360
    let csv = "\
department,region,amount
HR,east,10
HR,east,20
HR,west,5
ENG,east,100
ENG,east,200
ENG,west,50
ENG,north,7
ENG,north,3
";
    let (counters, dlq, output) =
        run_pipeline(MULTI_ROW_PIPELINE, csv).expect("pipeline must execute");
    assert_eq!(counters.dlq_count, 0);
    assert!(dlq.is_empty());

    // Map (department, region) → row.
    let mut by_key: HashMap<(String, String), HashMap<String, String>> = HashMap::new();
    let mut lines = output.lines();
    let header_line = lines.next().expect("header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let dept_idx = headers.iter().position(|h| *h == "department").unwrap();
    let region_idx = headers.iter().position(|h| *h == "region").unwrap();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let values: Vec<&str> = line.split(',').collect();
        let row: HashMap<String, String> = headers
            .iter()
            .zip(values.iter())
            .map(|(h, v)| ((*h).to_string(), (*v).to_string()))
            .collect();
        by_key.insert(
            (values[dept_idx].to_string(), values[region_idx].to_string()),
            row,
        );
    }

    // Every HR row's dept_total = 35; every ENG row's dept_total = 360.
    for ((dept, region), row) in &by_key {
        let dept_total = row.get("dept_total").expect("dept_total emitted").as_str();
        let expected = match dept.as_str() {
            "HR" => "35",
            "ENG" => "360",
            other => panic!("unexpected dept {other}"),
        };
        assert_eq!(
            dept_total, expected,
            "{dept}/{region}: dept_total must equal {expected} (sum across partition)"
        );
    }

    // Pin the per-row aggregate `total` values too so the test catches
    // regressions in the aggregate emit (the load-bearing claim is that
    // a node-rooted window sees the aggregate's emitted `total` column,
    // not Null).
    assert_eq!(
        by_key[&("HR".into(), "east".into())].get("total").unwrap(),
        "30"
    );
    assert_eq!(
        by_key[&("HR".into(), "west".into())].get("total").unwrap(),
        "5"
    );
    assert_eq!(
        by_key[&("ENG".into(), "east".into())].get("total").unwrap(),
        "300"
    );
    assert_eq!(
        by_key[&("ENG".into(), "north".into())]
            .get("total")
            .unwrap(),
        "10"
    );
    assert_eq!(
        by_key[&("ENG".into(), "west".into())].get("total").unwrap(),
        "50"
    );
}

// ── #3: divide-by-zero on a post-aggregate predicate ───────────────

const DIV_BY_ZERO_PIPELINE: &str = r#"
pipeline:
  name: post_agg_div_zero
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
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
      emit running_total = $window.sum(total)
      emit ratio = 1 / (total - 60)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: ratio
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

/// `1 / (total - 60)` evaluates against the post-aggregate row whose
/// `total == 60`; the divide-by-zero must surface as a runtime error
/// that routes to the DLQ. The successful row is preserved.
#[test]
fn post_aggregate_window_divide_by_zero_routes_to_dlq() {
    // HR's total = 60 → divisor zero; ENG's total = 600 → ratio defined.
    let csv = "\
department,amount
HR,10
HR,20
HR,30
ENG,100
ENG,200
ENG,300
";
    let (counters, dlq, output) =
        run_pipeline(DIV_BY_ZERO_PIPELINE, csv).expect("pipeline must execute");
    assert_eq!(
        counters.dlq_count, 1,
        "exactly one row (HR, total=60) must hit divide-by-zero"
    );
    assert_eq!(dlq.len(), 1, "DLQ must carry the failing row");

    // Surviving output: only ENG.
    let rows = csv_rows_by_key(&output, "department");
    assert!(
        !rows.contains_key("HR"),
        "HR row must NOT reach the writer (DLQ'd by div-by-zero)"
    );
    let eng = rows.get("ENG").expect("ENG row must be preserved");
    assert_eq!(eng.get("total").unwrap(), "600");
    assert_eq!(eng.get("running_total").unwrap(), "600");
}

// ── #4: E150b on missing column ────────────────────────────────────

#[test]
fn post_aggregate_window_partition_by_missing_field_raises_e150b() {
    // The aggregate emits {department, total}. The window's
    // `partition_by: [missing_field]` references a column the aggregate
    // does not emit; lowering must reject with E150b.
    let yaml = r#"
pipeline:
  name: post_agg_e150b
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
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
  name: bad
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit running_total = $window.sum(total)
    analytic_window:
      group_by: [missing_field]
- type: output
  name: out
  input: bad
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let config = crate::config::parse_config(yaml).expect("parse");
    let result = config.compile(&crate::config::CompileContext::default());
    let diags = result.expect_err("E150b expected on missing partition_by field");
    let codes: Vec<&str> = diags.iter().map(|d| d.code.as_str()).collect();
    assert!(
        codes.contains(&"E150b"),
        "expected E150b among diagnostics, got: {codes:?}"
    );
}

// ── #6: Sort/Route walk-through roots at upstream Aggregate ────────

#[test]
fn post_aggregate_window_walks_through_sort_to_root_at_aggregate() {
    use crate::plan::execution::PlanNode;
    use crate::plan::index::PlanIndexRoot;
    let yaml = r#"
pipeline:
  name: post_agg_sort_walkthrough
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    sort_order: [department]
    schema:
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
  name: running
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: running
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let config = crate::config::parse_config(yaml).expect("parse");
    let plan = config
        .compile(&crate::config::CompileContext::default())
        .expect("compile must succeed");
    let dag = plan.dag();

    // Locate the aggregate node and the windowed transform's IndexSpec.
    let mut agg_idx = None;
    let mut window_idx_num = None;
    for idx in dag.graph.node_indices() {
        match &dag.graph[idx] {
            PlanNode::Aggregation { name, .. } if name == "dept_totals" => agg_idx = Some(idx),
            PlanNode::Transform {
                name, window_index, ..
            } if name == "running" => window_idx_num = *window_index,
            _ => {}
        }
    }
    let agg_idx = agg_idx.expect("aggregate node must be present");
    let window_idx_num = window_idx_num.expect("transform must carry window_index");
    let spec = dag
        .indices_to_build
        .get(window_idx_num)
        .expect("indices_to_build must hold the spec");
    match &spec.root {
        PlanIndexRoot::Node { upstream, .. } => assert_eq!(
            *upstream, agg_idx,
            "node-rooted spec must point at the Aggregate even when a Sort \
             enforcer was synthesized in between (Sort/Route are pass-through \
             at lowering time)"
        ),
        other => panic!("expected PlanIndexRoot::Node rooted at the Aggregate, got: {other:?}"),
    }
}

// ── #12: Merge upstream raises E150d ───────────────────────────────

#[test]
fn merge_upstream_of_windowed_transform_raises_e150d() {
    // Two sources merge into a single stream; the downstream Transform
    // declares an `analytic_window`, which has no single producer
    // identity. Lowering must reject with E150d.
    let yaml = r#"
pipeline:
  name: merge_then_window
error_handling:
  strategy: continue
nodes:
- type: source
  name: src_a
  config:
    name: src_a
    path: a.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: source
  name: src_b
  config:
    name: src_b
    path: b.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: merge
  name: combined
  inputs: [src_a, src_b]
- type: transform
  name: windowed
  input: combined
  config:
    cxl: |
      emit department = department
      emit amount = amount
      emit running_total = $window.sum(amount)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: windowed
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let config = crate::config::parse_config(yaml).expect("parse");
    let result = config.compile(&crate::config::CompileContext::default());
    let diags = result.expect_err("E150d expected on Merge upstream of windowed transform");
    let codes: Vec<&str> = diags.iter().map(|d| d.code.as_str()).collect();
    assert!(
        codes.contains(&"E150d"),
        "expected E150d among diagnostics, got: {codes:?}"
    );
}
