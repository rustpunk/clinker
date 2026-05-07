//! lag/lead determinism over hash-aggregate's non-deterministic emit.
//!
//! `$window.lag(1)` and `$window.lead(1)` evaluate against the
//! per-partition record array AFTER `IndexSpec.sort_by` has been
//! applied. Hash aggregation emits group rows in a hash-bucket order
//! that varies run-to-run — but the windowed Transform's `sort_by`
//! normalizes that. Three runs of the same pipeline against identical
//! input must produce byte-identical writer output, and the
//! `prev_total` column populated via `$window.lag(1).total` must
//! reference the prior row in the partition's `sort_by` order.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

const LAG_LEAD_PIPELINE: &str = r#"
pipeline:
  name: post_agg_lag_lead
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
  name: lagged
  input: dept_region
  config:
    cxl: |
      emit department = department
      emit region = region
      emit total = total
      emit prev_total = $window.lag(1).total
    analytic_window:
      group_by: [department]
      sort_by:
        - field: region
          order: asc
- type: output
  name: out
  input: lagged
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

fn run_once(csv: &str) -> String {
    let config = crate::config::parse_config(LAG_LEAD_PIPELINE).expect("parse");
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
    };
    let primary = "src".to_string();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
        .expect("pipeline must run");
    buf.as_string()
}

/// Sort partition rows by department, then region — the canonical order
/// the writer would receive after `sort_by: [region asc]`.
fn canonicalize(s: &str) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = Vec::new();
    let mut lines = s.lines();
    let header_line = lines.next().unwrap_or("");
    out.push(header_line.split(',').map(String::from).collect());
    let mut rows: Vec<Vec<String>> = lines
        .filter(|l| !l.is_empty())
        .map(|l| l.split(',').map(String::from).collect())
        .collect();
    rows.sort();
    out.extend(rows);
    out
}

#[test]
fn lag_window_byte_identical_across_runs_under_hash_aggregate() {
    let csv = "\
department,region,amount
HR,east,10
HR,west,20
HR,north,30
ENG,east,100
ENG,west,200
ENG,north,300
ENG,south,400
";

    let run1 = run_once(csv);
    let run2 = run_once(csv);
    let run3 = run_once(csv);

    let canon1 = canonicalize(&run1);
    let canon2 = canonicalize(&run2);
    let canon3 = canonicalize(&run3);
    assert_eq!(
        canon1, canon2,
        "two runs over the same input must produce byte-identical \
         canonicalized output (rows sorted): \
         run1={run1:?} run2={run2:?}"
    );
    assert_eq!(canon2, canon3, "third run must also match");

    // The fixture's input has 7 rows producing 7 aggregate output rows
    // (3 HR + 4 ENG combos), each in its own (department, region) group.
    // The windowed Transform emits one row per aggregate row → 7 output
    // rows.
    let row_count = run1.lines().filter(|l| !l.is_empty()).count() - 1;
    assert_eq!(
        row_count, 7,
        "post-aggregate window must emit one row per aggregate output row"
    );

    // Pin per-row totals AND `prev_total` against the partition's
    // `sort_by: region asc` order. Sort_by within partition is:
    //   HR  → [east(10), north(30), west(20)]
    //   ENG → [east(100), north(300), south(400), west(200)]
    // So `lag(1).total` chains as the prior row's `total` per partition,
    // with the first row in each partition emitting Null (empty CSV cell).
    let mut hr_regions: Vec<String> = Vec::new();
    let mut lines = run1.lines();
    let header_line = lines.next().expect("header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let dept_idx = headers.iter().position(|h| *h == "department").unwrap();
    let region_idx = headers.iter().position(|h| *h == "region").unwrap();
    let total_idx = headers.iter().position(|h| *h == "total").unwrap();
    let prev_idx = headers.iter().position(|h| *h == "prev_total").unwrap();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let v: Vec<&str> = line.split(',').collect();
        if v[dept_idx] == "HR" {
            hr_regions.push(v[region_idx].to_string());
        }
        let total: i64 = v[total_idx].parse().expect("total parses");
        let prev = v[prev_idx];
        match (v[dept_idx], v[region_idx]) {
            ("HR", "east") => {
                assert_eq!(total, 10);
                assert_eq!(
                    prev, "",
                    "HR/east is first in partition; prev_total is Null"
                );
            }
            ("HR", "north") => {
                assert_eq!(total, 30);
                assert_eq!(prev, "10", "HR/north's prev is HR/east's total (10)");
            }
            ("HR", "west") => {
                assert_eq!(total, 20);
                assert_eq!(prev, "30", "HR/west's prev is HR/north's total (30)");
            }
            ("ENG", "east") => {
                assert_eq!(total, 100);
                assert_eq!(
                    prev, "",
                    "ENG/east is first in partition; prev_total is Null"
                );
            }
            ("ENG", "north") => {
                assert_eq!(total, 300);
                assert_eq!(prev, "100", "ENG/north's prev is ENG/east's total (100)");
            }
            ("ENG", "south") => {
                assert_eq!(total, 400);
                assert_eq!(prev, "300", "ENG/south's prev is ENG/north's total (300)");
            }
            ("ENG", "west") => {
                assert_eq!(total, 200);
                assert_eq!(prev, "400", "ENG/west's prev is ENG/south's total (400)");
            }
            other => panic!("unexpected (dept, region) {other:?}"),
        }
    }
    assert_eq!(hr_regions.len(), 3, "HR partition has three rows");
}
