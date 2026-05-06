//! End-to-end pipeline coverage for `$window.any` / `$window.all`.
//!
//! Pins two properties:
//!   1. The predicate iterates over partition rows, not the current row —
//!      every row in a partition emits the same any/all value.
//!   2. Short-circuit evaluation produces the right Bool over a real
//!      Arena partition produced by the executor.
//!
//! Three-valued / Null semantics are pinned exhaustively in
//! `cxl::eval::tests::any_all` (mod-level unit tests) where Value::Null
//! can be injected directly without depending on CSV coercion.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

const ANY_ALL_PIPELINE: &str = r#"
pipeline:
  name: post_agg_any_all
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
      - { name: score, type: int }
- type: transform
  name: predicates
  input: src
  config:
    cxl: |
      emit department = department
      emit region = region
      emit score = score
      emit any_high = $window.any(score > 25)
      emit all_high = $window.all(score > 25)
      emit any_low = $window.any(score < 25)
      emit all_low = $window.all(score < 25)
    analytic_window:
      group_by: [department]
      sort_by:
        - field: region
          order: asc
- type: output
  name: out
  input: predicates
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

fn run(csv: &str) -> String {
    let config = crate::config::parse_config(ANY_ALL_PIPELINE).expect("parse");
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
    };
    let primary = "src".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        "src".to_string(),
        Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())) as Box<dyn std::io::Read + Send>,
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

#[test]
fn any_all_window_predicates_partition_scoped() {
    // Two partitions:
    //   HR  scores = [10, 20, 30]   → > 25: [F, F, T] → any=T, all=F
    //                                  < 25: [T, T, F] → any=T, all=F
    //   ENG scores = [50, 60, 70]   → > 25: [T, T, T] → any=T, all=T
    //                                  < 25: [F, F, F] → any=F, all=F
    let csv = "\
department,region,score
HR,a,10
HR,b,20
HR,c,30
ENG,a,50
ENG,b,60
ENG,c,70
";

    let out = run(csv);
    let mut lines = out.lines();
    let header_line = lines.next().expect("header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let dept_idx = headers.iter().position(|h| *h == "department").unwrap();
    let region_idx = headers.iter().position(|h| *h == "region").unwrap();
    let any_high_idx = headers.iter().position(|h| *h == "any_high").unwrap();
    let all_high_idx = headers.iter().position(|h| *h == "all_high").unwrap();
    let any_low_idx = headers.iter().position(|h| *h == "any_low").unwrap();
    let all_low_idx = headers.iter().position(|h| *h == "all_low").unwrap();

    let mut row_count = 0;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        row_count += 1;
        let v: Vec<&str> = line.split(',').collect();
        let dept = v[dept_idx];
        let region = v[region_idx];
        let any_high = v[any_high_idx];
        let all_high = v[all_high_idx];
        let any_low = v[any_low_idx];
        let all_low = v[all_low_idx];

        // Same any/all values must appear on every row of the partition —
        // confirms predicate iterates the partition, not the current row.
        let (exp_ah, exp_lh, exp_al, exp_ll) = match dept {
            "HR" => ("true", "false", "true", "false"),
            "ENG" => ("true", "true", "false", "false"),
            other => panic!("unexpected department {other:?}"),
        };

        assert_eq!(any_high, exp_ah, "any_high for ({dept}, {region})");
        assert_eq!(all_high, exp_lh, "all_high for ({dept}, {region})");
        assert_eq!(any_low, exp_al, "any_low for ({dept}, {region})");
        assert_eq!(all_low, exp_ll, "all_low for ({dept}, {region})");
    }
    assert_eq!(row_count, 6, "all 6 source rows fan through transform");
}
