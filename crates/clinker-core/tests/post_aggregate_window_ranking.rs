//! End-to-end pipeline coverage for the ranking and first/last-value
//! window builtins introduced alongside the typed AnalyticWindowSpec.
//!
//! Each test runs a Source → Transform-with-window pipeline against a
//! small CSV input and verifies the emitted columns against arithmetic
//! ground truth computed independently of the planner. The Arena-backed
//! partition exercises tie detection (rank/dense_rank) and first/last
//! row projection (first_value/last_value); the cxl unit tests under
//! `cxl::eval::tests::ranking_and_value` already pin the value-shape
//! contract against a minimal in-memory implementation.

mod common;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::executor::PipelineRunParams;
use std::collections::HashMap;

const RANKING_PIPELINE: &str = r#"
pipeline:
  name: window_ranking
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
      - { name: dept, type: string }
      - { name: score, type: int }
- type: transform
  name: ranked
  input: src
  config:
    cxl: |
      emit dept = dept
      emit score = score
      emit rn = $window.row_number()
      emit rk = $window.rank()
      emit drk = $window.dense_rank()
      emit fv = $window.first_value(score)
      emit lv = $window.last_value(score)
    analytic_window:
      group_by: [dept]
      sort_by:
        - field: score
          order: asc
- type: output
  name: out
  input: ranked
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

fn run(csv: &str) -> String {
    let config = clinker_plan::config::parse_config(RANKING_PIPELINE).expect("parse");
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
        ..Default::default()
    };
    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    common::run_config(&config, readers, writers, &params).expect("pipeline must run");
    buf.as_string()
}

#[test]
fn ranking_and_value_columns_over_tied_partition() {
    // Partition `eng` has a clean ascending sequence (no ties);
    // partition `hr` has two pairs of tied scores so rank() must skip
    // and dense_rank() must not.
    //
    //   eng sorted asc: 10 20 30 40
    //                   rn=[1,2,3,4]  rk=[1,2,3,4]  drk=[1,2,3,4]
    //
    //   hr  sorted asc: 10 10 20 20 30
    //                   rn=[1,2,3,4,5] rk=[1,1,3,3,5] drk=[1,1,2,2,3]
    //
    // first_value/last_value pin the partition's extremes (sorted
    // asc, so first is the smallest score and last is the largest).
    let csv = "\
dept,score
eng,40
eng,10
eng,20
eng,30
hr,20
hr,30
hr,10
hr,20
hr,10
";

    let out = run(csv);
    let mut lines = out.lines();
    let header_line = lines.next().expect("header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let idx = |name: &str| {
        headers
            .iter()
            .position(|h| *h == name)
            .unwrap_or_else(|| panic!("header missing: {name}"))
    };
    let dept_idx = idx("dept");
    let score_idx = idx("score");
    let rn_idx = idx("rn");
    let rk_idx = idx("rk");
    let drk_idx = idx("drk");
    let fv_idx = idx("fv");
    let lv_idx = idx("lv");

    // Collect rows by (dept, score) ordering — sort the captured rows
    // by (dept, score asc) to align with the partition's sorted view,
    // since output emission order is the input-arrival order.
    let mut rows: Vec<Vec<String>> = lines
        .filter(|l| !l.is_empty())
        .map(|l| l.split(',').map(str::to_string).collect())
        .collect();
    rows.sort_by(|a, b| {
        let dept_cmp = a[dept_idx].cmp(&b[dept_idx]);
        if !dept_cmp.is_eq() {
            return dept_cmp;
        }
        let sa: i64 = a[score_idx].parse().unwrap();
        let sb: i64 = b[score_idx].parse().unwrap();
        sa.cmp(&sb)
    });

    let expect_eng: [(i64, i64, i64, i64); 4] = [
        // (score, rn, rk, drk)
        (10, 1, 1, 1),
        (20, 2, 2, 2),
        (30, 3, 3, 3),
        (40, 4, 4, 4),
    ];
    let expect_hr: [(i64, i64, i64, i64); 5] = [
        (10, 1, 1, 1),
        (10, 2, 1, 1),
        (20, 3, 3, 2),
        (20, 4, 3, 2),
        (30, 5, 5, 3),
    ];

    let mut eng_seen = 0;
    let mut hr_seen = 0;
    for row in &rows {
        let dept = &row[dept_idx];
        let score: i64 = row[score_idx].parse().unwrap();
        let rn: i64 = row[rn_idx].parse().unwrap();
        let rk: i64 = row[rk_idx].parse().unwrap();
        let drk: i64 = row[drk_idx].parse().unwrap();
        let fv: i64 = row[fv_idx].parse().unwrap();
        let lv: i64 = row[lv_idx].parse().unwrap();

        match dept.as_str() {
            "eng" => {
                let (es, ern, erk, edrk) = expect_eng[eng_seen];
                assert_eq!(score, es, "eng score order at {eng_seen}");
                assert_eq!(rn, ern, "eng row_number at {eng_seen}");
                assert_eq!(rk, erk, "eng rank at {eng_seen}");
                assert_eq!(drk, edrk, "eng dense_rank at {eng_seen}");
                assert_eq!(fv, 10, "eng first_value");
                assert_eq!(lv, 40, "eng last_value");
                eng_seen += 1;
            }
            "hr" => {
                let (es, ern, erk, edrk) = expect_hr[hr_seen];
                assert_eq!(score, es, "hr score order at {hr_seen}");
                assert_eq!(rn, ern, "hr row_number at {hr_seen}");
                assert_eq!(rk, erk, "hr rank at {hr_seen}");
                assert_eq!(drk, edrk, "hr dense_rank at {hr_seen}");
                assert_eq!(fv, 10, "hr first_value");
                assert_eq!(lv, 30, "hr last_value");
                hr_seen += 1;
            }
            other => panic!("unexpected department {other:?}"),
        }
    }
    assert_eq!(eng_seen, 4);
    assert_eq!(hr_seen, 5);
}

const EVERY_EXISTS_PIPELINE: &str = r#"
pipeline:
  name: window_every_exists
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
      - { name: dept, type: string }
      - { name: score, type: int }
- type: transform
  name: predicates
  input: src
  config:
    cxl: |
      emit dept = dept
      emit score = score
      emit every_high = $window.every(score > 25)
      emit exists_low = $window.exists(score < 25)
      emit none_neg = $window.not_exists(score < 0)
    analytic_window:
      group_by: [dept]
- type: output
  name: out
  input: predicates
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

#[test]
fn every_exists_not_exists_partition_scoped() {
    // HR scores = [10, 20, 30]   → every >25: false, exists <25: true,  none_neg: true
    // ENG scores = [50, 60, 70]  → every >25: true,  exists <25: false, none_neg: true
    let csv = "\
dept,score
HR,10
HR,20
HR,30
ENG,50
ENG,60
ENG,70
";

    let config = clinker_plan::config::parse_config(EVERY_EXISTS_PIPELINE).expect("parse");
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
        ..Default::default()
    };
    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    common::run_config(&config, readers, writers, &params).expect("pipeline must run");
    let out = buf.as_string();

    let mut lines = out.lines();
    let header_line = lines.next().expect("header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let idx = |name: &str| headers.iter().position(|h| *h == name).unwrap();
    let dept_idx = idx("dept");
    let every_idx = idx("every_high");
    let exists_idx = idx("exists_low");
    let none_neg_idx = idx("none_neg");

    let mut rows = 0;
    for line in lines.filter(|l| !l.is_empty()) {
        rows += 1;
        let v: Vec<&str> = line.split(',').collect();
        let (exp_every, exp_exists) = match v[dept_idx] {
            "HR" => ("false", "true"),
            "ENG" => ("true", "false"),
            other => panic!("unexpected dept {other:?}"),
        };
        assert_eq!(v[every_idx], exp_every, "every_high for {}", v[dept_idx]);
        assert_eq!(v[exists_idx], exp_exists, "exists_low for {}", v[dept_idx]);
        assert_eq!(v[none_neg_idx], "true", "none_neg for {}", v[dept_idx]);
    }
    assert_eq!(rows, 6);
}
