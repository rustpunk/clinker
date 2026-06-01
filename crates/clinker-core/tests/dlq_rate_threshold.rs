//! End-to-end coverage for per-source and pipeline-wide DLQ rate
//! thresholds, per-source DLQ sidecar routing, and the source-name
//! attribution that flows through the entire stack.
//!
//! Topology is the canonical multi-source merge-attribution shape:
//! `[src_a, src_b] → merge → tfm → out`. `tfm` raises a divide-by-zero
//! on records from `src_b` only, so the DLQ stream has clean source
//! attribution and the per-source threshold can be exercised in
//! isolation against `src_b`'s denominator.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::error::PipelineError;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_core::source::multi_file::FileSlot;

fn slot(name: &str, csv: &str) -> FileSlot {
    FileSlot::new(
        PathBuf::from(format!("{name}.csv")),
        Box::new(Cursor::new(csv.as_bytes().to_vec())),
    )
}

fn writer(buf: &SharedBuffer) -> Box<dyn std::io::Write + Send> {
    Box::new(buf.clone())
}

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// Pipeline that fails every record originating from `src_b`. `src_a`
/// records pass through; `src_b` records hit `1 / 0`. Returns the
/// parsed config + the canned reader registry so each test can layer
/// its own `error_handling` block on top.
fn fail_src_b_yaml(error_handling: &str) -> String {
    format!(
        r#"
pipeline:
  name: dlq_rate_threshold
{error_handling}
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: amt, type: int }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: amt, type: int }}
  - type: merge
    name: m
    inputs: [src_a, src_b]
  - type: transform
    name: tfm
    input: m
    config:
      cxl: |
        emit id = id
        emit ratio = if($source.name == "src_b") then (1 / 0) else amt
  - type: output
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

fn five_each_readers() -> SourceReaders {
    HashMap::from([
        (
            "src_a".to_string(),
            vec![slot("a", "id,amt\n1,10\n2,20\n3,30\n4,40\n5,50\n")],
        ),
        (
            "src_b".to_string(),
            vec![slot("b", "id,amt\n10,10\n11,11\n12,12\n13,13\n14,14\n")],
        ),
    ])
}

/// AC1 reinforcement: every DLQ entry carries the originating Source
/// name through Merge. With `tfm` failing 100% of `src_b` records and
/// no per-source override, every entry in the in-memory DLQ vector
/// must report `source_name == "src_b"`. CSV column ordering is
/// validated separately in `dlq.rs` unit tests; this asserts the
/// in-memory plumbing through the full executor walk.
#[test]
fn dlq_entries_carry_source_b_attribution_under_merge() {
    let yaml = fail_src_b_yaml(
        r#"
error_handling:
  strategy: continue
"#,
    );
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        five_each_readers(),
        writers,
        &run_params(),
    )
    .expect("pipeline must complete under Continue strategy");
    assert_eq!(report.counters.dlq_count, 5, "5 src_b rows fail");
    assert!(
        report
            .dlq_entries
            .iter()
            .all(|e| e.source_name.as_ref() == "src_b"),
        "every DLQ entry must attribute to src_b; got: {:?}",
        report
            .dlq_entries
            .iter()
            .map(|e| e.source_name.as_ref())
            .collect::<Vec<_>>()
    );
    // The eval failed inside `emit ratio = ...`. The emit-statement
    // boundary in `cxl::eval::ProgramEvaluator::eval_record_inner`
    // attaches the target field name to `EvalError.triggering_field`,
    // so every DLQ entry derived from an emit-statement subexpression
    // names the offending column.
    assert!(
        report
            .dlq_entries
            .iter()
            .all(|e| e.triggering_field.as_deref() == Some("ratio")),
        "every DLQ entry must name 'ratio' as the triggering field"
    );
}

/// AC2/AC3/AC5: per-source `max_rate: 0.1` on `src_b` halts the
/// pipeline once `dlq_count_for_src_b / total_per_src_b` crosses 10%
/// (and the `min_records` floor has been met). `tfm` fails every src_b
/// record so the first failure past the floor trips the threshold; the
/// resulting error is `DlqRateExceeded { source: Some("src_b"), .. }`
/// — AC3's "names the offending source" requirement.
#[test]
fn per_source_threshold_halts_with_attributed_error() {
    let yaml = fail_src_b_yaml(
        r#"
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    min_records: 2
    per_source:
      src_b:
        max_rate: 0.1
"#,
    );
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let err = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        five_each_readers(),
        writers,
        &run_params(),
    )
    .expect_err("per-source threshold of 0.1 must halt the run on the first src_b failure");
    match err {
        PipelineError::DlqRateExceeded {
            source,
            observed_rate,
            max_rate,
            ..
        } => {
            let name = source.expect("E316 must name the offending source");
            assert_eq!(name.as_ref(), "src_b");
            assert!(observed_rate >= 0.1);
            assert_eq!(max_rate, 0.1);
        }
        other => panic!("expected E316 DlqRateExceeded(src_b), got: {other:?}"),
    }
}

/// Pipeline-wide threshold halts the run when the cumulative DLQ
/// fraction crosses `max_rate`. The `source: None` variant of
/// `DlqRateExceeded` carries E315 and reports the aggregate rate
/// rather than blaming a specific source.
#[test]
fn pipeline_wide_threshold_halts_with_e315() {
    let yaml = fail_src_b_yaml(
        r#"
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    min_records: 4
    max_rate: 0.4
"#,
    );
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let err = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        five_each_readers(),
        writers,
        &run_params(),
    )
    .expect_err("pipeline-wide max_rate 0.4 must halt the run once 5/10 records DLQ");
    match err {
        PipelineError::DlqRateExceeded {
            source,
            observed_rate,
            max_rate,
            ..
        } => {
            assert!(source.is_none(), "E315 carries source: None");
            assert!(observed_rate >= 0.4);
            assert_eq!(max_rate, 0.4);
        }
        other => panic!("expected E315 DlqRateExceeded(pipeline-wide), got: {other:?}"),
    }
}

/// `min_records` floor suppresses early halts even when the rate
/// exceeds `max_rate`. Without the floor, a 1/1 first failure would
/// trip a 0.5 threshold; with `min_records: 100` the floor is never
/// reached and the run completes normally despite a 50% DLQ rate.
#[test]
fn min_records_floor_suppresses_early_halt() {
    let yaml = fail_src_b_yaml(
        r#"
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    min_records: 100
    max_rate: 0.01
"#,
    );
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        five_each_readers(),
        writers,
        &run_params(),
    )
    .expect("min_records floor (100) is above this run's total (10); no halt expected");
    assert_eq!(report.counters.dlq_count, 5);
}

/// AC4: per-source `path:` partitions DLQ entries into their own
/// sidecar file. The `partition_dlq_entries` helper drives the CLI
/// flush loop; this test exercises the partitioner directly so the
/// routing semantics are pinned independent of the I/O layer.
#[test]
fn per_source_path_partitions_dlq_entries() {
    use std::path::PathBuf;
    use std::sync::Arc;

    use clinker_core::config::{DlqConfig, DlqPerSourceConfig};
    use clinker_core::dlq::{DlqErrorCategory, partition_dlq_entries};
    use clinker_core::executor::DlqEntry;
    use clinker_record::{Record, Schema, Value};

    let schema = Arc::new(Schema::new(vec!["id".into()]));
    let mk = |src: &str| DlqEntry {
        source_row: 0,
        category: DlqErrorCategory::TypeCoercionFailure,
        error_message: String::new(),
        original_record: Record::new(Arc::clone(&schema), vec![Value::Integer(0)]),
        stage: None,
        route: None,
        trigger: true,
        source_name: Arc::from(src),
        triggering_field: None,
        triggering_value: None,
    };
    let entries = vec![mk("src_a"), mk("src_b"), mk("src_a"), mk("src_b")];

    let mut per_source = std::collections::BTreeMap::new();
    per_source.insert(
        "src_b".to_string(),
        DlqPerSourceConfig {
            path: Some("dlq_b.csv".to_string()),
            max_rate: None,
            min_records: None,
        },
    );
    let cfg = DlqConfig {
        path: Some("dlq.csv".to_string()),
        include_reason: None,
        include_source_row: None,
        max_rate: None,
        min_records: None,
        per_source,
    };

    let buckets = partition_dlq_entries(&entries, &cfg);
    let dlq_bucket = buckets
        .iter()
        .find(|(p, _)| p == &PathBuf::from("dlq.csv"))
        .expect("default bucket must exist");
    assert_eq!(dlq_bucket.1.len(), 2);
    assert!(
        dlq_bucket
            .1
            .iter()
            .all(|e| e.source_name.as_ref() == "src_a")
    );

    let b_bucket = buckets
        .iter()
        .find(|(p, _)| p == &PathBuf::from("dlq_b.csv"))
        .expect("per-source bucket must exist");
    assert_eq!(b_bucket.1.len(), 2);
    assert!(b_bucket.1.iter().all(|e| e.source_name.as_ref() == "src_b"));
}

/// E317: `per_source` map key that does not name a declared Source
/// surfaces at compile time. The compile result is `Err(Vec<Diagnostic>)`
/// because the diagnostic is `Severity::Error`.
#[test]
fn unknown_per_source_key_emits_e317() {
    let yaml = r#"
pipeline:
  name: e317_unknown_per_source
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    per_source:
      ghost:
        max_rate: 0.5
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src_a
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("ghost per_source key must produce E317 at compile time");
    let combined = diags
        .iter()
        .map(|d| format!("{}: {}", d.code, d.message))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        combined.contains("E317") && combined.contains("ghost"),
        "expected E317 naming 'ghost', got:\n{combined}"
    );
}

/// E318: out-of-range `max_rate` (both pipeline-wide and per-source).
/// The accepted interval is the half-open `(0.0, 1.0]` — zero is
/// rejected as a footgun, see [`crate::plan::bind_schema::validate_dlq_per_source`].
#[test]
fn zero_max_rate_emits_e318() {
    let yaml = r#"
pipeline:
  name: e318_zero_max_rate
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    max_rate: 0.0
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src_a
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    let diags = config.compile(&CompileContext::default()).expect_err(
        "max_rate: 0.0 must produce E318 (halt-on-first-failure is not a supported config)",
    );
    let e318_count = diags.iter().filter(|d| d.code == "E318").count();
    assert!(
        e318_count >= 1,
        "expected at least one E318 diagnostic for zero max_rate; got: {:?}",
        diags
            .iter()
            .map(|d| format!("{}: {}", d.code, d.message))
            .collect::<Vec<_>>()
    );
}

#[test]
fn out_of_range_max_rate_emits_e318() {
    let yaml = r#"
pipeline:
  name: e318_out_of_range
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    max_rate: 1.5
    per_source:
      src_a:
        max_rate: -0.2
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src_a
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("out-of-range max_rate values must produce E318");
    let e318_count = diags.iter().filter(|d| d.code == "E318").count();
    assert!(
        e318_count >= 2,
        "expected at least two E318 diagnostics (pipeline-wide + src_a); got: {:?}",
        diags
            .iter()
            .map(|d| format!("{}: {}", d.code, d.message))
            .collect::<Vec<_>>()
    );
}

/// E318: per-source DLQ `path:` collides with the pipeline-wide path.
#[test]
fn duplicate_dlq_path_emits_e318() {
    let yaml = r#"
pipeline:
  name: e318_path_collision
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    per_source:
      src_a:
        path: dlq.csv
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src_a
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("duplicate DLQ path must produce E318");
    let combined = diags
        .iter()
        .map(|d| format!("{}: {}", d.code, d.message))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        combined.contains("E318") && combined.contains("collides"),
        "expected E318 path collision, got:\n{combined}"
    );
}
