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
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;

#[path = "common/dlq_encode.rs"]
mod dlq_encode;
#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use dlq_sink::CollectingDlqSink;

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
  - type: sink
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
            clinker_exec::executor::SourceInput::Files(vec![slot(
                "a",
                "id,amt\n1,10\n2,20\n3,30\n4,40\n5,50\n",
            )]),
        ),
        (
            "src_b".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot(
                "b",
                "id,amt\n10,10\n11,11\n12,12\n13,13\n14,14\n",
            )]),
        ),
    ])
}

/// The collecting sink holds exactly the rows the executor counted when every
/// failure has a destination: one parsed row per dead letter, each under the
/// header the compiled plan fixed for the bucket.
#[test]
fn collecting_sink_rows_match_counted_dead_letters() {
    let yaml = fail_src_b_yaml(
        r#"
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
"#,
    );
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
    let bucket = layout.bucket_for_source("src_b").expect("src_b routes");
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);
    let sink = CollectingDlqSink::new();

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        five_each_readers(),
        dlq_sink::registry(writers, &sink),
        &run_params(),
    )
    .expect("pipeline must complete under Continue strategy");

    let rows = sink.rows();
    assert_eq!(report.counters.dlq_count, 5, "5 src_b rows fail");
    assert_eq!(rows.len() as u64, report.counters.dlq_count);
    assert_eq!(
        sink.header_for("dlq.csv").expect("the bucket received rows"),
        layout.bucket(bucket).header()
    );
    assert!(
        rows.iter()
            .all(|row| row.bucket_path() == PathBuf::from("dlq.csv")
                && row.source_name() == "src_b")
    );
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
    // The eval failed inside `emit ratio = ...`. The compiled emit node
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

/// AC4: a per-source `path:` routes that Source's dead-lettered rows to a
/// sidecar file of their own, while a Source without an override falls
/// through to the pipeline-wide file. Routing is the compiled plan's
/// dead-letter layout, the one rule the CLI publishes through; the run's
/// real entries are encoded under the header that layout fixed.
#[test]
fn per_source_path_partitions_dlq_entries() {
    let yaml = fail_src_b_yaml(
        r#"
error_handling:
  strategy: continue
  dlq:
    path: dlq.csv
    per_source:
      src_b:
        path: dlq_b.csv
"#,
    );
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
    assert_eq!(layout.buckets().len(), 2);

    let wide = layout.bucket_for_source("src_a").expect("src_a routes");
    assert!(layout.is_fallback(wide));
    assert_eq!(layout.bucket(wide).path(), PathBuf::from("dlq.csv"));
    let own = layout.bucket_for_source("src_b").expect("src_b routes");
    assert_ne!(own, wide);
    assert_eq!(layout.bucket(own).path(), PathBuf::from("dlq_b.csv"));
    assert_eq!(layout.sources_for(own).collect::<Vec<_>>(), ["src_b"]);

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
    assert_eq!(report.dlq_entries.len(), 5, "5 src_b rows fail");

    // Every failure is src_b's, so every row lands in src_b's own file and
    // none in the pipeline-wide one.
    let csv = dlq_encode::dlq_csv(&plan, &report.dlq_entries);
    let mut lines = csv.lines();
    let header: Vec<&str> = lines.next().expect("header").split(',').collect();
    assert_eq!(header, layout.bucket(own).header());
    let name_col = header
        .iter()
        .position(|c| *c == "_cxl_dlq_source_name")
        .expect("source-name column");
    let rows: Vec<&str> = lines.collect();
    assert_eq!(rows.len(), 5);
    assert!(
        rows.iter()
            .all(|row| row.split(',').nth(name_col) == Some("src_b"))
    );
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
  - type: sink
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
  - type: sink
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
  - type: sink
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
  - type: sink
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

/// E322: two Sink nodes writing the same path resolve to one physical file
/// and would silently overwrite each other. The exact-path case collides on
/// every platform (no case-folding needed), so this is deterministic.
#[test]
fn two_outputs_same_path_emit_e322() {
    let yaml = r#"
pipeline:
  name: e322_output_collision
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: sink
    name: out1
    input: src_a
    config:
      name: out1
      type: csv
      path: shared.csv
  - type: sink
    name: out2
    input: src_a
    config:
      name: out2
      type: csv
      path: shared.csv
"#;
    let config = parse_config(yaml).expect("parse");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("two outputs writing one path must produce E322");
    let combined = diags
        .iter()
        .map(|d| format!("{}: {}", d.code, d.message))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        combined.contains("E322") && combined.contains("collides"),
        "expected E322 output-path collision, got:\n{combined}"
    );
}

/// E318: two DLQ paths differing only in case (`errors.csv` vs `Errors.csv`)
/// resolve to one physical file on a case-insensitive filesystem and must
/// collide there — while remaining two distinct files (no collision) on a
/// case-sensitive one. The expectation is therefore conditioned on the *actual*
/// case-folding of the directory the writers target, so the assertion is
/// deterministic on every CI runner: it requires the collision on
/// case-insensitive macOS/Windows volumes and forbids the false positive on
/// case-sensitive Linux ones.
///
/// Both the observation and the engine operate on the same isolated absolute
/// directory. The DLQ paths are absolute paths inside a fresh temp dir, so the
/// validator's collision check resolves its case-fold probe against *that* dir
/// rather than the process-global cwd; and the ground truth is read by actually
/// creating `errors.csv` there and asking whether its re-cased twin resolves to
/// the same file. An earlier version sampled the case-folding of the ambient
/// cwd with a separate probe, which raced the validator's own cwd probe under
/// cargo's parallel, shared-cwd test execution: a transient probe failure in
/// the engine falls back to "case-sensitive" (no collision) while the test's
/// independent probe had already read the volume as case-insensitive, so the
/// expectation and the engine disagreed intermittently on case-insensitive
/// macOS runners. Anchoring both on one absolute dir removes that race.
#[test]
fn case_variant_dlq_paths_emit_e318_only_when_filesystem_folds_case() {
    let dir = tempfile::tempdir().expect("temp dir");

    // Ground truth for *this* directory: create the lowercase DLQ file, then ask
    // whether its uppercased twin resolves to the same physical file. This is
    // the filesystem's real behavior in the exact directory the engine will key
    // its collision check against — not an inference from a separate probe.
    let lower = dir.path().join("errors.csv");
    std::fs::write(&lower, b"").expect("create probe file");
    let upper_twin = dir.path().join("ERRORS.CSV");
    let folds_case = upper_twin.exists();
    std::fs::remove_file(&lower).expect("remove probe file");

    // Absolute DLQ paths inside the observed dir, differing only in case. The
    // validator resolves `collision_key` against each path's parent — this temp
    // dir — so its case-fold verdict matches `folds_case` above by construction.
    let pipeline_dlq = dir.path().join("errors.csv");
    let per_source_dlq = dir.path().join("Errors.csv");
    let yaml = format!(
        r#"
pipeline:
  name: e318_case_collision
error_handling:
  strategy: continue
  dlq:
    path: {pipeline_dlq:?}
    per_source:
      src_a:
        path: {per_source_dlq:?}
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - {{ name: id, type: int }}
  - type: sink
    name: out
    input: src_a
    config:
      name: out
      type: csv
      path: out.csv
"#,
        pipeline_dlq = pipeline_dlq.display(),
        per_source_dlq = per_source_dlq.display(),
    );

    let config = parse_config(&yaml).expect("parse");
    let result = config.compile(&CompileContext::default());

    if !folds_case {
        // Case-sensitive filesystem: `errors.csv` and `Errors.csv` are two
        // distinct files, so no path collision is raised. (The pipeline still
        // compiles; absence of an E318 *collision* diagnostic is the point.)
        if let Err(diags) = result {
            let collision = diags
                .iter()
                .any(|d| d.code == "E318" && d.message.contains("collides"));
            assert!(
                !collision,
                "case-sensitive filesystem must not flag a case-only DLQ collision, got: {:?}",
                diags
                    .iter()
                    .map(|d| (&d.code, &d.message))
                    .collect::<Vec<_>>()
            );
        }
    } else {
        // Case-insensitive filesystem: the two paths name one file, so the
        // per-source writer would silently overwrite the pipeline-wide one —
        // E318 must fire.
        let diags =
            result.expect_err("case-insensitive filesystem must flag the case-only DLQ collision");
        let combined = diags
            .iter()
            .map(|d| format!("{}: {}", d.code, d.message))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            combined.contains("E318") && combined.contains("collides"),
            "expected E318 case-variant collision, got:\n{combined}"
        );
    }
}
