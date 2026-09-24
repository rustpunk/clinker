//! `clinker run --explain` shows every dead-letter file's compiled header
//! before any data is read, in text and in JSON, and the two agree with the
//! layout an in-process compile derives.

use std::path::Path;
use std::process::{Command, Output};

use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::plan::{CompiledPlan, DlqLayout};

/// Two Sources, each failing in its own Transform under `continue`, with a
/// `per_source` override for `refunds` only: `orders` falls through to the
/// pipeline-wide file.
const WITH_DLQ: &str = r#"pipeline:
  name: dlq_explain
error_handling:
  strategy: continue
  dlq:
    path: rejects.csv
    per_source:
      refunds:
        path: refunds_rejects.csv
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    schema:
      - { name: order_id, type: int }
      - { name: order_total, type: int }
- type: source
  name: refunds
  config:
    name: refunds
    path: refunds.csv
    type: csv
    schema:
      - { name: refund_id, type: int }
      - { name: refund_amount, type: int }
      - { name: reason, type: string }
- type: transform
  name: order_ratio
  input: orders
  config:
    cxl: |
      emit order_id = order_id
      emit ratio = order_id / order_total
- type: transform
  name: refund_ratio
  input: refunds
  config:
    cxl: |
      emit refund_id = refund_id
      emit ratio = refund_id / refund_amount
- type: sink
  name: orders_out
  input: order_ratio
  config:
    name: orders_out
    path: orders_out.csv
    type: csv
- type: sink
  name: refunds_out
  input: refund_ratio
  config:
    name: refunds_out
    path: refunds_out.csv
    type: csv
"#;

/// The same shape with no `error_handling.dlq` block.
const WITHOUT_DLQ: &str = r#"pipeline:
  name: no_dlq_explain
error_handling:
  strategy: continue
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    schema:
      - { name: order_id, type: int }
      - { name: order_total, type: int }
- type: transform
  name: order_ratio
  input: orders
  config:
    cxl: |
      emit order_id = order_id
      emit ratio = order_id / order_total
- type: sink
  name: orders_out
  input: order_ratio
  config:
    name: orders_out
    path: orders_out.csv
    type: csv
"#;

const SECTION: &str = "=== Dead-Letter Output ===";

/// One bucket as `--explain` reports it: path, source list, header.
#[derive(Debug, PartialEq, Eq)]
struct ReportedBucket {
    path: String,
    sources: Vec<String>,
    fallback: bool,
    header: Vec<String>,
}

fn write_pipeline(dir: &Path, yaml: &str) -> std::path::PathBuf {
    let path = dir.join("pipeline.yaml");
    std::fs::write(&path, yaml).expect("write pipeline");
    path
}

fn explain(dir: &Path, pipeline: &Path, format: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_clinker"));
    command
        .current_dir(dir)
        .arg("run")
        .arg(pipeline)
        .arg("--explain");
    if let Some(format) = format {
        command.arg(format);
    }
    let output = command.output().expect("spawn clinker");
    assert!(
        output.status.success(),
        "explain must succeed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    // Explain reads no data and publishes nothing.
    assert!(
        !dir.join("rejects.csv").exists() && !dir.join("refunds_rejects.csv").exists(),
        "explain must not create a dead-letter file"
    );
    output
}

fn compile(dir: &Path, yaml: &str) -> CompiledPlan {
    parse_config(yaml)
        .expect("pipeline parses")
        .compile(&CompileContext::new(dir.to_path_buf()))
        .expect("pipeline compiles")
}

/// The buckets the in-process layout derives, in layout order, keyed by the
/// Sources this pipeline declares.
fn expected_buckets(layout: &DlqLayout) -> Vec<ReportedBucket> {
    let mut ids = ["orders", "refunds"]
        .into_iter()
        .map(|source| {
            layout
                .bucket_for_source(source)
                .unwrap_or_else(|| panic!("{source} routes to a bucket"))
        })
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        layout.buckets().len(),
        "every bucket is reached by a declared Source"
    );
    ids.into_iter()
        .map(|id| {
            let bucket = layout.bucket(id);
            ReportedBucket {
                path: bucket.path().to_string_lossy().into_owned(),
                sources: layout.sources_for(id).map(String::from).collect(),
                fallback: layout.is_fallback(id),
                header: bucket.header().to_vec(),
            }
        })
        .collect()
}

/// Parse the text section into buckets. `sources: (pipeline-wide fallback)`
/// marks the fallback bucket; any other entries are `per_source` names.
fn text_buckets(stdout: &str) -> Vec<ReportedBucket> {
    let start = stdout
        .find(SECTION)
        .unwrap_or_else(|| panic!("explain text lacks {SECTION:?}:\n{stdout}"));
    let mut lines = stdout[start + SECTION.len()..]
        .lines()
        .skip_while(|l| l.is_empty());
    let mut buckets = Vec::new();
    while let Some(path_line) = lines.next() {
        let Some(path) = path_line.strip_prefix("  ") else {
            break;
        };
        if path.starts_with(' ') || path.is_empty() {
            break;
        }
        let sources_line = lines.next().expect("sources line");
        let columns_line = lines.next().expect("columns line");
        let sources = sources_line
            .strip_prefix("    sources: ")
            .unwrap_or_else(|| panic!("malformed sources line {sources_line:?}"));
        let columns = columns_line
            .strip_prefix("    columns: ")
            .unwrap_or_else(|| panic!("malformed columns line {columns_line:?}"));
        let mut fallback = false;
        let sources = sources
            .split(", ")
            .filter(|s| {
                let marker = *s == "(pipeline-wide fallback)";
                fallback |= marker;
                !marker
            })
            .map(String::from)
            .collect();
        buckets.push(ReportedBucket {
            path: path.to_owned(),
            sources,
            fallback,
            header: columns.split(", ").map(String::from).collect(),
        });
    }
    buckets
}

fn json_buckets(json: &serde_json::Value) -> Vec<ReportedBucket> {
    let strings = |value: &serde_json::Value| -> Vec<String> {
        value
            .as_array()
            .expect("array")
            .iter()
            .map(|s| s.as_str().expect("string").to_owned())
            .collect()
    };
    json["dead_letter"]["buckets"]
        .as_array()
        .unwrap_or_else(|| panic!("explain json lacks dead_letter.buckets:\n{json:#}"))
        .iter()
        .map(|bucket| ReportedBucket {
            path: bucket["path"].as_str().expect("path").to_owned(),
            sources: strings(&bucket["sources"]),
            fallback: bucket["fallback"].as_bool().expect("fallback"),
            header: strings(&bucket["header"]),
        })
        .collect()
}

#[test]
fn explain_dead_letter_text_matches_layout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = write_pipeline(dir.path(), WITH_DLQ);
    let output = explain(dir.path(), &pipeline, None);
    let stdout = String::from_utf8_lossy(&output.stdout);

    let plan = compile(dir.path(), WITH_DLQ);
    let expected = expected_buckets(plan.dlq_layout().expect("a DLQ block yields a layout"));
    let reported = text_buckets(&stdout);
    assert_eq!(reported, expected, "text section:\n{stdout}");

    // Pin the concrete shape so the comparison cannot pass vacuously.
    assert_eq!(reported.len(), 2);
    assert_eq!(reported[0].path, "rejects.csv");
    assert!(reported[0].fallback && reported[0].sources.is_empty());
    assert!(reported[0].header.iter().any(|c| c == "order_total"));
    assert_eq!(reported[1].path, "refunds_rejects.csv");
    assert!(!reported[1].fallback);
    assert_eq!(reported[1].sources, ["refunds"]);
    assert!(reported[1].header.iter().any(|c| c == "refund_amount"));
    assert!(
        !reported[1].header.iter().any(|c| c == "order_total"),
        "an orders column cannot reach the refunds file"
    );
    assert!(
        stdout.contains("    sources: (pipeline-wide fallback)\n"),
        "the fallback bucket is labelled:\n{stdout}"
    );
}

#[test]
fn explain_dead_letter_json_matches_text() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = write_pipeline(dir.path(), WITH_DLQ);
    let json_output = explain(dir.path(), &pipeline, Some("json"));
    let json: serde_json::Value =
        serde_json::from_slice(&json_output.stdout).expect("explain json parses");
    let from_json = json_buckets(&json);

    let text_output = explain(dir.path(), &pipeline, Some("text"));
    let from_text = text_buckets(&String::from_utf8_lossy(&text_output.stdout));
    assert_eq!(from_json, from_text, "JSON and text report one layout");

    let plan = compile(dir.path(), WITH_DLQ);
    assert_eq!(
        from_json,
        expected_buckets(plan.dlq_layout().expect("a DLQ block yields a layout")),
        "JSON reports the compiled layout"
    );
}

#[test]
fn explain_without_dlq_has_no_dead_letter_section() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = write_pipeline(dir.path(), WITHOUT_DLQ);
    assert!(compile(dir.path(), WITHOUT_DLQ).dlq_layout().is_none());

    let text = explain(dir.path(), &pipeline, None);
    let stdout = String::from_utf8_lossy(&text.stdout);
    assert!(
        stdout.contains("Spill root:"),
        "the text explain ran to its end:\n{stdout}"
    );
    assert!(
        !stdout.contains("Dead-Letter"),
        "no DLQ block, no dead-letter section:\n{stdout}"
    );

    let json_output = explain(dir.path(), &pipeline, Some("json"));
    let json: serde_json::Value =
        serde_json::from_slice(&json_output.stdout).expect("explain json parses");
    let object = json.as_object().expect("explain json is an object");
    assert!(object.contains_key("nodes"), "the JSON view was rendered");
    assert!(
        !object.contains_key("dead_letter"),
        "no DLQ block, no dead_letter field: {:?}",
        object.keys().collect::<Vec<_>>()
    );
}
