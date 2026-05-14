//! Integration tests for `merge.mode: interleave`.
//!
//! These exercise the round-robin Merge arm's ordering correctness:
//! per-source FIFO is preserved; cross-source order follows the
//! seeded fastrand schedule (or a deterministic round-robin when
//! `interleave_seed` is absent). Live-channel back-pressure ("a slow
//! upstream doesn't starve a fast one") is not exercised here: the
//! executor's prologue currently materializes each Source's mpsc
//! receiver into a per-input deque before the Merge arm runs, so the
//! Interleave loop reads pre-buffered inputs. Lifting the prologue's
//! pre-pass is tracked separately from the runtime migration.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
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

/// Build a two-source Merge pipeline. `merge_config_yaml` is spliced
/// in *as* the Merge `config:` block (e.g. `"mode: interleave"` or
/// `"mode: interleave\n      interleave_seed: 42"`); empty string omits
/// the `config:` key entirely, leaving the default `MergeBody`
/// (`mode: concat`, no seed).
fn pipeline_yaml(merge_config_yaml: &str) -> String {
    let merge_config_block = if merge_config_yaml.is_empty() {
        String::new()
    } else {
        format!("    config:\n      {merge_config_yaml}\n")
    };
    format!(
        r#"
pipeline:
  name: merge_interleave_test
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: tag, type: string }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: tag, type: string }}
  - type: merge
    name: merged
    inputs: [src_a, src_b]
{merge_config_block}  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// CSV body for `src_a`: ids 1..=count with a stable `a-N` tag.
fn src_a_csv(count: u32) -> String {
    let mut s = String::from("id,tag\n");
    for i in 1..=count {
        s.push_str(&format!("{i},a-{i}\n"));
    }
    s
}

/// CSV body for `src_b`: ids offset by 100 with a stable `b-N` tag.
fn src_b_csv(count: u32) -> String {
    let mut s = String::from("id,tag\n");
    for i in 1..=count {
        s.push_str(&format!("{},b-{}\n", 100 + i, i));
    }
    s
}

/// Run the pipeline once and return the output body lines (header skipped).
async fn run_pipeline(yaml: &str, a_count: u32, b_count: u32) -> Vec<String> {
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        ("src_a".to_string(), vec![slot("a", &src_a_csv(a_count))]),
        ("src_b".to_string(), vec![slot("b", &src_b_csv(b_count))]),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("pipeline executes");
    assert_eq!(report.counters.dlq_count, 0);
    let output = buf.as_string();
    output.lines().skip(1).map(|s| s.to_string()).collect()
}

/// Extract the `tag` column (`a-N` / `b-N`) from a CSV body row of the
/// shape `id,tag`. Robust against a possible trailing carriage return.
fn tag_of(row: &str) -> &str {
    row.split(',').nth(1).unwrap_or("").trim_end_matches('\r')
}

/// Per-source FIFO check: a-tags appear in increasing N order across
/// the output, and b-tags do the same — the relative interleave
/// between a-* and b-* may vary, but each source's subsequence is
/// arrival-ordered.
fn assert_per_source_fifo(body: &[String]) {
    let mut last_a: u32 = 0;
    let mut last_b: u32 = 0;
    for row in body {
        let tag = tag_of(row);
        if let Some(rest) = tag.strip_prefix("a-") {
            let n: u32 = rest.parse().expect("a-tag parses");
            assert!(
                n > last_a,
                "src_a subsequence not FIFO: saw {tag} after a-{last_a}"
            );
            last_a = n;
        } else if let Some(rest) = tag.strip_prefix("b-") {
            let n: u32 = rest.parse().expect("b-tag parses");
            assert!(
                n > last_b,
                "src_b subsequence not FIFO: saw {tag} after b-{last_b}"
            );
            last_b = n;
        } else {
            panic!("unexpected tag in output row {row:?}");
        }
    }
}

/// `mode: interleave` preserves per-source FIFO for both sources and
/// emits every record exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn interleave_preserves_per_source_fifo() {
    let yaml = pipeline_yaml("mode: interleave");
    let body = run_pipeline(&yaml, 3, 3).await;
    assert_eq!(body.len(), 6, "expected 6 records, got {body:?}");
    assert_per_source_fifo(&body);

    // Every record appears exactly once.
    let mut tags: Vec<&str> = body.iter().map(|r| tag_of(r)).collect();
    tags.sort();
    assert_eq!(
        tags,
        vec!["a-1", "a-2", "a-3", "b-1", "b-2", "b-3"],
        "every input record must appear exactly once"
    );
}

/// Without `mode:` the Merge defaults to `concat`, which drains
/// predecessors in declaration order (all `src_a` before any `src_b`).
#[tokio::test(flavor = "multi_thread")]
async fn concat_default_drains_in_declaration_order() {
    let yaml = pipeline_yaml("");
    let body = run_pipeline(&yaml, 3, 3).await;
    assert_eq!(body.len(), 6);

    // First three rows are src_a; last three are src_b.
    let tags: Vec<&str> = body.iter().map(|r| tag_of(r)).collect();
    assert_eq!(
        tags,
        vec!["a-1", "a-2", "a-3", "b-1", "b-2", "b-3"],
        "concat must drain src_a before src_b in declaration order"
    );
}

/// `interleave_seed: 42` produces the same output across two
/// independent runs. The seeded `fastrand::Rng` schedule is
/// deterministic regardless of executor scheduling because the
/// Interleave arm consumes pre-buffered records in a single fold.
#[tokio::test(flavor = "multi_thread")]
async fn seeded_interleave_is_reproducible() {
    let yaml = pipeline_yaml("mode: interleave\n      interleave_seed: 42");
    let first = run_pipeline(&yaml, 5, 5).await;
    let second = run_pipeline(&yaml, 5, 5).await;
    assert_eq!(
        first, second,
        "same seed must yield identical ordering across runs"
    );
    assert_per_source_fifo(&first);
    assert_eq!(first.len(), 10);
}

/// Two different seeds produce different interleavings. With 8+8
/// records the probability of two distinct fastrand schedules
/// coinciding bit-for-bit is negligible; this would only fail on a
/// regression that ignores the seed.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_seeds_diverge() {
    let yaml_one = pipeline_yaml("mode: interleave\n      interleave_seed: 1");
    let yaml_two = pipeline_yaml("mode: interleave\n      interleave_seed: 2");
    let with_one = run_pipeline(&yaml_one, 8, 8).await;
    let with_two = run_pipeline(&yaml_two, 8, 8).await;
    assert_per_source_fifo(&with_one);
    assert_per_source_fifo(&with_two);
    assert_ne!(
        with_one, with_two,
        "distinct seeds must yield distinct round-robin schedules"
    );
}
