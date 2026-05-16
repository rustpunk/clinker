//! Integration tests for streaming-Output writes under fused
//! `Merge.interleave` (issue #72).
//!
//! Sibling to `merge_interleave.rs`: those tests exercise the Merge
//! arm's record ordering and live back-pressure with a buffered Output
//! (records pile up in `node_buffers[merge_idx]` until the Merge
//! finishes, then the Output arm writes them all). These tests cover
//! the next step of that pipeline: a single Output downstream of a
//! fused Merge.interleave takes the streaming path, so
//! `Writer::write_record` fires per record as Merge emits, concurrent
//! with Merge production.
//!
//! Black-box wall-clock discrimination of streaming-vs-buffered at the
//! `std::io::Write` layer is defeated by the 64 KB `BufWriter` that
//! `build_format_writer` wraps every raw writer in — the underlying
//! `Write::write` callback fires only at end-of-task flush regardless
//! of mode. These tests instead verify what the user-visible contract
//! actually guarantees:
//!
//! 1. Output correctness (per-source FIFO, no DLQ, total counters).
//! 2. End-to-end back-pressure under a slow source — the pipeline
//!    must not deadlock when the source's bounded channel fills while
//!    the streaming writer is mid-drain.
//! 3. Topology repeat-stability — the per-source FIFO invariant must
//!    survive many runs because the streaming task and the Merge arm
//!    race on a bounded `tokio::sync::mpsc::channel` and tokio's
//!    scheduler is non-deterministic.

use std::collections::HashMap;
use std::io::{self, Cursor, Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

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

fn writer(buf: &SharedBuffer) -> Box<dyn Write + Send> {
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

/// Two-source `Merge.interleave → Output` pipeline. Hard-codes
/// `mode: interleave` and a single downstream Output so the executor's
/// pre-pass at entry classifies this as a streamable topology under
/// issue #72.
fn pipeline_yaml() -> String {
    r#"
pipeline:
  name: streaming_output_test
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: merge
    name: merged
    inputs: [src_a, src_b]
    config:
      mode: interleave
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#
    .to_string()
}

fn src_a_csv(count: u32) -> String {
    let mut s = String::from("id,tag\n");
    for i in 1..=count {
        s.push_str(&format!("{i},a-{i}\n"));
    }
    s
}

fn src_b_csv(count: u32) -> String {
    let mut s = String::from("id,tag\n");
    for i in 1..=count {
        s.push_str(&format!("{},b-{}\n", 100 + i, i));
    }
    s
}

/// `std::io::Read` adapter that sleeps for `delay` after each CSV row
/// boundary. Same shape as `merge_interleave.rs::DelayedRowReader`;
/// copied here to keep tests independent.
struct DelayedRowReader {
    bytes: Vec<u8>,
    pos: usize,
    delay: Duration,
    rows_read: usize,
}

impl DelayedRowReader {
    fn new(csv: &str, delay: Duration) -> Self {
        Self {
            bytes: csv.as_bytes().to_vec(),
            pos: 0,
            delay,
            rows_read: 0,
        }
    }
}

impl Read for DelayedRowReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.bytes.len() {
            return Ok(0);
        }
        let remaining = &self.bytes[self.pos..];
        let chunk_end = remaining
            .iter()
            .position(|&b| b == b'\n')
            .map(|p| p + 1)
            .unwrap_or(remaining.len());
        let n = chunk_end.min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.pos += n;
        if n > 0 && remaining[..n].ends_with(b"\n") {
            self.rows_read += 1;
            if self.rows_read >= 1 && self.pos < self.bytes.len() {
                std::thread::sleep(self.delay);
            }
        }
        Ok(n)
    }
}

fn slow_slot(name: &str, csv: &str, delay: Duration) -> FileSlot {
    FileSlot::new(
        PathBuf::from(format!("{name}.csv")),
        Box::new(DelayedRowReader::new(csv, delay)),
    )
}

/// Extract the `tag` column from a CSV body row (`id,tag\n`).
fn tag_of(row: &str) -> &str {
    row.split(',').nth(1).unwrap_or("").trim_end_matches('\r')
}

/// Per-source FIFO: `a-` records strictly ascending in N, `b-` records
/// strictly ascending in N; cross-source order is unconstrained.
fn assert_per_source_fifo(body: &[String]) {
    let mut last_a: u32 = 0;
    let mut last_b: u32 = 0;
    for row in body {
        let tag = tag_of(row);
        if let Some(rest) = tag.strip_prefix("a-") {
            let n: u32 = rest.parse().expect("a-tag parses");
            assert!(n > last_a, "a-FIFO violated: a-{n} after a-{last_a}");
            last_a = n;
        } else if let Some(rest) = tag.strip_prefix("b-") {
            let n: u32 = rest.parse().expect("b-tag parses");
            assert!(n > last_b, "b-FIFO violated: b-{n} after b-{last_b}");
            last_b = n;
        } else {
            panic!("unexpected tag in body: {tag:?}");
        }
    }
}

/// Single run of the streaming pipeline. Returns `(body_lines, counters)`.
/// Body lines exclude the header row.
async fn run_streaming_pipeline(
    a_count: u32,
    b_count: u32,
    a_delay: Duration,
) -> (Vec<String>, clinker_record::PipelineCounters) {
    let yaml = pipeline_yaml();
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            vec![slow_slot("a", &src_a_csv(a_count), a_delay)],
        ),
        ("src_b".to_string(), vec![slot("b", &src_b_csv(b_count))]),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("pipeline executes");
    let output = buf.as_string();
    let body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    (body, report.counters)
}

/// Streaming-Output correctness under a slow `src_a`. Asserts every
/// record reaches the writer (per-source FIFO + total count), no DLQ,
/// counters match the buffered Output arm's semantics, and the pipeline
/// completes within a generous wall-clock bound (no deadlock when the
/// streaming task's bounded channel meets a slow Source's bounded
/// channel mid-drain).
#[tokio::test(flavor = "multi_thread")]
async fn streaming_writes_correct_output_under_slow_source() {
    let start = Instant::now();
    let (body, counters) = run_streaming_pipeline(10, 10, Duration::from_millis(50)).await;
    let elapsed = start.elapsed();

    assert_eq!(body.len(), 20, "expected 20 records, got {body:?}");
    assert_per_source_fifo(&body);

    // Every input record appears exactly once. Group by prefix then
    // by numeric suffix so the comparison is stable across the run's
    // arrival order — the streaming path interleaves cross-source
    // records arbitrarily, which would defeat a string sort
    // (`a-10` < `a-2` lexicographically).
    let mut a_seen: Vec<u32> = body
        .iter()
        .filter_map(|r| tag_of(r).strip_prefix("a-").and_then(|s| s.parse().ok()))
        .collect();
    let mut b_seen: Vec<u32> = body
        .iter()
        .filter_map(|r| tag_of(r).strip_prefix("b-").and_then(|s| s.parse().ok()))
        .collect();
    a_seen.sort_unstable();
    b_seen.sort_unstable();
    assert_eq!(
        a_seen,
        (1..=10).collect::<Vec<u32>>(),
        "src_a records must appear exactly once, got {a_seen:?}",
    );
    assert_eq!(
        b_seen,
        (1..=10).collect::<Vec<u32>>(),
        "src_b records must appear exactly once, got {b_seen:?}",
    );

    // Counters match the buffered Output arm's semantics. `ok_count`
    // counts DISTINCT `row_num` values across sources, and src_a /
    // src_b both produce row_nums 1..=10, so the cross-source
    // duplicates collide in the executor's `ok_source_rows` HashSet
    // and `ok_count` lands at 10 (one count per distinct row_num).
    assert_eq!(counters.dlq_count, 0, "no DLQ entries expected");
    assert_eq!(
        counters.records_written, 20,
        "expected 20 writes (10 per source); counters={counters:?}",
    );
    assert_eq!(
        counters.ok_count, 10,
        "expected 10 distinct row_nums across sources; counters={counters:?}",
    );
    assert_eq!(
        counters.total_count, 20,
        "expected 20 total ingested records; counters={counters:?}",
    );

    // No deadlock. src_a's ~500 ms drain dominates; even under heavy
    // CI jitter the pipeline must finish well under 2 s.
    assert!(
        elapsed < Duration::from_secs(2),
        "pipeline took {elapsed:?}, expected < 2 s — \
         streaming task back-pressure may have deadlocked the run",
    );
}

/// Streaming-Output preserves per-source FIFO across many independent
/// runs. The streaming task and the fused Merge arm race on a
/// bounded `tokio::sync::mpsc::channel` and tokio's scheduler is
/// non-deterministic, so this test rerun catches a regression that
/// would tear per-source order only on a fraction of runs.
///
/// 20 runs is the issue-72 stability bar; bumping this number is
/// safe but each run costs ~50 ms of slow-source delay so the test
/// is bounded at ~1 s when stable.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_writes_stable_across_repeats() {
    for run in 0..20 {
        let (body, counters) = run_streaming_pipeline(5, 5, Duration::from_millis(10)).await;
        assert_eq!(
            body.len(),
            10,
            "run {run}: expected 10 records, got {body:?}",
        );
        assert_per_source_fifo(&body);
        assert_eq!(counters.dlq_count, 0, "run {run}: no DLQ expected");
        assert_eq!(
            counters.records_written, 10,
            "run {run}: counters={counters:?}",
        );
        let mut tags: Vec<&str> = body.iter().map(|r| tag_of(r)).collect();
        tags.sort();
        assert_eq!(
            tags,
            vec![
                "a-1", "a-2", "a-3", "a-4", "a-5", "b-1", "b-2", "b-3", "b-4", "b-5"
            ],
            "run {run}: every input record must appear exactly once",
        );
    }
}

/// Streaming-Output sees every record from a fast pair of sources
/// (no slow side) and produces a correct merged stream. Sanity check
/// that the streaming task drains its `mpsc::Receiver` cleanly when
/// both sources finish microseconds apart and the bounded channel
/// never fills.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_writes_correct_output_under_fast_sources() {
    let (body, counters) = run_streaming_pipeline(20, 20, Duration::from_millis(0)).await;
    assert_eq!(body.len(), 40, "expected 40 records, got {body:?}");
    assert_per_source_fifo(&body);
    assert_eq!(counters.dlq_count, 0);
    assert_eq!(counters.records_written, 40);
    assert_eq!(counters.total_count, 40);
    let mut a_seen: Vec<u32> = body
        .iter()
        .filter_map(|r| tag_of(r).strip_prefix("a-").and_then(|s| s.parse().ok()))
        .collect();
    let mut b_seen: Vec<u32> = body
        .iter()
        .filter_map(|r| tag_of(r).strip_prefix("b-").and_then(|s| s.parse().ok()))
        .collect();
    a_seen.sort_unstable();
    b_seen.sort_unstable();
    assert_eq!(a_seen, (1..=20).collect::<Vec<u32>>());
    assert_eq!(b_seen, (1..=20).collect::<Vec<u32>>());
}
