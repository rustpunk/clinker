//! Integration tests for `merge.mode: interleave`.
//!
//! These exercise the round-robin Merge arm's ordering correctness:
//! per-source FIFO is preserved; cross-source order follows the
//! seeded fastrand schedule (or a deterministic round-robin when
//! `interleave_seed` is absent). Live-channel back-pressure ("a slow
//! upstream doesn't starve a fast one") is exercised by
//! [`interleave_does_not_block_peer_on_slow_source`] — see that test
//! for the topology and assertion.

use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::time::Duration;

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

/// `std::io::Read` adapter that sleeps for `delay` after each CSV
/// row boundary (newline byte). Lives in `spawn_blocking` alongside
/// the format reader, so `std::thread::sleep` does not block the
/// tokio reactor. Used to gate `src_a` so the CSV format reader
/// yields one row every `delay` while `src_b`'s reader pushes all
/// rows immediately.
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
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.bytes.len() {
            return Ok(0);
        }
        // Read up to the next newline (or buffer end), one row at a
        // time. The CSV format reader buffers internally, but
        // returning row-bounded chunks keeps the sleep aligned with
        // record boundaries.
        let remaining = &self.bytes[self.pos..];
        let chunk_end = remaining
            .iter()
            .position(|&b| b == b'\n')
            .map(|p| p + 1)
            .unwrap_or(remaining.len());
        let n = chunk_end.min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.pos += n;
        // After the buffer ends with a newline, the next read starts
        // a new row. Sleep before yielding the next row's bytes —
        // skip the first sleep (header row needs no delay).
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

/// Live-channel back-pressure: a slow `src_a` (50 ms sleep at the
/// reader between every record) does not block `src_b` from
/// flowing through the Merge.interleave arm.
///
/// Pre-rip behavior (sequential per-Source drain): the executor
/// drains `src_a` fully before reading `src_b`'s channel, so the
/// Merge round-robin sees both inputs as pre-buffered Vecs and
/// emits them interleaved (`a-1, b-1, a-2, b-2, …`). Half of
/// `src_b`'s records appear in the back half of the output stream.
///
/// Post-rip (fused `tokio::select!` over both source receivers):
/// `src_b`'s records arrive into the Merge fold immediately while
/// `src_a` is still sleeping between rows. The fused arm pulls
/// every `src_b` record in the first ~5 ms, then waits 50 ms for
/// each `src_a` record. Result: every `src_b` record sits in the
/// first half of the output stream.
///
/// Discriminator: the position of the last `b-` tag in the output
/// must be `<= 5` (first half of 10 total records). Sequential
/// drain places the last `b-` at position 9; fused select! places
/// it at position 4.
#[tokio::test(flavor = "multi_thread")]
async fn interleave_does_not_block_peer_on_slow_source() {
    use std::time::Instant;

    let yaml = pipeline_yaml("mode: interleave");
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            // 5 records, each preceded by 50 ms — total ~250 ms.
            vec![slow_slot("a", &src_a_csv(5), Duration::from_millis(50))],
        ),
        (
            "src_b".to_string(),
            // 5 records, produced as fast as the channel admits.
            vec![slot("b", &src_b_csv(5))],
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let start = Instant::now();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("pipeline executes");
    let elapsed = start.elapsed();

    assert_eq!(report.counters.dlq_count, 0);
    let output = buf.as_string();
    let body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    assert_eq!(body.len(), 10, "expected 10 records, got {body:?}");
    assert_per_source_fifo(&body);

    // Discriminator assertion: the LAST b-tag must appear in the
    // first half of the output. Sequential-drain regimes place it
    // at the back; fused select! places it at the front.
    let last_b_pos = body
        .iter()
        .rposition(|row| tag_of(row).starts_with("b-"))
        .expect("at least one b-tag in output");
    assert!(
        last_b_pos < 5,
        "Merge.interleave failed to back-pressure: last src_b record \
         landed at position {last_b_pos} (>=5) in {body:?}. \
         Pre-rip pre-buffered round-robin places b records throughout \
         the output; the fused select! path concentrates them at the \
         front because src_a is still sleeping when src_b finishes \
         producing."
    );

    // Sanity: pipeline finished within a reasonable window. 5 ×
    // 50 ms slow-side delays + overhead should fit well under 2 s
    // even under CI jitter; failure here suggests something
    // hangs.
    assert!(
        elapsed < Duration::from_secs(2),
        "pipeline took {elapsed:?}, expected < 2s",
    );
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
