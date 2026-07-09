//! Producer-pause liveness: a Source the resume controller paused mid-walk
//! must never stall the single-threaded dispatch walk.
//!
//! Each test seeds sustained memory pressure through a registered,
//! non-back-pressureable consumer pinned above the soft limit — so
//! `current_pressure()` stays over the pause threshold without faking OS
//! RSS — and drives a topology where one blocking-admit boundary elects a
//! still-live Source as the pause victim. When the walk later reaches that
//! Source's drain turn, resume-on-entry must unpark its ingest thread and
//! active-exemption must keep it unpaused for the whole drain.
//!
//! Pre-change these hang: the only production `resume()` was the end-of-run
//! teardown sweep, unreachable while the walk is blocked on a paused
//! Source's `recv()` / `select()`. Each run is driven on a worker thread
//! behind a deadline, so a regression fails bounded instead of hanging CI,
//! and every test asserts exact per-source row counts so completion is
//! proven non-vacuous. Each paused Source carries more rows than the ingest
//! channel capacity (1024), so its producer is still backed — parked on a
//! push — when the pause fires.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Hard limit far above any real test-process RSS, so the RSS arm of the
/// spill / abort gates stays inert and the pinned charged-byte consumer is
/// the sole pressure signal. soft = 0.80 * 8 GiB = 6.4 GiB.
const HARD_LIMIT: u64 = 8 * 1024 * 1024 * 1024;
const SPILL_FRAC: f64 = 0.80;
const RESUME_FRAC: f64 = 0.70;
/// Pinned charged bytes: above the 6.4 GiB soft limit (so pressure holds)
/// and well below the 8 GiB hard limit (so nothing aborts).
const PINNED_BYTES: u64 = 7 * 1024 * 1024 * 1024;
/// Rows for a Source that must stay producer-backed when paused — beyond
/// the 1024-event ingest channel capacity, so its ingest thread is still
/// parked on a push (not finished and disconnected) when the pause fires.
const BACKED_ROWS: usize = 2048;
/// A tiny Source that drains fast; used where a Source only needs to
/// exist, not to be the paused-while-live victim.
const SMALL_ROWS: usize = 3;
const DEADLINE: Duration = Duration::from_secs(30);

/// Non-back-pressureable consumer pinned at a fixed footprint. Frees
/// nothing on spill, so pressure persists for the whole run and every
/// blocking-admit boundary re-elects a live Source as the pause victim.
struct PinnedPressure {
    bytes: u64,
}

impl crate::pipeline::memory::MemoryConsumer for PinnedPressure {
    fn current_usage(&self) -> u64 {
        self.bytes
    }
    fn spill_priority(&self) -> i32 {
        30
    }
    fn try_spill(&self, _t: u64) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        Ok(0)
    }
    fn can_back_pressure(&self) -> bool {
        false
    }
}

/// Arbitrator under the production `pause` policy (`BackPressurePreferred
/// -> Priority`) with the pinned-pressure consumer already registered.
fn pinned_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    let arb = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        SPILL_FRAC,
        RESUME_FRAC,
        crate::pipeline::memory::MemoryArbitrator::default_policy(),
    ));
    arb.register_consumer(Arc::new(PinnedPressure {
        bytes: PINNED_BYTES,
    }));
    arb
}

/// A CSV with `rows` `id,region` body rows.
fn csv_rows(rows: usize) -> String {
    let mut s = String::from("id,region\n");
    for i in 0..rows {
        s.push_str(&format!("e{i},north\n"));
    }
    s
}

fn csv_reader(name: &str, body: String) -> (String, crate::source::SourceInput) {
    (
        name.to_string(),
        crate::executor::single_file_reader(
            format!("{name}.csv"),
            Box::new(std::io::Cursor::new(body.into_bytes())),
        ),
    )
}

fn sink_writers(names: &[&str]) -> HashMap<String, Box<dyn std::io::Write + Send>> {
    names
        .iter()
        .map(|n| {
            (
                n.to_string(),
                Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect()
}

/// Run the pipeline on a worker thread behind [`DEADLINE`]. A run that
/// completes returns its report; a run that hangs (a paused Source stalled
/// the walk) trips the deadline and fails the test bounded rather than
/// hanging CI forever.
fn run_within_deadline(
    yaml: &'static str,
    readers: crate::executor::SourceReaders,
    writers: HashMap<String, Box<dyn std::io::Write + Send>>,
    arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>,
    execution_id: &str,
) -> ExecutionReport {
    let (tx, rx) = std::sync::mpsc::channel();
    let execution_id = execution_id.to_string();
    std::thread::spawn(move || {
        let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
        let params = PipelineRunParams {
            execution_id,
            batch_id: "batch-0".to_string(),
            ..Default::default()
        };
        let result = PipelineExecutor::run_with_readers_writers_with_arbitrator(
            &config,
            readers,
            writers.into(),
            &params,
            clinker_plan::config::CompileContext::default(),
            arbitrator,
        );
        let _ = tx.send(result);
    });
    match rx.recv_timeout(DEADLINE) {
        Ok(result) => result.expect("pipeline must run to completion"),
        Err(_) => panic!(
            "producer-pause liveness: the run did not complete within {DEADLINE:?} — \
             a paused Source stalled the single-threaded walk"
        ),
    }
}

/// P0 repro — the non-fused Source arm. Two Sources feed a `concat` Merge
/// (non-fused, so the Sources drain strictly sequentially). Admitting the
/// first-dispatched Source's node buffer trips the soft limit; the resume
/// controller pauses the still-registered second Source. Both Sources
/// carry more than the channel capacity, so whichever drains second is
/// paused while its producer is still backed — its drain turn hangs
/// pre-change and must flow to EOF post-change via resume-on-entry.
#[test]
fn concat_merge_second_source_resumes_at_its_drain_turn() {
    const YAML: &str = r#"
pipeline:
  name: source_pause_concat
nodes:
- type: source
  name: src_a
  config:
    name: src_a
    type: csv
    path: src_a.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: source
  name: src_b
  config:
    name: src_b
    type: csv
    path: src_b.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: merge
  name: merged
  inputs: [src_a, src_b]
  config:
    mode: concat
- type: output
  name: out
  input: merged
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let report = run_within_deadline(
        YAML,
        HashMap::from([
            csv_reader("src_a", csv_rows(BACKED_ROWS)),
            csv_reader("src_b", csv_rows(BACKED_ROWS)),
        ]),
        sink_writers(&["out"]),
        pinned_arbitrator(),
        "source-pause-concat",
    );

    assert_eq!(
        report.per_source_record_counts.get("src_a"),
        Some(&(BACKED_ROWS as u64)),
        "both sources must drain fully — neither may be left paused"
    );
    assert_eq!(
        report.per_source_record_counts.get("src_b"),
        Some(&(BACKED_ROWS as u64)),
        "the paused second source must resume at its drain turn and drain every row"
    );
}

/// Fused `Merge.interleave` arm. A separate `Source -> Aggregate -> Output`
/// chain (blocking, so it admits a node buffer and polls `should_spill`)
/// pauses the first-registered interleave Source before the merge's turn.
/// The pauser chain is declared LAST so it dispatches FIRST (the walk order
/// follows the plan's topological order, which processes later-declared
/// independent chains first), guaranteeing its admit runs before the fused
/// merge. The fused arm must mark ALL its `Select` receivers active and
/// resume them, or `Select` blocks on the paused Source forever.
#[test]
fn fused_interleave_sources_resume_when_paused_before_the_merge_turn() {
    const YAML: &str = r#"
pipeline:
  name: source_pause_fused_merge
nodes:
- type: source
  name: src_a
  config:
    name: src_a
    type: csv
    path: src_a.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: source
  name: src_b
  config:
    name: src_b
    type: csv
    path: src_b.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
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
- type: source
  name: pre
  config:
    name: pre
    type: csv
    path: pre.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: aggregate
  name: agg_pre
  input: pre
  config:
    group_by: [region]
    cxl: |
      emit region = region
      emit n = count()
- type: output
  name: out_pre
  input: agg_pre
  config:
    name: out_pre
    type: csv
    path: out_pre.csv
"#;
    let report = run_within_deadline(
        YAML,
        HashMap::from([
            csv_reader("pre", csv_rows(SMALL_ROWS)),
            csv_reader("src_a", csv_rows(BACKED_ROWS)),
            csv_reader("src_b", csv_rows(SMALL_ROWS)),
        ]),
        sink_writers(&["out", "out_pre"]),
        pinned_arbitrator(),
        "source-pause-fused-merge",
    );

    assert_eq!(
        report.per_source_record_counts.get("pre"),
        Some(&(SMALL_ROWS as u64))
    );
    assert_eq!(
        report.per_source_record_counts.get("src_a"),
        Some(&(BACKED_ROWS as u64)),
        "the fused-interleave Source paused before the merge turn must resume and drain fully — \
         Select must never be left blocked on a paused receiver"
    );
    assert_eq!(
        report.per_source_record_counts.get("src_b"),
        Some(&(SMALL_ROWS as u64)),
        "the other fused-interleave Source must drain fully too"
    );
}

/// Fused `Source -> Transform` arm. A separate `Source -> Aggregate ->
/// Output` chain (blocking, so it admits and polls `should_spill`), declared
/// last so it dispatches first, pauses the fused Transform's upstream Source
/// before the Transform's turn; the fused recv loop must resume-on-entry and
/// mark it active, or it blocks on the paused channel forever.
#[test]
fn fused_transform_source_resumes_when_paused_before_its_turn() {
    const YAML: &str = r#"
pipeline:
  name: source_pause_fused_transform
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: transform
  name: shape
  input: events
  config:
    cxl: |
      emit id = id
      emit region = region
- type: output
  name: out
  input: shape
  config:
    name: out
    type: csv
    path: out.csv
- type: source
  name: pre
  config:
    name: pre
    type: csv
    path: pre.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: aggregate
  name: agg_pre
  input: pre
  config:
    group_by: [region]
    cxl: |
      emit region = region
      emit n = count()
- type: output
  name: out_pre
  input: agg_pre
  config:
    name: out_pre
    type: csv
    path: out_pre.csv
"#;
    let report = run_within_deadline(
        YAML,
        HashMap::from([
            csv_reader("pre", csv_rows(SMALL_ROWS)),
            csv_reader("events", csv_rows(BACKED_ROWS)),
        ]),
        sink_writers(&["out", "out_pre"]),
        pinned_arbitrator(),
        "source-pause-fused-transform",
    );

    assert_eq!(
        report.per_source_record_counts.get("pre"),
        Some(&(SMALL_ROWS as u64))
    );
    assert_eq!(
        report.per_source_record_counts.get("events"),
        Some(&(BACKED_ROWS as u64)),
        "a fused-Transform Source paused before its turn must resume and drain every row"
    );
}
