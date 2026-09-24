//! Dead-lettering holds no state that grows with the number of failures:
//! sweeping the share of rows a Transform dead-letters from 0 to 1 leaves
//! peak resident memory and the arbitrator's peak consumer usage flat.
//!
//! Failure site. Every dead letter here comes from a Transform's expression
//! failing on the walk thread (stage `transform:tfm`), written through the
//! walk thread's [`StagedDlqSink`] writer into a staged file of a real run
//! attempt. No row routes through a stage that holds its failures until the
//! stage finishes (a streaming Sink's pending collisions, Aggregate
//! streaming ingest, a Combine's streaming probe or join kernel), so this
//! sweep measures the walk-thread path alone.
//!
//! Shape. One on-disk CSV is streamed to a temp file before the first
//! measurement, so building it never sets the high-water mark, and every run
//! reads it through [`FileSlot::from_path`], so no reader holds it whole.
//! Column `k` cycles `0..100`; a run with threshold `t` fails exactly the rows
//! with `k < t`, so thresholds 0, 50 and 100 are fractions 0, 0.5 and 1 over
//! byte-identical input. The fractions run in that order in one process, each
//! in its own run attempt.
//!
//! Measurement. [`peak_rss_bytes`] is the process's monotonic high-water mark,
//! so each run's rise is taken from one sample before the first run: fraction
//! 0 absorbs the one-time warm-up, and a later fraction may rise past it by at
//! most [`RSS_TOLERANCE_BYTES`]. The arbitrator's `peak_consumer_usage_bytes`
//! is per run and may exceed fraction 0's by at most
//! [`CONSUMER_TOLERANCE_BYTES`]. A failed row stops at the Transform, so a
//! higher fraction may lower the consumer peak; only a rise is residency.
//!
//! Control. Last, the fraction-1 run is repeated through a sink that keeps a
//! copy of every row it is handed. That run must push the RSS rise past the
//! tolerance, or the sweep could not have seen retention and the test fails.
//! Where the platform reports no peak RSS, the RSS assertions and the control
//! are skipped with a message; the arbitrator assertions still run.
//!
//! Sizes. `CLINKER_DLQ_SWEEP_ROWS` overrides the default row count for a
//! large local qualification run; `CLINKER_DLQ_SWEEP_REPORT` prints one
//! `DLQ-SWEEP` line per run.

use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use clinker_exec::dlq::{
    DlqArtifact, DlqBucketTarget, DlqOrigin, DlqPartReceipt, DlqPartSegment, DlqPartWriter,
    DlqRowWriter, DlqSink,
};
use clinker_exec::executor::{
    PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders, WriterRegistry,
};
use clinker_exec::output::attempt::RunAttemptPublication;
use clinker_exec::output::dlq_sink::StagedDlqSink;
use clinker_exec::output::staging::OutputStagingRegistry;
use clinker_exec::pipeline::memory::peak_rss_bytes;
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{ClinkerToml, CompileContext, parse_config};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::CompiledPlan;
use clinker_plan::security::validate_path;

/// How far a later fraction's peak-RSS rise may exceed fraction 0's.
///
/// The executor's fixed dead-letter residency is one header and one 64 KiB
/// write buffer per open bucket, and this pipeline has one bucket, so a
/// correct run adds well under 1 MiB for dead-lettering at any fraction.
/// 16 MiB absorbs allocator arena growth and thread-stack residue between
/// runs, and stays below a quarter of what retaining every fraction-1 row
/// costs at the default size (about 200 bytes of input per row, plus the
/// dead-letter columns), which the control checks.
const RSS_TOLERANCE_BYTES: u64 = 16 * 1024 * 1024;

/// How far a later fraction's peak arbitrator consumer usage may exceed
/// fraction 0's.
///
/// Dead-letter writes are charged to no arbitrator consumer, so the only
/// terms that move between runs are the in-flight streaming batches, whose
/// size is set by the batch size and channel capacity, not by the failure
/// count. 1 MiB is several batches of these rows.
const CONSUMER_TOLERANCE_BYTES: u64 = 1024 * 1024;

/// Rows in the generated input unless `CLINKER_DLQ_SWEEP_ROWS` overrides it.
const DEFAULT_ROWS: usize = 200_000;

/// Bytes of padding per row, bringing each input row to about 200 bytes.
const PAD_LEN: usize = 180;

/// A huge memory limit keeps the run off the spill and back-pressure paths,
/// so no budget reaction can hide or cause a residency difference.
const HUGE_LIMIT: &str = "100G";

/// The pipeline for one sweep run: a Transform that fails every row whose
/// `k` is below `threshold`, dead-lettering it into `dlq_path`.
fn plan(threshold: u32, dlq_path: &Path) -> CompiledPlan {
    parse_config(&format!(
        r#"
pipeline:
  name: dlq_fraction_peak
  memory:
    limit: {HUGE_LIMIT}
error_handling:
  strategy: continue
  dlq:
    path: {dlq}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: k, type: int }}
        - {{ name: pad, type: string }}
  - type: transform
    name: tfm
    input: src
    config:
      cxl: |
        emit id = id
        emit q = id / (if k < {threshold} then 0 else 1)
  - type: sink
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#,
        dlq = dlq_path.display()
    ))
    .expect("sweep pipeline parses")
    .compile(&CompileContext::default())
    .expect("sweep pipeline compiles")
}

/// Stream `rows` input rows to `path`. Row `i` has `k = i % 100`.
fn write_fixture(path: &Path, rows: usize) {
    let file = std::fs::File::create(path).expect("create fixture file");
    let mut out = BufWriter::new(file);
    out.write_all(b"id,k,pad\n").expect("write fixture header");
    let mut pad = [0u8; PAD_LEN];
    for i in 0..rows {
        let letter = b'a' + (i % 26) as u8;
        pad.fill(letter);
        write!(out, "{i},{},", i % 100).expect("write fixture row");
        out.write_all(&pad).expect("write fixture padding");
        out.write_all(b"\n").expect("write fixture newline");
    }
    out.flush().expect("flush fixture");
}

/// Rows of an input of `rows` rows that a threshold of `threshold` fails.
fn expected_failures(rows: usize, threshold: u32) -> u64 {
    (0..rows).filter(|i| ((i % 100) as u32) < threshold).count() as u64
}

/// Staging over a fresh run attempt rooted at `root`, as the CLI builds it.
fn attempt_staging(root: &Path) -> OutputStagingRegistry {
    let policy = ClinkerToml::parse(
        "[storage.publication]\nfailed_retention_seconds = 300\nmax_attempt_bytes = \"4GB\"\n",
    )
    .expect("parse publication policy")
    .storage
    .publication
    .resolve(root, 1_024, 64_000_000_000)
    .expect("resolve publication policy");
    let now: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis()
        .try_into()
        .expect("milliseconds fit u64");
    let attempt = RunAttemptPublication::create(
        policy,
        &uuid::Uuid::now_v7().to_string(),
        now,
        now + 300_000,
        vec![validate_path(Path::new("."), root, false).expect("destination root")],
    )
    .expect("create run attempt");
    OutputStagingRegistry::for_run_attempt(attempt)
}

/// The retention control: a sink that hands every row to a [`StagedDlqSink`]
/// and also keeps a copy of it, as a regression that held dead letters for
/// the run would.
struct RetainingSink {
    inner: Arc<StagedDlqSink>,
    retained: Arc<Mutex<Vec<Vec<u8>>>>,
}

struct RetainingWriter {
    inner: Box<dyn DlqRowWriter>,
    retained: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl DlqSink for RetainingSink {
    fn open_walk_writer(&self) -> Result<Box<dyn DlqRowWriter>, PipelineError> {
        Ok(Box::new(RetainingWriter {
            inner: self.inner.open_walk_writer()?,
            retained: Arc::clone(&self.retained),
        }))
    }

    fn open_part_writer(&self, origin: DlqOrigin) -> Result<Box<dyn DlqPartWriter>, PipelineError> {
        Ok(Box::new(RetainingPartWriter {
            inner: self.inner.open_part_writer(origin)?,
            retained: Arc::clone(&self.retained),
        }))
    }

    fn finish(&self) -> Result<Vec<DlqArtifact>, PipelineError> {
        self.inner.finish()
    }
}

impl DlqRowWriter for RetainingWriter {
    fn write_row(&mut self, target: &DlqBucketTarget<'_>, row: &[u8]) -> Result<(), PipelineError> {
        self.retained
            .lock()
            .expect("retained rows lock")
            .push(row.to_vec());
        self.inner.write_row(target, row)
    }

    fn close(self: Box<Self>) -> Result<(), PipelineError> {
        self.inner.close()
    }

    fn splice(
        &mut self,
        target: &DlqBucketTarget<'_>,
        segment: DlqPartSegment,
    ) -> Result<u64, PipelineError> {
        self.inner.splice(target, segment)
    }
}

/// A side thread's writer of the retention control: it keeps a copy of
/// every row it hands to the staged sink's part writer.
struct RetainingPartWriter {
    inner: Box<dyn DlqPartWriter>,
    retained: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl DlqPartWriter for RetainingPartWriter {
    fn write_row(&mut self, target: &DlqBucketTarget<'_>, row: &[u8]) -> Result<(), PipelineError> {
        self.retained
            .lock()
            .expect("retained rows lock")
            .push(row.to_vec());
        self.inner.write_row(target, row)
    }

    fn close(self: Box<Self>) -> Result<DlqPartReceipt, PipelineError> {
        self.inner.close()
    }
}

/// What one sweep run measured.
struct RunOutcome {
    /// Peak RSS after the run, or `None` where the platform cannot say.
    peak_rss_after: Option<u64>,
    consumer_peak: u64,
    dlq_count: u64,
    /// Data rows in the staged dead-letter file, the header not counted.
    staged_rows: u64,
    /// Bytes the retaining control kept; zero for a plain sweep run.
    retained_bytes: u64,
}

/// Data rows of the CSV file at `path`, read as a stream.
fn csv_data_rows(path: &Path) -> u64 {
    let mut reader = csv::Reader::from_path(path).expect("open staged dead-letter file");
    let mut record = csv::ByteRecord::new();
    let mut rows = 0;
    while reader
        .read_byte_record(&mut record)
        .expect("read staged dead-letter row")
    {
        rows += 1;
    }
    rows
}

/// Run the sweep pipeline once at `threshold` over `fixture`, dead-lettering
/// through a [`StagedDlqSink`] over a run attempt in its own tempdir, wrapped
/// in the retention control when `retain` is set.
fn run_once(fixture: &Path, threshold: u32, retain: bool) -> RunOutcome {
    let root = tempfile::tempdir().expect("run attempt root");
    let dlq_path = root.path().join("dlq.csv");
    let plan = plan(threshold, &dlq_path);
    let staging = attempt_staging(root.path());
    let staged = Arc::new(StagedDlqSink::new(staging.clone(), None));
    let retained = Arc::new(Mutex::new(Vec::new()));
    let sink: Arc<dyn DlqSink> = if retain {
        Arc::new(RetainingSink {
            inner: Arc::clone(&staged),
            retained: Arc::clone(&retained),
        })
    } else {
        staged.clone()
    };

    let slot = FileSlot::from_path(fixture.to_path_buf(), fixture.to_path_buf());
    let readers: SourceReaders =
        HashMap::from([("src".to_string(), SourceInput::Files(vec![slot]))]);
    let writers = WriterRegistry {
        single: HashMap::from([(
            "out".to_string(),
            Box::new(std::io::sink()) as Box<dyn Write + Send>,
        )]),
        output_staging: staging.clone(),
        auto_commit_staged: false,
        dlq_sink: Some(Arc::clone(&sink)),
        ..WriterRegistry::default()
    };
    let params = PipelineRunParams {
        execution_id: "dlq-fraction-peak".to_string(),
        batch_id: "dlq-fraction-peak".to_string(),
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("a continue-strategy run dead-letters and completes");
    let peak_rss_after = peak_rss_bytes();

    assert_eq!(
        report.cumulative_spill_bytes, 0,
        "the huge memory limit must keep the run off the spill path"
    );
    let artifacts = sink.finish().expect("the walk writer closed before return");
    let staged_rows = match artifacts.as_slice() {
        [] => 0,
        [artifact] => {
            let partial = staging
                .partials()
                .into_iter()
                .find(|partial| partial.final_path == artifact.final_path)
                .expect("the dead-letter bucket was staged in the run attempt");
            let rows = csv_data_rows(&partial.partial_path);
            assert_eq!(
                rows, artifact.rows,
                "the staged file holds exactly the rows the sink reports written"
            );
            rows
        }
        more => panic!("one dead-letter bucket expected, got {}", more.len()),
    };
    let retained_bytes = retained
        .lock()
        .expect("retained rows lock")
        .iter()
        .map(|row| row.len() as u64)
        .sum();

    RunOutcome {
        peak_rss_after,
        consumer_peak: report.peak_consumer_usage_bytes,
        dlq_count: report.counters.dlq_count,
        staged_rows,
        retained_bytes,
    }
}

fn sweep_rows() -> usize {
    match std::env::var("CLINKER_DLQ_SWEEP_ROWS") {
        Ok(value) => value.parse().unwrap_or_else(|_| {
            panic!("CLINKER_DLQ_SWEEP_ROWS must be a row count, got {value:?}")
        }),
        Err(_) => DEFAULT_ROWS,
    }
}

fn report(label: &str, rss_delta: Option<u64>, outcome: &RunOutcome) {
    if std::env::var_os("CLINKER_DLQ_SWEEP_REPORT").is_some() {
        let rss = rss_delta.map_or_else(|| "unmeasured".to_string(), |delta| delta.to_string());
        eprintln!(
            "DLQ-SWEEP fraction={label} rss_delta={rss} consumer_peak={} dlq_count={} staged_rows={} retained_bytes={}",
            outcome.consumer_peak, outcome.dlq_count, outcome.staged_rows, outcome.retained_bytes,
        );
    }
}

#[test]
fn dlq_fraction_sweep_keeps_peak_flat() {
    let rows = sweep_rows();
    let input = tempfile::tempdir().expect("fixture dir");
    let fixture: PathBuf = input.path().join("in.csv");
    write_fixture(&fixture, rows);

    let peak_before_first_run = peak_rss_bytes();
    if peak_before_first_run.is_none() {
        eprintln!(
            "peak_rss_bytes() is unavailable on this platform: the peak-RSS assertions and the \
             retention control are skipped; the arbitrator assertions still run"
        );
    }
    let rise = |outcome: &RunOutcome| -> Option<u64> {
        Some(
            outcome
                .peak_rss_after?
                .saturating_sub(peak_before_first_run?),
        )
    };

    let sweep = [("0", 0u32), ("0.5", 50), ("1", 100)];
    let mut outcomes = Vec::with_capacity(sweep.len());
    for (label, threshold) in sweep {
        let outcome = run_once(&fixture, threshold, false);
        report(label, rise(&outcome), &outcome);
        let expected = expected_failures(rows, threshold);
        assert_eq!(
            outcome.dlq_count, expected,
            "fraction {label}: every row below the threshold is dead-lettered"
        );
        assert_eq!(
            outcome.staged_rows, expected,
            "fraction {label}: every counted dead letter has a row in the staged file"
        );
        outcomes.push((label, outcome));
    }
    assert!(
        outcomes[2].1.staged_rows > 0,
        "fraction 1 must write rows through the staged sink, or the sweep measured nothing"
    );

    let (_, baseline) = &outcomes[0];
    for (label, outcome) in &outcomes[1..] {
        assert!(
            outcome.consumer_peak <= baseline.consumer_peak + CONSUMER_TOLERANCE_BYTES,
            "fraction {label}: peak arbitrator consumer usage {} exceeds fraction 0's {} by more \
             than {CONSUMER_TOLERANCE_BYTES} bytes; dead-letter state is being charged or held",
            outcome.consumer_peak,
            baseline.consumer_peak,
        );
        if let (Some(base_rise), Some(this_rise)) = (rise(baseline), rise(outcome)) {
            assert!(
                this_rise <= base_rise + RSS_TOLERANCE_BYTES,
                "fraction {label}: peak RSS rose {this_rise} bytes against fraction 0's \
                 {base_rise}, more than {RSS_TOLERANCE_BYTES} bytes above it; dead letters are \
                 being retained in proportion to the failure count"
            );
        }
    }

    let Some(base_rise) = rise(baseline) else {
        return;
    };
    let control = run_once(&fixture, 100, true);
    report("1-retaining-control", rise(&control), &control);
    assert_eq!(control.dlq_count, rows as u64);
    assert_eq!(control.staged_rows, rows as u64);
    assert!(
        control.retained_bytes > 2 * RSS_TOLERANCE_BYTES,
        "the control retains {} bytes, too few to show against the {RSS_TOLERANCE_BYTES}-byte \
         tolerance; raise CLINKER_DLQ_SWEEP_ROWS",
        control.retained_bytes,
    );
    let control_rise = rise(&control).expect("peak RSS was measurable before the sweep");
    assert!(
        control_rise > base_rise + RSS_TOLERANCE_BYTES,
        "retention control: a sink that kept {} bytes of dead letters raised peak RSS only \
         {control_rise} bytes against fraction 0's {base_rise}, within the \
         {RSS_TOLERANCE_BYTES}-byte tolerance, so the sweep cannot detect retention",
        control.retained_bytes,
    );
}
