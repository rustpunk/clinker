#![cfg(feature = "test-utils")]
//! A dead-letter destination that runs out of room fails the run with a
//! diagnostic that names the file and the guard to configure, and publishes
//! nothing; an interrupted run publishes no dead-letter file; each staged
//! dead-letter file is one telemetry work unit.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use clinker_exec::dlq::{
    DlqBucketTarget, DlqOrigin, DlqPartSegment, DlqPartWriter, DlqRowWriter, DlqSink,
};
use clinker_exec::executor::{
    PipelineExecutor, PipelineRunParams, SourceReaders, WriterRegistry, single_file_reader,
};
use clinker_exec::output::attempt::{ArtifactKind, RunAttemptPublication};
use clinker_exec::output::dlq_sink::StagedDlqSink;
use clinker_exec::output::staging::OutputStagingRegistry;
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_exec::telemetry::{
    MetricKey, SpanName, SpanStatus, TelemetryArena, TelemetryProducer, TelemetryReceiver,
};
use clinker_plan::config::{ClinkerToml, CompileContext, IfExistsPolicy, parse_config};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::CompiledPlan;
use clinker_plan::security::validate_path;

/// Every row fails the transform's division, so every row dead-letters at
/// `transform:tfm` into the one pipeline-wide bucket.
fn every_row_fails(dlq_path: &Path) -> CompiledPlan {
    parse_config(&format!(
        r#"
pipeline:
  name: dlq_exhaustion
error_handling:
  strategy: continue
  dlq:
    path: {}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: amt, type: int }}
  - type: transform
    name: tfm
    input: src
    config:
      cxl: |
        emit id = id
        emit ratio = amt / (amt - amt)
  - type: sink
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#,
        dlq_path.display()
    ))
    .expect("pipeline parses")
    .compile(&CompileContext::default())
    .expect("pipeline compiles")
}

fn rows(count: usize) -> SourceReaders {
    let mut csv = String::from("id,amt\n");
    for id in 0..count {
        csv.push_str(&format!("{id},{}\n", id + 1));
    }
    HashMap::from([(
        "src".to_string(),
        single_file_reader("in.csv", Box::new(std::io::Cursor::new(csv.into_bytes()))),
    )])
}

/// Staging over a run attempt rooted at `root`, as the CLI builds it.
fn attempt_staging(root: &Path) -> OutputStagingRegistry {
    let policy = ClinkerToml::parse(
        "[storage.publication]\nfailed_retention_seconds = 300\nmax_attempt_bytes = \"64MB\"\n",
    )
    .expect("parse publication policy")
    .storage
    .publication
    .resolve(root, 1_024, 8_000_000_000)
    .expect("resolve publication policy");
    let now: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis()
        .try_into()
        .expect("milliseconds fit u64");
    let attempt = RunAttemptPublication::create_for_testing(
        policy,
        &uuid::Uuid::now_v7().to_string(),
        now,
        now + 300_000,
        vec![validate_path(Path::new("."), root, false).expect("destination root")],
    )
    .expect("create run attempt");
    OutputStagingRegistry::for_run_attempt(attempt)
}

/// An auto-commit registry whose primary output is staged in `staging`.
fn auto_commit_writers(
    staging: &OutputStagingRegistry,
    out_path: &Path,
    sink: Arc<dyn DlqSink>,
) -> WriterRegistry {
    let staged_out = out_path.to_path_buf();
    let (_, out_file) = staging
        .stage_attempt_output(
            ArtifactKind::Primary,
            "out",
            IfExistsPolicy::Overwrite,
            false,
            move |_| Ok(staged_out.clone()),
        )
        .expect("stage the primary output");
    WriterRegistry {
        single: HashMap::from([(
            "out".to_string(),
            Box::new(out_file) as Box<dyn Write + Send>,
        )]),
        output_staging: staging.clone(),
        auto_commit_staged: true,
        dlq_sink: Some(sink),
        ..WriterRegistry::default()
    }
}

fn telemetry() -> (TelemetryProducer, TelemetryReceiver) {
    let config = ClinkerToml::parse(
        r#"
[observability]
arena_bytes = "768KB"
ordinary_lane_bytes = "512KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
rate_limit_per_second = 100000
rate_limit_burst = 100000
[observability.otlp]
endpoint = "https://collector.invalid"
[observability.otlp.auth]
mode = "none"
"#,
    )
    .expect("parse observability policy");
    TelemetryArena::reserve(
        &config
            .resolve_observability(None)
            .expect("resolve observability"),
    )
    .expect("reserve the arena")
}

/// Every dead-letter metric and span the receiver holds, summed over its
/// batches.
#[derive(Default, Debug)]
struct DeadLetterSignals {
    started: u64,
    completed: u64,
    failed: u64,
    interrupted: u64,
    records: u64,
    bytes: u64,
    spans: Vec<SpanStatus>,
}

fn dead_letter_signals(receiver: &TelemetryReceiver) -> DeadLetterSignals {
    let mut signals = DeadLetterSignals::default();
    while let Some(batch) = receiver.try_recv_batch() {
        signals.started += batch.metric(MetricKey::DeadLetterStarted);
        signals.completed += batch.metric(MetricKey::DeadLetterCompleted);
        signals.failed += batch.metric(MetricKey::DeadLetterFailed);
        signals.interrupted += batch.metric(MetricKey::DeadLetterInterrupted);
        signals.records += batch.metric(MetricKey::DeadLetterRecords);
        signals.bytes += batch.metric(MetricKey::DeadLetterBytes);
        signals.spans.extend(
            batch
                .traces()
                .iter()
                .filter(|span| span.name == SpanName::DeadLetter)
                .map(|span| span.status),
        );
    }
    signals
}

#[test]
fn dlq_write_failure_fails_the_run_and_promotes_nothing() {
    const ROWS: usize = 3_000;
    let root = tempfile::tempdir().expect("destination root");
    let dlq_path = root.path().join("dlq.csv");
    let out_path = root.path().join("out.csv");
    let plan = every_row_fails(&dlq_path);
    let staging = attempt_staging(root.path());
    // The destination fills after 1 KiB: the first time the 64 KiB row buffer
    // spills into the file, the write fails.
    let sink = Arc::new(
        StagedDlqSink::new(staging.clone(), None)
            .with_write_fault_for_testing(1_024, std::io::ErrorKind::StorageFull),
    );
    let writers = auto_commit_writers(&staging, &out_path, sink);

    let error = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        rows(ROWS),
        writers,
        &PipelineRunParams::default(),
    )
    .expect_err("a dead-letter destination that is full fails the run");

    let PipelineError::Io(io) = &error else {
        panic!("the failure is an I/O error, got {error:?}");
    };
    assert_eq!(io.kind(), std::io::ErrorKind::StorageFull, "{error}");
    let message = error.to_string();
    assert!(
        message.contains(&dlq_path.display().to_string()),
        "names the dead-letter file: {message}"
    );
    let count: u64 = message
        .split("after ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|digits| digits.parse().ok())
        .unwrap_or_else(|| panic!("names the dead-letter count: {message}"));
    assert!(
        count > 0 && count < ROWS as u64,
        "the count is the rows dead-lettered when the write failed: {message}"
    );
    assert!(
        message.contains("stage transform:tfm"),
        "names the top stage: {message}"
    );
    assert!(
        message.contains("category "),
        "names the top category: {message}"
    );
    let snippet = message
        .find("error_handling:")
        .unwrap_or_else(|| panic!("carries a paste-ready snippet: {message}"));
    assert!(
        message[snippet..].contains("max_rate:"),
        "the snippet sets a dead-letter rate ceiling: {message}"
    );
    assert!(
        message[snippet..].contains("type_error_threshold:"),
        "the snippet sets a type-error threshold: {message}"
    );

    assert!(!dlq_path.exists(), "no dead-letter file is published");
    assert!(!out_path.exists(), "no primary output is published");
}

/// Wraps a sink so the run is interrupted the moment the walk has written
/// its last dead letter: the interrupt then lands after every row is staged
/// and before publication.
struct InterruptOnClose {
    inner: Arc<StagedDlqSink>,
    token: ShutdownToken,
}

struct InterruptingWriter {
    inner: Box<dyn DlqRowWriter>,
    token: ShutdownToken,
}

impl DlqSink for InterruptOnClose {
    fn open_walk_writer(&self) -> Result<Box<dyn DlqRowWriter>, PipelineError> {
        Ok(Box::new(InterruptingWriter {
            inner: self.inner.open_walk_writer()?,
            token: self.token.clone(),
        }))
    }

    fn open_part_writer(&self, origin: DlqOrigin) -> Result<Box<dyn DlqPartWriter>, PipelineError> {
        self.inner.open_part_writer(origin)
    }

    fn finish(&self) -> Result<Vec<clinker_exec::dlq::DlqArtifact>, PipelineError> {
        self.inner.finish()
    }
}

impl DlqRowWriter for InterruptingWriter {
    fn write_row(&mut self, target: &DlqBucketTarget<'_>, row: &[u8]) -> Result<(), PipelineError> {
        self.inner.write_row(target, row)
    }

    fn close(self: Box<Self>) -> Result<(), PipelineError> {
        let Self { inner, token } = *self;
        let closed = inner.close();
        token.request();
        closed
    }

    fn splice(
        &mut self,
        target: &DlqBucketTarget<'_>,
        segment: DlqPartSegment,
    ) -> Result<u64, PipelineError> {
        self.inner.splice(target, segment)
    }
}

#[test]
fn interrupted_run_promotes_no_dlq() {
    let root = tempfile::tempdir().expect("destination root");
    let dlq_path = root.path().join("dlq.csv");
    let out_path = root.path().join("out.csv");
    let plan = every_row_fails(&dlq_path);
    let staging = attempt_staging(root.path());
    let token = ShutdownToken::detached();
    let (producer, receiver) = telemetry();
    let staged = Arc::new(
        StagedDlqSink::new(staging.clone(), Some(producer)).with_shutdown_token(token.clone()),
    );
    let sink = Arc::new(InterruptOnClose {
        inner: Arc::clone(&staged),
        token: token.clone(),
    });
    let writers = auto_commit_writers(&staging, &out_path, sink);
    let params = PipelineRunParams {
        shutdown_token: Some(token),
        ..PipelineRunParams::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, rows(3), writers, &params)
        .expect("an interrupted run returns its report");

    assert!(report.interrupted, "the run ends interrupted");
    assert_eq!(report.counters.dlq_count, 3);
    assert!(
        staging
            .partials()
            .iter()
            .any(|partial| partial.final_path == dlq_path),
        "the rows were staged before the interrupt"
    );
    assert!(
        !dlq_path.exists(),
        "the staged dead-letter file is not published"
    );
    assert!(!out_path.exists(), "no primary output is published");

    drop(staged);
    let signals = dead_letter_signals(&receiver);
    assert_eq!(signals.started, 1, "{signals:?}");
    assert_eq!(signals.interrupted, 1, "{signals:?}");
    assert_eq!(signals.completed + signals.failed, 0, "{signals:?}");
    assert_eq!(signals.spans, vec![SpanStatus::Unset], "{signals:?}");
}

#[test]
fn dead_letter_work_unit_reports_records_and_bytes() {
    let root = tempfile::tempdir().expect("destination root");
    let dlq_path = root.path().join("dlq.csv");
    let plan = every_row_fails(&dlq_path);
    let staging = attempt_staging(root.path());
    let (producer, receiver) = telemetry();
    let sink = Arc::new(StagedDlqSink::new(staging.clone(), Some(producer)));
    let writers = WriterRegistry {
        single: HashMap::from([(
            "out".to_string(),
            Box::new(std::io::sink()) as Box<dyn Write + Send>,
        )]),
        output_staging: staging.clone(),
        auto_commit_staged: false,
        dlq_sink: Some(sink.clone()),
        ..WriterRegistry::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        rows(3),
        writers,
        &PipelineRunParams::default(),
    )
    .expect("a continue-strategy run dead-letters and completes");
    assert_eq!(report.counters.dlq_count, 3);
    let artifacts = sink.finish().expect("the walk writer closed before return");
    assert_eq!(artifacts.len(), 1);

    let signals = dead_letter_signals(&receiver);
    assert_eq!(
        signals.started, 1,
        "one unit per staged bucket: {signals:?}"
    );
    assert_eq!(signals.completed, 1, "{signals:?}");
    assert_eq!(signals.failed + signals.interrupted, 0, "{signals:?}");
    assert_eq!(signals.records, 3, "{signals:?}");
    assert!(signals.bytes > 0, "{signals:?}");
    assert_eq!(signals.spans, vec![SpanStatus::Ok], "{signals:?}");
}
