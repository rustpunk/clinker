//! Sink-specific observability and lineage contract tests.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, Cursor, Write};
use std::path::Path;
#[cfg(feature = "lineage")]
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

use clinker_exec::executor::{
    PipelineExecutor, PipelineRunParams, SourceReaders, single_file_reader,
};
use clinker_exec::telemetry::{
    MetricKey, SpanFact, SpanName, SpanStatus, TelemetryArena, TelemetryProducer,
    TelemetryReceiver, TraceSpan, unix_nanos_now,
};
#[cfg(feature = "lineage")]
use clinker_lineage::logical_identity::{
    ExternalDatasetIdentity, LineageIdentityContext, LineageNodeBinding,
};
#[cfg(feature = "lineage")]
use clinker_lineage::{
    OutputColumnLineage, TransformationSubtype, TransformationType, column_lineage_external,
    column_lineage_local_diagnostic_paths,
};
use clinker_plan::config::{ClinkerToml, CompileContext, parse_config};
use clinker_plan::error::PipelineError;

const INPUT: &str = "id,label\n2,beta\n1,alpha\n";
const RETIRED_TERMINAL_DISCRIMINATOR: &str = "type: output";

const SYNC_PIPELINE: &str = r#"
pipeline: { name: sink_sync }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
        - { name: label, type: string }
  - type: sink
    name: delivered
    input: rows
    config:
      name: delivered
      type: csv
      path: delivered.csv
      sort_order: [id]
"#;

const STREAMING_PIPELINE: &str = r#"
pipeline: { name: sink_streaming }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
        - { name: label, type: string }
  - type: transform
    name: shape
    input: rows
    config:
      cxl: |
        emit id = id
        emit label = label
  - type: sink
    name: delivered
    input: shape
    config:
      name: delivered
      type: csv
      path: delivered.csv
"#;

const CORRELATED_PIPELINE: &str = r#"
pipeline: { name: sink_correlated }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: label, type: string }
  - type: sink
    name: delivered
    input: rows
    config:
      name: delivered
      type: csv
      path: delivered.csv
"#;

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn bytes(&self) -> Vec<u8> {
        self.0.lock().expect("buffer lock").clone()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("buffer lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("fixture writer refused bytes"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("fixture writer refused flush"))
    }
}

struct InterruptingWriter {
    token: clinker_exec::pipeline::shutdown::ShutdownToken,
    output: SharedBuffer,
}

impl Write for InterruptingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.token.request();
        self.output.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

fn observability_policy() -> clinker_plan::config::ResolvedObservabilityPolicy {
    ClinkerToml::parse(
        r#"
[observability]
arena_bytes = "64KB"
ordinary_lane_bytes = "32KB"
high_severity_lane_bytes = "32KB"
max_batch_bytes = "4KB"
max_attributes_per_event = 4
max_attribute_bytes = "256B"
drop_policy = "drop_newest"
sample_every = 1
rate_limit_per_second = 100000
rate_limit_burst = 100000
flush_timeout_ms = 1000

[observability.otlp]
endpoint = "https://collector.invalid"
connect_timeout_ms = 100
request_timeout_ms = 200
retry_max_attempts = 1
retry_total_timeout_ms = 500
max_response_bytes = "1KB"

[observability.otlp.auth]
mode = "none"
"#,
    )
    .expect("telemetry policy parses")
    .resolve_observability(None)
    .expect("telemetry policy resolves")
}

fn readers() -> SourceReaders {
    HashMap::from([(
        "rows".to_string(),
        single_file_reader(
            "input.csv",
            Box::new(Cursor::new(INPUT.as_bytes().to_vec())),
        ),
    )])
}

fn params(producer: TelemetryProducer) -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "sink-surface-execution".to_string(),
        batch_id: "sink-surface-batch".to_string(),
        telemetry_producer: Some(producer),
        ..PipelineRunParams::default()
    }
}

fn compile(yaml: &str) -> clinker_plan::plan::CompiledPlan {
    parse_config(yaml)
        .expect("pipeline parses")
        .compile(&CompileContext::default())
        .expect("pipeline compiles")
}

fn run_with_writer(
    yaml: &str,
    writer: Box<dyn Write + Send>,
    producer: TelemetryProducer,
) -> Result<clinker_exec::executor::ExecutionReport, PipelineError> {
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("delivered".to_string(), writer)]);
    PipelineExecutor::run_plan_with_readers_writers(
        &compile(yaml),
        readers(),
        writers,
        &params(producer),
    )
}

#[derive(Debug, Default)]
struct SinkTelemetry {
    started: u64,
    completed: u64,
    failed: u64,
    interrupted: u64,
    records: u64,
    errors: u64,
    bytes: u64,
    spans: Vec<TraceSpan>,
}

fn drain_sink(receiver: &TelemetryReceiver) -> SinkTelemetry {
    let mut sink = SinkTelemetry::default();
    while let Some(batch) = receiver.try_recv_batch() {
        sink.started += batch.metric(MetricKey::SinkStarted);
        sink.completed += batch.metric(MetricKey::SinkCompleted);
        sink.failed += batch.metric(MetricKey::SinkFailed);
        sink.interrupted += batch.metric(MetricKey::SinkInterrupted);
        sink.records += batch.metric(MetricKey::SinkRecords);
        sink.errors += batch.metric(MetricKey::SinkErrors);
        sink.bytes += batch.metric(MetricKey::SinkBytes);
        sink.spans.extend(
            batch
                .traces()
                .iter()
                .filter(|span| span.name == SpanName::Sink)
                .cloned(),
        );
    }
    sink
}

fn assert_success_signals(actual: &SinkTelemetry, records: u64, bytes: u64) {
    assert_eq!(actual.started, 1, "one real Sink work unit starts");
    assert_eq!(actual.completed, 1, "the work unit reaches completion");
    assert_eq!(actual.failed, 0);
    assert_eq!(actual.interrupted, 0);
    assert_eq!(actual.records, records, "records count handled Sink rows");
    assert_eq!(actual.errors, 0, "the successful Sink reports no errors");
    assert_eq!(actual.bytes, bytes, "bytes come from the writer boundary");
    assert!(
        actual.spans.len() <= 1,
        "Sink spans are admission-controlled"
    );
    assert_admitted_spans(actual, SpanStatus::Ok);
}

fn assert_admitted_spans(actual: &SinkTelemetry, status: SpanStatus) {
    assert!(
        actual.spans.len() <= 1,
        "Sink spans are admission-controlled"
    );
    for span in &actual.spans {
        assert_eq!(span.status, status);
        assert_eq!(span.logical_node, "delivered");
        assert!(span.started_at_unix_nanos > 0);
        assert!(span.ended_at_unix_nanos >= span.started_at_unix_nanos);
    }
}

#[test]
fn telemetry_sync_sink_reports_the_completed_writer_work() {
    let (producer, receiver) = TelemetryArena::reserve(&observability_policy()).expect("arena");
    let output = SharedBuffer::default();
    let report = run_with_writer(SYNC_PIPELINE, Box::new(output.clone()), producer)
        .expect("synchronous Sink succeeds");

    assert_eq!(report.counters.records_written, 2);
    assert_eq!(output.bytes(), b"id,label\n1,alpha\n2,beta\n");
    assert_success_signals(&drain_sink(&receiver), 2, output.bytes().len() as u64);
}

#[test]
fn telemetry_streaming_sink_reports_the_writer_thread_work_once() {
    let (producer, receiver) = TelemetryArena::reserve(&observability_policy()).expect("arena");
    let output = SharedBuffer::default();
    let report = run_with_writer(STREAMING_PIPELINE, Box::new(output.clone()), producer)
        .expect("streaming Sink succeeds");

    assert_eq!(report.counters.records_written, 2);
    assert_eq!(output.bytes(), b"id,label\n2,beta\n1,alpha\n");
    assert_success_signals(&drain_sink(&receiver), 2, output.bytes().len() as u64);
}

#[test]
fn telemetry_correlation_sink_reports_the_deferred_writer_work_once() {
    let (producer, receiver) = TelemetryArena::reserve(&observability_policy()).expect("arena");
    let output = SharedBuffer::default();
    let report = run_with_writer(CORRELATED_PIPELINE, Box::new(output.clone()), producer)
        .expect("correlation-deferred Sink succeeds");

    assert_eq!(report.counters.records_written, 2);
    assert_eq!(output.bytes(), b"id,label\n1,alpha\n2,beta\n");
    assert_success_signals(&drain_sink(&receiver), 2, output.bytes().len() as u64);
}

#[test]
fn telemetry_sink_failure_has_one_error_span_and_no_completion() {
    let (producer, receiver) = TelemetryArena::reserve(&observability_policy()).expect("arena");
    let result = run_with_writer(SYNC_PIPELINE, Box::new(FailingWriter), producer);
    assert!(
        result.is_err(),
        "the writer failure must remain a run failure"
    );

    let actual = drain_sink(&receiver);
    assert_eq!(actual.started, 1);
    assert_eq!(actual.completed, 0, "failed work did not complete");
    assert_eq!(actual.failed, 1, "failed work has one terminal outcome");
    assert_eq!(actual.interrupted, 0);
    assert_eq!(actual.records, 0, "the failed operation never committed");
    assert_eq!(actual.errors, 1, "one destination refusal");
    assert_eq!(actual.bytes, 0, "an Err write accepted no bytes");
    assert!(
        actual.spans.len() <= 1,
        "Sink spans are admission-controlled"
    );
    assert_admitted_spans(&actual, SpanStatus::Error);
}

#[test]
fn telemetry_streaming_sink_interruption_has_one_terminal_outcome() {
    let (producer, receiver) = TelemetryArena::reserve(&observability_policy()).expect("arena");
    let token = clinker_exec::pipeline::shutdown::ShutdownToken::detached();
    let output = SharedBuffer::default();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "delivered".to_string(),
        Box::new(InterruptingWriter {
            token: token.clone(),
            output: output.clone(),
        }) as _,
    )]);
    let run_params = PipelineRunParams {
        shutdown_token: Some(token),
        ..params(producer)
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &compile(STREAMING_PIPELINE),
        readers(),
        writers,
        &run_params,
    )
    .expect("shutdown unwinds without changing Sink error semantics");
    assert!(report.interrupted);

    let actual = drain_sink(&receiver);
    assert_eq!(actual.started, 1);
    assert_eq!(actual.completed, 0);
    assert_eq!(actual.failed, 0);
    assert_eq!(actual.interrupted, 1);
    assert_eq!(actual.errors, 0);
    assert_eq!(actual.bytes, output.bytes().len() as u64);
    assert!(
        actual.spans.len() <= 1,
        "Sink spans are admission-controlled"
    );
    assert_admitted_spans(&actual, SpanStatus::Unset);
}

#[test]
fn telemetry_full_arena_cannot_change_sink_bytes_or_exit_status() {
    let (producer, receiver) = TelemetryArena::reserve(&observability_policy()).expect("arena");
    let now = unix_nanos_now();
    while producer.snapshot().ordinary_full_drops == 0 {
        let _ = producer.emit_span(SpanFact {
            name: SpanName::Transform,
            status: SpanStatus::Ok,
            logical_node: "arena-prefill-transform-with-a-bounded-engine-identity",
            started_at_unix_nanos: now,
            ended_at_unix_nanos: now,
        });
    }
    let drops_before = producer.snapshot().ordinary_full_drops;
    let output = SharedBuffer::default();
    let report = run_with_writer(SYNC_PIPELINE, Box::new(output.clone()), producer.clone())
        .expect("telemetry admission loss cannot fail the Sink");

    assert_eq!(report.counters.records_written, 2);
    assert_eq!(output.bytes(), b"id,label\n1,alpha\n2,beta\n");
    assert!(producer.snapshot().ordinary_full_drops > drops_before);
    let actual = drain_sink(&receiver);
    assert_eq!(
        actual.started, 1,
        "fixed metrics remain coalesced out of lane"
    );
    assert_eq!(actual.completed, 1);
    assert_eq!(actual.failed, 0);
    assert_eq!(actual.interrupted, 0);
    assert_eq!(actual.records, 2);
    assert_eq!(actual.bytes, output.bytes().len() as u64);
    assert!(
        actual.spans.is_empty(),
        "the full ordinary lane drops the optional Sink span"
    );
}

#[derive(Clone, Copy, Debug)]
enum DestinationFault {
    Refuse { prefix: usize },
    Flush,
    CancelAfterWrite,
}

struct FaultWriter {
    fault: DestinationFault,
    shutdown: Option<clinker_exec::pipeline::shutdown::ShutdownToken>,
    output: SharedBuffer,
    calls: Arc<Mutex<(usize, usize)>>,
}

impl Write for FaultWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut calls = self.calls.lock().unwrap();
        calls.0 += 1;
        match self.fault {
            DestinationFault::Refuse { prefix } => {
                if prefix > 0 && calls.0 == 1 {
                    return self.output.write(&bytes[..prefix.min(bytes.len())]);
                }
                if let Some(token) = &self.shutdown {
                    token.request();
                }
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "destination refusal",
                ))
            }
            DestinationFault::Flush => self.output.write(bytes),
            DestinationFault::CancelAfterWrite => {
                let accepted = self.output.write(bytes)?;
                self.shutdown.as_ref().unwrap().request();
                Ok(accepted)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.calls.lock().unwrap().1 += 1;
        assert!(
            matches!(self.fault, DestinationFault::Flush),
            "terminal writer reused"
        );
        if let Some(token) = &self.shutdown {
            token.request();
        }
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "flush refusal"))
    }
}

fn fill_both_lanes(producer: &TelemetryProducer) {
    for status in [SpanStatus::Ok, SpanStatus::Error] {
        loop {
            let snapshot = producer.snapshot();
            if (status == SpanStatus::Ok && snapshot.ordinary_full_drops > 0)
                || (status == SpanStatus::Error && snapshot.high_full_drops > 0)
            {
                break;
            }
            let now = unix_nanos_now();
            let _ = producer.emit_span(SpanFact {
                name: SpanName::Transform,
                status,
                logical_node: "bounded-prefill",
                started_at_unix_nanos: now,
                ended_at_unix_nanos: now,
            });
        }
    }
}

fn assert_broken_pipe(error: &PipelineError) {
    match error {
        PipelineError::Multiple(errors) => {
            assert_eq!(errors.len(), 1, "no derivative errors: {errors:?}");
            assert_broken_pipe(&errors[0]);
        }
        PipelineError::Format(clinker_format::FormatError::Io(error))
        | PipelineError::Io(error) => assert_eq!(error.kind(), io::ErrorKind::BrokenPipe),
        other => panic!("original destination error lost: {other:?}"),
    }
}

fn destination_fault_matrix(yaml: &str) {
    for fault in [
        DestinationFault::Refuse { prefix: 0 },
        DestinationFault::Refuse { prefix: 1 },
        DestinationFault::Flush,
        DestinationFault::CancelAfterWrite,
    ] {
        for requests_shutdown in [false, true] {
            if matches!(fault, DestinationFault::CancelAfterWrite) && !requests_shutdown {
                continue;
            }
            // Absence, ordinary admission, and both lanes full use the same
            // destination and exact outcome assertions.
            for telemetry_mode in 0..3 {
                let (producer, receiver) =
                    TelemetryArena::reserve(&observability_policy()).unwrap();
                if telemetry_mode == 2 {
                    fill_both_lanes(&producer);
                }
                let before = producer.snapshot();
                let token = clinker_exec::pipeline::shutdown::ShutdownToken::detached();
                let output = SharedBuffer::default();
                let calls = Arc::new(Mutex::new((0, 0)));
                let writer = FaultWriter {
                    fault,
                    shutdown: requests_shutdown.then(|| token.clone()),
                    output: output.clone(),
                    calls: calls.clone(),
                };
                let writers: HashMap<String, Box<dyn Write + Send>> =
                    HashMap::from([("delivered".to_string(), Box::new(writer) as _)]);
                let result = PipelineExecutor::run_plan_with_readers_writers(
                    &compile(yaml),
                    readers(),
                    writers,
                    &PipelineRunParams {
                        shutdown_token: Some(token),
                        telemetry_producer: (telemetry_mode != 0).then(|| producer.clone()),
                        ..PipelineRunParams::default()
                    },
                );
                let interrupted = matches!(fault, DestinationFault::CancelAfterWrite);
                if interrupted {
                    let report = result.expect("explicit cancellation remains interruption");
                    assert!(report.interrupted);
                    assert_eq!(
                        report.counters.records_written, 0,
                        "accepted bytes do not commit a cancelled operation"
                    );
                } else {
                    assert_broken_pipe(&result.expect_err("real failure wins over shutdown"));
                }
                let (writes, flushes) = *calls.lock().unwrap();
                let records = if matches!(fault, DestinationFault::Flush) {
                    2
                } else {
                    0
                };
                match fault {
                    DestinationFault::Refuse { prefix } => {
                        assert_eq!(output.bytes().len(), prefix);
                        assert_eq!(writes, usize::from(prefix > 0) + 1);
                        assert_eq!(flushes, 0);
                    }
                    DestinationFault::Flush => {
                        let expected = if yaml == STREAMING_PIPELINE {
                            b"id,label\n2,beta\n1,alpha\n"
                        } else {
                            b"id,label\n1,alpha\n2,beta\n"
                        };
                        assert_eq!(output.bytes(), expected);
                        assert_eq!(flushes, 1);
                    }
                    DestinationFault::CancelAfterWrite => {
                        let expected = if yaml == STREAMING_PIPELINE {
                            b"id,label\n2,beta\n".as_slice()
                        } else {
                            b"id,label\n1,alpha\n".as_slice()
                        };
                        assert_eq!(output.bytes(), expected);
                        assert_eq!((writes, flushes), (1, 0));
                    }
                }
                let after = producer.snapshot();
                assert_eq!(after.owned_bytes, before.owned_bytes);
                assert_eq!(
                    after.ordinary_capacity_bytes,
                    before.ordinary_capacity_bytes
                );
                assert_eq!(after.high_capacity_bytes, before.high_capacity_bytes);
                let actual = drain_sink(&receiver);
                if telemetry_mode == 0 {
                    assert_eq!(actual.started, 0);
                } else {
                    assert_eq!(actual.started, 1, "{fault:?}");
                    assert_eq!(actual.completed, 0);
                    assert_eq!(actual.failed, u64::from(!interrupted));
                    assert_eq!(actual.interrupted, u64::from(interrupted));
                    assert_eq!(actual.errors, u64::from(!interrupted));
                    assert_eq!(actual.records, records);
                    assert_eq!(actual.bytes, output.bytes().len() as u64);
                    assert_admitted_spans(
                        &actual,
                        if interrupted {
                            SpanStatus::Unset
                        } else {
                            SpanStatus::Error
                        },
                    );
                    if telemetry_mode == 2 {
                        assert!(actual.spans.is_empty());
                        assert!(after.full_drops > before.full_drops);
                    }
                }
            }
        }
    }
}

#[test]
fn sync_destination_faults_preserve_outcomes_under_shutdown_and_saturation() {
    destination_fault_matrix(SYNC_PIPELINE);
}

#[test]
fn streaming_destination_faults_preserve_outcomes_under_shutdown_and_saturation() {
    destination_fault_matrix(STREAMING_PIPELINE);
}

#[test]
fn correlated_destination_faults_preserve_outcomes_under_shutdown_and_saturation() {
    destination_fault_matrix(CORRELATED_PIPELINE);
}

#[test]
fn terminal_fan_out_writers_are_not_reused_and_sibling_failures_survive() {
    use clinker_exec::executor::WriterRegistry;
    for cancelled in [false, true] {
        let (producer, receiver) = TelemetryArena::reserve(&observability_policy()).unwrap();
        let token = clinker_exec::pipeline::shutdown::ShutdownToken::detached();
        let output = SharedBuffer::default();
        let primary_calls = Arc::new(Mutex::new((0, 0)));
        let sibling_calls = Arc::new(Mutex::new((0, 0)));
        let mut files: HashMap<Arc<str>, Box<dyn Write + Send>> = HashMap::from([(
            Arc::from("input.csv"),
            Box::new(FaultWriter {
                fault: if cancelled {
                    DestinationFault::CancelAfterWrite
                } else {
                    DestinationFault::Refuse { prefix: 1 }
                },
                shutdown: cancelled.then(|| token.clone()),
                output: output.clone(),
                calls: primary_calls.clone(),
            }) as _,
        )]);
        if !cancelled {
            files.insert(
                Arc::from("independent.csv"),
                Box::new(FaultWriter {
                    fault: DestinationFault::Flush,
                    shutdown: None,
                    output: SharedBuffer::default(),
                    calls: sibling_calls.clone(),
                }),
            );
        }
        let registry = WriterRegistry {
            fan_out: HashMap::from([("delivered".into(), files)]),
            ..WriterRegistry::default()
        };
        let result = PipelineExecutor::run_plan_with_readers_writers(
            &compile(SYNC_PIPELINE),
            readers(),
            registry,
            &PipelineRunParams {
                shutdown_token: Some(token),
                ..params(producer)
            },
        );
        if cancelled {
            let report = result.expect("no poisoned continuation after cancellation");
            assert!(report.interrupted);
            assert_eq!(report.counters.records_written, 0);
            assert_eq!(*primary_calls.lock().unwrap(), (1, 0));
            assert_eq!(output.bytes(), b"id,label\n1,alpha\n");
        } else {
            let PipelineError::Multiple(errors) = result.unwrap_err() else {
                panic!("two independent errors")
            };
            assert_eq!(errors.len(), 2);
            for error in errors {
                assert_broken_pipe(&error);
            }
            assert_eq!(*primary_calls.lock().unwrap(), (2, 0));
            assert_eq!(*sibling_calls.lock().unwrap(), (0, 1));
            assert_eq!(output.bytes(), b"i");
        }
        let actual = drain_sink(&receiver);
        assert_eq!(actual.started, 1);
        assert_eq!(actual.completed, 0);
        assert_eq!(actual.failed, u64::from(!cancelled));
        assert_eq!(actual.interrupted, u64::from(cancelled));
        assert_eq!(actual.errors, if cancelled { 0 } else { 2 });
        assert_eq!(actual.records, 0);
        assert_eq!(actual.bytes, output.bytes().len() as u64);
        assert_admitted_spans(
            &actual,
            if cancelled {
                SpanStatus::Unset
            } else {
                SpanStatus::Error
            },
        );
    }
}

#[test]
fn retired_terminal_spelling_reports_source_located_e376_without_output_effects() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("input.csv"), INPUT).expect("input fixture");
    let pipeline = workspace.path().join("retired-terminal.yaml");
    std::fs::write(
        &pipeline,
        r#"pipeline: { name: retired_terminal }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
        - { name: label, type: string }
  - type: output
    name: delivered
    input: rows
    config:
      name: delivered
      type: csv
      path: delivered.csv
"#,
    )
    .expect("retired terminal fixture");

    let output = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(workspace.path())
        .args(["run", "retired-terminal.yaml"])
        .output()
        .expect("run clinker");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty(), "a rejected config has no stdout");
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    for required in [
        "retired-terminal.yaml",
        "E376",
        RETIRED_TERMINAL_DISCRIMINATOR,
        "Correction: type: sink",
    ] {
        assert!(
            diagnostic.contains(required),
            "diagnostic must contain {required:?}:\n{diagnostic}"
        );
    }
    assert!(
        !workspace.path().join("delivered.csv").exists(),
        "the rejected terminal cannot publish output"
    );
    assert!(
        !workspace.path().join(".clinker-attempts").exists(),
        "the rejected terminal cannot start a publication attempt"
    );
}

fn retired_terminal_count(contents: &str) -> usize {
    contents.matches(RETIRED_TERMINAL_DISCRIMINATOR).count()
}

fn relative_path(workspace: &Path, path: &Path) -> io::Result<String> {
    let relative = path
        .strip_prefix(workspace)
        .map_err(|error| io::Error::other(error.to_string()))?;
    relative
        .components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| io::Error::other("workspace path is not UTF-8"))
        })
        .collect::<io::Result<Vec<_>>>()
        .map(|components| components.join("/"))
}

fn scan_authored_surfaces(
    workspace: &Path,
    root: &Path,
    occurrences: &mut BTreeMap<String, usize>,
) -> io::Result<()> {
    if !root.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            scan_authored_surfaces(workspace, &path, occurrences)?;
        } else if file_type.is_file()
            && path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| matches!(extension, "yaml" | "yml" | "rs" | "md"))
        {
            let contents = std::fs::read_to_string(&path)?;
            let count = retired_terminal_count(&contents);
            if count > 0 {
                occurrences.insert(relative_path(workspace, &path)?, count);
            }
        }
    }
    Ok(())
}

#[test]
fn authored_surfaces_contain_only_explicit_retired_terminal_examples() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .parent()
        .and_then(Path::parent)
        .expect("clinker crate lives under the workspace crates directory");
    let mut occurrences = BTreeMap::new();
    for root in [
        workspace.join("benches"),
        workspace.join("crates"),
        workspace.join("docs"),
        workspace.join("examples"),
    ] {
        scan_authored_surfaces(workspace, &root, &mut occurrences)
            .expect("scan authored YAML, Rust, and Markdown surfaces");
    }
    for file in [workspace.join("CHANGELOG.md"), workspace.join("README.md")] {
        let contents = std::fs::read_to_string(&file).expect("read root author surface");
        let count = retired_terminal_count(&contents);
        if count > 0 {
            occurrences.insert(
                relative_path(workspace, &file).expect("root-relative author surface"),
                count,
            );
        }
    }

    let expected = BTreeMap::from([
        ("CHANGELOG.md".to_owned(), 1),
        ("crates/clinker-core-types/src/diagnostic.rs".to_owned(), 1),
        (
            "crates/clinker-core-types/tests/registry_no_orphan_codes.rs".to_owned(),
            1,
        ),
        (
            "crates/clinker-exec/tests/node_taxonomy_lift_test.rs".to_owned(),
            2,
        ),
        ("crates/clinker/tests/sink_surface.rs".to_owned(), 2),
        ("docs/ai/10_ARCHITECTURE.md".to_owned(), 1),
        ("docs/ai/15_PRODUCTION_CONTRACTS.md".to_owned(), 1),
        ("docs/ai/20_CRATE_MAP.md".to_owned(), 1),
        ("docs/ai/70_GLOSSARY.md".to_owned(), 1),
        ("docs/engine/src/architecture.md".to_owned(), 1),
        ("docs/user/src/nodes/sink.md".to_owned(), 1),
        ("docs/user/src/ops/explain.md".to_owned(), 1),
    ]);
    assert_eq!(
        occurrences, expected,
        "every retired terminal spelling must be an explicit migration or negative-test example"
    );
}

#[cfg(feature = "lineage")]
fn local_lineage(yaml: &str) -> clinker_lineage::PlanColumnLineage {
    column_lineage_local_diagnostic_paths(&compile(yaml), Path::new("/workspace"))
}

#[cfg(feature = "lineage")]
fn output_named<'a>(
    lineage: &'a clinker_lineage::PlanColumnLineage,
    suffix: &str,
) -> &'a OutputColumnLineage {
    lineage
        .outputs
        .iter()
        .find(|output| output.dataset.name.ends_with(suffix))
        .unwrap_or_else(|| panic!("missing output dataset ending in {suffix:?}"))
}

#[cfg(feature = "lineage")]
fn has_influence(
    output: &OutputColumnLineage,
    field: &str,
    subtype: TransformationSubtype,
) -> bool {
    output.facet.dataset.iter().any(|input| {
        input.field == field
            && input.transformations.iter().any(|transformation| {
                transformation.transformation_type == TransformationType::Indirect
                    && transformation.subtype == Some(subtype)
            })
    })
}

#[cfg(feature = "lineage")]
#[test]
fn lineage_sink_mapping_reads_the_renamed_source_column_directly() {
    let lineage = local_lineage(
        r#"
pipeline: { name: mapped_sink_lineage }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: data/input.csv
      schema:
        - { name: customer_id, type: string }
        - { name: region, type: string }
  - type: sink
    name: delivered
    input: rows
    config:
      name: delivered
      type: csv
      path: out/mapped.csv
      mapping:
        - sold_to: customer_id
      include_unmapped: false
"#,
    );
    let output = output_named(&lineage, "out/mapped.csv");
    let sold_to = output
        .facet
        .fields
        .get("sold_to")
        .expect("mapped output column has lineage");
    assert_eq!(sold_to.input_fields.len(), 1);
    assert_eq!(sold_to.input_fields[0].field, "customer_id");
    assert_eq!(
        sold_to.input_fields[0].transformations[0].transformation_type,
        TransformationType::Direct
    );
    assert_eq!(
        sold_to.input_fields[0].transformations[0].subtype,
        Some(TransformationSubtype::Identity)
    );
}

#[cfg(feature = "lineage")]
#[test]
fn lineage_sink_preserves_filter_and_authored_order_influence() {
    let lineage = local_lineage(
        r#"
pipeline: { name: filtered_ordered_sink_lineage }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: data/input.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: route
    name: selected
    input: rows
    config:
      mode: exclusive
      conditions: { kept: "amount > 100" }
      default: rejected
  - type: sink
    name: delivered
    input: selected.kept
    config:
      name: delivered
      type: csv
      path: out/ordered.csv
      sort_order: [id]
"#,
    );
    let output = output_named(&lineage, "out/ordered.csv");
    assert!(has_influence(
        output,
        "amount",
        TransformationSubtype::Filter
    ));
    assert!(has_influence(output, "id", TransformationSubtype::Sort));
}

#[cfg(feature = "lineage")]
#[test]
fn lineage_sink_fan_out_keeps_each_branch_filter_influence() {
    let lineage = local_lineage(
        r#"
pipeline: { name: fan_out_sink_lineage }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: data/input.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: route
    name: split
    input: rows
    config:
      mode: exclusive
      conditions: { high: "amount > 100" }
      default: low
  - type: sink
    name: high
    input: split.high
    config: { name: high, type: csv, path: out/high.csv }
  - type: sink
    name: low
    input: split.low
    config: { name: low, type: csv, path: out/low.csv }
"#,
    );
    assert_eq!(lineage.outputs.len(), 2);
    for suffix in ["out/high.csv", "out/low.csv"] {
        assert!(has_influence(
            output_named(&lineage, suffix),
            "amount",
            TransformationSubtype::Filter
        ));
    }
}

#[cfg(feature = "lineage")]
fn binding(node: &str, dataset: &str) -> LineageNodeBinding {
    LineageNodeBinding::new(
        node,
        ExternalDatasetIdentity::catalog("analytics", dataset).expect("catalog identity"),
    )
}

#[cfg(feature = "lineage")]
#[test]
fn lineage_two_body_scoped_sinks_keep_distinct_external_identities() {
    let workspace = tempfile::tempdir().expect("workspace");
    let compositions = workspace.path().join("compositions");
    std::fs::create_dir_all(&compositions).expect("composition directory");
    std::fs::write(
        compositions.join("audit.comp.yaml"),
        r#"_compose:
  name: audit
  inputs:
    inp:
      schema:
        - { name: id, type: int }
  outputs:
    out: shape
  config_schema: {}

nodes:
  - type: transform
    name: shape
    input: inp
    config:
      cxl: "emit id = id"
  - type: sink
    name: audit
    input: shape
    config: { name: audit, type: csv, path: audit.csv }
"#,
    )
    .expect("write composition");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("pipeline directory");
    let config = parse_config(
        r#"
pipeline: { name: scoped_sink_lineage }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: data/input.csv
      schema:
        - { name: id, type: int }
  - type: composition
    name: first
    input: rows
    use: ../compositions/audit.comp.yaml
    inputs: { inp: rows }
  - type: composition
    name: second
    input: rows
    use: ../compositions/audit.comp.yaml
    inputs: { inp: rows }
  - type: sink
    name: published
    input: first
    config: { name: published, type: csv, path: published.csv }
"#,
    )
    .expect("pipeline parses");
    let compiled = config
        .compile(&CompileContext::with_pipeline_dir(
            workspace.path(),
            PathBuf::from("pipelines"),
        ))
        .expect("pipeline compiles");
    let identities = LineageIdentityContext::external([
        binding("rows", "source_rows"),
        binding("first.audit", "first_audit"),
        binding("second.audit", "second_audit"),
        binding("published", "published_rows"),
    ])
    .expect("identity context");
    let lineage = column_lineage_external(&compiled, &identities).expect("external lineage");
    let mut outputs: Vec<&str> = lineage
        .outputs
        .iter()
        .map(|output| output.dataset.name.as_str())
        .collect();
    outputs.sort_unstable();
    assert_eq!(outputs, ["first_audit", "published_rows", "second_audit"]);
    for dataset in ["first_audit", "second_audit"] {
        let output = lineage
            .outputs
            .iter()
            .find(|output| output.dataset.name == dataset)
            .expect("scoped Sink output");
        let id = output.facet.fields.get("id").expect("id field lineage");
        assert_eq!(id.input_fields[0].name, "source_rows");
        assert_eq!(id.input_fields[0].field, "id");
    }
}
