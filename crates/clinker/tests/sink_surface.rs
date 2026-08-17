//! Sink-specific observability and lineage contract tests.

use std::collections::HashMap;
use std::io::{self, Cursor, Write};
#[cfg(feature = "lineage")]
use std::path::{Path, PathBuf};
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
    assert!(
        actual
            .spans
            .iter()
            .all(|span| span.status == SpanStatus::Ok && span.logical_node == "delivered")
    );
    assert!(
        actual.spans[0].ended_at_unix_nanos >= actual.spans[0].started_at_unix_nanos,
        "the complete span is closed at both ends"
    );
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
    assert_eq!(
        actual.records, 2,
        "both rows reached the buffered writer before its flush failed"
    );
    assert!(
        actual.errors >= 1,
        "the writer failure is counted: {actual:?}"
    );
    assert_eq!(
        actual.bytes,
        b"id,label\n1,alpha\n2,beta\n".len() as u64,
        "bytes accepted before the flush failure remain observable"
    );
    assert!(
        actual.spans.len() <= 1,
        "Sink spans are admission-controlled"
    );
    assert!(
        actual
            .spans
            .iter()
            .all(|span| span.status == SpanStatus::Error)
    );
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
    assert!(
        actual
            .spans
            .iter()
            .all(|span| span.status == SpanStatus::Unset)
    );
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
