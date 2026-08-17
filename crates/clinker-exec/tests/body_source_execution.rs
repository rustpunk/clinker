use std::collections::HashMap;
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::capabilities::{
    AdmittedActivationGroup, AdmittedRunCapabilities, AdmittedSourceOpener, CapabilityOpenError,
    CapabilityOpener, CapabilityReservationError, CapabilitySession, GroupCapacityLease,
    GroupCapacityReservation,
};
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceInput};
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_exec::source::multi_file::FileSlot;
use clinker_exec::telemetry::{
    MetricKey, SpanName, SpanStatus, TelemetryArena, TelemetryReceiver, TraceSpan,
};
use clinker_plan::config::{ClinkerToml, CompileContext, PipelineConfig, parse_config};
use clinker_plan::plan::execution::{CompiledSourceInstanceId, CompiledSourceScope};

const BODY: &str = r#"_compose:
  name: fixed_reader
  inputs: {}
  outputs: { out: read }
  config_schema: {}
  resources_schema:
    input: { kind: file, required: true }
nodes:
  - type: source
    name: read
    config:
      name: read
      type: fixed_width
      resource: input
      on_unmapped: { mode: drop }
      schema:
        discriminator: { start: 0, width: 1 }
        records:
          - id: header
            tag: H
            columns: [{ name: batch, type: string, start: 1, width: 4 }]
          - id: detail
            tag: D
            columns: [{ name: id, type: int, start: 1, width: 4 }]
"#;

const CSV_DROP_BODY: &str = r#"_compose:
  name: csv_drop_reader
  inputs: {}
  outputs: { out: read }
  config_schema: {}
  resources_schema:
    input: { kind: file, required: true }
nodes:
  - type: source
    name: read
    config:
      name: read
      type: csv
      resource: input
      on_unmapped: { mode: drop }
      schema: [{ name: id, type: string }]
"#;

const INTERLEAVE_BODY: &str = r#"_compose:
  name: interleave_reader
  inputs: {}
  outputs: { out: mixed }
  config_schema: {}
  resources_schema:
    input: { kind: file, required: true }
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      resource: input
      schema: [{ name: id, type: string }]
  - type: source
    name: right
    config:
      name: right
      type: csv
      resource: input
      schema: [{ name: id, type: string }]
  - type: merge
    name: mixed
    inputs: [left, right]
    config: { mode: interleave }
"#;

const PIPELINE: &str = r#"pipeline: { name: body_source_runtime }
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      path: driver.csv
      schema: [{ name: seed, type: string }]
  - type: composition
    name: first
    input: driver
    use: ../compositions/fixed_reader.comp.yaml
    inputs: {}
    resources: { input: shared_input }
  - type: composition
    name: second
    input: driver
    use: ../compositions/fixed_reader.comp.yaml
    inputs: {}
    resources: { input: shared_input }
  - type: output
    name: first_out
    input: first
    config: { name: first_out, type: csv, path: first.csv }
  - type: output
    name: second_out
    input: second
    config: { name: second_out, type: csv, path: second.csv }
"#;

fn workspace(body: &str) -> tempfile::TempDir {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(workspace.path().join("compositions")).expect("composition dir");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("pipeline dir");
    std::fs::create_dir_all(workspace.path().join("data")).expect("data dir");
    std::fs::write(
        workspace.path().join("compositions/fixed_reader.comp.yaml"),
        body,
    )
    .expect("body");
    std::fs::write(workspace.path().join("data/input.txt"), "H0001\nD0002\n")
        .expect("resource file");
    std::fs::write(
        workspace.path().join("clinker.toml"),
        r#"[catalog.resources.shared_input]
kind = "file"
path = "data/input.txt"
access = "read"
"#,
    )
    .expect("catalog");
    workspace
}

fn compile(workspace: &Path) -> clinker_plan::plan::CompiledPlan {
    let config: PipelineConfig = parse_config(PIPELINE).expect("pipeline parses");
    config
        .compile(&CompileContext::with_pipeline_dir(
            workspace,
            PathBuf::from("pipelines"),
        ))
        .unwrap_or_else(|diagnostics| panic!("pipeline compiles: {diagnostics:?}"))
}

#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<String>>>);

impl Events {
    fn push(&self, event: impl Into<String>) {
        self.0.lock().expect("event log").push(event.into());
    }

    fn snapshot(&self) -> Vec<String> {
        self.0.lock().expect("event log").clone()
    }
}

struct Lease {
    label: String,
    events: Events,
}
impl GroupCapacityLease for Lease {}
impl Drop for Lease {
    fn drop(&mut self) {
        self.events.push(format!("release:{}", self.label));
    }
}

struct Reservation {
    label: String,
    events: Events,
}
impl GroupCapacityReservation for Reservation {
    fn reserve(self: Box<Self>) -> Result<Box<dyn GroupCapacityLease>, CapabilityReservationError> {
        self.events.push(format!("reserve:{}", self.label));
        Ok(Box::new(Lease {
            label: self.label,
            events: self.events,
        }))
    }
}

struct FixtureOpener {
    label: String,
    events: Events,
    body: Option<Vec<u8>>,
    fail: bool,
    fail_read: bool,
    request_shutdown: Option<ShutdownToken>,
}

impl CapabilityOpener for FixtureOpener {
    fn open(self: Box<Self>) -> Result<Box<dyn CapabilitySession>, CapabilityOpenError> {
        self.events.push(format!("open:{}", self.label));
        if self.fail {
            return Err(CapabilityOpenError::Unavailable);
        }
        if let Some(token) = &self.request_shutdown {
            token.request();
        }
        let fail_read = self.fail_read;
        Ok(Box::new(FixtureSession {
            label: self.label,
            events: self.events,
            input: self.body.map(|body| {
                let reader: Box<dyn Read + Send> = if fail_read {
                    Box::new(FailingReader)
                } else {
                    Box::new(Cursor::new(body))
                };
                SourceInput::Files(vec![FileSlot::new(
                    PathBuf::from("logical-input.txt"),
                    reader,
                )])
            }),
        }))
    }
}

struct FailingReader;

impl Read for FailingReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("fixture body Source read failed"))
    }
}

struct FixtureSession {
    label: String,
    events: Events,
    input: Option<SourceInput>,
}

impl CapabilitySession for FixtureSession {
    fn take_source_input(&mut self) -> Result<SourceInput, CapabilityOpenError> {
        self.input.take().ok_or(CapabilityOpenError::Unavailable)
    }
}

impl Drop for FixtureSession {
    fn drop(&mut self) {
        self.events.push(format!("close:{}", self.label));
    }
}

fn admitted(
    plan: &clinker_plan::plan::CompiledPlan,
    events: &Events,
    body_input: &[u8],
    failing_source: Option<&str>,
    failing_read_source: Option<&str>,
    shutdown_on_open: Option<(&str, &ShutdownToken)>,
) -> AdmittedRunCapabilities {
    let activation = plan.dag().source_activation();
    let groups = activation
        .groups()
        .iter()
        .map(|group| {
            let sources = group
                .members()
                .iter()
                .copied()
                .map(|member| {
                    let source_name = activation
                        .instances()
                        .iter()
                        .find(|instance| instance.id() == member)
                        .expect("group member is inventoried")
                        .source_name();
                    let body = matches!(member.scope, CompiledSourceScope::CompositionBody(_))
                        .then(|| body_input.to_vec());
                    AdmittedSourceOpener::new(
                        member,
                        Box::new(FixtureOpener {
                            label: format!("{}:{source_name}", instance_label(member)),
                            events: events.clone(),
                            body,
                            fail: failing_source == Some(source_name),
                            fail_read: failing_read_source == Some(source_name),
                            request_shutdown: shutdown_on_open
                                .filter(|(name, _)| *name == source_name)
                                .map(|(_, token)| token.clone()),
                        }),
                    )
                })
                .collect();
            AdmittedActivationGroup::new(
                group.id(),
                group.capacity(),
                sources,
                Box::new(Reservation {
                    label: format!("group-{}", group.id().index()),
                    events: events.clone(),
                }),
            )
        })
        .collect();
    AdmittedRunCapabilities::admit(activation, groups).expect("capabilities admit")
}

fn telemetry_policy() -> clinker_plan::config::ResolvedObservabilityPolicy {
    telemetry_policy_sampling(1)
}

fn telemetry_policy_sampling(
    sample_every: u32,
) -> clinker_plan::config::ResolvedObservabilityPolicy {
    ClinkerToml::parse(&format!(
        r#"
[observability]
arena_bytes = "64KB"
ordinary_lane_bytes = "48KB"
high_severity_lane_bytes = "16KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 8
max_attribute_bytes = "256B"
drop_policy = "drop_newest"
sample_every = {sample_every}
rate_limit_per_second = 100000
rate_limit_burst = 100000
flush_timeout_ms = 1000
"#
    ))
    .expect("telemetry policy parses")
    .resolve_observability(None)
    .expect("telemetry policy resolves")
}

#[derive(Default)]
struct SourceSignals {
    started: u64,
    completed: u64,
    failed: u64,
    interrupted: u64,
    spans: Vec<TraceSpan>,
}

fn drain_source_signals(receiver: &TelemetryReceiver) -> SourceSignals {
    let mut signals = SourceSignals::default();
    while let Some(batch) = receiver.try_recv_batch() {
        signals.started += batch.metric(MetricKey::SourceStarted);
        signals.completed += batch.metric(MetricKey::SourceCompleted);
        signals.failed += batch.metric(MetricKey::SourceFailed);
        signals.interrupted += batch.metric(MetricKey::SourceInterrupted);
        signals.spans.extend(
            batch
                .traces()
                .iter()
                .filter(|span| span.name == SpanName::Source)
                .cloned(),
        );
    }
    signals
}

fn instance_label(instance: CompiledSourceInstanceId) -> String {
    match instance.scope {
        CompiledSourceScope::TopLevel => "top-level".to_string(),
        CompiledSourceScope::CompositionBody(scope) => format!("body-{}", scope.0),
    }
}

#[test]
fn tracer_two_calls_open_distinct_finite_body_source_sessions() {
    let workspace = workspace(BODY);
    let plan = compile(workspace.path());
    let events = Events::default();
    let capabilities = admitted(&plan, &events, b"H0001\nD0002\n", None, None, None);
    let (producer, receiver) =
        TelemetryArena::reserve(&telemetry_policy()).expect("telemetry arena reserves");
    let readers = HashMap::from([(
        "driver".to_string(),
        SourceInput::Files(vec![FileSlot::new(
            "driver.csv",
            Box::new(Cursor::new(b"seed\ngo\n".to_vec())),
        )]),
    )]);
    let first = SharedBuffer::new();
    let second = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        ("first_out".to_string(), Box::new(first.clone()) as _),
        ("second_out".to_string(), Box::new(second.clone()) as _),
    ]);

    let report = PipelineExecutor::run_admitted_plan_with_readers_writers_in_context(
        &plan,
        capabilities,
        readers,
        writers,
        &PipelineRunParams {
            telemetry_producer: Some(producer),
            ..Default::default()
        },
        CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines")),
    )
    .expect("body Sources execute");

    assert_eq!(report.counters.ok_count, 4);
    assert_eq!(report.per_source_record_counts.get("first.read"), Some(&2));
    assert_eq!(report.per_source_record_counts.get("second.read"), Some(&2));
    assert!(!report.per_source_record_counts.contains_key("read"));
    assert!(first.as_string().contains("0001"));
    assert!(first.as_string().contains('2'));
    assert!(second.as_string().contains("0001"));
    assert!(second.as_string().contains('2'));
    let events = events.snapshot();
    let opens: Vec<_> = events
        .iter()
        .filter(|event| event.starts_with("open:body-"))
        .collect();
    let closes: Vec<_> = events
        .iter()
        .filter(|event| event.starts_with("close:body-"))
        .collect();
    assert_eq!(opens.len(), 2, "{events:?}");
    assert_eq!(closes.len(), 2, "{events:?}");
    assert_ne!(
        opens[0], opens[1],
        "call sites must keep distinct identities"
    );

    let mut resource_started = 0;
    let mut resource_completed = 0;
    let mut resource_spans = Vec::new();
    let mut source_signals = SourceSignals::default();
    while let Some(batch) = receiver.try_recv_batch() {
        resource_started += batch.metric(MetricKey::ResourceOpenStarted);
        resource_completed += batch.metric(MetricKey::ResourceOpenCompleted);
        resource_spans.extend(
            batch
                .traces()
                .iter()
                .filter(|span| span.name == SpanName::ResourceOpen)
                .cloned(),
        );
        source_signals.started += batch.metric(MetricKey::SourceStarted);
        source_signals.completed += batch.metric(MetricKey::SourceCompleted);
        source_signals.failed += batch.metric(MetricKey::SourceFailed);
        source_signals.interrupted += batch.metric(MetricKey::SourceInterrupted);
        source_signals.spans.extend(
            batch
                .traces()
                .iter()
                .filter(|span| span.name == SpanName::Source)
                .cloned(),
        );
    }
    assert_eq!(resource_started, 2);
    assert_eq!(resource_completed, 2);
    // Both body workers emit without blocking. Natural arena contention may
    // shed a span, while the fixed metric counters still prove every lifecycle
    // attempt and terminal outcome. Validate every admitted span as a subset.
    assert!(resource_spans.len() <= 2);
    assert!(
        resource_spans
            .iter()
            .all(|span| span.status == SpanStatus::Ok)
    );
    let mut logical_nodes: Vec<_> = resource_spans
        .iter()
        .map(|span| span.logical_node.as_str())
        .collect();
    logical_nodes.sort_unstable();
    assert!(
        logical_nodes
            .iter()
            .all(|node| matches!(*node, "first.read" | "second.read"))
    );
    assert_eq!(source_signals.started, 3);
    assert_eq!(source_signals.completed, 3);
    assert_eq!(source_signals.failed, 0);
    assert_eq!(source_signals.interrupted, 0);
    assert!(source_signals.spans.len() <= 3);
    assert!(
        source_signals
            .spans
            .iter()
            .all(|span| span.status == SpanStatus::Ok && span.logical_node == "source")
    );
    let rendered = format!("{:?}{:?}", resource_spans, source_signals.spans);
    assert!(!rendered.contains("shared_input"));
    assert!(!rendered.contains("logical-input"));
    assert!(!rendered.contains("H0001"));
    let source_rendered = format!("{:?}", source_signals.spans);
    assert!(!source_rendered.contains("first.read"));
    assert!(!source_rendered.contains("second.read"));
}

fn run_with_fixture(
    plan: &clinker_plan::plan::CompiledPlan,
    workspace: &Path,
    capabilities: AdmittedRunCapabilities,
    params: &PipelineRunParams,
) -> (
    Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError>,
    SharedBuffer,
    SharedBuffer,
) {
    let readers = HashMap::from([(
        "driver".to_string(),
        SourceInput::Files(vec![FileSlot::new(
            "driver.csv",
            Box::new(Cursor::new(b"seed\ngo\n".to_vec())),
        )]),
    )]);
    let first = SharedBuffer::new();
    let second = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        ("first_out".to_string(), Box::new(first.clone()) as _),
        ("second_out".to_string(), Box::new(second.clone()) as _),
    ]);
    let result = PipelineExecutor::run_admitted_plan_with_readers_writers_in_context(
        plan,
        capabilities,
        readers,
        writers,
        params,
        CompileContext::with_pipeline_dir(workspace, PathBuf::from("pipelines")),
    );
    (result, first, second)
}

#[test]
fn body_source_honors_the_retained_unmapped_drop_policy() {
    let workspace = workspace(CSV_DROP_BODY);
    let plan = compile(workspace.path());
    let events = Events::default();
    let capabilities = admitted(
        &plan,
        &events,
        b"id,extra\none,hidden\ntwo,private\n",
        None,
        None,
        None,
    );

    let (result, first, second) = run_with_fixture(
        &plan,
        workspace.path(),
        capabilities,
        &PipelineRunParams::default(),
    );
    let report = result.expect("body CSV Sources execute");

    assert_eq!(report.counters.ok_count, 4);
    for source in ["first.read", "second.read"] {
        assert_eq!(report.per_source_record_counts.get(source), Some(&2));
    }
    for output in [first.as_string(), second.as_string()] {
        assert!(output.contains("one"));
        assert!(output.contains("two"));
        assert!(!output.contains("extra"));
        assert!(!output.contains("hidden"));
        assert!(!output.contains("private"));
    }
}

#[test]
fn partial_group_open_failure_closes_sessions_without_starting_downstream() {
    let workspace = workspace(INTERLEAVE_BODY);
    let plan = compile(workspace.path());
    let events = Events::default();
    let capabilities = admitted(&plan, &events, b"id\n1\n", Some("right"), None, None);
    let (producer, receiver) =
        TelemetryArena::reserve(&telemetry_policy()).expect("telemetry arena reserves");
    let params = PipelineRunParams {
        telemetry_producer: Some(producer),
        ..Default::default()
    };

    let (result, first, second) = run_with_fixture(&plan, workspace.path(), capabilities, &params);
    let error = result.expect_err("second opener fails the complete group");
    let rendered_error = error.to_string();
    assert!(rendered_error.contains("admitted Source capability could not be opened"));
    assert!(!rendered_error.contains("shared_input"));
    assert!(!rendered_error.contains("logical-input"));
    assert!(first.as_string().is_empty());
    assert!(second.as_string().is_empty());

    let events = events.snapshot();
    let open_left = events
        .iter()
        .position(|event| event.contains("open:body-") && event.ends_with(":left"))
        .expect("left opens");
    let open_right = events
        .iter()
        .position(|event| event.contains("open:body-") && event.ends_with(":right"))
        .expect("right open is attempted");
    let close_left = events
        .iter()
        .position(|event| event.contains("close:body-") && event.ends_with(":left"))
        .expect("left session closes");
    assert!(
        open_left < open_right && open_right < close_left,
        "{events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("release:group-"))
            .count(),
        plan.dag().source_activation().groups().len(),
        "every reserved group lease is released: {events:?}"
    );

    let mut started = 0;
    let mut completed = 0;
    let mut failed = 0;
    let mut spans = Vec::new();
    let mut source_started = 0;
    let mut source_terminal = 0;
    let mut source_spans = 0;
    while let Some(batch) = receiver.try_recv_batch() {
        started += batch.metric(MetricKey::ResourceOpenStarted);
        completed += batch.metric(MetricKey::ResourceOpenCompleted);
        failed += batch.metric(MetricKey::ResourceOpenFailed);
        spans.extend(
            batch
                .traces()
                .iter()
                .filter(|span| span.name == SpanName::ResourceOpen)
                .cloned(),
        );
        source_started += batch.metric(MetricKey::SourceStarted);
        source_terminal += batch.metric(MetricKey::SourceCompleted)
            + batch.metric(MetricKey::SourceFailed)
            + batch.metric(MetricKey::SourceInterrupted);
        source_spans += batch
            .traces()
            .iter()
            .filter(|span| span.name == SpanName::Source)
            .count();
    }
    assert_eq!((started, completed, failed), (2, 1, 1));
    assert_eq!(spans.len(), 2);
    assert!(spans.iter().any(|span| span.status == SpanStatus::Error));
    assert_eq!((source_started, source_terminal), (1, 1));
    assert!(
        source_spans <= source_started as usize,
        "Source spans are admission-controlled and may be dropped"
    );
}

#[test]
fn read_failure_after_open_emits_one_failed_terminal_per_started_source() {
    let workspace = workspace(CSV_DROP_BODY);
    let plan = compile(workspace.path());
    let events = Events::default();
    let capabilities = admitted(&plan, &events, b"id\none\n", None, Some("read"), None);
    let (producer, receiver) =
        TelemetryArena::reserve(&telemetry_policy()).expect("telemetry arena reserves");
    let params = PipelineRunParams {
        telemetry_producer: Some(producer),
        ..Default::default()
    };

    let (result, _, _) = run_with_fixture(&plan, workspace.path(), capabilities, &params);
    result.expect_err("the admitted reader fails after its group opens");

    let signals = drain_source_signals(&receiver);
    assert_eq!(signals.started, 2);
    assert_eq!(signals.failed, 1);
    assert_eq!(signals.completed, 1);
    assert_eq!(signals.interrupted, 0);
    assert!(
        signals.spans.len() as u64 <= signals.started,
        "Source spans are admission-controlled and may be dropped"
    );
    assert!(
        signals
            .spans
            .iter()
            .all(|span| span.logical_node == "source")
    );
    assert!(
        signals
            .spans
            .iter()
            .all(|span| matches!(span.status, SpanStatus::Ok | SpanStatus::Error))
    );

    let events = events.snapshot();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("open:body-"))
            .count(),
        events
            .iter()
            .filter(|event| event.starts_with("close:body-"))
            .count(),
        "every opened session closes after worker failure: {events:?}"
    );
}

#[test]
fn simultaneous_group_opens_every_member_before_body_records_flow() {
    let workspace = workspace(INTERLEAVE_BODY);
    let plan = compile(workspace.path());
    let events = Events::default();
    let capabilities = admitted(&plan, &events, b"id\n1\n", None, None, None);

    let (result, first, second) = run_with_fixture(
        &plan,
        workspace.path(),
        capabilities,
        &PipelineRunParams::default(),
    );
    let report = result.expect("simultaneous body Source group executes");

    assert_eq!(report.counters.ok_count, 4);
    for source in ["first.left", "first.right", "second.left", "second.right"] {
        assert_eq!(report.per_source_record_counts.get(source), Some(&1));
    }
    assert_eq!(first.as_string().lines().count(), 3);
    assert_eq!(second.as_string().lines().count(), 3);
    let events = events.snapshot();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("open:body-"))
            .count(),
        4,
        "two members open for each distinct call scope: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("close:body-"))
            .count(),
        4,
        "every opened session closes: {events:?}"
    );
}

#[test]
fn cancellation_after_open_closes_the_active_session_and_group_lease() {
    let workspace = workspace(CSV_DROP_BODY);
    let plan = compile(workspace.path());
    let events = Events::default();
    let shutdown = ShutdownToken::detached();
    let capabilities = admitted(
        &plan,
        &events,
        b"id\none\ntwo\n",
        None,
        None,
        Some(("read", &shutdown)),
    );
    let (producer, receiver) =
        TelemetryArena::reserve(&telemetry_policy()).expect("telemetry arena reserves");
    let params = PipelineRunParams {
        shutdown_token: Some(shutdown),
        telemetry_producer: Some(producer),
        ..Default::default()
    };

    let (result, _, _) = run_with_fixture(&plan, workspace.path(), capabilities, &params);
    let report = result.expect("cancellation unwinds cleanly");
    assert!(report.interrupted);
    let events = events.snapshot();
    assert!(events.iter().any(|event| event.starts_with("open:body-")));
    assert!(events.iter().any(|event| event.starts_with("close:body-")));
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("release:group-"))
            .count(),
        plan.dag().source_activation().groups().len(),
        "all leases release on cancellation: {events:?}"
    );

    let signals = drain_source_signals(&receiver);
    assert_eq!(signals.started, 2);
    assert!(signals.interrupted >= 1);
    assert_eq!(signals.completed + signals.interrupted, signals.started);
    assert_eq!(signals.failed, 0);
    assert!(
        signals.spans.len() as u64 <= signals.started,
        "Source spans are admission-controlled and may be dropped"
    );
    assert!(
        signals
            .spans
            .iter()
            .all(|span| span.logical_node == "source")
    );
    assert!(
        signals
            .spans
            .iter()
            .all(|span| matches!(span.status, SpanStatus::Ok | SpanStatus::Unset))
    );
}

#[test]
fn source_span_admission_loss_does_not_change_execution_or_cleanup() {
    let workspace = workspace(BODY);
    let plan = compile(workspace.path());
    let events = Events::default();
    let capabilities = admitted(&plan, &events, b"H0001\nD0002\n", None, None, None);
    let (producer, receiver) = TelemetryArena::reserve(&telemetry_policy_sampling(2))
        .expect("sampled telemetry arena reserves");
    let observer = producer.clone();
    let params = PipelineRunParams {
        telemetry_producer: Some(producer),
        ..Default::default()
    };

    let (result, first, second) = run_with_fixture(&plan, workspace.path(), capabilities, &params);
    let report = result.expect("telemetry admission cannot change Source work");

    assert_eq!(report.counters.ok_count, 4);
    assert!(first.as_string().contains("0001"));
    assert!(second.as_string().contains("0001"));
    assert!(observer.snapshot().sampled_drops > 0);
    let signals = drain_source_signals(&receiver);
    assert_eq!((signals.started, signals.completed), (3, 3));
    assert_eq!((signals.failed, signals.interrupted), (0, 0));

    let events = events.snapshot();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("open:body-"))
            .count(),
        2,
        "both Sources still open: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("close:body-"))
            .count(),
        2,
        "both Sources still close: {events:?}"
    );
}
