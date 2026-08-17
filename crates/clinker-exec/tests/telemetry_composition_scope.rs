//! Span identity across composition scopes.
//!
//! One pipeline can invoke the same composition at several call sites, and
//! every one of them runs the same body node names through the same telemetry
//! producer. The exported span has to say which call site it came from, or an
//! operator holding two identical spans cannot attribute either.

use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, single_file_reader};
use clinker_exec::telemetry::{SpanName, TelemetryArena};
use clinker_plan::config::{
    ClinkerToml, CompileContext, ResolvedObservabilityPolicy, parse_config,
};

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

fn fixture_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn telemetry_policy() -> ResolvedObservabilityPolicy {
    ClinkerToml::parse(
        r#"
[observability]
arena_bytes = "768KB"
ordinary_lane_bytes = "512KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 8
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

[observability.lineage]
queue_bytes = "1KB"
max_event_bytes = "512B"
drop_policy = "drop_newest"
flush_timeout_ms = 500
identity_mode = "local_diagnostic_paths"
"#,
    )
    .expect("telemetry policy parses")
    .resolve_observability(None)
    .expect("telemetry policy resolves")
}

/// Two call sites of one composition, plus a top-level transform deliberately
/// sharing the body node's name. Body and top-level names live in separate
/// scopes and may legally collide, which is exactly why a bare body name cannot
/// identify the node that produced a span.
const TWO_CALL_SITES: &str = r#"
pipeline:
  name: composition_span_scope
nodes:
  - type: source
    name: eu_orders
    config:
      name: eu_orders
      type: csv
      path: eu.csv
      schema:
        - { name: a, type: int }
  - type: transform
    name: doubler
    input: eu_orders
    config:
      cxl: |
        emit a = a
  - type: composition
    name: enrich_eu
    input: doubler
    use: ../compositions/exec_transform_check.comp.yaml
    inputs:
      inp: doubler
  - type: sink
    name: eu_out
    input: enrich_eu
    config:
      name: eu_out
      type: csv
      path: eu-out.csv
      include_unmapped: false
  - type: source
    name: us_orders
    config:
      name: us_orders
      type: csv
      path: us.csv
      schema:
        - { name: a, type: int }
  - type: composition
    name: enrich_us
    input: us_orders
    use: ../compositions/exec_transform_check.comp.yaml
    inputs:
      inp: us_orders
  - type: sink
    name: us_out
    input: enrich_us
    config:
      name: us_out
      type: csv
      path: us-out.csv
      include_unmapped: false
"#;

/// Run `TWO_CALL_SITES` with telemetry enabled and return every transform span
/// name the receiver saw, sorted, paired with the two output CSVs.
fn transform_span_names() -> (Vec<String>, HashMap<String, String>) {
    let config = parse_config(TWO_CALL_SITES).expect("pipeline fixture parses");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("pipeline fixture compiles");

    let policy = telemetry_policy();
    let (producer, receiver) = TelemetryArena::reserve(&policy).expect("arena reserves");

    let readers: clinker_exec::executor::SourceReaders = HashMap::from([
        (
            "eu_orders".to_string(),
            single_file_reader("eu.csv", Box::new(Cursor::new(b"a\n5\n".to_vec()))),
        ),
        (
            "us_orders".to_string(),
            single_file_reader("us.csv", Box::new(Cursor::new(b"a\n7\n".to_vec()))),
        ),
    ]);
    let buffers: HashMap<String, SharedBuffer> = config
        .output_configs()
        .map(|output| (output.name.clone(), SharedBuffer::default()))
        .collect();
    let writers: HashMap<String, Box<dyn Write + Send>> = buffers
        .iter()
        .map(|(name, buffer)| {
            (
                name.clone(),
                Box::new(buffer.clone()) as Box<dyn Write + Send>,
            )
        })
        .collect();
    let params = PipelineRunParams {
        execution_id: "composition-span-scope".to_string(),
        batch_id: "batch-001".to_string(),
        telemetry_producer: Some(producer),
        ..Default::default()
    };
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("pipeline run");

    let mut spans = Vec::new();
    while let Some(batch) = receiver.try_recv_batch() {
        spans.extend(
            batch
                .traces()
                .iter()
                .filter(|span| span.name == SpanName::Transform)
                .map(|span| span.logical_node.clone()),
        );
    }
    spans.sort();
    let outputs = buffers
        .into_iter()
        .map(|(name, buffer)| {
            (
                name,
                String::from_utf8(buffer.0.lock().unwrap().clone()).expect("utf-8 output"),
            )
        })
        .collect();
    (spans, outputs)
}

/// Both call sites run a body transform named `doubler`, and so does the top
/// level. Three spans, three names: the body's are qualified by the call site
/// that invoked them and the top-level one is not.
#[test]
fn body_transform_spans_name_the_call_site_that_ran_them() {
    let (spans, outputs) = transform_span_names();

    assert_eq!(
        spans,
        vec![
            "doubler".to_string(),
            "enrich_eu.doubler".to_string(),
            "enrich_us.doubler".to_string(),
        ],
        "each transform dispatch closes one span under its own exported identity"
    );

    // The run has to be a real one: identical span names would also be produced
    // by a pipeline that never executed either body.
    assert_eq!(outputs["eu_out"], "a,computed\n5,10\n");
    assert_eq!(outputs["us_out"], "a,computed\n7,14\n");
}
