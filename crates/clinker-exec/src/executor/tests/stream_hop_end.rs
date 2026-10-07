//! A producer streaming into a step on another thread hands that step every
//! row it produced.
//!
//! A bounded preview runs a Transform apart from its Source, so the Sources
//! drain in a fixed order, while the Sink keeps the streaming writer the
//! compiled plan gave it. The Transform's rows reach that writer, not a
//! buffer nobody reads.
//!
//! A producer that fails delivers the rows it emitted before its failure,
//! then reports its own error.
//!
//! The writers are raw in-memory buffers, so anything a step finished is
//! visible here even though a production run would leave it staged and
//! unpublished.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

struct Run {
    result: Result<ExecutionReport, PipelineError>,
    outputs: HashMap<&'static str, String>,
}

/// Run `yaml` over `readers`, writing each of `sinks` to an in-memory
/// buffer and reading no process memory, under `policy` when given.
fn run_with(
    yaml: &str,
    readers: crate::executor::SourceReaders,
    sinks: &[&'static str],
    params: PipelineRunParams,
    policy: Option<RunPolicy>,
) -> Run {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let buffers: Vec<(&'static str, SharedBuffer)> = sinks
        .iter()
        .map(|sink| (*sink, SharedBuffer::new()))
        .collect();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = buffers
        .iter()
        .map(|(sink, buffer)| {
            (
                sink.to_string(),
                Box::new(buffer.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let params = PipelineRunParams {
        execution_id: "stream-hop-end".to_string(),
        batch_id: "batch-0".to_string(),
        memory_test: crate::executor::MemoryTestOverrides::default().with_no_process_memory(),
        ..params
    };
    let result = match policy {
        // A preview never publishes, so its writers are not committed.
        Some(policy) => PipelineExecutor::run_with_readers_writers_in_context_and_activation(
            &config,
            readers,
            WriterRegistry {
                auto_commit_staged: false,
                ..WriterRegistry::from(writers)
            },
            &params,
            policy,
            clinker_plan::config::CompileContext::default(),
            None,
        ),
        None => {
            PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params)
        }
    };
    Run {
        result,
        outputs: buffers
            .into_iter()
            .map(|(sink, buffer)| (sink, buffer.as_string()))
            .collect(),
    }
}

fn csv_reader(name: &str, csv: String) -> (String, crate::source::SourceInput) {
    (
        name.to_string(),
        crate::executor::single_file_reader(
            format!("{name}.csv"),
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )
}

fn pipeline(name: &str, nodes: &[&str]) -> String {
    format!("pipeline:\n  name: {name}\nnodes:{}", nodes.concat())
}

const ROWS_SOURCE: &str = r#"
- type: source
  name: src
  config:
    name: src
    type: csv
    path: src.csv
    schema:
      - { name: grp, type: string }
      - { name: id, type: string }
"#;

/// Source -> Transform -> Sink, the Transform fusable with its Source.
fn fusable_transform_sink() -> String {
    pipeline(
        "fusable_transform_sink",
        &[
            ROWS_SOURCE,
            r#"
- type: transform
  name: pass
  input: src
  config:
    cxl: |
      emit grp = grp
      emit id = id
- type: sink
  name: rows_out
  input: pass
  config:
    name: rows_out
    type: csv
    path: rows_out.csv
"#,
        ],
    )
}

/// A bounded preview reads at most its limit from each Source and writes
/// those rows through every step, a Sink fed by a Transform that a full
/// run would fuse with its Source included.
#[test]
fn a_preview_of_a_fusable_transform_chain_writes_its_rows() {
    let csv = "grp,id\ng0,a\ng1,b\ng2,c\ng3,d\ng0,e\n".to_string();
    let run = run_with(
        &fusable_transform_sink(),
        HashMap::from([csv_reader("src", csv)]),
        &["rows_out"],
        PipelineRunParams::default(),
        Some(RunPolicy::new(
            std::num::NonZeroUsize::MIN,
            PreviewPolicy::RecordsPerSource(std::num::NonZeroU64::new(2).unwrap()),
        )),
    );
    run.result.as_ref().expect("a bounded preview succeeds");
    assert_eq!(run.outputs["rows_out"], "grp,id\ng0,a\ng1,b\n");
}

/// Source -> Transform -> Sink whose Transform divides by `d`, failing the
/// run on the first row where `d` is zero.
const DIVIDING_CHAIN: &str = r#"
pipeline:
  name: dividing_chain
error_handling:
  strategy: fail_fast
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: src.csv
    schema:
      - { name: id, type: int }
      - { name: d, type: int }
- type: transform
  name: pass
  input: src
  config:
    cxl: |
      emit id = id
      emit q = id / d
- type: sink
  name: rows_out
  input: pass
  config:
    name: rows_out
    type: csv
    path: rows_out.csv
"#;

const DIVIDING_ROWS: &str = "id,d\n1,1\n2,1\n3,0\n4,1\n5,1\n";

/// A telemetry producer with room for one small run's signals.
fn telemetry_arena() -> (
    crate::telemetry::TelemetryProducer,
    crate::telemetry::TelemetryReceiver,
) {
    let policy = clinker_plan::config::ClinkerToml::parse(
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
    .expect("the telemetry policy parses")
    .resolve_observability(None)
    .expect("the telemetry policy resolves");
    crate::telemetry::TelemetryArena::reserve(&policy).expect("arena reserves")
}

/// A preview whose Transform fails hands its Sink the rows it produced before
/// the failing row, then reports the Transform's error, the same error a full
/// run of the pipeline reports.
#[test]
fn a_preview_transform_delivers_its_rows_before_its_failure_and_reports_the_same_error() {
    let (producer, receiver) = telemetry_arena();
    let preview = run_with(
        DIVIDING_CHAIN,
        HashMap::from([csv_reader("src", DIVIDING_ROWS.to_string())]),
        &["rows_out"],
        PipelineRunParams {
            telemetry_producer: Some(producer),
            ..PipelineRunParams::default()
        },
        Some(RunPolicy::new(
            std::num::NonZeroUsize::MIN,
            PreviewPolicy::RecordsPerSource(std::num::NonZeroU64::new(5).unwrap()),
        )),
    );
    let preview_error = preview
        .result
        .expect_err("the preview fails on the zero divisor");
    // `q` is emitted only by `pass`, so its division error is the Transform's.
    assert!(
        matches!(
            &preview_error,
            PipelineError::Eval(error)
                if matches!(error.kind, cxl::eval::EvalErrorKind::DivisionByZero)
                    && error.triggering_field.as_deref() == Some("q")
                    && error.source_row == Some(3)
        ),
        "the error is the Transform's division failure on row 3: {preview_error:?}"
    );
    let message = preview_error.to_string();

    let full = run_with(
        DIVIDING_CHAIN,
        HashMap::from([csv_reader("src", DIVIDING_ROWS.to_string())]),
        &["rows_out"],
        PipelineRunParams::default(),
        None,
    );
    let full_error = full
        .result
        .expect_err("the full run fails on the zero divisor");
    assert_eq!(message, full_error.to_string());

    let mut sink_records = 0;
    while let Some(batch) = receiver.try_recv_batch() {
        sink_records += batch.metric(crate::telemetry::MetricKey::SinkRecords);
    }
    assert_eq!(
        sink_records, 2,
        "the Sink received the two rows produced before the failing row"
    );
}
