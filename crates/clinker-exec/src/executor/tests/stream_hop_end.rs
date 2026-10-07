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
//! A step fed over a streaming hop finishes its work only on its producer's
//! end, which the producer's driver sends once the producer has finished
//! without failing. A streaming Aggregate whose producer failed or was
//! stopped finalizes no group over the rows it was given. Because each step
//! meets its rows in data order, the run reports the first failure in that
//! order: a step's failure on an earlier row stands over its producer's
//! later failure, and over a later cancellation, and the later failure is
//! logged on the walk's thread.
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
    let memory_test = params.memory_test.with_no_process_memory();
    let params = PipelineRunParams {
        execution_id: "stream-hop-end".to_string(),
        batch_id: "batch-0".to_string(),
        memory_test,
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

/// The read error a scripted reader reports.
const READ_FAILURE: &str = "disk read failed mid-file";

/// The `id` value the Aggregate cannot convert.
const AGGREGATE_BAD_ID: &str = "aggregate_bad";

/// The `v` value the Transform cannot convert.
const TRANSFORM_BAD_V: &str = "transform_bad";

/// What a scripted reader does once it has yielded its `at`-th row.
enum ReadStop {
    /// Fails its next read with [`READ_FAILURE`].
    Fail,
    /// Requests the run's cancellation; the reader then sees it and stops.
    Cancel(crate::pipeline::shutdown::ShutdownToken),
}

/// Rows `1..=rows` of `grp,id,v` in four groups, with `id` a whole number and
/// `v` `1`, except where a failure is scripted: the row whose `id` the
/// Aggregate cannot convert, the row whose `v` the Transform cannot convert,
/// and the row after which the reader stops.
struct ScriptedRows {
    schema: clinker_record::owned_storage::SharedStorage<clinker_record::Schema>,
    rows: usize,
    sent: usize,
    bad_id_at: Option<usize>,
    bad_v_at: Option<usize>,
    stop: Option<(usize, ReadStop)>,
}

impl ScriptedRows {
    fn new(rows: usize) -> Self {
        Self {
            schema: clinker_record::SchemaBuilder::new()
                .with_field("grp")
                .with_field("id")
                .with_field("v")
                .build(),
            rows,
            sent: 0,
            bad_id_at: None,
            bad_v_at: None,
            stop: None,
        }
    }

    /// The Aggregate cannot convert row `row`'s `id`.
    fn bad_id_at(mut self, row: usize) -> Self {
        self.bad_id_at = Some(row);
        self
    }

    /// The Transform cannot convert row `row`'s `v`.
    fn bad_v_at(mut self, row: usize) -> Self {
        self.bad_v_at = Some(row);
        self
    }

    /// The reader fails its read after row `row`.
    fn fail_after(mut self, row: usize) -> Self {
        self.stop = Some((row, ReadStop::Fail));
        self
    }

    /// The reader requests `token` as it yields row `row`.
    fn cancel_at(mut self, row: usize, token: &crate::pipeline::shutdown::ShutdownToken) -> Self {
        self.stop = Some((row, ReadStop::Cancel(token.clone())));
        self
    }
}

impl crate::source::RecordSource for ScriptedRows {
    fn schema(
        &mut self,
    ) -> Result<
        clinker_record::owned_storage::SharedStorage<clinker_record::Schema>,
        clinker_format::FormatError,
    > {
        Ok(self.schema.clone())
    }

    fn next_record(
        &mut self,
    ) -> Result<Option<clinker_record::Record>, clinker_format::FormatError> {
        match &self.stop {
            Some((at, ReadStop::Fail)) if self.sent == *at => {
                return Err(clinker_format::FormatError::Io(std::io::Error::other(
                    READ_FAILURE,
                )));
            }
            Some((at, ReadStop::Cancel(token))) if self.sent + 1 == *at => token.request(),
            _ => {}
        }
        if self.sent == self.rows {
            return Ok(None);
        }
        self.sent += 1;
        let row = self.sent;
        let id = if self.bad_id_at == Some(row) {
            AGGREGATE_BAD_ID.to_string()
        } else {
            row.to_string()
        };
        let v = if self.bad_v_at == Some(row) {
            TRANSFORM_BAD_V
        } else {
            "1"
        };
        Ok(Some(clinker_record::Record::new(
            self.schema.clone(),
            vec![
                clinker_record::Value::from(format!("g{}", row % 4).as_str()),
                clinker_record::Value::from(id.as_str()),
                clinker_record::Value::from(v),
            ],
        )))
    }
}

/// Source -> Transform -> Aggregate -> Sink, the Transform fused with its
/// Source and streaming into the Aggregate's ingest. The Transform fails on
/// a row whose `v` is not a number; the Aggregate, when `summed` is
/// `sum(id.to_int())`, fails on a row whose `id` is not one.
fn fused_chain_into_aggregate(summed: &str) -> String {
    format!(
        r#"
pipeline:
  name: fused_chain_into_aggregate
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
      - {{ name: grp, type: string }}
      - {{ name: id, type: string }}
      - {{ name: v, type: string }}
- type: transform
  name: pass
  input: src
  config:
    cxl: |
      emit grp = grp
      emit id = id
      emit v = v.to_int()
- type: aggregate
  name: totals
  input: pass
  config:
    group_by: [grp]
    cxl: |
      emit grp = grp
      emit n = {summed}
- type: sink
  name: agg_out
  input: totals
  config:
    name: agg_out
    type: csv
    path: agg_out.csv
"#
    )
}

/// The Aggregate sums every row's `id`, so it fails on a row it cannot
/// convert.
const SUM_OF_IDS: &str = "sum(id.to_int())";

/// One run of [`run_scripted`]: its result and outputs, every warning logged
/// on the walk's thread, and how each streaming consumer stopped.
struct ScriptedRun {
    run: Run,
    warnings: Vec<String>,
    ends: Vec<crate::executor::StreamingEnd>,
}

/// Run `yaml` with `src` read by `reader`, writing each of `sinks`, under
/// `token` when given.
fn run_scripted(
    yaml: &str,
    reader: ScriptedRows,
    sinks: &[&'static str],
    token: Option<&crate::pipeline::shutdown::ShutdownToken>,
) -> ScriptedRun {
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::source::SourceInput::Records(Box::new(reader)),
    )]);
    let ends = crate::executor::StreamingEnds::default();
    let params = PipelineRunParams {
        shutdown_token: token.cloned(),
        memory_test: crate::executor::MemoryTestOverrides::default()
            .with_streaming_ends(ends.clone()),
        ..PipelineRunParams::default()
    };
    let (run, warnings) = super::capture_warnings(|| run_with(yaml, readers, sinks, params, None));
    ScriptedRun {
        run,
        warnings,
        ends: ends.ends(),
    }
}

/// The one streaming consumer end `ends` records, for `node`.
fn only_end(ends: &[crate::executor::StreamingEnd], node: &str) -> crate::executor::StreamingEnd {
    let mine: Vec<_> = ends.iter().filter(|end| end.node == node).collect();
    assert_eq!(mine.len(), 1, "{node} streams and stops once: {ends:?}");
    mine[0].clone()
}

/// The run fails with the Aggregate's own conversion error, the first
/// failure in data order, and its output holds nothing.
fn assert_the_aggregates_failure(run: &Run) {
    let error = run
        .result
        .as_ref()
        .expect_err("the Aggregate's failure fails the run");
    assert!(
        error
            .to_string()
            .contains(&format!("cannot convert '{AGGREGATE_BAD_ID}'")),
        "the run reports the Aggregate's own failure, the first in data order: {error}"
    );
    assert_eq!(run.outputs["agg_out"], "", "nothing is written");
}

/// The warnings that name the Aggregate `totals`.
fn lines_naming_totals(warnings: &[String]) -> Vec<&String> {
    warnings
        .iter()
        .filter(|line| line.contains(r#"node="totals""#))
        .collect()
}

/// The Aggregate `totals` failed on its own row, before its input ended.
fn assert_the_aggregate_failed_first(ends: &[crate::executor::StreamingEnd]) {
    assert_eq!(
        only_end(ends, "totals").input,
        crate::executor::StreamingInputEnd::ConsumerFailed,
        "the Aggregate streams and fails on its own row"
    );
}

/// The Aggregate fails on its first row; the reader feeding its producer
/// fails later, past the producer's first batch. The run reports the
/// Aggregate's failure, and the reader's, which came later in data order, is
/// logged once on the walk's thread naming the Aggregate.
#[test]
fn a_streaming_aggregates_earlier_failure_beats_a_later_reader_failure() {
    let scripted = run_scripted(
        &fused_chain_into_aggregate(SUM_OF_IDS),
        ScriptedRows::new(6000).bad_id_at(1).fail_after(3000),
        &["agg_out"],
        None,
    );
    assert_the_aggregates_failure(&scripted.run);
    assert_the_aggregate_failed_first(&scripted.ends);
    let lines = lines_naming_totals(&scripted.warnings);
    assert_eq!(
        lines.len(),
        1,
        "the later failure is logged once: {:?}",
        scripted.warnings
    );
    assert!(
        lines[0].contains(READ_FAILURE),
        "the logged failure is the reader's: {lines:?}"
    );
}

/// The Aggregate fails on its first row; the Transform feeding it fails on
/// row 5,001. The run reports the Aggregate's failure and logs the
/// Transform's.
#[test]
fn a_streaming_aggregates_earlier_failure_beats_its_producers_later_failure() {
    let scripted = run_scripted(
        &fused_chain_into_aggregate(SUM_OF_IDS),
        ScriptedRows::new(6000).bad_id_at(1).bad_v_at(5001),
        &["agg_out"],
        None,
    );
    assert_the_aggregates_failure(&scripted.run);
    assert_the_aggregate_failed_first(&scripted.ends);
    let lines = lines_naming_totals(&scripted.warnings);
    assert_eq!(
        lines.len(),
        1,
        "the later failure is logged once: {:?}",
        scripted.warnings
    );
    assert!(
        lines[0].contains(&format!("cannot convert '{TRANSFORM_BAD_V}'")),
        "the logged failure is the Transform's: {lines:?}"
    );
}

/// The Aggregate fails on row 4,999 and the Transform on row 5,000, both
/// past the Transform's second batch. The Transform hands the Aggregate
/// every row it produced before its failure, so the Aggregate meets its own
/// failing row and the run reports it.
#[test]
fn a_streaming_aggregate_failing_on_the_row_before_its_producer_fails_reports_its_own_error() {
    let scripted = run_scripted(
        &fused_chain_into_aggregate(SUM_OF_IDS),
        ScriptedRows::new(6000).bad_id_at(4999).bad_v_at(5000),
        &["agg_out"],
        None,
    );
    assert_the_aggregates_failure(&scripted.run);
    assert_the_aggregate_failed_first(&scripted.ends);
}

/// The Aggregate fails on its first row; the run is cancelled at row 3,000.
/// The Aggregate's failure stands: the run fails rather than stopping as
/// cancelled, and the cancellation is not logged as a failure.
#[test]
fn a_streaming_aggregates_failure_stands_when_the_run_is_cancelled_after_it() {
    let token = crate::pipeline::shutdown::ShutdownToken::detached();
    let scripted = run_scripted(
        &fused_chain_into_aggregate(SUM_OF_IDS),
        ScriptedRows::new(6000).bad_id_at(1).cancel_at(3000, &token),
        &["agg_out"],
        Some(&token),
    );
    assert!(
        token.is_requested(),
        "the reader requested the cancellation"
    );
    assert_the_aggregates_failure(&scripted.run);
    assert_the_aggregate_failed_first(&scripted.ends);
    assert_eq!(
        lines_naming_totals(&scripted.warnings),
        Vec::<&String>::new(),
        "a cancellation is not a failure to log"
    );
}

/// A streaming Aggregate whose producer fails or is stopped never finalizes
/// a group over the rows it was given: its input closed without its
/// producer's end. Three ways the producer stops: its reader fails, its
/// reader cancels the run, the Transform itself fails.
#[test]
fn a_streaming_aggregate_never_finalizes_without_its_producers_end() {
    let counting = fused_chain_into_aggregate("count(*)");
    let token = crate::pipeline::shutdown::ShutdownToken::detached();
    let cases = [
        (
            "the reader fails",
            ScriptedRows::new(6000).fail_after(3000),
            None,
        ),
        (
            "the reader cancels the run",
            ScriptedRows::new(6000).cancel_at(3000, &token),
            Some(&token),
        ),
        (
            "the Transform fails",
            ScriptedRows::new(6000).bad_v_at(3000),
            None,
        ),
    ];
    for (case, reader, token) in cases {
        let scripted = run_scripted(&counting, reader, &["agg_out"], token);
        assert_eq!(
            only_end(&scripted.ends, "totals"),
            crate::executor::StreamingEnd {
                node: "totals".to_string(),
                input: crate::executor::StreamingInputEnd::Incomplete,
                finished: false,
            },
            "{case}: the Aggregate's input closed without its producer's end, and it \
             finalized nothing"
        );
        assert_eq!(
            scripted.run.outputs["agg_out"], "",
            "{case}: nothing written"
        );
    }
}

/// A complete read through the fused chain into the streaming Aggregate
/// ends the Aggregate's input, and it writes every group's total over every
/// row.
#[test]
fn a_complete_fused_chain_into_a_streaming_aggregate_writes_every_total() {
    let scripted = run_scripted(
        &fused_chain_into_aggregate(SUM_OF_IDS),
        ScriptedRows::new(6000),
        &["agg_out"],
        None,
    );
    scripted
        .run
        .result
        .as_ref()
        .expect("a complete read succeeds");
    assert_eq!(
        only_end(&scripted.ends, "totals"),
        crate::executor::StreamingEnd {
            node: "totals".to_string(),
            input: crate::executor::StreamingInputEnd::Ended,
            finished: true,
        }
    );
    let mut lines: Vec<&str> = scripted.run.outputs["agg_out"].lines().collect();
    lines.sort_unstable();
    // Group g is every row r <= 6000 with r % 4 == g: 1,500 rows each.
    assert_eq!(
        lines,
        vec![
            "g0,4503000",
            "g1,4498500",
            "g2,4500000",
            "g3,4501500",
            "grp,n",
        ]
    );
}

/// The full-run counterpart of the preview delivery test: the Transform runs
/// fused with its Source and fails on row 3. Its Sink receives the two rows
/// produced before the failure, as the preview's does.
#[test]
fn a_failing_fused_transform_delivers_its_rows_before_its_failure_to_its_sink() {
    let (producer, receiver) = telemetry_arena();
    let full = run_with(
        DIVIDING_CHAIN,
        HashMap::from([csv_reader("src", DIVIDING_ROWS.to_string())]),
        &["rows_out"],
        PipelineRunParams {
            telemetry_producer: Some(producer),
            ..PipelineRunParams::default()
        },
        None,
    );
    full.result
        .expect_err("the full run fails on the zero divisor");
    let mut sink_records = 0;
    while let Some(batch) = receiver.try_recv_batch() {
        sink_records += batch.metric(crate::telemetry::MetricKey::SinkRecords);
    }
    assert_eq!(
        sink_records, 2,
        "the Sink received the two rows produced before the failing row"
    );
}
