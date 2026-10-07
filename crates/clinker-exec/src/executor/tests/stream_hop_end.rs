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
//! stopped finalizes no group over the rows it was given, a streaming
//! Combine completes no probe, and a streaming Sink leaves its output
//! unclosed and records a failure or an interruption, never a completion.
//! Because each step meets its rows in data order, the run reports the first
//! failure in that order: a step's failure on an earlier row stands over its
//! producer's later failure, and over a later cancellation, and the later
//! failure is logged on the walk's thread.
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
    run_scripted_with(yaml, reader, Vec::new(), sinks, token, None)
}

/// [`run_scripted`] with every Source in `csv_sources` read from its CSV
/// text, reporting the run's telemetry to `telemetry` when given.
fn run_scripted_with(
    yaml: &str,
    reader: ScriptedRows,
    csv_sources: Vec<(&str, &str)>,
    sinks: &[&'static str],
    token: Option<&crate::pipeline::shutdown::ShutdownToken>,
    telemetry: Option<crate::telemetry::TelemetryProducer>,
) -> ScriptedRun {
    let mut readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::source::SourceInput::Records(Box::new(reader)),
    )]);
    readers.extend(
        csv_sources
            .into_iter()
            .map(|(name, csv)| csv_reader(name, csv.to_string())),
    );
    let ends = crate::executor::StreamingEnds::default();
    let params = PipelineRunParams {
        shutdown_token: token.cloned(),
        telemetry_producer: telemetry,
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

/// The build side of [`fused_chain_into_combine`]: one product per group.
const PRODUCTS: &str = "grp,name\ng0,p0\ng1,p1\ng2,p2\ng3,p3\n";

/// Source -> Transform -> Combine(driver) -> Sink, with `products` as the
/// Combine's build side. The Transform is fused with its Source and streams
/// into the Combine's probe; it fails on a row whose `v` is not a number.
/// `body` is the Combine's CXL, run on every matched driver row inside the
/// probe.
fn fused_chain_into_combine(body: &str) -> String {
    format!(
        r#"
pipeline:
  name: fused_chain_into_combine
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
- type: source
  name: products
  config:
    name: products
    type: csv
    path: products.csv
    schema:
      - {{ name: grp, type: string }}
      - {{ name: name, type: string }}
- type: transform
  name: pass
  input: src
  config:
    cxl: |
      emit grp = grp
      emit id = id
      emit v = v.to_int()
- type: combine
  name: joined
  input:
    pass: pass
    products: products
  config:
    where: "pass.grp == products.grp"
    match: first
    on_miss: skip
    drive: pass
    cxl: |
{body}
    propagate_ck: driver
- type: sink
  name: joined_out
  input: joined
  config:
    name: joined_out
    type: csv
    path: joined_out.csv
"#
    )
}

/// The Combine's body converts every driver row's `id`, so under
/// `fail_fast` the probe fails on the first driver row whose `id` is not a
/// number, inside its probe loop.
const CONVERTING_BODY: &str = "      emit id = pass.id\n      emit n = pass.id.to_int()";

/// The Combine's body only copies columns, so it never fails.
const COPYING_BODY: &str = "      emit id = pass.id\n      emit name = products.name";

/// Run [`fused_chain_into_combine`] with `body` over `reader`.
fn run_combine(
    body: &str,
    reader: ScriptedRows,
    token: Option<&crate::pipeline::shutdown::ShutdownToken>,
) -> ScriptedRun {
    run_scripted_with(
        &fused_chain_into_combine(body),
        reader,
        vec![("products", PRODUCTS)],
        &["joined_out"],
        token,
        None,
    )
}

/// The run fails with the Combine's own conversion error, the first failure
/// in data order; the Combine streamed and failed before its input ended,
/// and nothing is written.
fn assert_the_probes_failure(scripted: &ScriptedRun) {
    let error = scripted
        .run
        .result
        .as_ref()
        .expect_err("the probe's failure fails the run");
    assert!(
        error
            .to_string()
            .contains(&format!("cannot convert '{AGGREGATE_BAD_ID}'")),
        "the run reports the probe's own failure, the first in data order: {error}"
    );
    assert_eq!(
        only_end(&scripted.ends, "joined").input,
        crate::executor::StreamingInputEnd::ConsumerFailed,
        "the Combine probes its streamed driver and fails on its own row"
    );
    assert_eq!(scripted.run.outputs["joined_out"], "", "nothing is written");
}

/// The warnings that name the Combine `joined`.
fn lines_naming_joined(warnings: &[String]) -> Vec<&String> {
    warnings
        .iter()
        .filter(|line| line.contains(r#"node="joined""#))
        .collect()
}

/// The Combine's probe fails on its first driver row; the reader feeding
/// the driver fails later, past the driver's first batch. The run reports
/// the probe's failure and logs the reader's once, naming the Combine.
#[test]
fn a_streaming_probes_earlier_failure_beats_a_later_reader_failure() {
    let scripted = run_combine(
        CONVERTING_BODY,
        ScriptedRows::new(6000).bad_id_at(1).fail_after(3000),
        None,
    );
    assert_the_probes_failure(&scripted);
    let lines = lines_naming_joined(&scripted.warnings);
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

/// The Combine's probe fails on its first driver row; the Transform driving
/// it fails on row 5,001. The run reports the probe's failure and logs the
/// Transform's.
#[test]
fn a_streaming_probes_earlier_failure_beats_its_producers_later_failure() {
    let scripted = run_combine(
        CONVERTING_BODY,
        ScriptedRows::new(6000).bad_id_at(1).bad_v_at(5001),
        None,
    );
    assert_the_probes_failure(&scripted);
    let lines = lines_naming_joined(&scripted.warnings);
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

/// The Combine's probe fails on its first driver row; the run is cancelled
/// at row 3,000. The probe's failure stands, and the cancellation is not
/// logged as a failure.
#[test]
fn a_streaming_probes_failure_stands_when_the_run_is_cancelled_after_it() {
    let token = crate::pipeline::shutdown::ShutdownToken::detached();
    let scripted = run_combine(
        CONVERTING_BODY,
        ScriptedRows::new(6000).bad_id_at(1).cancel_at(3000, &token),
        Some(&token),
    );
    assert!(
        token.is_requested(),
        "the reader requested the cancellation"
    );
    assert_the_probes_failure(&scripted);
    assert_eq!(
        lines_naming_joined(&scripted.warnings),
        Vec::<&String>::new(),
        "a cancellation is not a failure to log"
    );
}

/// A streaming Combine whose driver fails or is stopped never completes its
/// probe over the driver rows it was given. Three ways the driver stops: its
/// reader fails, its reader cancels the run, the Transform itself fails.
#[test]
fn a_streaming_probe_never_completes_without_its_producers_end() {
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
        let scripted = run_combine(COPYING_BODY, reader, token);
        assert_eq!(
            only_end(&scripted.ends, "joined"),
            crate::executor::StreamingEnd {
                node: "joined".to_string(),
                input: crate::executor::StreamingInputEnd::Incomplete,
                finished: false,
            },
            "{case}: the probe's input closed without its driver's end, and it \
             completed nothing"
        );
        assert_eq!(
            scripted.run.outputs["joined_out"], "",
            "{case}: nothing written"
        );
    }
}

/// A complete read through the fused chain into the streaming Combine ends
/// the probe's input, completes the probe and joins every driver row.
#[test]
fn a_complete_fused_chain_into_a_streaming_probe_joins_every_row() {
    let scripted = run_combine(COPYING_BODY, ScriptedRows::new(6000), None);
    scripted
        .run
        .result
        .as_ref()
        .expect("a complete read succeeds");
    let output = &scripted.run.outputs["joined_out"];
    let mut lines: Vec<&str> = output.lines().skip(1).collect();
    lines.sort_unstable();
    let mut expected: Vec<String> = (1..=6000)
        .map(|row| format!("{row},p{}", row % 4))
        .collect();
    expected.sort_unstable();
    assert_eq!(output.lines().next(), Some("id,name"));
    assert_eq!(lines, expected);
    assert_eq!(
        only_end(&scripted.ends, "joined"),
        crate::executor::StreamingEnd {
            node: "joined".to_string(),
            input: crate::executor::StreamingInputEnd::Ended,
            finished: true,
        }
    );
}

/// Source -> Transform -> JSON Sink: the Transform is fused with its Source
/// and streams into the Sink's writer thread.
const FUSED_CHAIN_INTO_JSON: &str = r#"
pipeline:
  name: fused_chain_into_json
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
      - { name: grp, type: string }
      - { name: id, type: string }
      - { name: v, type: string }
- type: transform
  name: pass
  input: src
  config:
    cxl: |
      emit id = id
- type: sink
  name: rows_out
  input: pass
  config:
    name: rows_out
    type: json
    path: rows_out.json
"#;

/// The rows the Sinks `receiver` reports on wrote, and how many of them
/// ended with each lifecycle outcome, as
/// `(records, (completed, failed, interrupted))`.
fn sink_signals(receiver: &crate::telemetry::TelemetryReceiver) -> (u64, (u64, u64, u64)) {
    let (mut records, mut completed, mut failed, mut interrupted) = (0, 0, 0, 0);
    while let Some(batch) = receiver.try_recv_batch() {
        records += batch.metric(crate::telemetry::MetricKey::SinkRecords);
        completed += batch.metric(crate::telemetry::MetricKey::SinkCompleted);
        failed += batch.metric(crate::telemetry::MetricKey::SinkFailed);
        interrupted += batch.metric(crate::telemetry::MetricKey::SinkInterrupted);
    }
    (records, (completed, failed, interrupted))
}

/// Run [`FUSED_CHAIN_INTO_JSON`] over `reader` with the run's telemetry kept,
/// asserting the Sink's JSON document was not closed, and return the run
/// with the Sink's signals.
fn run_unclosed_json(
    case: &str,
    reader: ScriptedRows,
    token: Option<&crate::pipeline::shutdown::ShutdownToken>,
) -> (ScriptedRun, (u64, (u64, u64, u64))) {
    let (producer, receiver) = telemetry_arena();
    let scripted = run_scripted_with(
        FUSED_CHAIN_INTO_JSON,
        reader,
        Vec::new(),
        &["rows_out"],
        token,
        Some(producer),
    );
    let written = &scripted.run.outputs["rows_out"];
    assert!(
        !written.trim_end().ends_with(']'),
        "{case}: the document is not closed: ...{:?}",
        &written[written.len().saturating_sub(40)..]
    );
    let signals = sink_signals(&receiver);
    (scripted, signals)
}

/// A streaming Sink whose producer fails or is stopped never closes its
/// output: the JSON document it was writing gets no closing bracket, and its
/// lifecycle records a failure, or an interruption when the run was
/// cancelled, never a completion.
#[test]
fn a_streaming_sink_never_closes_its_output_without_its_producers_end() {
    let case = "the reader fails";
    let (scripted, (records, outcomes)) =
        run_unclosed_json(case, ScriptedRows::new(6000).fail_after(3000), None);
    assert_eq!(
        records, 3000,
        "{case}: the Sink wrote every row its producer handed it before failing"
    );
    assert_eq!(
        outcomes,
        (0, 1, 0),
        "{case}: (completed, failed, interrupted)"
    );
    assert_eq!(
        only_end(&scripted.ends, "rows_out"),
        crate::executor::StreamingEnd {
            node: "rows_out".to_string(),
            input: crate::executor::StreamingInputEnd::Incomplete,
            finished: false,
        },
        "{case}: the Sink's input closed without its producer's end"
    );

    // Once the run is cancelled the Sink's writer may also refuse to start,
    // depending on whether the cancellation came before the Sink's first row:
    // either way the Sink does not close its output.
    let case = "the reader cancels the run";
    let token = crate::pipeline::shutdown::ShutdownToken::detached();
    let (scripted, (_, outcomes)) = run_unclosed_json(
        case,
        ScriptedRows::new(6000).cancel_at(3000, &token),
        Some(&token),
    );
    assert_eq!(
        outcomes,
        (0, 0, 1),
        "{case}: (completed, failed, interrupted)"
    );
    assert!(
        !only_end(&scripted.ends, "rows_out").finished,
        "{case}: the Sink did not close its output"
    );
}

/// A complete run ends its streaming Sink's input once the producer's turn
/// returns, and the Sink closes its document and completes.
#[test]
fn a_completed_walk_ends_every_streaming_sink() {
    let (producer, receiver) = telemetry_arena();
    let scripted = run_scripted_with(
        FUSED_CHAIN_INTO_JSON,
        ScriptedRows::new(6000),
        Vec::new(),
        &["rows_out"],
        None,
        Some(producer),
    );
    scripted
        .run
        .result
        .as_ref()
        .expect("a complete read succeeds");
    let written = &scripted.run.outputs["rows_out"];
    assert!(written.starts_with('['), "{written:?}");
    assert!(written.trim_end().ends_with(']'), "the document is closed");
    assert_eq!(
        written.matches(r#""id":"#).count(),
        6000,
        "every row is written"
    );
    assert_eq!(sink_signals(&receiver), (6000, (1, 0, 0)));
    assert_eq!(
        only_end(&scripted.ends, "rows_out"),
        crate::executor::StreamingEnd {
            node: "rows_out".to_string(),
            input: crate::executor::StreamingInputEnd::Ended,
            finished: true,
        }
    );
}

/// Two Sources -> Merge (interleave) -> Aggregate -> Sink: the fused
/// interleave streams into the Aggregate's ingest. `other` is empty, so the
/// interleave hands on `src`'s rows in their order.
fn interleave_into_aggregate() -> String {
    pipeline(
        "interleave_into_aggregate",
        &[r#"
- type: source
  name: src
  config:
    name: src
    type: csv
    path: src.csv
    schema:
      - { name: grp, type: string }
      - { name: id, type: string }
      - { name: v, type: string }
- type: source
  name: other
  config:
    name: other
    type: csv
    path: other.csv
    schema:
      - { name: grp, type: string }
      - { name: id, type: string }
      - { name: v, type: string }
- type: merge
  name: merged
  inputs: [src, other]
  config:
    mode: interleave
- type: aggregate
  name: totals
  input: merged
  config:
    group_by: [grp]
    cxl: |
      emit grp = grp
      emit n = sum(id.to_int())
- type: sink
  name: agg_out
  input: totals
  config:
    name: agg_out
    type: csv
    path: agg_out.csv
"#],
    )
}

/// The Aggregate fails on row 4,999 and `src`'s reader fails right after
/// it, both past the interleave's second batch. The interleave hands the
/// Aggregate every row it took before the reader's failure, so the
/// Aggregate meets its own failing row and the run reports it.
#[test]
fn an_interleave_feeding_a_streaming_aggregate_delivers_its_rows_before_a_read_failure() {
    let scripted = run_scripted_with(
        &interleave_into_aggregate(),
        ScriptedRows::new(6000).bad_id_at(4999).fail_after(4999),
        vec![("other", "grp,id,v\n")],
        &["agg_out"],
        None,
        None,
    );
    assert_the_aggregates_failure(&scripted.run);
    assert_the_aggregate_failed_first(&scripted.ends);
}
