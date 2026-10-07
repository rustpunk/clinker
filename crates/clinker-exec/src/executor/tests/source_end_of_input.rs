//! A Source's input is complete only when its reader says so.
//!
//! A reader that stops on an error (an I/O or parse failure, a memory
//! refusal, a panic) has delivered a prefix of its input, not its input. The
//! run fails with the reader's own error at that point, and no step after
//! the Source finishes on the prefix: an Aggregate never emits totals over
//! part of a file, and a Sink never receives them. Each walk path that reads
//! a Source's channel is covered: the Source arm, the fused Transform and the
//! fused `Merge.interleave`.
//!
//! A read the run's cancellation cuts off is a prefix too. The run stops as
//! an interruption rather than a failure, and no step finishes on the rows
//! read before the cut-off.
//!
//! The writers are raw in-memory buffers, so anything a step finished is
//! visible here even though a production run would leave it staged and
//! unpublished.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

const READ_FAILURE: &str = "disk read failed mid-file";

/// `rows` CSV rows in four groups, each id about 100 bytes.
fn csv(rows: usize) -> Vec<u8> {
    let mut csv = String::from("grp,id\n");
    for i in 0..rows {
        csv.push_str(&format!("g{},id_{i:0100}\n", i % 4));
    }
    csv.into_bytes()
}

/// Serves the first `left` bytes, then fails every read.
struct FailAfter {
    inner: std::io::Cursor<Vec<u8>>,
    left: usize,
}

impl std::io::Read for FailAfter {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.left == 0 {
            return Err(std::io::Error::other(READ_FAILURE));
        }
        let n = buf.len().min(self.left);
        let got = self.inner.read(&mut buf[..n])?;
        self.left -= got;
        Ok(got)
    }
}

/// Serves the first `left` bytes, then panics.
struct PanicAfter {
    inner: std::io::Cursor<Vec<u8>>,
    left: usize,
}

impl std::io::Read for PanicAfter {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        assert!(self.left > 0, "the Source's reader panicked mid-file");
        let n = buf.len().min(self.left);
        let got = self.inner.read(&mut buf[..n])?;
        self.left -= got;
        Ok(got)
    }
}

/// Half of an 8,000-row file, then a read error.
fn failing_half() -> Box<dyn std::io::Read + Send> {
    let bytes = csv(8000);
    let left = bytes.len() / 2;
    Box::new(FailAfter {
        inner: std::io::Cursor::new(bytes),
        left,
    })
}

fn whole(rows: usize) -> Box<dyn std::io::Read + Send> {
    Box::new(std::io::Cursor::new(csv(rows)))
}

struct Run {
    result: Result<ExecutionReport, PipelineError>,
    outputs: HashMap<&'static str, String>,
}

/// Run `yaml` over `sources`, writing each of `sinks` to an in-memory
/// buffer, reading no process memory and, when given, holding the ledger to
/// `capacity`.
fn run(
    yaml: &str,
    sources: Vec<(&str, Box<dyn std::io::Read + Send>)>,
    sinks: &[&'static str],
    capacity: Option<u64>,
) -> Run {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let readers: crate::executor::SourceReaders = sources
        .into_iter()
        .map(|(name, reader)| {
            (
                name.to_string(),
                crate::executor::single_file_reader(format!("{name}.csv"), reader),
            )
        })
        .collect();
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
    let mut memory_test = crate::executor::MemoryTestOverrides::default().with_no_process_memory();
    if let Some(capacity) = capacity {
        memory_test = memory_test.with_ledger_capacity(capacity);
    }
    let params = PipelineRunParams {
        execution_id: "source-end-of-input".to_string(),
        batch_id: "batch-0".to_string(),
        memory_test,
        ..Default::default()
    };
    let result =
        PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params);
    Run {
        result,
        outputs: buffers
            .into_iter()
            .map(|(sink, buffer)| (sink, buffer.as_string()))
            .collect(),
    }
}

/// The E310 a refused reader of `src` reports: its requester is the Source,
/// for the rows it read.
fn assert_source_refusal(error: &PipelineError) {
    let PipelineError::MemoryBudgetExceeded { report } = error else {
        panic!("the reader's refusal must fail the run with its E310; got {error:?}");
    };
    assert_eq!(
        report.requester,
        Some(clinker_plan::runtime_error::ConsumerLabel {
            node: "src".to_string(),
            surface: clinker_plan::runtime_error::MemorySurface::RowsRead,
        }),
        "the refusal is the Source's own, for the rows it read: {report:?}"
    );
}

const SOURCE: &str = r#"
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

const TOTALS_SINK: &str = r#"
- type: sink
  name: agg_out
  input: totals
  config:
    name: agg_out
    type: csv
    path: agg_out.csv
"#;

fn pipeline(name: &str, nodes: &[&str]) -> String {
    format!("pipeline:\n  name: {name}\nnodes:{}", nodes.concat())
}

/// Source -> Aggregate -> Sink: the Source arm reads the channel.
fn source_aggregate() -> String {
    pipeline(
        "source_aggregate",
        &[
            SOURCE,
            r#"
- type: aggregate
  name: totals
  input: src
  config:
    group_by: [grp]
    cxl: |
      emit grp = grp
      emit n = count(*)
"#,
            TOTALS_SINK,
        ],
    )
}

/// Source -> Transform -> Aggregate -> Sink: the Transform is fused with
/// its only Source and reads the channel itself.
fn fused_transform_aggregate() -> String {
    pipeline(
        "fused_transform_aggregate",
        &[
            SOURCE,
            r#"
- type: transform
  name: pass
  input: src
  config:
    cxl: |
      emit grp = grp
- type: aggregate
  name: totals
  input: pass
  config:
    group_by: [grp]
    cxl: |
      emit grp = grp
      emit n = count(*)
"#,
            TOTALS_SINK,
        ],
    )
}

/// Two Sources -> Merge (interleave) -> Aggregate -> Sink: the fused
/// interleave selects over both channels.
fn interleave_aggregate() -> String {
    pipeline(
        "interleave_aggregate",
        &[
            SOURCE,
            r#"
- type: source
  name: other
  config:
    name: other
    type: csv
    path: other.csv
    schema:
      - { name: grp, type: string }
      - { name: id, type: string }
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
      emit n = count(*)
"#,
            TOTALS_SINK,
        ],
    )
}

/// The Source fans out to an Aggregate and a Transform, each to a Sink.
fn fan_out() -> String {
    pipeline(
        "fan_out",
        &[
            SOURCE,
            r#"
- type: aggregate
  name: totals
  input: src
  config:
    group_by: [grp]
    cxl: |
      emit grp = grp
      emit n = count(*)
"#,
            TOTALS_SINK,
            r#"
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
    type: csv
    path: rows_out.csv
"#,
        ],
    )
}

fn assert_read_failure(run: &Run) {
    let error = run
        .result
        .as_ref()
        .expect_err("a reader that fails mid-file fails the run");
    assert!(
        error.to_string().contains(READ_FAILURE),
        "the run reports the reader's own error: {error}"
    );
    assert_eq!(
        run.outputs["agg_out"], "",
        "no total is finished over the rows read before the failure"
    );
}

#[test]
fn an_io_error_mid_file_fails_the_run_before_an_aggregate_finishes() {
    let run = run(
        &source_aggregate(),
        vec![("src", failing_half())],
        &["agg_out"],
        None,
    );
    assert_read_failure(&run);
}

#[test]
fn an_io_error_read_by_a_fused_transform_fails_the_run_before_an_aggregate_finishes() {
    let run = run(
        &fused_transform_aggregate(),
        vec![("src", failing_half())],
        &["agg_out"],
        None,
    );
    assert_read_failure(&run);
}

#[test]
fn an_io_error_read_by_an_interleave_fails_the_run_before_an_aggregate_finishes() {
    let run = run(
        &interleave_aggregate(),
        vec![("src", failing_half()), ("other", whole(100))],
        &["agg_out"],
        None,
    );
    assert_read_failure(&run);
}

/// The reader is refused by the memory limit part-way through its file. The
/// run fails with the reader's E310, and the Aggregate after the fused
/// Transform never finishes on the rows read before the refusal.
#[test]
fn a_refused_reader_fails_with_its_own_e310_before_an_aggregate_finishes() {
    let run = run(
        &fused_transform_aggregate(),
        vec![("src", whole(8000))],
        &["agg_out"],
        Some(256 * 1024),
    );
    let error = run
        .result
        .as_ref()
        .expect_err("8,000 rows of about 105 bytes cannot be read within 256 KiB");
    assert_source_refusal(error);
    assert_eq!(
        run.outputs["agg_out"], "",
        "no total is finished over the rows read before the refusal"
    );
}

/// With the Source fanned out, the walk buffers its rows for both readers.
/// The reader is refused first, so its refusal is the run's error, not a
/// later step's refusal whose figures come from the rows the reader managed
/// to deliver.
#[test]
fn a_refused_fanned_out_reader_reports_its_own_e310() {
    let run = run(
        &fan_out(),
        vec![("src", whole(8000))],
        &["agg_out", "rows_out"],
        Some(256 * 1024),
    );
    let error = run
        .result
        .as_ref()
        .expect_err("8,000 rows of about 105 bytes cannot be read within 256 KiB");
    assert_source_refusal(error);
    assert_eq!(run.outputs["agg_out"], "");
    assert_eq!(run.outputs["rows_out"], "");
}

/// A reader that panics has not reached the end of its input: the run fails
/// naming the Source, and nothing after it finishes.
#[test]
fn a_reader_panic_fails_the_run_naming_the_source() {
    let bytes = csv(8000);
    let left = bytes.len() / 2;
    let run = run(
        &source_aggregate(),
        vec![(
            "src",
            Box::new(PanicAfter {
                inner: std::io::Cursor::new(bytes),
                left,
            }),
        )],
        &["agg_out"],
        None,
    );
    let error = run
        .result
        .as_ref()
        .expect_err("a reader that panics fails the run");
    assert_eq!(
        run.outputs["agg_out"], "",
        "no total is finished over the rows read before the panic"
    );
    assert!(
        matches!(error, PipelineError::Internal { node, .. } if node == "src"),
        "the failure names the Source whose reader stopped: {error:?}"
    );
}

/// A reader that reads its whole input still ends normally on every path,
/// and its rows reach the Sink unchanged and in order.
#[test]
fn a_complete_read_ends_normally_with_every_row() {
    let rows = 3000;
    let expected = String::from_utf8(csv(rows)).unwrap();

    let passthrough = run(
        &pipeline(
            "passthrough",
            &[
                SOURCE,
                r#"
- type: sink
  name: rows_out
  input: src
  config:
    name: rows_out
    type: csv
    path: rows_out.csv
"#,
            ],
        ),
        vec![("src", whole(rows))],
        &["rows_out"],
        None,
    );
    passthrough
        .result
        .as_ref()
        .expect("a complete read succeeds");
    assert_eq!(passthrough.outputs["rows_out"], expected);

    let fused = run(
        &pipeline(
            "fused_passthrough",
            &[
                SOURCE,
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
        ),
        vec![("src", whole(rows))],
        &["rows_out"],
        None,
    );
    fused.result.as_ref().expect("a complete read succeeds");
    assert_eq!(fused.outputs["rows_out"], expected);

    for (name, yaml, sources) in [
        ("source arm", source_aggregate(), vec![("src", whole(rows))]),
        (
            "fused transform",
            fused_transform_aggregate(),
            vec![("src", whole(rows))],
        ),
        (
            "interleave",
            interleave_aggregate(),
            vec![("src", whole(rows)), ("other", whole(rows))],
        ),
    ] {
        let per_source = if name == "interleave" { 2 } else { 1 };
        let run = run(&yaml, sources, &["agg_out"], None);
        let report = run.result.as_ref().expect("a complete read succeeds");
        assert_eq!(
            report.counters.total_count,
            (rows * per_source) as u64,
            "{name}: every row was read"
        );
        // Each Source's file is its own document, and the Aggregate emits
        // its totals per document.
        let mut lines: Vec<String> = run.outputs["agg_out"].lines().map(String::from).collect();
        lines.sort_unstable();
        let mut expected: Vec<String> = (0..4)
            .flat_map(|group| (0..per_source).map(move |_| format!("g{group},{}", rows / 4)))
            .collect();
        expected.push("grp,n".to_string());
        assert_eq!(lines, expected, "{name}: the totals cover the whole input");
    }
}

const INTERRUPTED_ROWS: usize = 10;

/// Row `i` of an interrupted read, in four groups like [`csv`].
fn interrupted_row(
    schema: &clinker_record::owned_storage::SharedStorage<clinker_record::Schema>,
    i: usize,
) -> clinker_record::Record {
    clinker_record::Record::new(
        schema.clone(),
        vec![
            clinker_record::Value::from(format!("g{}", i % 4).as_str()),
            clinker_record::Value::from(format!("id_{i}").as_str()),
        ],
    )
}

fn interrupted_schema() -> clinker_record::owned_storage::SharedStorage<clinker_record::Schema> {
    clinker_record::SchemaBuilder::new()
        .with_field("grp")
        .with_field("id")
        .build()
}

/// [`INTERRUPTED_ROWS`] rows, then the reader reports that its read was
/// cancelled. Nothing else tells the walk: the run's shutdown token is never
/// requested, so the walk's own polls never see the cancellation.
struct CancelledRead {
    schema: clinker_record::owned_storage::SharedStorage<clinker_record::Schema>,
    sent: usize,
}

impl crate::source::RecordSource for CancelledRead {
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
        if self.sent < INTERRUPTED_ROWS {
            self.sent += 1;
            return Ok(Some(interrupted_row(&self.schema, self.sent - 1)));
        }
        Err(clinker_format::FormatError::Interrupted)
    }
}

/// [`INTERRUPTED_ROWS`] rows, then, once the walk has taken every one of
/// them, the run's cancellation and the end of the reader's input. Fewer rows
/// than the walk's own shutdown poll interval, so only the reader can tell
/// the walk that the read was cut off.
struct SignalledRead {
    schema: clinker_record::owned_storage::SharedStorage<clinker_record::Schema>,
    sent: usize,
    progress: crate::progress::RunProgress,
    shutdown: crate::pipeline::shutdown::ShutdownToken,
}

impl crate::source::RecordSource for SignalledRead {
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
        if self.sent < INTERRUPTED_ROWS {
            self.sent += 1;
            return Ok(Some(interrupted_row(&self.schema, self.sent - 1)));
        }
        // The Source arm publishes its count before it waits on an empty
        // channel, so a count of every row means the walk is inside the
        // Source's read, past the shutdown poll before each step.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while self.progress.sample().records_read < INTERRUPTED_ROWS as u64 {
            assert!(
                std::time::Instant::now() < deadline,
                "the walk never took the rows read before the interruption"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        self.shutdown.request();
        Ok(None)
    }
}

/// Run `yaml` with `src` read by `reader`, its one Sink `agg_out` writing
/// to an in-memory buffer.
fn run_interrupted(
    yaml: &str,
    reader: Box<dyn crate::source::RecordSource>,
    shutdown: &crate::pipeline::shutdown::ShutdownToken,
    progress: &crate::progress::RunProgress,
) -> (ExecutionReport, String) {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::source::SourceInput::Records(reader),
    )]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "agg_out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "source-end-of-input".to_string(),
        batch_id: "batch-0".to_string(),
        shutdown_token: Some(shutdown.clone()),
        progress: Some(progress.clone()),
        memory_test: crate::executor::MemoryTestOverrides::default().with_no_process_memory(),
        ..Default::default()
    };
    let report =
        PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params)
            .expect("an interrupted run stops gracefully");
    (report, output.as_string())
}

/// The reader of a Source feeding a streaming Aggregate (through the
/// Transform fused with it) reports that its read was cancelled. The read
/// ends as interrupted, not as complete: the Aggregate never finishes on the
/// rows read before the cut-off, and the run reports the interruption.
#[test]
fn an_interrupted_read_stops_a_streaming_aggregate_before_it_finishes() {
    let shutdown = crate::pipeline::shutdown::ShutdownToken::detached();
    let progress = crate::progress::RunProgress::new();
    let (report, output) = run_interrupted(
        &fused_transform_aggregate(),
        Box::new(CancelledRead {
            schema: interrupted_schema(),
            sent: 0,
        }),
        &shutdown,
        &progress,
    );
    assert!(report.interrupted, "the run reports the interruption");
    assert_eq!(
        output, "",
        "no total is finished over the rows read before the interruption"
    );
    assert_eq!(
        report.per_source_record_counts.get("src"),
        None,
        "an interrupted read never finalizes the Source's count"
    );
}

/// The run is cancelled while the walk is reading a Source. The read ends as
/// interrupted, not as complete: the Source's count is never finalized and
/// nothing reaches the Sink.
#[test]
fn a_read_cut_off_by_a_cancelled_run_never_finalizes_the_sources_count() {
    let shutdown = crate::pipeline::shutdown::ShutdownToken::detached();
    let progress = crate::progress::RunProgress::new();
    let (report, output) = run_interrupted(
        &source_aggregate(),
        Box::new(SignalledRead {
            schema: interrupted_schema(),
            sent: 0,
            progress: progress.clone(),
            shutdown: shutdown.clone(),
        }),
        &shutdown,
        &progress,
    );
    assert!(shutdown.is_requested());
    assert!(report.interrupted, "the run reports the interruption");
    assert_eq!(
        progress.sample().records_read,
        INTERRUPTED_ROWS as u64,
        "every row the reader sent was read"
    );
    assert_eq!(
        report.per_source_record_counts.get("src"),
        None,
        "an interrupted read never finalizes the Source's count"
    );
    assert_eq!(output, "", "no total reaches the Sink");
}
