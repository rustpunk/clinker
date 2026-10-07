//! The run reports the first failure the walk met, in the order of its turns.
//!
//! A streaming Sink runs on its own thread, fed by its producer during the
//! producer's turn. The walk settles the Sink at the end of that turn, so a
//! Sink that failed on a row its producer emitted before the producer's own
//! failure is the run's error, and the producer's later failure is logged on
//! the walk's thread. A failure in an earlier turn stands over a failure in a
//! later one, and every failure the run does not report is logged with the
//! step it belongs to. Sinks that fail while the walk completes are reported
//! together, in turn order.
//!
//! The writers are in-memory: a writer that refuses every write stands in for
//! a full disk, so a Sink fails on the first bytes it hands its writer.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

/// The error a [`RefusingWriter`] returns.
const SINK_WRITE_FAILURE: &str = "the sink's disk is full";

/// The `v` value the Transform cannot convert.
const TRANSFORM_BAD_V: &str = "transform_bad";

/// A writer whose every write fails, as on a full disk.
struct RefusingWriter;

impl std::io::Write for RefusingWriter {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other(SINK_WRITE_FAILURE))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// What one Sink writes to.
enum SinkWriter {
    /// Every write fails.
    Refusing,
    /// The bytes land in this buffer; clones share it.
    Buffer(SharedBuffer),
}

impl SinkWriter {
    fn boxed(&self) -> Box<dyn std::io::Write + Send> {
        match self {
            Self::Refusing => Box::new(RefusingWriter),
            Self::Buffer(buffer) => Box::new(buffer.clone()),
        }
    }
}

/// One run: its result, every warning logged on the walk's thread, and how
/// each streaming consumer stopped.
struct Run {
    result: Result<ExecutionReport, PipelineError>,
    warnings: Vec<String>,
    ends: Vec<crate::executor::StreamingEnd>,
}

/// Run `yaml` over `readers` with each Sink in `sinks` writing to its
/// writer, under `token` when given, as a bounded preview of `preview` rows
/// per Source when given.
fn run(
    yaml: &str,
    readers: crate::executor::SourceReaders,
    sinks: &[(&str, &SinkWriter)],
    token: Option<&crate::pipeline::shutdown::ShutdownToken>,
    preview: Option<u64>,
) -> Run {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = sinks
        .iter()
        .map(|(sink, writer)| (sink.to_string(), writer.boxed()))
        .collect();
    let ends = crate::executor::StreamingEnds::default();
    let params = PipelineRunParams {
        execution_id: "walk-failure-order".to_string(),
        batch_id: "batch-0".to_string(),
        shutdown_token: token.cloned(),
        memory_test: crate::executor::MemoryTestOverrides::default()
            .with_streaming_ends(ends.clone())
            .with_no_process_memory(),
        ..PipelineRunParams::default()
    };
    let (result, warnings) = super::capture_warnings(|| match preview {
        // A preview never publishes, so its writers are not committed.
        Some(rows) => PipelineExecutor::run_with_readers_writers_in_context_and_activation(
            &config,
            readers,
            WriterRegistry {
                auto_commit_staged: false,
                ..WriterRegistry::from(writers)
            },
            &params,
            RunPolicy::new(
                std::num::NonZeroUsize::MIN,
                PreviewPolicy::RecordsPerSource(
                    std::num::NonZeroU64::new(rows).expect("a preview reads rows"),
                ),
            ),
            clinker_plan::config::CompileContext::default(),
            None,
        ),
        None => {
            PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params)
        }
    });
    Run {
        result,
        warnings,
        ends: ends.ends(),
    }
}

/// Rows `1..=rows` of `grp,id,v`, with `v` `1` except on the row the
/// Transform cannot convert, requesting the run's cancellation as it yields
/// a given row.
struct ScriptedRows {
    schema: clinker_record::owned_storage::SharedStorage<clinker_record::Schema>,
    rows: usize,
    sent: usize,
    bad_v_at: Option<usize>,
    cancel_at: Option<(usize, crate::pipeline::shutdown::ShutdownToken)>,
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
            bad_v_at: None,
            cancel_at: None,
        }
    }

    /// The Transform cannot convert row `row`'s `v`.
    fn bad_v_at(mut self, row: usize) -> Self {
        self.bad_v_at = Some(row);
        self
    }

    /// The reader requests `token` as it yields row `row`.
    fn cancel_at(mut self, row: usize, token: &crate::pipeline::shutdown::ShutdownToken) -> Self {
        self.cancel_at = Some((row, token.clone()));
        self
    }

    fn readers(self) -> crate::executor::SourceReaders {
        HashMap::from([(
            "src".to_string(),
            crate::source::SourceInput::Records(Box::new(self)),
        )])
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
        if self.sent == self.rows {
            return Ok(None);
        }
        self.sent += 1;
        let row = self.sent;
        if let Some((at, token)) = &self.cancel_at
            && *at == row
        {
            token.request();
        }
        let v = if self.bad_v_at == Some(row) {
            TRANSFORM_BAD_V
        } else {
            "1"
        };
        Ok(Some(clinker_record::Record::new(
            self.schema.clone(),
            vec![
                clinker_record::Value::from(format!("g{}", row % 4).as_str()),
                clinker_record::Value::from(row.to_string().as_str()),
                clinker_record::Value::from(v),
            ],
        )))
    }
}

/// The same rows as [`ScriptedRows`], as the CSV text of Source `src`.
fn csv_rows(rows: usize, bad_v_at: usize) -> crate::executor::SourceReaders {
    let mut csv = String::from("grp,id,v\n");
    for row in 1..=rows {
        let v = if row == bad_v_at {
            TRANSFORM_BAD_V
        } else {
            "1"
        };
        csv.push_str(&format!("g{},{row},{v}\n", row % 4));
    }
    HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "src.csv".to_string(),
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )])
}

/// Source -> Transform -> Sink, the Transform fused with its Source in a
/// full run and streaming into the Sink. The Transform fails on a row whose
/// `v` is not a number.
const CONVERTING_CHAIN: &str = r#"
pipeline:
  name: converting_chain
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
      emit grp = grp
      emit id = id
      emit v = v.to_int()
- type: sink
  name: rows_out
  input: pass
  config:
    name: rows_out
    type: csv
    path: rows_out.csv
"#;

/// The run fails with the Sink's write failure.
fn assert_the_sinks_failure(result: &Result<ExecutionReport, PipelineError>) {
    let error = result
        .as_ref()
        .expect_err("the Sink's failure fails the run");
    assert!(
        error.to_string().contains(SINK_WRITE_FAILURE),
        "the run reports the Sink's failure, the first in data order: {error}"
    );
}

/// The warnings that carry the Transform's failure.
fn lines_carrying_the_transforms_failure(warnings: &[String]) -> Vec<&String> {
    warnings
        .iter()
        .filter(|line| line.contains(&format!("cannot convert '{TRANSFORM_BAD_V}'")))
        .collect()
}

/// The Sink streams and fails on its own row, before its input ends.
fn assert_the_sink_failed_first(ends: &[crate::executor::StreamingEnd]) {
    let mine: Vec<_> = ends.iter().filter(|end| end.node == "rows_out").collect();
    assert_eq!(mine.len(), 1, "rows_out streams and stops once: {ends:?}");
    assert_eq!(
        mine[0].input,
        crate::executor::StreamingInputEnd::ConsumerFailed,
        "the Sink fails on its own row"
    );
}

/// The Sink's writer refuses its first bytes, a few hundred rows in; the
/// Transform feeding it fails on row 5,001. The run reports the Sink's
/// failure, and the Transform's, later in data order, is logged once on the
/// walk's thread naming the Transform.
#[test]
fn a_streaming_sinks_failure_beats_its_producers_later_failure() {
    let refusing = SinkWriter::Refusing;
    let run = run(
        CONVERTING_CHAIN,
        ScriptedRows::new(6000).bad_v_at(5001).readers(),
        &[("rows_out", &refusing)],
        None,
        None,
    );
    assert_the_sinks_failure(&run.result);
    assert_the_sink_failed_first(&run.ends);
    let lines = lines_carrying_the_transforms_failure(&run.warnings);
    assert_eq!(
        lines.len(),
        1,
        "the later failure is logged once: {:?}",
        run.warnings
    );
    assert!(
        lines[0].contains(r#"upstream="pass""#),
        "the logged failure names the Transform: {lines:?}"
    );
}

/// The Sink's writer refuses its first bytes; the run is cancelled at row
/// 40,000. The Sink's failure stands: the run fails rather than stopping as
/// cancelled.
#[test]
fn a_streaming_sinks_failure_stands_when_the_run_is_cancelled_after_it() {
    let refusing = SinkWriter::Refusing;
    let token = crate::pipeline::shutdown::ShutdownToken::detached();
    let run = run(
        CONVERTING_CHAIN,
        ScriptedRows::new(50_000)
            .cancel_at(40_000, &token)
            .readers(),
        &[("rows_out", &refusing)],
        Some(&token),
        None,
    );
    assert!(
        token.is_requested(),
        "the reader requested the cancellation"
    );
    assert_the_sinks_failure(&run.result);
    assert_the_sink_failed_first(&run.ends);
}

/// A preview runs the Transform apart from its Source and still streams its
/// rows into the Sink. The Sink fails on its first bytes and the Transform
/// on row 5,001: the preview reports the Sink's failure, as the full run of
/// the same pipeline does, and both log the Transform's.
#[test]
fn a_preview_and_a_full_run_report_a_streaming_sinks_earlier_failure() {
    let refusing = SinkWriter::Refusing;
    for preview in [Some(6000), None] {
        let run = run(
            CONVERTING_CHAIN,
            csv_rows(6000, 5001),
            &[("rows_out", &refusing)],
            None,
            preview,
        );
        assert_the_sinks_failure(&run.result);
        assert_eq!(
            lines_carrying_the_transforms_failure(&run.warnings).len(),
            1,
            "preview {preview:?}: the Transform's later failure is logged once: {:?}",
            run.warnings
        );
    }
}

/// Two Source -> Transform -> Sink chains whose Sinks share one writer, as
/// a preview's Sinks share its destination. Each chain's turn settles its
/// Sink before the next chain runs, so the writer holds the first chain's
/// rows and then the second's, the same bytes on every run.
#[test]
fn a_two_chain_preview_writes_each_sinks_rows_as_one_block() {
    const ROWS: usize = 5_000;
    const LIMIT: u64 = 4_000;
    let chain = |name: &str| {
        format!(
            r#"
- type: source
  name: src_{name}
  config:
    name: src_{name}
    type: csv
    path: src_{name}.csv
    schema:
      - {{ name: id, type: string }}
- type: transform
  name: pass_{name}
  input: src_{name}
  config:
    cxl: |
      emit id = id
- type: sink
  name: {name}_out
  input: pass_{name}
  config:
    name: {name}_out
    type: csv
    path: {name}_out.csv
"#
        )
    };
    let yaml = format!(
        "pipeline:\n  name: two_chains\nnodes:{}{}",
        chain("a"),
        chain("b")
    );
    let source = |name: &str| {
        let mut csv = String::from("id\n");
        for row in 1..=ROWS {
            csv.push_str(&format!("{name}{row}\n"));
        }
        (
            format!("src_{name}"),
            crate::executor::single_file_reader(
                format!("src_{name}.csv"),
                Box::new(std::io::Cursor::new(csv.into_bytes())),
            ),
        )
    };
    let block = |name: &str| {
        let mut block = String::from("id\n");
        for row in 1..=LIMIT {
            block.push_str(&format!("{name}{row}\n"));
        }
        block
    };
    // Which chain runs first is the scheduler's choice; either way each
    // Sink's rows form one block.
    let orders = [
        format!("{}{}", block("a"), block("b")),
        format!("{}{}", block("b"), block("a")),
    ];
    let mut first: Option<String> = None;
    for attempt in 0..20 {
        let buffer = SharedBuffer::new();
        let shared = SinkWriter::Buffer(buffer.clone());
        let run = run(
            &yaml,
            HashMap::from([source("a"), source("b")]),
            &[("a_out", &shared), ("b_out", &shared)],
            None,
            Some(LIMIT),
        );
        run.result.as_ref().expect("the preview succeeds");
        let written = buffer.as_string();
        let expected = first.get_or_insert_with(|| {
            orders
                .iter()
                .find(|order| **order == written)
                .cloned()
                .unwrap_or_else(|| orders[0].clone())
        });
        if let Some(difference) = first_difference(&written, expected) {
            panic!("preview {attempt} wrote different bytes: {difference}");
        }
    }
}

/// Where `written` first differs from `expected`, with the text around it.
fn first_difference(written: &str, expected: &str) -> Option<String> {
    let at = written
        .bytes()
        .zip(expected.bytes())
        .position(|(written, expected)| written != expected)
        .or_else(|| (written.len() != expected.len()).then(|| written.len().min(expected.len())))?;
    let around = |text: &str| {
        text.get(at.saturating_sub(40)..(at + 40).min(text.len()))
            .unwrap_or_default()
            .to_string()
    };
    Some(format!(
        "from byte {at} ({} written, {} expected): wrote {:?}, expected {:?}",
        written.len(),
        expected.len(),
        around(written),
        around(expected)
    ))
}
