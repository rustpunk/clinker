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

/// The error a [`RefusingWriter`] returns, followed by its Sink's name.
const SINK_WRITE_FAILURE: &str = "the sink's disk is full";

/// The `v` value the Transform cannot convert.
const TRANSFORM_BAD_V: &str = "transform_bad";

/// The Sinks whose writers were written to, in the order each was first
/// written to. A buffered Sink writes only in its own turn, so this is the
/// order of those turns.
#[derive(Clone, Default)]
struct WriteAttempts(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl WriteAttempts {
    fn record(&self, sink: &str) {
        let mut attempts = self.0.lock().expect("attempts lock");
        if !attempts.iter().any(|seen| seen == sink) {
            attempts.push(sink.to_string());
        }
    }

    fn sinks(&self) -> Vec<String> {
        self.0.lock().expect("attempts lock").clone()
    }
}

/// A writer whose every write fails, as on a full disk.
struct RefusingWriter {
    sink: String,
    attempts: WriteAttempts,
}

impl std::io::Write for RefusingWriter {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        self.attempts.record(&self.sink);
        Err(std::io::Error::other(format!(
            "{SINK_WRITE_FAILURE} ({})",
            self.sink
        )))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// What one Sink writes to.
enum SinkWriter {
    /// Every write fails, and is recorded.
    Refusing(WriteAttempts),
    /// The bytes land in this buffer; clones share it.
    Buffer(SharedBuffer),
}

impl SinkWriter {
    fn refusing() -> Self {
        Self::Refusing(WriteAttempts::default())
    }

    fn boxed(&self, sink: &str) -> Box<dyn std::io::Write + Send> {
        match self {
            Self::Refusing(attempts) => Box::new(RefusingWriter {
                sink: sink.to_string(),
                attempts: attempts.clone(),
            }),
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
        .map(|(sink, writer)| (sink.to_string(), writer.boxed(sink)))
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
    let refusing = SinkWriter::refusing();
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
    let refusing = SinkWriter::refusing();
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
    let refusing = SinkWriter::refusing();
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

/// The nodes `first`, then one Source -> Sink branch per name in
/// `buffered`, each Sink buffered (fed by its Source, not streamed).
fn buffered_branches(name: &str, first: &str, buffered: &[&str]) -> String {
    let mut nodes = String::new();
    for branch in buffered {
        nodes.push_str(&format!(
            r#"
- type: source
  name: src_{branch}
  config:
    name: src_{branch}
    type: csv
    path: src_{branch}.csv
    schema:
      - {{ name: id, type: string }}
- type: sink
  name: {branch}_out
  input: src_{branch}
  config:
    name: {branch}_out
    type: csv
    path: {branch}_out.csv
"#
        ));
    }
    format!(
        "pipeline:\n  name: {name}\nerror_handling:\n  strategy: fail_fast\nnodes:{first}{nodes}"
    )
}

/// Three rows of `id` as the CSV text of Source `src_{branch}`.
fn branch_rows(branch: &str) -> (String, crate::source::SourceInput) {
    (
        format!("src_{branch}"),
        crate::executor::single_file_reader(
            format!("src_{branch}.csv"),
            Box::new(std::io::Cursor::new(b"id\n1\n2\n3\n".to_vec())),
        ),
    )
}

/// The Source -> Transform -> Sink chain of [`CONVERTING_CHAIN`], as nodes
/// to append to another pipeline.
fn converting_chain_nodes() -> &'static str {
    let at = CONVERTING_CHAIN
        .find("nodes:")
        .expect("the chain lists its nodes");
    &CONVERTING_CHAIN[at + "nodes:".len()..]
}

/// Two independent branches: a buffered Sink `a_out` fed by its own Source,
/// whose writer refuses its bytes, and the converting chain, whose Transform
/// fails on row 5,001. With no memory estimates the scheduler takes the
/// runnable step that comes first in the plan's topological order, which
/// here puts `a_out`'s turn before the Transform's: the test confirms it
/// from the writes the walk made before it stopped. The run reports the
/// Sink's failure and logs the Transform's with its step.
#[test]
fn a_sinks_failure_in_an_earlier_turn_beats_a_later_steps_failure() {
    let attempts = WriteAttempts::default();
    let refusing = SinkWriter::Refusing(attempts.clone());
    let rows_out = SinkWriter::Buffer(SharedBuffer::new());
    let mut readers = csv_rows(6000, 5001);
    readers.extend([branch_rows("a")]);
    let run = run(
        &buffered_branches("two_branches", converting_chain_nodes(), &["a"]),
        readers,
        &[("a_out", &refusing), ("rows_out", &rows_out)],
        None,
        None,
    );
    assert_eq!(
        attempts.sinks(),
        vec!["a_out".to_string()],
        "a_out wrote before the walk stopped, so its turn came first"
    );
    assert!(
        !run.ends.iter().any(|end| end.node == "a_out"),
        "a_out is buffered, so it writes only in its own turn: {:?}",
        run.ends
    );
    let error = run
        .result
        .as_ref()
        .expect_err("the Sink's failure fails the run");
    assert!(
        error
            .to_string()
            .contains(&format!("{SINK_WRITE_FAILURE} (a_out)")),
        "the run reports the earlier turn's failure: {error}"
    );
    let lines = lines_carrying_the_transforms_failure(&run.warnings);
    assert_eq!(
        lines.len(),
        1,
        "the later failure is logged once: {:?}",
        run.warnings
    );
    assert!(
        lines[0].contains(r#"node="pass""#),
        "the logged failure names the Transform: {lines:?}"
    );
}

/// Two buffered Sinks fail and the walk completes: the run reports both, in
/// the order of their turns, and logs neither.
#[test]
fn sink_only_failures_still_report_every_sink() {
    let attempts = WriteAttempts::default();
    let refusing = SinkWriter::Refusing(attempts.clone());
    let run = run(
        &buffered_branches("two_sinks", "", &["a", "b"]),
        HashMap::from([branch_rows("a"), branch_rows("b")]),
        &[("a_out", &refusing), ("b_out", &refusing)],
        None,
        None,
    );
    let turns = attempts.sinks();
    assert_eq!(turns.len(), 2, "both Sinks wrote: {turns:?}");
    match run.result {
        Err(PipelineError::Multiple(errors)) => {
            let reported: Vec<String> = errors.iter().map(ToString::to_string).collect();
            assert_eq!(reported.len(), 2, "{reported:?}");
            for (error, sink) in reported.iter().zip(&turns) {
                assert!(
                    error.contains(&format!("{SINK_WRITE_FAILURE} ({sink})")),
                    "the failures are in turn order {turns:?}: {reported:?}"
                );
            }
        }
        other => panic!("both Sinks' failures are the run's error: {other:?}"),
    }
    assert!(
        !run.warnings
            .iter()
            .any(|line| line.contains(SINK_WRITE_FAILURE)),
        "a reported failure is not also logged: {:?}",
        run.warnings
    );
}

/// The reclaim pass a step's request starts fails its spill; the step then
/// fails too. The spill's failure, met first, is the step's result, and the
/// step's own error is logged with the step's name, not dropped.
#[test]
fn a_reclaim_spill_failure_wins_and_the_steps_own_error_is_logged() {
    use crate::pipeline::memory::walk::{
        VictimOutcome, WalkContextGuard, WalkReclaim, WalkReclaimSet, WalkSpillSettings,
        with_test_reclaim,
    };
    use crate::pipeline::memory::{ConsumerHandle, ConsumerId, MemoryArbitrator, MemoryConsumer};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    const KIB: u64 = 1024;
    const SPILL_FAILURE: &str = "the spill file could not be written";
    const STEP_FAILURE: &str = "the step's own failure";

    /// Holds its handle's charge and frees nothing until spilled.
    struct Held(Arc<ConsumerHandle>);
    impl MemoryConsumer for Held {
        fn current_usage(&self) -> u64 {
            self.0.bytes()
        }
        fn spill_priority(&self) -> i32 {
            0
        }
        fn try_spill(&self, _: u64) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
            Ok(0)
        }
        fn can_back_pressure(&self) -> bool {
            false
        }
    }

    /// Every spill the pass asks for fails.
    struct FailingSpill;
    impl WalkReclaim for FailingSpill {
        fn spill_victim(
            &mut self,
            _: ConsumerId,
            _: &MemoryArbitrator,
        ) -> Result<VictimOutcome, PipelineError> {
            Err(PipelineError::Internal {
                op: "spill",
                node: "held".to_string(),
                detail: SPILL_FAILURE.to_string(),
            })
        }
    }

    let arbitrator = Arc::new(MemoryArbitrator::with_policy(
        1024 * KIB,
        0.80,
        0.70,
        Box::new(crate::pipeline::memory::Priority),
    ));
    let handle = ConsumerHandle::new();
    arbitrator
        .register_node_consumer(
            Arc::new(Held(Arc::clone(&handle))),
            Arc::clone(&handle),
            clinker_plan::runtime_error::ConsumerLabel {
                node: "held".to_string(),
                surface: clinker_plan::runtime_error::MemorySurface::BufferedRows {
                    from: "held".to_string(),
                    to: clinker_plan::runtime_error::NonEmptyReaders::one("totals".to_string()),
                },
            },
        )
        .expect("a fresh handle registers");
    handle.set_bytes(200 * KIB);
    let _filler = arbitrator
        .reserve(
            600 * KIB,
            crate::pipeline::memory::ledger::Requester::governed(),
        )
        .expect("the filler fits");
    let set = Rc::new(RefCell::new(WalkReclaimSet::new(WalkSpillSettings {
        spill_root: Arc::from(std::env::temp_dir().as_path()),
        spill_compress: clinker_plan::config::CompressMode::Auto,
        batch_size: 1024,
    })));
    let _walk = WalkContextGuard::install(&arbitrator, set);
    let stand_in: Rc<RefCell<dyn WalkReclaim>> = Rc::new(RefCell::new(FailingSpill));
    let refused = with_test_reclaim(stand_in, || {
        arbitrator.reserve(
            500 * KIB,
            crate::pipeline::memory::ledger::Requester::governed(),
        )
    });
    assert!(refused.is_err(), "the step's request falls short");

    let (result, warnings) = super::capture_warnings(|| {
        crate::executor::dispatch::settle_reclaim_slot(
            &arbitrator,
            "totals",
            Err(PipelineError::Internal {
                op: "test",
                node: "totals".to_string(),
                detail: STEP_FAILURE.to_string(),
            }),
        )
    });
    let error = result.expect_err("the step fails");
    assert!(
        error.to_string().contains(SPILL_FAILURE),
        "the spill's failure is the step's result: {error}"
    );
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains(r#"node="totals""#) && warnings[0].contains(STEP_FAILURE),
        "the step's own error is logged with its name: {warnings:?}"
    );
}
