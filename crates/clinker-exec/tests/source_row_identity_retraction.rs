//! Source-scoped identity coverage for failure evidence and retraction state.

#[path = "common/dlq_sink.rs"]
mod dlq_sink;
#[path = "common/pipeline_resource_fixtures.rs"]
mod resource_fixtures;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::Duration;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders,
};
use clinker_exec::source::multi_file::FileSlot;
use clinker_format::FormatError;
use clinker_plan::config::{CompileContext, ConcurrencyConfig, parse_config};
use clinker_plan::plan::CompiledPlan;
use dlq_sink::{CollectingDlqSink, DlqRow};

fn compile_failure_pipeline(granularity: &str, memory_limit: &str) -> CompiledPlan {
    let yaml = format!(
        r#"
pipeline:
  name: source_row_identity_retraction
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      dlq_granularity: {granularity}
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
        - {{ name: note, type: string }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      dlq_granularity: {granularity}
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
        - {{ name: note, type: string }}
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: transform
    name: validate
    input: merged
    config:
      cxl: |
        emit parsed = value.to_int()
  - type: sink
    name: out
    input: validate
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#,
    );
    parse_config(&yaml)
        .expect("failure pipeline parses")
        .compile(&CompileContext::default())
        .expect("failure pipeline compiles")
}

fn slot(path: &str, csv: String) -> FileSlot {
    FileSlot::new(PathBuf::from(path), Box::new(Cursor::new(csv.into_bytes())))
}

fn run_failure_pipeline(
    plan: &CompiledPlan,
    src_a: String,
    src_b: String,
) -> (ExecutionReport, String, Vec<DlqRow>) {
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            SourceInput::Files(vec![slot("a.csv", src_a)]),
        ),
        (
            "src_b".to_string(),
            SourceInput::Files(vec![slot("b.csv", src_b)]),
        ),
    ]);
    run_failure_pipeline_with_readers(plan, readers)
}

fn run_failure_pipeline_with_readers(
    plan: &CompiledPlan,
    readers: SourceReaders,
) -> (ExecutionReport, String, Vec<DlqRow>) {
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "source-row-identity-retraction".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let sink = CollectingDlqSink::new();
    let report = PipelineExecutor::run_plan_with_readers_writers(
        plan,
        readers,
        dlq_sink::registry(writers, &sink),
        &params,
    )
    .expect("failure pipeline executes");
    (report, output.as_string(), sink.rows())
}

fn small_source(prefix: &str, failing: bool) -> String {
    let first = if failing { "bad" } else { "10" };
    format!("id,value,note\n{prefix}1,{first},first\n{prefix}2,20,second\n")
}

/// Each source's dead-letter row identities, as `(source_name, source_row)`
/// pairs keyed by source name.
fn identities_by_source(rows: &[DlqRow]) -> BTreeMap<String, BTreeSet<(String, u64)>> {
    let mut identities = BTreeMap::<String, BTreeSet<(String, u64)>>::new();
    for row in rows {
        identities
            .entry(row.source_name().to_string())
            .or_default()
            .insert((row.source_name().to_string(), row.source_row()));
    }
    identities
}

#[test]
fn dlq_row_and_document_evidence_distinguish_same_ordinals() {
    let row_plan = compile_failure_pipeline("record", "1G");
    let (row_report, _, row_rows) =
        run_failure_pipeline(&row_plan, small_source("a", true), small_source("b", true));
    assert_eq!(row_report.counters.dlq_count, 2);
    assert_eq!(row_rows.len(), 2);
    assert!(row_rows.iter().all(|row| row.trigger()));
    let row_ids = identities_by_source(&row_rows);
    assert_eq!(row_ids["src_a"].len(), 1);
    assert_eq!(row_ids["src_b"].len(), 1);
    let (a_source, a_ordinal) = row_ids["src_a"].first().expect("src_a row identity");
    let (b_source, b_ordinal) = row_ids["src_b"].first().expect("src_b row identity");
    assert_eq!(a_ordinal, b_ordinal);
    assert_ne!(a_source, b_source);

    let document_plan = compile_failure_pipeline("document", "1G");
    let (document_report, output, document_rows) = run_failure_pipeline(
        &document_plan,
        small_source("a", true),
        small_source("b", true),
    );
    assert_eq!(document_report.counters.dlq_count, 4);
    assert_eq!(document_rows.len(), 4);
    assert_eq!(document_report.counters.ok_count, 0);
    assert!(output.lines().nth(1).is_none());
    let document_ids = identities_by_source(&document_rows);
    assert_eq!(document_ids["src_a"].len(), 2);
    assert_eq!(document_ids["src_b"].len(), 2);
    assert_eq!(
        document_ids
            .values()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>()
            .len(),
        4
    );
    assert_eq!(
        document_rows.iter().filter(|row| row.trigger()).count(),
        2,
        "each source document keeps its own root cause"
    );
}

fn large_source(prefix: &str) -> String {
    const ROWS: usize = 320;
    let note = "x".repeat(8 * 1024);
    let mut csv = String::from("id,value,note\n");
    csv.push_str(&format!("{prefix}000,bad,{note}\n"));
    for index in 1..ROWS {
        csv.push_str(&format!("{prefix}{index:03},{index},{note}\n"));
    }
    csv
}

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

// Each sender lives in its Source's startup callback. If ingest fails or is
// cancelled before reaching that callback, dropping it disconnects the peer.
struct SourceStartup {
    ready: SyncSender<()>,
    peer_ready: Receiver<()>,
}

impl SourceStartup {
    fn pair() -> [Self; 2] {
        let (a_tx, a_rx) = mpsc::sync_channel(1);
        let (b_tx, b_rx) = mpsc::sync_channel(1);
        [
            Self {
                ready: a_tx,
                peer_ready: b_rx,
            },
            Self {
                ready: b_tx,
                peer_ready: a_rx,
            },
        ]
    }

    fn wait(self, timeout: Duration) -> Result<(), FormatError> {
        self.ready.send(()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "peer source exited before startup",
            )
        })?;
        self.peer_ready.recv_timeout(timeout).map_err(|error| {
            std::io::Error::new(
                match error {
                    mpsc::RecvTimeoutError::Timeout => std::io::ErrorKind::TimedOut,
                    mpsc::RecvTimeoutError::Disconnected => std::io::ErrorKind::BrokenPipe,
                },
                "peer source did not complete startup",
            )
        })?;
        Ok(())
    }
}

fn pressure_plan(memory_limit: &str) -> CompiledPlan {
    let mut config = compile_failure_pipeline("document", memory_limit)
        .config()
        .clone();
    // The startup callbacks hold read permits, so both must be able to run
    // concurrently even when the host reports only one available CPU.
    config.pipeline.concurrency = Some(ConcurrencyConfig {
        threads: Some(2),
        chunk_size: None,
    });
    config
        .compile(&CompileContext::default())
        .expect("pressure plan compiles")
}

#[test]
fn predecoded_startup_requires_peer_readiness_before_the_second_row() {
    let plan = pressure_plan("1M");
    let csv = small_source("a", false);
    for peer_ready in [false, true] {
        let [startup, peer] = SourceStartup::pair();
        if peer_ready {
            peer.ready.send(()).expect("peer startup notification");
        }
        let SourceInput::Records(mut source) =
            resource_fixtures::predecoded_csv_source_with_startup(
                plan.config(),
                &CompileContext::default(),
                "src_a",
                &[("a.csv", &csv)],
                Some(Box::new(move || startup.wait(Duration::ZERO))),
            )
        else {
            panic!("predecoded fixture must be a RecordSource");
        };
        assert!(
            source
                .next_record()
                .expect("first row precedes startup")
                .is_some()
        );
        assert!(matches!(
            peer.peer_ready.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        let second = source.next_record();
        assert_eq!(
            peer.peer_ready.try_recv(),
            Ok(()),
            "first row was accepted before startup"
        );
        if peer_ready {
            assert!(second.expect("both sources ready").is_some());
            assert!(
                source
                    .next_record()
                    .expect("startup callback runs once")
                    .is_none()
            );
        } else {
            assert!(
                matches!(second, Err(FormatError::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
            );
        }
    }
}

#[test]
fn predecoded_startup_releases_waiter_when_peer_exits() {
    let [startup, peer] = SourceStartup::pair();
    let waiter = std::thread::spawn(move || startup.wait(STARTUP_TIMEOUT));
    // Observe the waiter at the rendezvous before simulating an ingest error
    // or cancellation dropping the other Source's unused startup callback.
    peer.peer_ready
        .recv_timeout(STARTUP_TIMEOUT)
        .expect("waiter reached startup");
    drop(peer);
    assert!(
        matches!(waiter.join().expect("waiter joins"), Err(FormatError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe)
    );

    let [startup, peer] = SourceStartup::pair();
    drop(peer);
    assert!(
        matches!(startup.wait(Duration::ZERO), Err(FormatError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe)
    );
}

#[test]
fn dlq_document_collateral_preserves_identity_and_records_when_spilled() {
    if clinker_exec::pipeline::memory::rss_bytes().is_none() {
        return;
    }

    let src_a = large_source("a");
    let src_b = large_source("b");
    let resident_plan = pressure_plan("1G");
    let spilled_plan = pressure_plan("1M");
    // Isolate downstream collateral spilling from initial document admission:
    // neither foreign-record queue may bulk-fill until both initial contexts
    // exist. This is a fixture precondition, not a Source startup guarantee.
    let readers = |plan: &CompiledPlan| {
        [("src_a", "a.csv", &src_a), ("src_b", "b.csv", &src_b)]
            .into_iter()
            .zip(SourceStartup::pair())
            .map(|((name, path, csv), startup)| {
                (
                    name.to_string(),
                    resource_fixtures::predecoded_csv_source_with_startup(
                        plan.config(),
                        &CompileContext::default(),
                        name,
                        &[(path, csv)],
                        Some(Box::new(move || startup.wait(STARTUP_TIMEOUT))),
                    ),
                )
            })
            .collect()
    };
    let (resident, _, resident_rows) =
        run_failure_pipeline_with_readers(&resident_plan, readers(&resident_plan));
    let (spilled, output, spilled_rows) =
        run_failure_pipeline_with_readers(&spilled_plan, readers(&spilled_plan));

    assert_eq!(resident.counters.dlq_count, 640);
    assert_eq!(resident_rows.len(), 640);
    assert_eq!(spilled.counters.dlq_count, resident.counters.dlq_count);
    assert_eq!(spilled_rows.len(), resident_rows.len());
    assert_eq!(spilled.counters.ok_count, 0);
    assert!(output.lines().nth(1).is_none());
    assert!(
        spilled
            .per_stage_spill_bytes
            .get("out")
            .is_some_and(|bytes| *bytes > 0),
        "the document collateral bucket must spill: {:?}",
        spilled.per_stage_spill_bytes
    );

    let evidence = |rows: &[DlqRow]| {
        rows.iter()
            .map(|row| {
                let id = row.field("id").expect("id field").to_string();
                let note = row.field("note").expect("note field");
                (
                    id,
                    note.len(),
                    (row.source_name().to_string(), row.source_row()),
                    row.trigger(),
                )
            })
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(evidence(&spilled_rows), evidence(&resident_rows));
    assert!(
        spilled_rows
            .iter()
            .all(|row| row.field("note").expect("note field").len() == 8 * 1024),
        "spill must retain each complete original record"
    );
}

#[test]
fn dlq_retry_reuses_compiled_plan_with_fresh_attempt_state() {
    let plan = compile_failure_pipeline("document", "1G");
    let (failed, failed_output, _) =
        run_failure_pipeline(&plan, small_source("a", true), small_source("b", true));
    assert_eq!(failed.counters.dlq_count, 4);
    assert!(failed_output.lines().nth(1).is_none());

    let (retried, retried_output, _) =
        run_failure_pipeline(&plan, small_source("a", false), small_source("b", false));
    assert_eq!(retried.counters.dlq_count, 0);
    assert_eq!(retried.counters.ok_count, 4);
    assert_eq!(retried_output.lines().skip(1).count(), 4);
}

#[test]
fn dlq_carriers_require_typed_identity_without_composite_dedup_keys() {
    let compact = |source: &str| source.split_whitespace().collect::<String>();
    let document_dlq = compact(include_str!("../src/executor/document_dlq.rs"));
    let dlq = compact(include_str!("../src/executor/dlq.rs"));

    assert!(dlq.contains("pubsource_row:crate::executor::stream_event::SourceRowId"));
    assert!(
        !document_dlq.contains("fnrecord_error_to_document_buffer_if_doc_dlq<R>")
            && !document_dlq.contains("R:Into<crate::executor::stream_event::SourceRowId>")
            && !document_dlq.contains("letrow_num=row_num.into()"),
        "document failure admission must accept SourceRowId directly"
    );
    assert!(
        !document_dlq.contains("HashSet<(Arc<str>,crate::executor::stream_event::SourceRowId)>"),
        "typed source identity must be the document collateral dedup key"
    );
}

fn compile_retraction_pipeline(failing_total: i64) -> CompiledPlan {
    let yaml = format!(
        r#"
pipeline:
  name: typed_commit_retraction
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      correlation_key: order_id
      schema:
        - {{ name: order_id, type: string }}
        - {{ name: department, type: string }}
        - {{ name: amount, type: int }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      correlation_key: order_id
      schema:
        - {{ name: order_id, type: string }}
        - {{ name: department, type: string }}
        - {{ name: amount, type: int }}
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: aggregate
    name: totals
    input: merged
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)
        emit n = count(*)
  - type: transform
    name: post_check
    input: totals
    config:
      cxl: |
        emit department = department
        emit total = total
        emit n = n
        emit ratio = 1 / (total - {failing_total})
  - type: sink
    name: out
    input: post_check
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#,
    );
    parse_config(&yaml)
        .expect("retraction pipeline parses")
        .compile(&CompileContext::default())
        .expect("retraction pipeline compiles")
}

fn run_retraction_pipeline(
    plan: &CompiledPlan,
    src_a: &str,
    src_b: &str,
) -> (ExecutionReport, String) {
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            SourceInput::Files(vec![slot("a.csv", src_a.to_string())]),
        ),
        (
            "src_b".to_string(),
            SourceInput::Files(vec![slot("b.csv", src_b.to_string())]),
        ),
    ]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "typed-commit-retraction".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(plan, readers, writers, &params)
        .expect("retraction pipeline executes");
    (report, output.as_string())
}

const RETRACT_A: &str = "\
order_id,department,amount
A1,HR,10
A2,HR,10
A3,HR,10
A4,ENG,100
A5,ENG,200
";

const RETRACT_B: &str = "\
order_id,department,amount
B1,HR,10
B2,HR,10
B3,HR,10
B4,ENG,300
";

const ENG_ONLY_A: &str = "\
order_id,department,amount
A4,ENG,100
A5,ENG,200
";

const ENG_ONLY_B: &str = "\
order_id,department,amount
B4,ENG,300
";

fn sorted_output_lines(output: &str) -> Vec<String> {
    let mut lines = output.lines().map(str::to_string).collect::<Vec<_>>();
    lines.sort();
    lines
}

#[test]
fn commit_detects_same_ordinal_contributors_from_both_sources() {
    let plan = compile_retraction_pipeline(60);
    let (retracted, retracted_output) = run_retraction_pipeline(&plan, RETRACT_A, RETRACT_B);
    let (baseline, baseline_output) = run_retraction_pipeline(&plan, ENG_ONLY_A, ENG_ONLY_B);

    assert_eq!(baseline.counters.dlq_count, 0);
    assert_eq!(
        retracted
            .counters
            .retraction
            .synthetic_ck_fanout_rows_expanded_total,
        6,
        "commit detection must harvest all six contributors despite ordinal collisions"
    );
    assert_eq!(
        sorted_output_lines(&retracted_output),
        sorted_output_lines(&baseline_output),
        "retraction must remove both sources' HR contributions"
    );
}

#[test]
fn commit_recompute_retracts_exact_row_and_preserves_colliding_source() {
    let plan = compile_retraction_pipeline(10);
    let src_a = "order_id,department,amount\nA1,HR,10\n";
    let src_b = "order_id,department,amount\nB1,ENG,20\n";
    let (report, output) = run_retraction_pipeline(&plan, src_a, src_b);

    assert!(
        !output.contains("HR"),
        "the failing src_a row must be retracted"
    );
    assert!(
        output.contains("ENG,20"),
        "src_b row 1 has the same ordinal but a distinct SourceRowId and must survive: {output}"
    );
    assert_eq!(report.counters.ok_count, 1);
    assert_eq!(report.counters.retraction.groups_recomputed, 1);
}

#[test]
fn commit_retry_reuses_compiled_plan_with_fresh_retraction_state() {
    let plan = compile_retraction_pipeline(60);
    let (first, first_output) = run_retraction_pipeline(&plan, RETRACT_A, RETRACT_B);
    assert!(first.counters.dlq_count > 0);
    assert!(!first_output.contains("HR"));

    let (retried, retried_output) = run_retraction_pipeline(&plan, ENG_ONLY_A, ENG_ONLY_B);
    assert_eq!(retried.counters.dlq_count, 0);
    assert_eq!(retried.counters.retraction.groups_recomputed, 0);
    assert!(retried_output.contains("ENG,600"));
}

#[test]
fn commit_state_uses_source_row_id_without_source_name_reconstruction() {
    let compact = |source: &str| source.split_whitespace().collect::<String>();
    let detect = compact(include_str!("../src/executor/commit/detect.rs"));
    let commit = compact(include_str!("../src/executor/commit/mod.rs"));
    let dispatch = compact(include_str!("../src/executor/commit/dispatch.rs"));
    let recompute = compact(include_str!("../src/executor/commit/recompute_agg.rs"));

    assert!(detect.contains("pub(crate)typeRetractRow=SourceRowId;"));
    assert!(
        !detect.contains("SourceRowId,Arc<str>")
            && !detect.contains("source_name_arc_of(&err.original_record)")
            && !detect.contains("letpair=(event.source_row"),
        "affected-row detection must use SourceRowId as the complete identity"
    );
    assert!(
        !commit.contains("pub(crate)source_name:std::sync::Arc<str>")
            && !dispatch.contains("source_name:std::sync::Arc::clone(&entry.source_name)"),
        "commit harvest must not reconstruct source identity from display names"
    );
    assert!(
        !recompute.contains("for(row_id,_)inretract_ids"),
        "aggregate recompute must consume SourceRowId directly"
    );
}
