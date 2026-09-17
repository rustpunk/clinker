//! End-to-end coverage for the spill root becoming unwritable *mid-run* —
//! after startup storage validation passed and the live per-run [`SpillDir`]
//! guard already exists.
//!
//! The pre-startup case (the configured spill dir is gone before the run
//! begins) is covered by run-startup validation, and the classifier that lifts
//! a directory-level I/O fault to [`SpillError::DirUnavailable`] is covered at
//! the unit level for every spill site. The missing link this test closes is
//! the genuinely mid-run case: a healthy spill root passes startup validation,
//! the run creates its per-run `clinker-spill-*` directory and takes its lock,
//! an operator begins spilling, and only *then* does the directory vanish (an
//! external cleaner, an NFS remount, a volume unmount). No production code path
//! ever invalidates a healthy live spill root, so this case can only be reached
//! through the `#[cfg(test)]` fault-injection seam in
//! [`crate::executor::spill_purge`].
//!
//! The test drives a real spilling Aggregate under a tiny memory budget, arms
//! the seam so the first operator spill-file open removes the live per-run
//! directory before opening, and asserts the run surfaces a clean
//! `DirUnavailable` — not a generic I/O error, not a panic, and not a hang. The
//! whole run is wrapped in a wall-clock timeout so a regression to a stall
//! fails fast instead of hanging CI.
//!
//! CSV fixtures are decoded before either run using the real configured reader.
//! This isolates operator spill behavior from decoder admission competing for
//! the deliberately tight aggregate budget. Both runs retain the same typed
//! records and physical file boundary; whole-input CSV admission is covered by
//! the format-resource tests, not this spill-directory fault test.
//!
//! [`SpillDir`]: crate::executor::spill_purge::SpillDir
//! [`SpillError::DirUnavailable`]: clinker_plan::SpillError::DirUnavailable

use clinker_record::owned_storage::SharedStorage;
use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc;
use std::time::Duration;

use clinker_bench_support::io::SharedBuffer;
use clinker_plan::SpillError;
use clinker_plan::config::{CompileContext, PipelineConfig};
use clinker_plan::error::PipelineError;

use crate::executor::spill_purge;
use crate::executor::{PipelineExecutor, PipelineRunParams};

// A layout-derived memory budget forces the HashAggregator's dual-threshold spill:
// with many distinct keys the group count crosses the budget-derived
// `max_groups` well before EOF, so `add_record` calls `spill()` mid-run — the
// open this test intercepts. The limit also admits all output rows at their
// compiled layout after the spilled aggregate completes, plus the existing
// fixed allowance and measured writer workspace.
// `backpressure: spill` is required: the budget can remain below the process
// baseline RSS, which the default `pause` policy rejects at startup (E312);
// the spill policy never pauses a producer and so spills mid-run as this test
// intends rather than being rejected.
//
// A fused passthrough Transform sits between the Source and the Aggregate so
// the Aggregate streaming-ingests its input per record (a fused
// Source→Transform is a certified streaming producer). Without it a direct
// Source→Aggregate would materialize its whole (spilled-under-pressure) input
// into RAM, which the re-materialized-drain budget gate aborts — the Aggregate
// would never reach its own hash spill. Streaming the input keeps the working
// set to one batch, so the group-table spill this test targets is the first
// (and only) spill open, exactly the seam it intercepts.
const PIPELINE_YAML: &str = r#"
pipeline:
  name: spill_dir_unavailable_midrun
  memory: { limit: "655360", backpressure: spill }
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      schema:
        - { name: k, type: string }
        - { name: v, type: int }
  - type: transform
    name: norm
    input: events
    config:
      cxl: |
        emit k = k
        emit v = v
  - type: aggregate
    name: by_key
    input: norm
    config:
      group_by:
        - k
      cxl: |
        emit k = k
        emit n = count(*)
  - type: sink
    name: out
    input: by_key
    config:
      name: out
      type: csv
      path: out.csv
"#;

const ROWS: usize = 4_000;

fn config_with_writer_headroom(root: &std::path::Path, csv: &str) -> PipelineConfig {
    use clinker_record::{Record, Schema, Value};
    use std::sync::Arc;
    let config: PipelineConfig = clinker_plan::yaml::from_str(PIPELINE_YAML).unwrap();
    let document_bytes = crate::test_support::single_csv_document_metadata_bytes(
        &config,
        &CompileContext::default(),
        &[("events", csv)],
    );
    let plan = config.compile(&CompileContext::default()).unwrap();
    let arb = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
        1024 * 1024,
        0.8,
        0.7,
        Box::new(crate::pipeline::memory::NoOpPolicy),
    ));
    let provider = crate::executor::preparation::ExecutorResources::with_spill_root(
        arb.clone(),
        crate::pipeline::shutdown::ShutdownToken::detached(),
        Some(root),
        std::num::NonZeroUsize::new(2).unwrap(),
        None,
    )
    .unwrap();
    let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["k".into(), "n".into()])));
    let mut writer = crate::executor::registry::build_format_writer(
        plan.config().sink_configs().next().unwrap(),
        Box::new(std::io::sink()),
        schema.clone(),
        crate::output::staging::OutputStagingRegistry::default(),
        None,
        provider.resources(),
    )
    .unwrap();
    writer
        .write_record(&Record::new(
            schema,
            vec![Value::String("key_3999".into()), Value::Integer(1)],
        ))
        .unwrap();
    writer.flush().unwrap();
    let headroom = arb.writer_resource_usage().peak_memory;
    assert!(
        headroom > 0 && headroom < 64 * 1024,
        "unexpected writer workspace: {headroom}"
    );
    // Preserve the original spare allowance above the materialized output,
    // while deriving the output itself from its compiled record layout. Both
    // runs keep the same distinct-key workload and prove a real aggregate spill.
    const MATERIALIZATION_SPARE: u64 = 655_360 - 608_000;
    let dag = plan.dag();
    let aggregate = dag
        .graph
        .node_indices()
        .find(|idx| dag.graph[*idx].name() == "by_key")
        .expect("aggregate node exists");
    // Compiled columns include the engine-owned identity carried to the sink.
    let width = dag.graph[aggregate].output_schema_in(dag).column_count();
    let materialized = super::super::node_buffer::record_byte_cost(width) * ROWS as u64;
    let aggregate_allowance = materialized + MATERIALIZATION_SPARE;
    eprintln!(
        "admitted CSV writer headroom: {headroom} bytes; aggregate allowance: {aggregate_allowance} bytes; empty document metadata: {document_bytes} bytes"
    );
    // One admitted empty document stays live alongside the existing writer
    // workspace and materialized output; this is measured storage, not padding.
    clinker_plan::yaml::from_str(&PIPELINE_YAML.replace(
        "655360",
        &(aggregate_allowance + headroom + document_bytes).to_string(),
    ))
    .unwrap()
}

/// Many distinct keys so the aggregate's group table outgrows the tiny budget
/// and spills before EOF.
fn build_events_csv() -> String {
    let mut s = String::with_capacity(ROWS * 24);
    s.push_str("k,v\n");
    for i in 0..ROWS {
        // Every row a distinct key maximizes group-table pressure.
        s.push_str(&format!("key_{i},{i}\n"));
    }
    s
}

#[test]
fn spill_dir_removed_mid_run_surfaces_dir_unavailable_without_panic_or_stall() {
    // A dedicated parent root so the seam matches only this run's per-run
    // `clinker-spill-*` directory and nothing else on the host.
    let spill_root = tempfile::tempdir().expect("create custom spill root");
    let spill_root_path = spill_root.path().to_path_buf();

    let csv = build_events_csv();
    let config = config_with_writer_headroom(spill_root.path(), &csv);
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let readers = crate::test_support::predecoded_csv_readers(
        &config,
        &CompileContext::default(),
        &[("events", &csv)],
    );

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(out) as Box<dyn Write + Send>)]);

    let params = PipelineRunParams {
        execution_id: "spill-dir-unavailable-midrun".to_string(),
        batch_id: "batch-0".to_string(),
        spill_root_dir: Some(spill_root_path.clone()),
        ..Default::default()
    };

    // Arm the mid-run fault: the first operator spill-file open under this root
    // removes the live per-run spill directory before opening, so that very open
    // fails with `NotFound` → `DirUnavailable`. Disarm in every exit path below
    // so an armed root never leaks into a sibling test sharing the process.
    spill_purge::arm_spill_root_invalidation_for_test(spill_root_path.clone());

    // Run on a worker thread joined under a wall-clock deadline. A regression
    // that turns the mid-run directory loss into a hang (a retry loop, a
    // blocked channel) trips the timeout and fails fast instead of hanging CI.
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result =
            PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params);
        // Send may fail only if the receiver already timed out and dropped; the
        // result is still observable via the join below in that case.
        let _ = tx.send(result);
    });

    let outcome = rx.recv_timeout(Duration::from_secs(60));
    spill_purge::disarm_spill_root_invalidation_for_test();

    let result = match outcome {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!(
                "run stalled: a mid-run spill-dir loss must surface a clean error within the \
                 deadline, never hang"
            );
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            // The worker dropped the sender without sending — i.e. it panicked.
            // Join to surface the panic payload rather than reporting a bare
            // disconnect.
            let _ = worker.join();
            panic!("run panicked instead of returning a clean DirUnavailable error");
        }
    };

    // No panic: joining the worker must succeed (a panic would have come back as
    // a `Disconnected` above, but join here also pins the no-panic guarantee on
    // the success path).
    worker.join().expect("run thread must not panic");

    // Clean, classified error: the mid-run directory loss must surface as
    // `DirUnavailable`, not a generic `Io` spill error and not an opaque
    // `Internal`.
    match result {
        Err(PipelineError::Spill(SpillError::DirUnavailable { dir, .. })) => {
            assert!(
                dir.contains("clinker-spill-"),
                "DirUnavailable must name the per-run spill directory, got {dir}"
            );
        }
        Err(other) => panic!(
            "mid-run spill-dir loss must surface as PipelineError::Spill(DirUnavailable), \
             got {other:?}"
        ),
        Ok(_) => panic!(
            "the run must fail once its live spill directory vanishes mid-run, not complete \
             successfully"
        ),
    }
}

#[test]
fn unarmed_seam_lets_a_real_spilling_run_complete() {
    // Companion control for the fault-injection test above: the identical
    // tiny-budget pipeline must complete successfully when the seam is NOT
    // armed. This proves two things at once — the seam is a true no-op while
    // disarmed (no release-shaped behavior change), and the `DirUnavailable`
    // the armed test observes is caused by the injected fault rather than a
    // pre-existing flaky failure in a spilling run.
    //
    // It also confirms the budget really does drive a mid-run spill: the run
    // exercises the same spill-open path the armed test intercepts, here
    // returning a healthy temp file, so the merge path runs end to end.
    let spill_root = tempfile::tempdir().expect("create custom spill root");
    let spill_root_path = spill_root.path().to_path_buf();

    let csv = build_events_csv();
    let config = config_with_writer_headroom(spill_root.path(), &csv);
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let readers = crate::test_support::predecoded_csv_readers(
        &config,
        &CompileContext::default(),
        &[("events", &csv)],
    );

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "spill-dir-unarmed-control".to_string(),
        batch_id: "batch-0".to_string(),
        spill_root_dir: Some(spill_root_path),
        ..Default::default()
    };

    // No arm here. The seam is root-scoped — it fires only for an open under
    // the exact parent root a test armed — so this control run's distinct root
    // is unaffected even if the armed sibling test runs concurrently in the same
    // process. That isolation is why this test does not (and must not) touch the
    // global arm state, which would race the sibling.
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("an unarmed spilling run must complete cleanly");
    assert_eq!(
        report.counters.total_count as usize, ROWS,
        "every input row must be ingested by the control run"
    );
    assert!(
        report.cumulative_spill_bytes > 0,
        "the tiny budget must have driven a real disk spill; otherwise the armed test would not \
         exercise the spill-open fault seam"
    );
    // Hash aggregation does not promise output order. Compare every literal
    // row after sorting, preserving duplicates so missing/repeated groups fail.
    let output = String::from_utf8(out.contents()).expect("CSV output is UTF-8");
    let (header, body) = output.split_once('\n').expect("CSV output has a header");
    assert_eq!(header, "k,n");
    assert!(body.ends_with('\n'), "CSV output ends with a full record");
    let mut actual: Vec<_> = body.split_inclusive('\n').collect();
    actual.sort_unstable();
    let mut expected: Vec<_> = (0..ROWS).map(|i| format!("key_{i},1\n")).collect();
    expected.sort_unstable();
    assert_eq!(actual, expected, "every distinct key has exactly one input");
}
