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
//! [`SpillDir`]: crate::executor::spill_purge::SpillDir
//! [`SpillError::DirUnavailable`]: clinker_plan::SpillError::DirUnavailable

use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc;
use std::time::Duration;

use clinker_bench_support::io::SharedBuffer;
use clinker_plan::SpillError;
use clinker_plan::config::{CompileContext, PipelineConfig};
use clinker_plan::error::PipelineError;

use crate::executor::spill_purge;
use crate::executor::{PipelineExecutor, PipelineRunParams, SourceReaders, single_file_reader};

// A 1 KiB memory budget forces the HashAggregator's dual-threshold spill: with
// many distinct keys the group count crosses the budget-derived `max_groups`
// well before EOF, so `add_record` calls `spill()` mid-run — the open this test
// intercepts. `backpressure: spill` is required: the budget is below the
// process baseline RSS, which the default `pause` policy rejects at startup
// (E312); the spill policy never pauses a producer and so spills mid-run as
// this test intends rather than being rejected.
const PIPELINE_YAML: &str = r#"
pipeline:
  name: spill_dir_unavailable_midrun
  memory: { limit: "1024", backpressure: spill }
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
  - type: aggregate
    name: by_key
    input: events
    config:
      group_by:
        - k
      cxl: |
        emit k = k
        emit n = count(*)
  - type: output
    name: out
    input: by_key
    config:
      name: out
      type: csv
      path: out.csv
"#;

const ROWS: usize = 4_000;

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
    let config: PipelineConfig =
        clinker_plan::yaml::from_str(PIPELINE_YAML).expect("parse pipeline YAML");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let mut readers: SourceReaders = HashMap::new();
    readers.insert(
        "events".to_string(),
        single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
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
    let config: PipelineConfig =
        clinker_plan::yaml::from_str(PIPELINE_YAML).expect("parse pipeline YAML");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let mut readers: SourceReaders = HashMap::new();
    readers.insert(
        "events".to_string(),
        single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    );

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(out) as Box<dyn Write + Send>)]);

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
}
