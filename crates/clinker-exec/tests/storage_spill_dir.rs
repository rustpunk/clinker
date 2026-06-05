//! End-to-end coverage for the workspace `[storage.spill] dir` redirect.
//!
//! Confirms that a configured spill root actually receives the per-run
//! `clinker-spill-*` directory — the configurable-spill-location acceptance
//! criterion.
//!
//! The per-run spill directory is created under the configured root at run
//! start (unconditionally — before any spill admission decision) and
//! recursively removed at run end, so a watcher thread polls the configured
//! root for a `clinker-spill-*` entry while the run is in flight. The
//! redirect therefore holds on every platform regardless of whether a real
//! RSS-driven spill ever fires, so this test runs and asserts the same
//! invariant on Linux, Windows, and macOS.

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, PipelineConfig};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const PIPELINE_YAML: &str = r#"
pipeline:
  name: storage_spill_dir
  memory: { limit: "1M" }
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      schema:
        - { name: id, type: string }
        - { name: region, type: string }
        - { name: payload, type: string }
        - { name: value, type: int }
        - { name: ts, type: int }
  - type: route
    name: by_region
    input: events
    config:
      mode: exclusive
      conditions:
        a: "region == \"a\""
      default: b
  - type: output
    name: out_a
    input: by_region.a
    config:
      name: out_a
      type: csv
      path: out_a.csv
  - type: output
    name: out_b
    input: by_region.b
    config:
      name: out_b
      type: csv
      path: out_b.csv
"#;

const ROWS: usize = 2_000;

fn build_events_csv() -> String {
    let mut s = String::with_capacity(ROWS * 48);
    s.push_str("id,region,payload,value,ts\n");
    for id in 1..=ROWS {
        let region = if id % 2 == 0 { 'a' } else { 'b' };
        s.push_str(&format!("id_{id},{region},payload_{id},{id},{id}\n"));
    }
    s
}

/// Whether `dir` currently contains a `clinker-spill-*` entry.
fn has_spill_subdir(dir: &std::path::Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|e| {
        e.file_name()
            .to_string_lossy()
            .starts_with("clinker-spill-")
    })
}

#[test]
fn spill_dir_setting_redirects_per_run_spill_directory() {
    // No RSS gate: the per-run spill root is created unconditionally at run
    // start, before any spill-admission decision, so the redirect is
    // observable on every platform — including those without RSS readings,
    // where the admission predicate never trips and no real spill fires.
    let spill_root = tempfile::tempdir().expect("create custom spill root");
    let spill_root_path = spill_root.path().to_path_buf();

    let csv = build_events_csv();
    let config: PipelineConfig =
        clinker_plan::yaml::from_str(PIPELINE_YAML).expect("parse pipeline YAML");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let mut readers: clinker_exec::executor::SourceReaders = HashMap::new();
    readers.insert(
        "events".to_string(),
        clinker_exec::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    );

    let out_a = SharedBuffer::new();
    let out_b = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([
        (
            "out_a".to_string(),
            Box::new(out_a.clone()) as Box<dyn Write + Send>,
        ),
        (
            "out_b".to_string(),
            Box::new(out_b.clone()) as Box<dyn Write + Send>,
        ),
    ]);

    // Watcher thread: polls the configured spill root for a
    // `clinker-spill-*` directory while the run is in flight. The per-run
    // directory is auto-removed at run end, so observing it requires
    // catching it mid-run rather than inspecting after the run returns.
    let observed = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let watcher = {
        let observed = Arc::clone(&observed);
        let done = Arc::clone(&done);
        let root = spill_root_path.clone();
        std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                if has_spill_subdir(&root) {
                    observed.store(true, Ordering::Relaxed);
                    break;
                }
                std::thread::yield_now();
            }
        })
    };

    let params = PipelineRunParams {
        execution_id: "storage-spill-dir".to_string(),
        batch_id: "batch-0".to_string(),
        spill_root_dir: Some(spill_root_path.clone()),
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("spill-dir run must complete");

    done.store(true, Ordering::Relaxed);
    watcher.join().expect("watcher thread must join");

    assert_eq!(
        report.counters.total_count as usize, ROWS,
        "total ingested row count must match input",
    );
    assert!(
        observed.load(Ordering::Relaxed),
        "a clinker-spill-* directory should have been created under the configured \
         spill root {}",
        spill_root_path.display(),
    );
    // The per-run directory must be cleaned up once the run returns.
    assert!(
        !has_spill_subdir(&spill_root_path),
        "per-run spill directory should be removed after the run completes",
    );
}
