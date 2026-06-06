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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

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
    spill_subdir(dir).is_some()
}

/// The first `clinker-spill-*` entry currently under `dir`, if any.
fn spill_subdir(dir: &std::path::Path) -> Option<std::path::PathBuf> {
    let entries = std::fs::read_dir(dir).ok()?;
    entries.flatten().find_map(|e| {
        e.file_name()
            .to_string_lossy()
            .starts_with("clinker-spill-")
            .then(|| e.path())
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
    // The per-run spill root holds verbatim record bytes; on Unix it must be
    // owner-only (0o700) so other users on a shared spill volume cannot list or
    // `stat` spilled file names + sizes. The directory is created at run start
    // and removed at run end, so the watcher captures its mode mid-run.
    // `u32::MAX` is the "not yet observed" sentinel.
    let observed_mode = Arc::new(AtomicU32::new(u32::MAX));
    let watcher = {
        let observed = Arc::clone(&observed);
        let observed_mode = Arc::clone(&observed_mode);
        let done = Arc::clone(&done);
        let root = spill_root_path.clone();
        std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                if let Some(spill_dir) = spill_subdir(&root) {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        if let Ok(meta) = std::fs::metadata(&spill_dir) {
                            observed_mode
                                .store(meta.permissions().mode() & 0o777, Ordering::Relaxed);
                        }
                    }
                    // `spill_dir` + `observed_mode` are consumed only by the
                    // `#[cfg(unix)]` block above; reference them so non-Unix
                    // builds don't flag them unused.
                    let _ = (&spill_dir, &observed_mode);
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
    #[cfg(unix)]
    {
        let mode = observed_mode.load(Ordering::Relaxed);
        assert_eq!(
            mode, 0o700,
            "per-run spill root must be owner-only (0o700), observed {mode:o}",
        );
    }
    // The per-run directory must be cleaned up once the run returns.
    assert!(
        !has_spill_subdir(&spill_root_path),
        "per-run spill directory should be removed after the run completes",
    );
}

#[test]
fn startup_purges_an_orphaned_spill_dir_from_a_crashed_prior_run() {
    // A crashed prior run (SIGKILL / OOM-killer) leaves its `clinker-spill-*`
    // directory behind under the spill root: the TempDir Drop that would remove
    // it never ran, but the OS released the directory's `.lock` on process
    // death. The next run's startup crash-purge must reap that orphan. Drive a
    // real pipeline run (which performs the purge before creating its own spill
    // dir) and assert the orphan is gone afterward.
    let spill_root = tempfile::tempdir().expect("create custom spill root");
    let spill_root_path = spill_root.path().to_path_buf();

    // Plant an orphan: a `clinker-spill-*` dir with an unlocked `.lock` (the
    // crashed owner released it) and a leaked spill file inside.
    let orphan = spill_root_path.join("clinker-spill-crashed0001");
    std::fs::create_dir(&orphan).expect("create orphan spill dir");
    std::fs::File::create(orphan.join(".lock")).expect("create orphan lock");
    std::fs::write(orphan.join("partition-0.spill"), b"leaked-bytes").expect("write leaked spill");

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

    let params = PipelineRunParams {
        execution_id: "spill-crash-purge".to_string(),
        batch_id: "batch-0".to_string(),
        spill_root_dir: Some(spill_root_path.clone()),
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run must complete");
    assert_eq!(report.counters.total_count as usize, ROWS);

    // The orphaned spill dir from the simulated crashed run must be gone.
    assert!(
        !orphan.exists(),
        "startup crash-purge should have reaped the orphaned spill directory {}",
        orphan.display(),
    );
}
