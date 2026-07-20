//! Every runnable example pipeline under `examples/pipelines/` must compile
//! through the plan-only `--explain` path.
//!
//! The examples double as runnable documentation and as a smoke-test surface.
//! A stale example silently rots — it keeps sitting in the tree until a reader
//! tries to run it and hits a config-schema or CXL-grammar drift the engine has
//! since moved past. This guard shells out to the compiled CLI (the surface an
//! operator actually invokes) and fails the moment any example stops parsing or
//! type-checking. `--explain` is plan-only: it resolves, compiles, and plans the
//! pipeline without ingesting records, so the check stays fast and needs no
//! fixture data beyond what the examples already carry.

use std::path::{Path, PathBuf};
use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Workspace-root `examples/pipelines` directory, resolved from this crate's
/// manifest dir (`crates/clinker`). Canonicalized so the spawned CLI gets an
/// unambiguous working directory regardless of where the test runner sits.
fn examples_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/pipelines")
        .canonicalize()
        .expect("examples/pipelines directory must exist")
}

/// Top-level `*.yaml` files under `examples/pipelines`, sorted for a
/// deterministic report order.
///
/// The check covers only the directory's direct entries. Subdirectories hold
/// composition fragments (`*.comp.yaml`), per-source schema files
/// (`*.schema.yaml`), and fixture CSVs — none of which are standalone
/// pipelines, so `clinker run` on them would fail by design.
fn example_pipelines() -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(examples_dir())
        .expect("read examples/pipelines")
        .map(|entry| entry.expect("read dir entry").path())
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "yaml"))
        .collect();
    files.sort();
    files
}

#[test]
fn every_example_pipeline_passes_explain() {
    let dir = examples_dir();
    let pipelines = example_pipelines();
    assert!(
        !pipelines.is_empty(),
        "expected at least one example pipeline under {}",
        dir.display()
    );

    // The examples reference their inputs and outputs with paths relative to
    // this directory (e.g. `./data/invoices.csv`), so run each from here — the
    // same working directory a reader following the docs would use.
    let mut failures = Vec::new();
    for path in &pipelines {
        let name = path
            .file_name()
            .expect("example path has a file name")
            .to_string_lossy()
            .into_owned();

        let output = Command::new(clinker_bin())
            .arg("run")
            .arg(&name)
            .arg("--explain")
            .current_dir(&dir)
            .output()
            .unwrap_or_else(|e| panic!("failed to spawn clinker for {name}: {e}"));

        if !output.status.success() {
            failures.push(format!(
                "{name} (exit {}):\n{}",
                output
                    .status
                    .code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "signal".to_string()),
                String::from_utf8_lossy(&output.stderr).trim_end(),
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} example pipeline(s) failed `run --explain`:\n\n{}",
        failures.len(),
        pipelines.len(),
        failures.join("\n\n---\n\n"),
    );
}
