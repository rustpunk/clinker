//! CLI integration coverage for the workspace `[storage]` block.
//!
//! `clinker run` reads `clinker.toml` at the workspace root, validates
//! `storage.spill.dir` before any input is opened, and fails the run at
//! startup with a config diagnostic when the directory is unusable. These
//! tests shell out to the compiled binary to exercise the full discovery +
//! validation path the way an operator would hit it.

use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Minimal pipeline that compiles but reads no real input — the storage
/// validation runs before any source is opened, so the source path never
/// needs to exist for the startup-failure tests.
const PIPELINE_YAML: &str = r#"pipeline:
  name: storage_cli
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn bad_spill_dir_fails_run_at_startup() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, PIPELINE_YAML).expect("write pipeline yaml");

    // clinker.toml at the workspace root (the pipeline file's directory)
    // points spill at a path that does not exist.
    let missing = tmp.join("nonexistent-spill-volume");
    std::fs::write(
        tmp.join("clinker.toml"),
        format!(
            "[storage.spill]\ndir = \"{}\"\n",
            missing.display().to_string().replace('\\', "\\\\")
        ),
    )
    .expect("write clinker.toml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "run with a missing spill dir must fail; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("storage.spill.dir"),
        "diagnostic must name the failing setting; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn explain_surfaces_resolved_spill_root() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, PIPELINE_YAML).expect("write pipeline yaml");

    // A real, writable spill directory at the workspace root.
    let spill = tmp.join("spill");
    std::fs::create_dir(&spill).expect("create spill dir");
    std::fs::write(
        tmp.join("clinker.toml"),
        format!(
            "[storage.spill]\ndir = \"{}\"\n",
            spill.display().to_string().replace('\\', "\\\\")
        ),
    )
    .expect("write clinker.toml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        .arg("--explain")
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "explain with a valid spill dir must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Spill root:") && stdout.contains("storage.spill.dir"),
        "explain must surface the resolved spill root and its source; got:\n{stdout}"
    );
    assert!(
        stdout.contains(&spill.display().to_string()),
        "explain must print the configured spill path; got:\n{stdout}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn explain_without_storage_block_shows_default_spill_root() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, PIPELINE_YAML).expect("write pipeline yaml");
    // No clinker.toml: spill root falls back to the OS temp dir.

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        .arg("--explain")
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "explain with no storage block must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Spill root:") && stdout.contains("OS temp dir (default)"),
        "explain must label the default spill root; got:\n{stdout}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

fn tempdir_path() -> std::path::PathBuf {
    let mut base = std::env::temp_dir();
    let name = format!(
        "clinker-storage-cli-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    base.push(name);
    std::fs::create_dir_all(&base).expect("create tempdir");
    base
}
