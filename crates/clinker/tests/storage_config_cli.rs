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
fn explain_surfaces_resolved_disk_cap() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, PIPELINE_YAML).expect("write pipeline yaml");

    // A configured disk cap is parsed via the shared ByteSize grammar and
    // threaded into the run; --explain echoes the resolved byte count so an
    // operator can confirm it before committing to a run that might spill.
    std::fs::write(
        tmp.join("clinker.toml"),
        "[storage.spill]\ndisk_cap_bytes = \"10GB\"\n",
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
        "explain with a valid disk cap must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        // 10GB in decimal ByteSize units = 10_000_000_000 bytes.
        stdout.contains("Spill disk cap: 10000000000 bytes [storage.spill.disk_cap_bytes]"),
        "explain must surface the resolved disk cap; got:\n{stdout}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn explain_without_disk_cap_shows_unlimited() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, PIPELINE_YAML).expect("write pipeline yaml");
    // No clinker.toml: the spill cap falls back to unlimited.

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        .arg("--explain")
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "explain with no disk cap must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Spill disk cap: unlimited (default)"),
        "explain must label the default unlimited cap; got:\n{stdout}"
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

/// A pipeline with a real CSV source and a blocking hash Aggregate, so the
/// per-stage spill estimate and the staging plan have a sized input + a
/// spilling stage to report.
const AGG_PIPELINE_YAML: &str = r#"pipeline:
  name: storage_obs
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: dept_totals
    input: orders
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)
  - type: output
    name: out
    input: dept_totals
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

fn write_orders_csv(dir: &std::path::Path) {
    let mut body = String::from("department,amount\n");
    for i in 0..200 {
        body.push_str(&format!("dept{},{}\n", i % 7, i * 3));
    }
    std::fs::write(dir.join("orders.csv"), body).expect("write orders.csv");
}

#[test]
fn explain_surfaces_per_stage_spill_estimate() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, AGG_PIPELINE_YAML).expect("write pipeline yaml");
    write_orders_csv(&tmp);

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        .arg("--explain")
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "explain must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("=== Estimated Spill Volume ==="),
        "explain must carry the per-stage spill-estimate section; got:\n{stdout}"
    );
    assert!(
        stdout.contains("dept_totals"),
        "the blocking Aggregate stage must appear in the estimate; got:\n{stdout}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn explain_surfaces_staging_plan_per_source() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, AGG_PIPELINE_YAML).expect("write pipeline yaml");
    write_orders_csv(&tmp);

    // Enable staging onto a sibling directory so the *.csv source matches a
    // pattern and the plan reports a staged path + reuse decision.
    let staging = tmp.join("staging");
    std::fs::create_dir(&staging).expect("create staging dir");
    std::fs::write(
        tmp.join("clinker.toml"),
        format!(
            "[storage.staging]\nenabled = true\ndir = \"{}\"\npatterns = [\"*.csv\"]\non_existing = \"reuse\"\n",
            staging.display().to_string().replace('\\', "\\\\")
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
        "explain with staging configured must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("=== Staging Plan ==="),
        "explain must carry the staging-plan section; got:\n{stdout}"
    );
    assert!(
        stdout.contains("Source 'orders':") && stdout.contains("staged: yes"),
        "the matched source must report staged: yes; got:\n{stdout}"
    );
    assert!(
        stdout.contains("reuse: miss"),
        "with no prior copy the reuse decision must be a miss; got:\n{stdout}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn real_run_warns_when_estimate_exceeds_eighty_percent_of_cap() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, AGG_PIPELINE_YAML).expect("write pipeline yaml");
    write_orders_csv(&tmp);

    // A spill dir plus a tiny 100-byte cap. The sized input's estimate (the CSV
    // is well over 100 bytes) dwarfs the cap, so the startup cap-headroom
    // warning fires on the REAL run (not gated behind --explain). The run still
    // completes — the warning is advisory, and the small input does not actually
    // trip the memory budget to spill.
    let spill = tmp.join("spill");
    std::fs::create_dir(&spill).expect("create spill dir");
    std::fs::write(
        tmp.join("clinker.toml"),
        format!(
            "[storage.spill]\ndir = \"{}\"\ndisk_cap_bytes = 100\n",
            spill.display().to_string().replace('\\', "\\\\")
        ),
    )
    .expect("write clinker.toml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        // Run from the tempdir so the pipeline's relative output `out.csv`
        // lands here, not in the crate working tree.
        .current_dir(&tmp)
        .output()
        .expect("spawn clinker");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("W331"),
        "a real run whose estimate exceeds 80% of the cap must warn at startup (W331), \
         NOT only under --explain; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("per invocation"),
        "the startup warning must disclaim sibling invocations sharing the volume; stderr:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn real_run_logs_per_stage_actual_spill() {
    // A real run that spills (1 MiB memory budget over a sized input) prints the
    // per-stage actual-spill section at end-of-run so an operator can compare it
    // against the --explain estimate (#176 AC#3).
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    // Inline a 1 MiB memory budget so the node-buffer admission spills.
    let yaml = AGG_PIPELINE_YAML.replace(
        "pipeline:\n  name: storage_obs\n",
        "pipeline:\n  name: storage_obs\n  memory: { limit: \"1M\" }\n",
    );
    std::fs::write(&pipeline, &yaml).expect("write pipeline yaml");
    // A larger input raises the chance the 1 MiB budget trips a spill.
    let mut body = String::from("department,amount\n");
    for i in 0..50_000 {
        body.push_str(&format!("dept{},{}\n", i % 50, i * 3));
    }
    std::fs::write(tmp.join("orders.csv"), body).expect("write orders.csv");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        // Run from the tempdir so the pipeline's relative output `out.csv`
        // lands here, not in the crate working tree.
        .current_dir(&tmp)
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "run must complete; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    // RSS-based spill is platform/host dependent; only assert the per-stage
    // actuals section when a spill actually happened (the section is omitted on
    // a fully in-memory run by design).
    if stdout.contains("=== Spill Volume (actual, per stage) ===") {
        assert!(
            stdout.contains("Total:") && stdout.contains("bytes"),
            "the actual-spill section must report a per-stage total; got:\n{stdout}"
        );
    }

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
