//! CLI integration coverage for the workspace `[storage]` block.
//!
//! `clinker run` reads `clinker.toml` at the workspace root, validates
//! `storage.spill.dir` before any input is opened, and fails the run at
//! startup with a config diagnostic when the directory is unusable. These
//! tests shell out to the compiled binary to exercise the full discovery +
//! validation path the way an operator would hit it.

use std::process::Command;

use clinker_plan::config::{
    ClinkerToml, DestinationProfile, PublicationCapacity, PublicationMode,
    PublicationSupportStatus, StorageConfigError,
};

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
fn explain_json_carries_storage_summary_at_text_parity() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, AGG_PIPELINE_YAML).expect("write pipeline yaml");
    write_orders_csv(&tmp);

    // Configure a spill dir and a generous disk cap so the storage summary
    // populates the spill-root, disk-cap, and cap-headroom fields rather than
    // their default-omitted forms.
    let spill = tmp.join("spill");
    std::fs::create_dir(&spill).expect("create spill dir");
    std::fs::write(
        tmp.join("clinker.toml"),
        format!(
            "[storage.spill]\ndir = \"{}\"\ndisk_cap_bytes = 1000000000\n",
            spill.display().to_string().replace('\\', "\\\\")
        ),
    )
    .expect("write clinker.toml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        .arg("--explain")
        .arg("json")
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "json explain must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("explain json must parse: {e}\n{stdout}"));

    let summary = json
        .get("storage_summary")
        .unwrap_or_else(|| panic!("json explain must carry storage_summary; got:\n{stdout}"));

    // Spill root reflects the configured dir.
    assert_eq!(
        summary["spill_root"]["source"], "storage.spill.dir",
        "spill_root source must name the configured dir; got:\n{summary:#}"
    );

    // Disk cap is the configured value, structured (not stringified).
    assert_eq!(
        summary["spill_disk_cap_bytes"], 1_000_000_000_u64,
        "spill_disk_cap_bytes must carry the configured cap; got:\n{summary:#}"
    );

    // Per-stage estimate lists the blocking hash Aggregate as a structured
    // entry — the same stage the text path reports.
    let per_stage = summary["estimated_spill"]["per_stage"]
        .as_array()
        .expect("estimated_spill.per_stage must be an array");
    assert!(
        per_stage.iter().any(|s| s["node_name"] == "dept_totals"),
        "the blocking Aggregate must appear in estimated_spill.per_stage; got:\n{summary:#}"
    );

    // Compression decision is structured, with the mode and a per-operator
    // breakdown that includes the same Aggregate stage.
    assert_eq!(
        summary["spill_compression"]["mode"], "auto",
        "spill_compression.mode must reflect the default auto mode; got:\n{summary:#}"
    );
    assert!(
        summary["spill_compression"]["per_operator"]
            .as_array()
            .expect("spill_compression.per_operator must be an array")
            .iter()
            .any(|o| o["node_name"] == "dept_totals"),
        "the Aggregate stage must appear in spill_compression.per_operator; got:\n{summary:#}"
    );

    // Cap headroom is present and structured (cap configured + non-zero
    // estimate), carrying the cap and the over-threshold flag.
    assert_eq!(
        summary["cap_headroom"]["cap_bytes"], 1_000_000_000_u64,
        "cap_headroom must carry the configured cap; got:\n{summary:#}"
    );
    assert!(
        summary["cap_headroom"]["over_threshold"].is_boolean(),
        "cap_headroom.over_threshold must be a boolean; got:\n{summary:#}"
    );

    // Staging defaults to disabled and is reported structurally.
    assert_eq!(
        summary["staging"]["enabled"], false,
        "staging must report disabled by default; got:\n{summary:#}"
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
    // A real run that spills prints the per-stage actual-spill section at
    // end-of-run so an operator can compare it against the --explain estimate
    // (#176 AC#3).
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    // Inline an 8 MiB memory budget: it admits the exact 7.6 MB terminal
    // materialization while the aggregate's 50,000-group table still spills.
    // `backpressure: spill` is required: 8 MiB is below the binary's
    // baseline RSS, which the default `pause` policy rejects at startup
    // (E312); the spill policy never pauses a producer and so spills as
    // this test intends rather than being rejected.
    //
    // A fused passthrough Transform (`norm`) sits between the Source and the
    // Aggregate so the Aggregate streaming-ingests its input per record. Without
    // it a direct Source→Aggregate materializes its whole (spilled-under-budget)
    // input into one Vec, which the #674 re-materialized-drain gate aborts (E310,
    // BudgetCategory::NodeBuffer) before the Aggregate ever reaches its own
    // group-table spill. Streaming the input keeps the working set to one batch,
    // so the only spill is the group-table spill this test targets.
    let yaml = AGG_PIPELINE_YAML
        .replace(
            "pipeline:\n  name: storage_obs\n",
            "pipeline:\n  name: storage_obs\n  memory: { limit: \"8M\", backpressure: spill }\n",
        )
        .replace(
            "  - type: aggregate\n    name: dept_totals\n    input: orders\n",
            "  - type: transform\n    name: norm\n    input: orders\n    config:\n      cxl: |\n        emit department = department\n        emit amount = amount\n  - type: aggregate\n    name: dept_totals\n    input: norm\n",
        );
    std::fs::write(&pipeline, &yaml).expect("write pipeline yaml");
    // Every row a distinct department: 50_000 groups dwarf the budget-derived
    // group-count cap (max_groups = 60% of the 8 MiB budget / est-bytes-per-group
    // ≈ a few thousand), so the group table crosses the cap and spills before
    // EOF. That cap is derived from the configured budget, not process RSS, so
    // the spill fires deterministically on every host — unlike the prior
    // RSS-driven node-buffer spill this test used to rely on.
    let mut body = String::from("department,amount\n");
    for i in 0..50_000 {
        body.push_str(&format!("dept{},{}\n", i, i * 3));
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
    // The group-table spill is budget-derived and deterministic, so the
    // per-stage actuals section must be present on every host.
    assert!(
        stdout.contains("=== Spill Volume (actual, per stage) ==="),
        "the high-cardinality aggregate must spill its group table under the 8 MiB \
         budget, so the per-stage actual-spill section must be printed; got:\n{stdout}"
    );
    assert!(
        stdout.contains("dept_totals"),
        "the spilling Aggregate stage must be named in the actual-spill section; got:\n{stdout}"
    );
    assert!(
        stdout.contains("Total:") && stdout.contains("bytes"),
        "the actual-spill section must report a per-stage total; got:\n{stdout}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

fn tempdir_path() -> std::path::PathBuf {
    // A per-process atomic counter guarantees a distinct directory per call.
    // pid + timestamp alone can collide: on a platform whose clock resolution
    // is coarser than the nanosecond unit (macOS), two concurrent tests can
    // read the same value, land in one shared directory, and leak one test's
    // clinker.toml into the other's config discovery.
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut base = std::env::temp_dir();
    let name = format!(
        "clinker-storage-cli-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    base.push(name);
    std::fs::create_dir_all(&base).expect("create tempdir");
    base
}

#[test]
fn publication_omission_resolves_exact_defaults_as_advisory() {
    let destination = tempdir_path();
    let doc = ClinkerToml::parse("").expect("parse default workspace config");

    let resolved = doc
        .storage
        .publication
        .resolve(&destination, 1_000_000_000, 8_000_000_000)
        .expect("resolve default publication policy");
    let explain = resolved.explain();

    assert_eq!(resolved.mode(), PublicationMode::Direct);
    assert_eq!(resolved.destination_profile(), DestinationProfile::Local);
    assert_eq!(resolved.failed_retention_seconds(), 86_400);
    assert_eq!(resolved.creation_grace_seconds(), 300);
    assert_eq!(resolved.max_attempt_bytes(), 4_000_000_000);
    assert_eq!(resolved.retained_byte_limit(), 8_000_000_000);
    assert_eq!(resolved.retained_attempt_limit(), 8);
    assert_eq!(resolved.min_free_bytes(), 2_000_000_000);
    assert_eq!(resolved.sweep_entry_limit(), 1_000);
    assert_eq!(resolved.sweep_byte_limit(), 8_000_000_000);
    assert_eq!(resolved.sweep_time_limit_ms(), 2_000);
    assert_eq!(explain.capacity, PublicationCapacity::AdvisoryObservation);
    assert_eq!(explain.estimated_attempt_bytes, 1_000_000_000);
    assert_eq!(explain.observed_free_bytes, 8_000_000_000);
    assert_eq!(explain.support_status, PublicationSupportStatus::Supported);
    assert!(!resolved.reserves_capacity());
    assert!(!resolved.guarantees_completion());
    assert!(resolved.late_enospc_or_edquot_possible());

    let _ = std::fs::remove_dir_all(&destination);
}

#[test]
fn publication_zero_retention_and_exact_modes_are_valid() {
    let destination = tempdir_path();
    let spool = destination.join("spool");
    std::fs::create_dir(&spool).expect("create spool");
    let config = format!(
        r#"
[storage.publication]
mode = "local_then_publish"
destination_profile = "local"
local_spool_dir = "{}"
failed_retention_seconds = 0
creation_grace_seconds = 3600
max_attempt_bytes = "16GB"
retained_byte_limit = "64GB"
retained_attempt_limit = 128
min_free_bytes = "64GB"
sweep_entry_limit = 10000
sweep_byte_limit = "64GB"
sweep_time_limit_ms = 30000
"#,
        spool.display().to_string().replace('\\', "\\\\")
    );

    let doc = ClinkerToml::parse(&config).expect("parse exact maximum values");
    let resolved = doc
        .storage
        .publication
        .resolve(&destination, 16_000_000_000, 80_000_000_000)
        .expect("resolve exact maximum values");

    assert_eq!(resolved.mode(), PublicationMode::LocalThenPublish);
    assert_eq!(resolved.failed_retention_seconds(), 0);
    assert_eq!(resolved.local_spool_dir(), Some(spool.as_path()));

    let _ = std::fs::remove_dir_all(&destination);
}

#[test]
fn publication_rejects_strict_schema_and_hard_limit_violations() {
    for (body, needle) in [
        ("unknown = true", "unknown"),
        ("mode = \"copy\"", "mode"),
        ("destination_profile = \"network\"", "destination_profile"),
        ("failed_retention_seconds = -1", "failed_retention_seconds"),
        (
            "failed_retention_seconds = \"one day\"",
            "failed_retention_seconds",
        ),
        ("retained_attempt_limit = -1", "retained_attempt_limit"),
        (
            "max_attempt_bytes = \"18446744073709551615GB\"",
            "max_attempt_bytes",
        ),
    ] {
        let err = ClinkerToml::parse(&format!("[storage.publication]\n{body}\n")).expect_err(body);
        assert!(
            err.to_string().contains(needle),
            "error for {body:?} must name {needle:?}: {err}"
        );
    }

    let destination = tempdir_path();
    for (body, needle, correction) in [
        (
            "failed_retention_seconds = 604801",
            "failed_retention_seconds",
            "failed_retention_seconds = 604800",
        ),
        (
            "creation_grace_seconds = 3601",
            "creation_grace_seconds",
            "creation_grace_seconds = 3600",
        ),
        (
            "max_attempt_bytes = \"17GB\"",
            "max_attempt_bytes",
            "max_attempt_bytes = \"16GB\"",
        ),
        (
            "retained_byte_limit = \"65GB\"",
            "retained_byte_limit",
            "retained_byte_limit = \"64GB\"",
        ),
        (
            "retained_attempt_limit = 129",
            "retained_attempt_limit",
            "retained_attempt_limit = 128",
        ),
        (
            "min_free_bytes = \"65GB\"",
            "min_free_bytes",
            "min_free_bytes = \"64GB\"",
        ),
        (
            "sweep_entry_limit = 10001",
            "sweep_entry_limit",
            "sweep_entry_limit = 10000",
        ),
        (
            "sweep_byte_limit = \"65GB\"",
            "sweep_byte_limit",
            "sweep_byte_limit = \"64GB\"",
        ),
        (
            "sweep_time_limit_ms = 30001",
            "sweep_time_limit_ms",
            "sweep_time_limit_ms = 30000",
        ),
    ] {
        let doc = ClinkerToml::parse(&format!("[storage.publication]\n{body}\n"))
            .expect("parse value for resolved validation");
        let err = doc
            .storage
            .publication
            .resolve(&destination, 1, u64::MAX)
            .expect_err(body);
        let rendered = err.to_string();
        assert!(rendered.contains(needle), "{rendered}");
        assert!(rendered.contains(correction), "{rendered}");
    }

    let _ = std::fs::remove_dir_all(&destination);
}

#[test]
fn publication_rejects_missing_spool_estimate_and_advisory_capacity() {
    let destination = tempdir_path();
    let local_then_publish =
        ClinkerToml::parse("[storage.publication]\nmode = \"local_then_publish\"\n")
            .expect("parse local_then_publish");
    let spool_err = local_then_publish
        .storage
        .publication
        .resolve(&destination, 1, u64::MAX)
        .expect_err("missing spool must fail");
    assert!(spool_err.to_string().contains("local_spool_dir"));
    assert!(
        spool_err
            .to_string()
            .contains("local_spool_dir = \"/path/to/local/spool\"")
    );

    let small_attempt = ClinkerToml::parse(
        "[storage.publication]\nmax_attempt_bytes = \"1GB\"\nretained_byte_limit = \"2GB\"\nmin_free_bytes = \"2GB\"\n",
    )
    .expect("parse bounded policy");
    let estimate_err = small_attempt
        .storage
        .publication
        .resolve(&destination, 1_000_000_001, u64::MAX)
        .expect_err("estimate over max attempt must fail");
    assert!(estimate_err.to_string().contains("max_attempt_bytes"));

    let retained_err = ClinkerToml::parse(
        "[storage.publication]\nmax_attempt_bytes = \"4GB\"\nretained_byte_limit = \"1GB\"\n",
    )
    .expect("parse retained limit")
    .storage
    .publication
    .resolve(&destination, 2_000_000_000, u64::MAX)
    .expect_err("estimate over retained byte limit must fail");
    assert!(retained_err.to_string().contains("retained_byte_limit"));

    let capacity_err = small_attempt
        .storage
        .publication
        .resolve(&destination, 1_000_000_000, 2_999_999_999)
        .expect_err("observed free below estimate plus headroom must fail");
    let rendered = capacity_err.to_string();
    assert!(rendered.contains("observed_free_bytes"), "{rendered}");
    assert!(rendered.contains("advisory"), "{rendered}");
    assert!(rendered.contains("does not reserve capacity"), "{rendered}");

    let overflow_err = small_attempt
        .storage
        .publication
        .resolve(&destination, u64::MAX, u64::MAX)
        .expect_err("checked capacity addition must not wrap");
    assert!(matches!(
        overflow_err,
        StorageConfigError::PublicationCapacityOverflow { .. }
    ));

    let _ = std::fs::remove_dir_all(&destination);
}

#[test]
fn invalid_publication_key_fails_before_attempt_creation() {
    let tmp = tempdir_path();
    let pipeline = tmp.join("pipeline.yaml");
    std::fs::write(&pipeline, PIPELINE_YAML).expect("write pipeline yaml");
    std::fs::write(
        tmp.join("clinker.toml"),
        "[storage.publication]\nmode = \"direct\"\ncopy_buffer_bytes = 1048576\n",
    )
    .expect("write clinker.toml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        .current_dir(&tmp)
        .output()
        .expect("spawn clinker");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("copy_buffer_bytes"), "{stderr}");
    assert!(!tmp.join(".clinker-attempts").exists());

    let _ = std::fs::remove_dir_all(&tmp);
}
