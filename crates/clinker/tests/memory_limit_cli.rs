//! CLI integration coverage for `memory.limit` overflow handling.
//!
//! A `memory.limit` whose binary-suffix scaling exceeds the addressable byte
//! range (`u64`) must fail the run with a config diagnostic, not panic or
//! silently wrap to a tiny budget. The check is the executor's startup gate,
//! reached once the source file set has been discovered, so these tests
//! provide a real input file and shell out to the compiled binary to exercise
//! the path an operator would hit.

use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Minimal single-row pipeline whose `memory.limit` is templated in. Source
/// and output use workspace-relative paths (absolute paths are rejected by the
/// path-security check), so the run executes with its working directory set to
/// the temp dir.
const PIPELINE_TEMPLATE: &str = r#"pipeline:
  name: mem_limit_overflow
  memory:
    limit: "__LIMIT__"
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

/// Write `pipeline.yaml` (with the limit templated in) plus a real one-row
/// input CSV into a fresh temp dir. Returns the temp dir (kept alive for the
/// run's duration).
fn pipeline_with_limit(limit: &str) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("create tempdir");
    std::fs::write(tmp.path().join("in.csv"), "amount\n1\n").expect("write input csv");
    let yaml = PIPELINE_TEMPLATE.replace("__LIMIT__", limit);
    std::fs::write(tmp.path().join("pipeline.yaml"), yaml).expect("write pipeline yaml");
    tmp
}

#[test]
fn overflowing_memory_limit_fails_run_at_startup() {
    // 2^34 G = 2^34 * 1024^3 = 2^64 bytes, one past u64::MAX — the numeric
    // part parses cleanly but the suffix multiply overflows.
    let tmp = pipeline_with_limit("17179869184G");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg("pipeline.yaml")
        .current_dir(tmp.path())
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "run with an overflowing memory.limit must fail; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("memory.limit"),
        "diagnostic must name the failing setting; got:\n{stderr}"
    );
}

#[test]
fn valid_memory_limit_passes_the_startup_gate() {
    // A well-formed `memory.limit` must clear the overflow gate and let the
    // run complete successfully.
    let tmp = pipeline_with_limit("256M");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg("pipeline.yaml")
        .current_dir(tmp.path())
        .output()
        .expect("spawn clinker");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "a valid memory.limit run must succeed; stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("memory.limit"),
        "a valid memory.limit must not trip the overflow gate; got:\n{stderr}"
    );
}

#[test]
fn cli_memory_limit_overrides_pipeline_yaml() {
    // The `--memory-limit` flag is documented as CLI-wins over `memory.limit`.
    // A pipeline whose YAML sets a generous 2 GiB budget runs cleanly on its
    // own; passing `--memory-limit 1` on the same pipeline must force a
    // sub-baseline budget the arbitrator rejects at startup (E312), proving the
    // flag reaches the executor rather than being silently ignored.
    let tmp = pipeline_with_limit("2G");

    // Baseline: the YAML 2 GiB budget alone clears startup and the run
    // completes, so any failure below is attributable to the CLI override, not
    // the pipeline itself.
    let without_flag = Command::new(clinker_bin())
        .arg("run")
        .arg("pipeline.yaml")
        .current_dir(tmp.path())
        .output()
        .expect("spawn clinker");
    assert!(
        without_flag.status.success(),
        "the 2G YAML budget alone must let the run pass; stderr:\n{}",
        String::from_utf8_lossy(&without_flag.stderr),
    );

    // Override: `--memory-limit 1` sets a 1-byte ceiling, below the process's
    // baseline resident memory, so the run aborts before any data loads. The
    // load-bearing signal is the differential itself — the same pipeline that
    // passed on its 2G YAML budget now fails — which proves the flag reached the
    // executor and won over the YAML value regardless of the abort mechanism.
    let with_flag = Command::new(clinker_bin())
        .arg("run")
        .arg("pipeline.yaml")
        .arg("--memory-limit")
        .arg("1")
        .current_dir(tmp.path())
        .output()
        .expect("spawn clinker");
    let stderr = String::from_utf8_lossy(&with_flag.stderr);
    assert!(
        !with_flag.status.success(),
        "`--memory-limit 1` must override the 2G YAML budget and abort the run; \
         stderr:\n{stderr}"
    );
    // On the supported CI targets (Linux, macOS, Windows) `rss_bytes()` is
    // available and the default backpressure policy pauses producers, so a
    // sub-baseline budget is rejected up front as E312 whose message echoes the
    // exact 1-byte ceiling — confirming the flag's specific value, not merely
    // some failure, propagated into the arbitrator sizing. (The E312 gate is
    // skipped only where `rss_bytes()` returns `None` or under a non-pausing
    // policy, neither of which applies on those targets.)
    assert!(
        stderr.contains("E312") && stderr.contains("of 1 bytes"),
        "the CLI-forced 1-byte budget must surface the E312 startup abort naming \
         that ceiling; got:\n{stderr}"
    );
}

#[test]
fn malformed_memory_limit_flag_fails_at_the_boundary_without_clobbering_yaml() {
    // Regression guard for the wiring's original hazard: before boundary
    // validation, a malformed `--memory-limit` value (the decimal `4GB` where
    // the binary `4G` is meant, or plain garbage) parsed to the 512 MiB default
    // downstream, silently overriding a larger YAML budget with no diagnostic.
    // The flag must now be rejected at the CLI boundary — naming the flag and
    // echoing the bad value — before it can touch the plan, so the generous YAML
    // budget below is never clobbered.
    let tmp = pipeline_with_limit("8G");

    for bad in ["4GB", "notanumber"] {
        let output = Command::new(clinker_bin())
            .arg("run")
            .arg("pipeline.yaml")
            .arg("--memory-limit")
            .arg(bad)
            .current_dir(tmp.path())
            .output()
            .expect("spawn clinker");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !output.status.success(),
            "a malformed --memory-limit {bad:?} must fail the run; stderr:\n{stderr}"
        );
        assert!(
            stderr.contains("--memory-limit"),
            "the diagnostic must name the flag the operator passed, not the YAML \
             `memory.limit` key; got:\n{stderr}"
        );
        assert!(
            stderr.contains(bad),
            "the diagnostic must echo the offending value {bad:?}; got:\n{stderr}"
        );
        // The failure must be the boundary rejection, not a downstream abort from
        // a silently-defaulted budget: had `4GB` collapsed to 512 MiB and
        // clobbered the 8G YAML value, this 1-row pipeline would still have run
        // to completion, so any E312 here would signal the exact regression.
        assert!(
            !stderr.contains("E312"),
            "a malformed flag must be rejected at the boundary, never coerced to a \
             budget the executor then judges; got:\n{stderr}"
        );
    }
}
