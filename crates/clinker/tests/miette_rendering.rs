//! Task 16b.8 — CLI integration gate test for miette-rendered
//! diagnostics.
//!
//! The `clinker run <bad.yaml>` path must surface errors via the
//! miette graphical reporter with the source file attached. This
//! test shells out to the compiled `clinker` binary (`CARGO_BIN_EXE`
//! is injected by Cargo for integration tests) and asserts stderr
//! contains the config filename — proof that `NamedSource` reached
//! the report handler.

use std::process::Command;

/// Path to the `clinker` binary built by Cargo for this test run.
fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

#[test]
fn test_diagnostic_renders_via_miette_in_cli() {
    // Write a deliberately broken YAML config: the `nodes:` section
    // is missing entirely, so `serde-saphyr` rejects it at parse
    // time. The error surfaces through `PipelineError::Config` and
    // is rendered by `render_pipeline_error` via miette.
    let tmp = tempdir_path();
    let bad_yaml_path = tmp.join("bad_pipeline.yaml");
    std::fs::write(
        &bad_yaml_path,
        "pipeline:\n  name: broken\n# missing required `nodes:` field\n",
    )
    .expect("write bad yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&bad_yaml_path)
        .output()
        .expect("spawn clinker");

    // Must have failed (non-zero exit).
    assert!(
        !output.status.success(),
        "clinker run on bad yaml must fail; got status {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // The rendered diagnostic must include the config filename.
    // `NamedSource` puts this in the diagnostic header; the
    // `WrappedPipelineError::Display` impl also injects it into the
    // main message, so either path satisfies the contract.
    assert!(
        stderr.contains("bad_pipeline.yaml"),
        "stderr must mention the config filename; got:\n{stderr}"
    );

    // And the miette diagnostic code marker must appear — proof
    // that the report went through miette's handler rather than
    // the fallback `tracing::error!` path.
    assert!(
        stderr.contains("clinker::pipeline_error"),
        "stderr must carry the miette diagnostic code; got:\n{stderr}"
    );

    // Cleanup.
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_explain_cxl_type_error_renders_via_miette() {
    // Task 16b.9: compile-time CXL typecheck must surface through the
    // `--explain` path as well. A transform that references an unknown
    // column against the declared schema is rejected at compile() time,
    // BEFORE any file on disk is read. The resulting diagnostic must
    // render via miette with the config filename in the header.
    let tmp = tempdir_path();
    let bad_yaml_path = tmp.join("cxl_type_error.yaml");
    std::fs::write(
        &bad_yaml_path,
        r#"pipeline:
  name: cxl_bad
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/definitely_not_on_disk.csv
      schema:
        - { name: amount, type: int }
  - type: transform
    name: t
    input: src
    config:
      cxl: "emit bogus = not_a_column + 1"
  - type: output
    name: out
    input: t
    config:
      name: out
      type: csv
      path: data/out.csv
"#,
    )
    .expect("write cxl_type_error yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&bad_yaml_path)
        .arg("--explain")
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "clinker run --explain on cxl type error must fail; got status {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // File-location annotation: the config filename must appear in
    // miette's rendered output — proof that `NamedSource` reached the
    // report handler through the --explain path.
    assert!(
        stderr.contains("cxl_type_error.yaml"),
        "stderr must mention the config filename; got:\n{stderr}"
    );

    // Diagnostic code marker — proof it went through miette, not the
    // tracing fallback.
    assert!(
        stderr.contains("clinker::pipeline_error"),
        "stderr must carry the miette diagnostic code; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// Create an ephemeral per-test temp directory under the system
/// temp root. Avoids adding a `tempfile` dev-dep.
fn tempdir_path() -> std::path::PathBuf {
    let mut base = std::env::temp_dir();
    let name = format!(
        "clinker-miette-test-{}-{}",
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
