//! CLI integration test for miette-rendered pipeline errors.
//!
//! The `clinker run <bad.yaml>` path must surface errors via the
//! miette graphical reporter with the source file attached. This
//! test shells out to the compiled `clinker` binary (`CARGO_BIN_EXE`
//! is injected by Cargo for integration tests) and asserts stderr
//! contains the config filename — proof that `NamedSource` reached
//! the report handler.
//!
//! A plan-time gate additionally has to keep its diagnostic code, its help
//! paragraph, and the YAML line it fired on. Those three are what let a user
//! reach `clinker explain --code <CODE>` and fix the config; the run path used
//! to drop all three by flattening the compile diagnostics into a message
//! list.

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
    // Compile-time CXL typecheck must surface through the `--explain`
    // path as well. A transform that references an unknown
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

    // The diagnostic's own code heads the report — proof it went through
    // miette carrying the structured diagnostic, not the tracing fallback
    // and not a flattened message list.
    assert!(
        stderr.contains("E203"),
        "stderr must carry the diagnostic's own code; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// A pipeline whose source declares a JSONPath-shaped `record_path`, which the
/// E363 grammar gate rejects at compile time.
fn jsonpath_record_path_yaml() -> &'static str {
    r#"pipeline:
  name: bad_record_path
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: in.json
      options:
        record_path: "$.rows"
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#
}

/// Assert the three things a plan-time diagnostic must keep on the way to the
/// terminal, plus the absence of the transform-compilation misattribution.
fn assert_plan_diagnostic_intact(stderr: &str, filename: &str) {
    // 1. The code, so `clinker explain --code E363` is reachable from the
    //    error the user is looking at.
    assert!(
        stderr.contains("E363"),
        "stderr must carry the diagnostic code; got:\n{stderr}"
    );
    // 2. The help paragraph naming the fix. Every gate attaches one; none of
    //    it used to reach the terminal.
    assert!(
        stderr.contains("dot-separated path of object keys"),
        "stderr must carry the diagnostic's help text; got:\n{stderr}"
    );
    // 3. The source line. The gate fires on the `- type: source` node, which
    //    is line 4 of the fixture, so miette's snippet header must name it.
    assert!(
        stderr.contains(&format!("{filename}:4:")),
        "stderr must point at the offending YAML line; got:\n{stderr}"
    );
    // The failure is in a source's config, not a CXL transform — and there is
    // no transform in this pipeline at all.
    assert!(
        !stderr.contains("CXL compilation failed for transform"),
        "a source-config gate must not be labelled a transform compilation \
         failure; got:\n{stderr}"
    );
}

#[test]
fn test_plan_time_gate_keeps_code_help_and_span_on_the_run_path() {
    let tmp = tempdir_path();
    let yaml_path = tmp.join("bad_record_path.yaml");
    std::fs::write(&yaml_path, jsonpath_record_path_yaml()).expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "clinker run on a rejected record_path must fail; got status {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_plan_diagnostic_intact(&stderr, "bad_record_path.yaml");

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_plan_time_gate_keeps_code_help_and_span_under_explain() {
    let tmp = tempdir_path();
    let yaml_path = tmp.join("bad_record_path.yaml");
    std::fs::write(&yaml_path, jsonpath_record_path_yaml()).expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .arg("--explain")
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "clinker run --explain on a rejected record_path must fail; got status {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_plan_diagnostic_intact(&stderr, "bad_record_path.yaml");

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_plan_diagnostic_without_its_own_pointer_gets_the_explain_hint() {
    // E203 (CXL name resolution) attaches no help of its own, so the renderer
    // is the only thing that can tell the user the code is explainable. A gate
    // whose help already names `clinker explain --code <CODE>` must not have a
    // second pointer stapled on.
    let tmp = tempdir_path();
    let yaml_path = tmp.join("cxl_unresolved.yaml");
    std::fs::write(
        &yaml_path,
        r#"pipeline:
  name: cxl_unresolved
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
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
      path: out.csv
"#,
    )
    .expect("write yaml");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(&yaml_path)
        .output()
        .expect("spawn clinker");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("See: clinker explain --code E203"),
        "a diagnostic with no help of its own must still be told where its \
         explain page is; got:\n{stderr}"
    );
    assert_eq!(
        stderr.matches("clinker explain --code E203").count(),
        1,
        "the explain pointer must appear exactly once; got:\n{stderr}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// Create an ephemeral per-test temp directory under the system
/// temp root. Avoids adding a `tempfile` dev-dep.
fn tempdir_path() -> std::path::PathBuf {
    // A per-process atomic counter guarantees a distinct directory per call.
    // pid + timestamp alone can collide on a platform whose clock resolution
    // is coarser than the nanosecond unit (macOS), letting two concurrent
    // tests share one directory and clobber each other's fixtures.
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut base = std::env::temp_dir();
    let name = format!(
        "clinker-miette-test-{}-{}-{}",
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
