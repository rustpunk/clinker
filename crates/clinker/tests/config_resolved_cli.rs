//! CLI integration coverage for `clinker config --resolved`.
//!
//! The command prints a pipeline config with the multi-value shorthand
//! (`split_to_rows` / `split_values` / `join_values`) expanded to canonical
//! full-mapping form, materializing every default, while preserving the rest of
//! the document byte-for-byte. These tests shell out to the compiled binary to
//! exercise the surface an author would use, and pin the three behaviors the
//! feature promises: expansion, byte-faithful preservation of everything else,
//! and idempotence (resolving a resolved config is a no-op).

use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// A pipeline exercising all three shorthand surfaces, with comments and an
/// already-explicit `multiple: true` schema column to prove they survive.
const PIPELINE: &str = r#"pipeline:
  name: canonical_demo   # trailing comment survives

# Header comment describing the source below.
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: in.csv
      split_values:
        - tags
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }   # keep me
  - type: output
    name: tagged
    input: orders
    config:
      name: tagged
      type: csv
      path: out.csv
      join_values:
        - tags
error_handling:
  strategy: fail_fast
"#;

fn write_pipeline(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    std::fs::write(dir.join("in.csv"), "order_id,tags\n1,a;b\n").expect("write input csv");
    let path = dir.join("pipeline.yaml");
    std::fs::write(&path, body).expect("write pipeline yaml");
    path
}

fn resolve(path: &std::path::Path) -> String {
    let out = Command::new(clinker_bin())
        .args(["config", "--resolved"])
        .arg(path)
        .output()
        .expect("run clinker config --resolved");
    assert!(
        out.status.success(),
        "config --resolved failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf8 stdout")
}

#[test]
fn resolved_expands_shorthand_and_preserves_document() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = write_pipeline(tmp.path(), PIPELINE);
    let resolved = resolve(&path);

    // split_values bare `- tags` materialized into a full mapping.
    assert!(resolved.contains("field: tags"), "\n{resolved}");
    // join_values bare `- tags` gains the default collision policy.
    assert!(resolved.contains("on_conflict: error"), "\n{resolved}");
    // Everything outside the shorthand blocks is preserved verbatim.
    assert!(
        resolved.contains("  name: canonical_demo   # trailing comment survives"),
        "\n{resolved}"
    );
    assert!(
        resolved.contains("# Header comment describing the source below."),
        "\n{resolved}"
    );
    assert!(
        resolved.contains("        - { name: tags, type: string, multiple: true }   # keep me"),
        "schema column with multiple: true preserved\n{resolved}"
    );
    assert!(resolved.contains("  strategy: fail_fast"), "\n{resolved}");
}

#[test]
fn resolved_output_is_idempotent_and_reloads() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = write_pipeline(tmp.path(), PIPELINE);
    let once = resolve(&path);

    // Re-resolving the resolved output changes nothing.
    let resolved_path = tmp.path().join("resolved.yaml");
    std::fs::write(&resolved_path, &once).expect("write resolved");
    let twice = resolve(&resolved_path);
    assert_eq!(once, twice, "config --resolved must be idempotent");

    // The resolved output re-loads as a valid plan.
    let dry = Command::new(clinker_bin())
        .args(["run", "--dry-run"])
        .arg(&resolved_path)
        .output()
        .expect("dry-run resolved config");
    assert!(
        dry.status.success(),
        "resolved config failed to re-load: {}",
        String::from_utf8_lossy(&dry.stderr)
    );
}

#[test]
fn config_without_resolved_flag_errors() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = write_pipeline(tmp.path(), PIPELINE);
    let out = Command::new(clinker_bin())
        .arg("config")
        .arg(&path)
        .output()
        .expect("run clinker config");
    assert!(
        !out.status.success(),
        "config with no mode flag should fail"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--resolved"),
        "error should point at --resolved"
    );
}
