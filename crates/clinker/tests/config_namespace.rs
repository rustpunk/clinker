//! End-to-end proof that a channel/group `config:` override of a composition
//! parameter changes executed behaviour, not just the rendered provenance
//! (issue #765).
//!
//! The workspace pairs a base pipeline that uses a `gate` composition whose
//! body filters rows against `$config.min_amount` against a standalone group
//! `big_orders` that clobbers `gate.min_amount`. A plain run keeps every row
//! (default threshold `0`); `run --group big_orders` drops the rows below the
//! overridden threshold, and `channels resolve --group big_orders` reports the
//! override winning over the base default.

use std::path::Path;
use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

const CLINKER_TOML: &str = "[channel]\nroot = \"channel\"\n\n[group]\nroot = \"group\"\n";
const ORDERS_CSV: &str = "order_id,amount\na1,150.0\na2,20.0\na3,200.0\n";

/// The `gate` composition: its body reads `$config.min_amount` in a `filter`,
/// so the resolved config value directly gates which rows survive. The base
/// pipeline calls it with `min_amount: 0.0`, keeping everything.
const COMPOSITION: &str = r#"_compose:
  name: gate
  inputs:
    inp:
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  outputs:
    out: gated
  config_schema:
    min_amount:
      type: float
      default: 0.0
nodes:
  - type: transform
    name: gated
    input: inp
    config:
      cxl: |
        filter amount >= $config.min_amount
        emit order_id = order_id
        emit amount = amount
"#;

const PIPELINE: &str = r#"pipeline:
  name: order_fulfillment
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  - type: transform
    name: normalize
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit amount = amount
  - type: composition
    name: gate
    input: normalize
    use: ../composition/gate.comp.yaml
    inputs:
      inp: normalize
    config:
      min_amount: 0.0
  - type: output
    name: out
    input: gate
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// A standalone group (no selector — applied by explicit `--group`) that
/// clobbers the `gate.min_amount` config param up to `100`.
const GROUP: &str = r#"group:
  name: big_orders
  priority: 20
config:
  gate.min_amount: 100.0
"#;

fn build_workspace(root: &Path) {
    write(root, "clinker.toml", CLINKER_TOML);
    write(root, "pipeline/orders.csv", ORDERS_CSV);
    write(root, "pipeline/order_fulfillment.yaml", PIPELINE);
    write(root, "composition/gate.comp.yaml", COMPOSITION);
    write(root, "group/big_orders.group.yaml", GROUP);
}

fn pipeline_path(root: &Path) -> std::path::PathBuf {
    root.join("pipeline/order_fulfillment.yaml")
}

fn run_and_read_output(root: &Path, extra_args: &[&str]) -> (String, String) {
    let out = Command::new(clinker_bin())
        .current_dir(root)
        .arg("run")
        .arg(pipeline_path(root))
        .args(["--base-dir", root.to_str().unwrap()])
        .args(extra_args)
        .output()
        .expect("spawn clinker");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "run must succeed (args {extra_args:?}).\nstdout: {stdout}\nstderr: {stderr}"
    );
    let output = std::fs::read_to_string(root.join("out.csv")).unwrap_or_else(|_| String::new());
    (output, stderr)
}

#[test]
fn base_default_keeps_every_row() {
    // No overlay: the fold substitutes the call-site default `min_amount: 0.0`,
    // so `filter amount >= 0.0` keeps all three rows. This is the baseline the
    // override must diverge from.
    let tmp = tempfile::tempdir().unwrap();
    build_workspace(tmp.path());

    let (output, _stderr) = run_and_read_output(tmp.path(), &[]);
    assert!(
        output.contains("a1,150"),
        "base run keeps a1.\nout.csv:\n{output}"
    );
    assert!(
        output.contains("a2,20"),
        "base run keeps a2.\nout.csv:\n{output}"
    );
    assert!(
        output.contains("a3,200"),
        "base run keeps a3.\nout.csv:\n{output}"
    );
}

#[test]
fn group_config_override_changes_executed_rows() {
    // `run --group big_orders` clobbers `gate.min_amount` to 100. The composition
    // body's `filter amount >= $config.min_amount` is folded to `>= 100`, so the
    // `a2` row (amount 20) is dropped — a runtime behaviour change driven purely
    // by the config override, which #765's provenance-only path could not do.
    let tmp = tempfile::tempdir().unwrap();
    build_workspace(tmp.path());

    let (output, stderr) = run_and_read_output(tmp.path(), &["--group", "big_orders"]);
    assert!(
        stderr.contains("applied overlay") && stderr.contains("big_orders"),
        "run --group must report the applied group.\nstderr: {stderr}"
    );
    assert!(
        output.contains("a1,150"),
        "a1 (150 >= 100) survives the overridden gate.\nout.csv:\n{output}"
    );
    assert!(
        output.contains("a3,200"),
        "a3 (200 >= 100) survives the overridden gate.\nout.csv:\n{output}"
    );
    assert!(
        !output.contains("a2,20"),
        "a2 (20 < 100) must be dropped by the overridden gate — the override must \
         reach execution, not just provenance.\nout.csv:\n{output}"
    );
}

#[test]
fn channels_resolve_reports_config_override_and_base() {
    // The other half of the contract: the override must remain visible in the
    // resolved provenance, winning over the composition default.
    let tmp = tempfile::tempdir().unwrap();
    build_workspace(tmp.path());

    let out = Command::new(clinker_bin())
        .current_dir(tmp.path())
        .args(["channels", "resolve"])
        .arg(pipeline_path(tmp.path()))
        .args(["--base-dir", tmp.path().to_str().unwrap()])
        .args(["--group", "big_orders"])
        .output()
        .expect("spawn clinker");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "channels resolve must succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("gate.min_amount = 100"),
        "resolve must show the overridden value.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("base: 0"),
        "resolve must still show the composition default as the base layer.\nstdout: {stdout}"
    );
}
