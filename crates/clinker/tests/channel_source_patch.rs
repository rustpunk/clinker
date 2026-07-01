//! End-to-end coverage for the channel `sources:` config-patch block (#550).
//!
//! A channel patches a source's parsed config (schema column ops, `array_paths`
//! ops, scalar `options`) before the pipeline is validated and compiled, so a
//! run behaves as if the source YAML had been hand-edited. These tests shell
//! out to the built `clinker` binary and assert the patch takes effect on the
//! normal run path, is reflected by `--explain`, and that a bad patch fails at
//! compile time with the documented diagnostic.

use std::path::Path;
use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Write `contents` to `dir/name`.
fn write(dir: &Path, name: &str, contents: &str) {
    std::fs::write(dir.join(name), contents).expect("write fixture file");
}

/// Run `clinker run <pipeline>` (plus any extra args) inside `dir`.
fn run_in(dir: &Path, extra: &[&str]) -> std::process::Output {
    Command::new(clinker_bin())
        .arg("run")
        .arg("pipe.yaml")
        .args(extra)
        .current_dir(dir)
        .output()
        .expect("spawn clinker")
}

/// A CSV pipeline whose transform reads a numeric `amount` and a `region`
/// column that the *base* schema does not provide — so the base pipeline
/// fails to compile, and only the channel patch (rename `amnt`→`amount`,
/// retype to int, add `region`) makes it valid.
const CSV_PIPELINE: &str = "\
pipeline:
  name: csv_patch
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amnt, type: string }
  - type: transform
    name: calc
    input: src
    config:
      cxl: |
        emit doubled = amount * 2
        emit zone = region
  - type: output
    name: out
    input: calc
    config:
      name: out
      type: csv
      path: out.csv
";

const CSV_CHANNEL: &str = "\
channel:
  name: fix
  target: ./pipe.yaml
sources:
  src:
    schema:
      amnt:   { rename: amount }
      amount: { retype: int }
      region: { add: { type: string } }
";

#[test]
fn csv_schema_patch_enables_run_and_is_visible_downstream() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,amount,region\n1,100,west\n2,250,east\n");
    write(p, "pipe.yaml", CSV_PIPELINE);
    write(p, "fix.channel.yaml", CSV_CHANNEL);

    // Base pipeline: `amount` (numeric) and `region` are not declared, so it
    // fails to compile.
    let base = run_in(p, &[]);
    assert!(
        !base.status.success(),
        "base pipeline should fail without the channel patch"
    );

    // Channel patch makes it valid; output reflects retype (doubled), add
    // (region readable by CXL → zone), and rename (amount bound to the CSV
    // column).
    let patched = run_in(p, &["--channel", "fix.channel.yaml"]);
    assert!(
        patched.status.success(),
        "patched run failed:\n{}",
        String::from_utf8_lossy(&patched.stderr)
    );
    let out = std::fs::read_to_string(p.join("out.csv")).expect("read out.csv");
    let header = out.lines().next().unwrap_or_default();
    assert!(header.contains("doubled"), "header: {header}");
    assert!(header.contains("zone"), "header: {header}");
    // amount retyped to int and doubled; region added and echoed as zone.
    assert!(out.contains("1,100,west,200,west"), "output:\n{out}");
    assert!(out.contains("2,250,east,500,east"), "output:\n{out}");
}

#[test]
fn csv_schema_patch_is_reflected_by_explain() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,amount,region\n1,100,west\n");
    write(p, "pipe.yaml", CSV_PIPELINE);
    write(p, "fix.channel.yaml", CSV_CHANNEL);

    // --explain compiles the same patched config: base fails, channel succeeds.
    let base = run_in(p, &["--explain"]);
    assert!(
        !base.status.success(),
        "base --explain should fail without the channel patch"
    );
    let patched = run_in(p, &["--channel", "fix.channel.yaml", "--explain"]);
    assert!(
        patched.status.success(),
        "patched --explain failed:\n{}",
        String::from_utf8_lossy(&patched.stderr)
    );
}

#[test]
fn json_options_and_array_paths_patch_changes_run_output() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(
        p,
        "in.json",
        "{ \"small\": { \"rows\": [ {\"id\":\"1\",\"tags\":[\"x\",\"y\"]} ] }, \
           \"big\": { \"rows\": [ {\"id\":\"1\",\"tags\":[\"x\",\"y\"]}, \
                                   {\"id\":\"2\",\"tags\":[\"z\"]}, \
                                   {\"id\":\"3\",\"tags\":[\"w\"]} ] } }\n",
    );
    write(
        p,
        "pipe.yaml",
        "\
pipeline:
  name: json_patch
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: in.json
      options:
        record_path: small.rows
      schema:
        - { name: id, type: string }
        - { name: tags, type: array }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
",
    );
    write(
        p,
        "jfix.channel.yaml",
        "\
channel:
  name: jfix
  target: ./pipe.yaml
sources:
  src:
    options:
      record_path: big.rows
    array_paths:
      tags: { mode: explode }
",
    );

    // Base: record_path points at the single-row array; no explosion.
    let base = run_in(p, &[]);
    assert!(base.status.success(), "base json run failed");
    let base_out = std::fs::read_to_string(p.join("out.csv")).expect("read out.csv");
    let base_rows = base_out.lines().skip(1).filter(|l| !l.is_empty()).count();
    assert_eq!(base_rows, 1, "base output:\n{base_out}");

    // Channel: record_path override selects the three-row array, and the
    // array_paths explode fans each record out per tag → 4 rows.
    let patched = run_in(p, &["--channel", "jfix.channel.yaml"]);
    assert!(
        patched.status.success(),
        "patched json run failed:\n{}",
        String::from_utf8_lossy(&patched.stderr)
    );
    let out = std::fs::read_to_string(p.join("out.csv")).expect("read out.csv");
    let rows = out.lines().skip(1).filter(|l| !l.is_empty()).count();
    assert_eq!(rows, 4, "expected 4 exploded rows, got:\n{out}");
    for tag in ["x", "y", "z", "w"] {
        assert!(out.contains(tag), "missing exploded tag {tag}:\n{out}");
    }
}

#[test]
fn unknown_source_name_fails_at_compile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,amount,region\n1,100,west\n");
    write(p, "pipe.yaml", CSV_PIPELINE);
    write(
        p,
        "bad.channel.yaml",
        "\
channel:
  name: bad
  target: ./pipe.yaml
sources:
  ghost:
    schema:
      id: remove
",
    );
    let out = run_in(p, &["--channel", "bad.channel.yaml"]);
    assert!(!out.status.success(), "run with unknown source should fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("E230"), "stderr:\n{stderr}");
}

#[test]
fn unknown_column_op_fails_at_compile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(p, "in.csv", "id,amount,region\n1,100,west\n");
    write(p, "pipe.yaml", CSV_PIPELINE);
    write(
        p,
        "bad.channel.yaml",
        "\
channel:
  name: bad
  target: ./pipe.yaml
sources:
  src:
    schema:
      not_a_column: { retype: int }
",
    );
    let out = run_in(p, &["--channel", "bad.channel.yaml"]);
    assert!(!out.status.success(), "run with unknown column should fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("E231"), "stderr:\n{stderr}");
}
